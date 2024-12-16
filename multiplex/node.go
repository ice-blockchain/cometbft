package multiplex

import (
	"context"
	"fmt"
	"strings"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	bc "github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/statesync"
	"github.com/ice-blockchain/cometbft/types"
	"github.com/ice-blockchain/cometbft/version"
)

// NodesMultiplexProvider takes a config and a logger and returns a
// ready-to-go nodes multiplex, i.e. [mx.MultiplexMap[*node.Node]].
//
// Note that providers *must not* start node instance.
type NodesMultiplexProvider func(
	*config.Config,
	cmtlog.Logger,
	...node.Option,
) (MultiplexMap[*node.Node], error)

// DefaultNewNodesMultiplex returns a CometBFT Nodes Multiplex with default
// settings for the PrivValidator, ClientCreator, GenesisDoc, and DBProvider.
//
// This method is used in `cmd/cometbft/main.go` to create a nodes multiplex.
//
// See also: [NewNodesMultiplex]
// This method implements [NodesMultiplexProvider].
func DefaultNewNodesMultiplex(
	globalCfg *config.Config,
	logger cmtlog.Logger,
	options ...node.Option,
) (MultiplexMap[*node.Node], error) {
	nodesMultiplex, _, err := NewNodesMultiplex(
		context.Background(),
		&client.DefaultAcceptor{},
		globalCfg,
		logger,
		options...,
	)
	if err != nil {
		return nil, err
	}

	return nodesMultiplex, nil
}

// ----------------------------------------------------------------------------
// NewNodesMultiplex

// NewNodesMultiplex returns a new, ready to go, CometBFT Nodes Multiplex.
// Multiplex-mode refers to a multi-network replication strategy whereby
// concurrent consensus instances are enabled for all replicated chains.
//
// Creates one [p2p.NodeKey] instance per nodes multiplex. This implies
// that the [p2p.ID] included in the format `id@host:port` is always the
// same for one nodes multiplex' listen addresses. i.e. the node ID is
// shared amongst all replicated chains.
//
// Note also that this method does *not* call the Start() method for the
// created node instances. It is important to note that each node instance's
// Start() method must be called in a separate goroutine to permit concurrent
// consensus instances, blocks production and state machines replication.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in a production
// environment. Please use these features with caution as it is still
// being actively developed.
//
// CAUTION: This method expects the genesis file to contain a GenesisDocSet.
func NewNodesMultiplex(
	ctx context.Context,
	acceptor client.Acceptor,
	globalCfg *config.Config,
	logger cmtlog.Logger,
	options ...node.Option,
) (
	MultiplexMap[*node.Node],
	*Reactor,
	error,
) {
	// Creates one [p2p.NodeKey] instance per nodes multiplex
	nodeKey, err := p2p.LoadOrGenNodeKey(globalCfg.NodeKeyFile())
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to load or gen node key %s: %w", globalCfg.NodeKeyFile(), err)
	}

	// Fallback to legacy node implementation as soon as possible
	// The returned MultiplexMap contains only one entry and the
	// node implementation used is `node/node.go`, i.e. no multiplex.
	if globalCfg.Strategy == DisableReplicationStrategy() {
		return NewLegacyNodeMultiplex(ctx, globalCfg, nodeKey, logger, options...)
	}
	// End fallback to legacy node implementation

	// CAUTION: this method expects the genesis file to contain a GenesisDocSet.
	genesisDocProvider := MultiplexGenesisDocProviderFunc(globalCfg)

	// Uses a singleton chain registry to interpret multiplex configurations
	chainRegistry, err := NewChainRegistry(&globalCfg.MultiplexConfig)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"failed to create the ChainRegistry: %w", err)
	}

	// Initialize a multiplex reactor which handles the configuration
	// of multiple parallel nodes, as many as there are replicated chains.
	// Create the reactor instance and safety-check genesis doc.
	reactor := NewReactor(
		nodeKey,
		globalCfg,
		logger.With("module", "multiplex"),
		chainRegistry,
		genesisDocProvider,
		WithAcceptor(acceptor),
	)

	// Warn the user about experimental status
	logger.Info("WARNING - EXPERIMENTAL: Starting a nodes multiplex", "nodeId", string(nodeKey.ID()))

	knownNetworks := chainRegistry.GetChains()
	logger.Debug("WARNING - EXPERIMENTAL: Known networks", "networks", strings.Join(knownNetworks, ", "))

	// Start the multiplex reactor, this initializes the filesystem,
	// then the databases and stores.
	// This process creates concurrent goroutines to configure nodes.
	//
	// This method calls `mx.NewConfigOverwrite()` for each network.
	if err := reactor.Start(); err != nil {
		return nil, nil, fmt.Errorf(
			"could not start the multiplex reactor: %w", err)
	}

	// Create the local ABCI client for the SnapsApp application.
	//
	// This application is forcefully enabled using the multiplex package,
	// note that we also *ignore* the ProxyApp field in [config.Config].
	//
	// The ABCI client is created once for the nodes multiplex, and we use
	// a breaking [proxy.ChainConns] interface rather than [proxy.AppConns].
	localABCISnapsApp := proxy.NewLocalClientCreator(snapsapp.NewSnapsApplication(
		reactor,
		logger.With("module", "snapsapp"),
	))

	// Start the ABCI client (proxyApp)
	// Note that we create only one ABCI client shared by all replicated chains.
	//
	// BREAKING: we use [proxy.ChainConns] interfaces rather than [proxy.AppConns].
	abciClient := proxy.NewMultiplexAppConn(
		reactor.GetNetworks(),
		localABCISnapsApp,
		proxy.PrometheusMetrics(globalCfg.Instrumentation.Namespace),
	)
	abciClient.SetLogger(logger.With("module", "proxy"))
	if err := abciClient.Start(); err != nil {
		return nil, nil, fmt.Errorf(
			"error starting proxy app connections: %w", err)
	}

	// Reactor: ABCI; ABCI: Reactor.
	reactor.abciClient = abciClient

	// Select a limited number of listeners message updates from
	// the multiplex reactor channel. This loop forbids duplicate
	// node initializations.
	for i := 0; i < len(knownNetworks); i++ {
		// The multiplex reactor communicates the ChainID on a channel
		// to tell this bootstrapper about the readiness of a node config
		chainID := <-reactor.chainReadyCh

		// Inform about the readiness of this chain
		logger.Info("Network configuration done", "chain_id", chainID)

		// Used to retrieve configuration and state per chain.
		statesProvider := reactor.GetInstanceProvider(InstanceKeyState)
		privvalProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)

		// The node config contains the configuration overwrite.
		stateMachine := statesProvider(chainID).(sm.State)
		privValidator := privvalProvider(chainID).(types.PrivValidator)

		// Make sure we can access the priv validator
		privValPubKey, err := privValidator.GetPubKey()
		if err != nil {
			return nil, nil, fmt.Errorf(
				"could not read public key from priv validator: %w", err)
		}

		// Since we do not run state-sync, we must execute a ABCI handshake
		// And following a successful handshake, we may load the state machine.
		//
		// e.g. This also happens on restart of a node.
		if err := reactor.PrepareConsensusInstanceWithReactor(ctx, chainID); err != nil {
			return nil, nil, fmt.Errorf(
				"error preparing consensus instance: %w", err)
		}

		// Inform about the state machine block height
		logger.Info(
			"State machine loaded",
			"chain_id", stateMachine.ChainID,
			"height", stateMachine.LastBlockHeight,
		)

		// Determine whether we should do block sync. This must happen after
		// the handshake, since the app may modify the validator set,
		// e.g. specifying ourself as the only validator.
		blockSync := !onlyValidatorIsUs(stateMachine.Copy(), privValPubKey)

		logNodeStartupInfo(stateMachine.Copy(), privValPubKey, logger)

		// Start the actual consensus instance.
		//
		// Creates a mempool, evidence pool, block executor, blocksync
		// and finally a consensus reactor.
		if err := reactor.CreateConsensusInstanceReactors(ctx, chainID, blockSync); err != nil {
			return nil, nil, fmt.Errorf(
				"error starting consensus reactors: %w", err)
		}

		// Inform about the consensus readiness
		logger.Info("Network is consensus ready", "chain_id", chainID)
	}
	// End of for loop, code following this is run *globally*
	// Note that reaching this section means that *all replicated chains* are
	// effectively *consensus-ready* and ready to produce blocks (validators).

	// Inform about all replicated chains being consensus ready
	logger.Info("All known networks are consensus ready", "nodeId", string(nodeKey.ID()))

	nodeInfo, err := makeNodeInfo(globalCfg.Moniker, nodeKey, reactor)
	if err != nil {
		return nil, nil, err
	}

	// Reactor: Network; Network: Reactor.
	reactor.nodeInfo = nodeInfo

	// Create the [p2p.MultiplexTransports] instances
	if err := reactor.CreateTransportSwitchesWithReactors(ctx, reactor.GetNetworks()); err != nil {
		return nil, nil, fmt.Errorf(
			"error creating p2p event switch: %w", err)
	}

	// Create the peer address books and set on switches
	if err := reactor.CreateAddressBooks(ctx, reactor.GetNetworks()); err != nil {
		return nil, nil, fmt.Errorf(
			"error creating the pex address books: %w", err)
	}

	// Inform about all replicated chains being configured
	logger.Info("All nodes are now configured", "nodeId", string(nodeKey.ID()))

	// Create node.Node instances (runtime) and inject "runtime/node" service.
	nodesMultiplex := reactor.createMultiplexNodesWithServices(
		ctx,
		reactor.GetNetworks(),
		options...,
	)

	return nodesMultiplex, reactor, nil
}

// ----------------------------------------------------------------------------
// NewLegacyNodeMultiplex

// NewLegacyNodeMultiplex implements a **fallback to default** implementation
// of [node.Node], such that *multiplex features are disabled* and that the
// implementation used is `node/node.go`.
//
// Note that the returned [Reactor] is always nil with this method.
//
// We provide this implementation as a fallback solution and to improve
// backwards-compatibility with the original `cometbft` source code.
func NewLegacyNodeMultiplex(
	ctx context.Context,
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
	logger cmtlog.Logger,
	options ...node.Option,
) (
	MultiplexMap[*node.Node],
	*Reactor,
	error,
) {
	multiplex := MultiplexMap[*node.Node]{}

	// Uses the default privValidator from config (FilePV)
	privValidator, err := privval.LoadOrGenFilePV(
		nodeCfg.PrivValidatorKeyFile(),
		nodeCfg.PrivValidatorStateFile(),
		func() (crypto.PrivKey, error) {
			return ed25519.GenPrivKey(), nil
		},
	)
	if err != nil {
		return multiplex, nil, err
	}

	// Uses the default genesisDoc provider functor
	genesisDocProvider := node.DefaultGenesisDocProviderFunc(nodeCfg)

	// IMPORTANT: Uses the implementation at `node/node.go`
	readyNode, err := node.NewNode(
		ctx,
		nodeCfg,
		privValidator,
		nodeKey,
		proxy.DefaultClientCreator(nodeCfg.ProxyApp, nodeCfg.ABCI, nodeCfg.DBDir()),
		genesisDocProvider,
		config.DefaultDBProvider,
		node.DefaultMetricsProvider(nodeCfg.Instrumentation),
		logger,
		options...,
	)
	if err != nil {
		return multiplex, nil, err
	}

	// Read the GenesisDoc, errors can be ignored as they would have triggered
	// already in the above statement as well.
	icsGenesisDoc, _ := genesisDocProvider()
	genesisDoc, _ := icsGenesisDoc.DefaultGenesisDoc()

	// Legacy implementation runs only one node, as implemented in `node/node.go`.
	//
	// We store the instance in a multiplex map to allow this method to be used
	// as a fallback for when multiplex configuration is inconsistent or missing.
	multiplex[genesisDoc.ChainID] = NewChainInstance[*node.Node](genesisDoc.ChainID, readyNode)

	return multiplex, &Reactor{}, nil
}

// ----------------------------------------------------------------------------
// Private helpers implementation

// logNodeStartupInfo logs useful node startup information such as the multiplex
// information: ChainID and height from state. It also logs version information
// for the software, including the block protocol.
//
// This method will also log whether this node is a validator or an observer.
func logNodeStartupInfo(
	state sm.State,
	pubKey crypto.PubKey,
	logger cmtlog.Logger,
) {
	// Log the Multiplex info.
	logger.Info("Multiplex info",
		"chain_id", state.ChainID,
		"height", state.LastBlockHeight,
	)

	// Log the version info.
	logger.Info("Version info",
		"tendermint_version", version.CMTSemVer,
		"abci", version.ABCISemVer,
		"block", version.BlockProtocol,
		"p2p", version.P2PProtocol,
		"commit_hash", version.CMTGitCommitHash,
	)

	// If the state and software differ in block version, at least log it.
	if state.Version.Consensus.Block != version.BlockProtocol {
		logger.Info("Software and state have different block protocols",
			"software", version.BlockProtocol,
			"state", state.Version.Consensus.Block,
		)
	}

	validatorAddress := pubKey.Address()
	consensusLogger := logger.With("module", "consensus")

	// Log whether this node is a validator or an observer
	if state.Validators.HasAddress(validatorAddress) {
		consensusLogger.Info("This node is a validator",
			"addr", validatorAddress, "pubKey", pubKey)
	} else {
		consensusLogger.Info("This node is not a validator",
			"addr", validatorAddress, "pubKey", pubKey)
	}
}

// makeNodeInfo creates the [MultiNetworkNodeInfo] instance given a P2P
// node key and a multiplex reactor.
//
// TODO(midas): txIndexer may be disabled but multiplex reports "on".
// txIndexer may be disabled but multiplex always *reports* it as enabled.
// The reason is that the `Other` part of the MultiNetworkNodeInfo is not
// available on a per-network basis. A fix would be to include this in a
// custom [ChainProtocolVersion] as the type is related to node capacities.
func makeNodeInfo(
	moniker string,
	nodeKey *p2p.NodeKey,
	reactor *Reactor,
) (MultiNetworkNodeInfo, error) {
	// Get an ordered list of replicated chains
	knownNetworks := reactor.GetChainRegistry().GetChains()
	countNetworks := len(knownNetworks)

	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	statesProvider := reactor.GetInstanceProvider(InstanceKeyState)

	// Fill ProtocolVersions and Networks fields
	protocolVersions := make([]ChainProtocolVersion, countNetworks)
	p2pListenAddrs := make([]ChainListenAddr, countNetworks)
	rpcListenAddrs := make([]ChainListenAddr, countNetworks)
	for i, chainID := range knownNetworks {
		cfgOverwrite := configProvider(chainID).(*config.Config)
		stateMachine := statesProvider(chainID).(sm.State)

		protocolVersions[i] = NewChainProtocolVersion(chainID, p2p.NewProtocolVersion(
			version.P2PProtocol,
			stateMachine.Version.Consensus.Block,
			stateMachine.Version.Consensus.App,
		))

		p2pListenAddrs[i] = NewChainListenAddr(chainID, cfgOverwrite.P2P.ListenAddress)
		rpcListenAddrs[i] = NewChainListenAddr(chainID, cfgOverwrite.RPC.ListenAddress)
	}

	txIndexerStatus := "on"
	nodeInfo := MultiNetworkNodeInfo{
		DefaultNodeID:    nodeKey.ID(),
		Networks:         knownNetworks,
		ProtocolVersions: protocolVersions,
		ListenAddrs:      p2pListenAddrs,
		RPCAddresses:     rpcListenAddrs,
		ListenAddr:       p2pListenAddrs[0].ListenAddr,
		Version:          version.CMTSemVer,
		Channels: []byte{
			bc.BlocksyncChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
			statesync.SnapshotChannel, statesync.ChunkChannel,
			pex.PexChannel,
		},
		Moniker: moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			RPCAddress: rpcListenAddrs[0].ListenAddr,
		},
	}

	err := nodeInfo.Validate()
	return nodeInfo, err
}
