package multiplex

import (
	"context"
	"fmt"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/version"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// NodesMultiplexProvider takes a config and a logger and returns a
// ready-to-go nodes multiplex nodeKey.
type NodesMultiplexProvider func(
	*config.Config,
	cmtlog.Logger,
	...node.Option,
) (*p2p.NodeKey, error)

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
) (*p2p.NodeKey, error) {
	nodeKey, err := NewNodesMultiplex(
		context.Background(),
		&client.DefaultAcceptor{},
		globalCfg,
		logger,
		options...,
	)
	if err != nil {
		return nil, err
	}

	return nodeKey, nil
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
	*p2p.NodeKey,
	error,
) {
	// Creates one [p2p.NodeKey] instance per nodes multiplex
	nodeKey, err := p2p.LoadOrGenNodeKey(globalCfg.NodeKeyFile())
	if err != nil {
		return nil, fmt.Errorf(
			"failed to load or gen node key %s: %w", globalCfg.NodeKeyFile(), err)
	}

	// // Fallback to legacy node implementation as soon as possible
	// // The returned MultiplexMap contains only one entry and the
	// // node implementation used is `node/node.go`, i.e. no multiplex.
	// if globalCfg.Strategy == DisableReplicationStrategy() {
	// 	return NewLegacyNodeMultiplex(ctx, globalCfg, nodeKey, logger, options...)
	// }
	// // End fallback to legacy node implementation

	// Warn the user about experimental status
	logger.Info("WARNING - EXPERIMENTAL: Starting a nodes multiplex", "nodeId", string(nodeKey.ID()))

	return nodeKey, nil
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
	helpers.MultiplexMap[*node.Node],
	error,
) {
	multiplex := helpers.MultiplexMap[*node.Node]{}

	// Uses the default privValidator from config (FilePV)
	privValidator, err := privval.LoadOrGenFilePV(
		nodeCfg.PrivValidatorKeyFile(),
		nodeCfg.PrivValidatorStateFile(),
		func() (crypto.PrivKey, error) {
			return ed25519.GenPrivKey(), nil
		},
	)
	if err != nil {
		return multiplex, err
	}

	// Uses the default genesisDoc provider functor
	genesisDocProvider := node.DefaultGenesisDocProviderFunc(nodeCfg)

	// IMPORTANT: Uses the implementation at `node/node.go`
	readyNode, err := node.NewNode(
		ctx,
		nodeCfg,
		privValidator,
		nodeKey,
		proxy.DefaultClientCreator(ctx, nodeCfg.ProxyApp, nodeCfg.ABCI, nodeCfg.DBDir()),
		genesisDocProvider,
		config.DefaultDBProvider,
		node.DefaultMetricsProvider(nodeCfg.Instrumentation),
		logger,
		options...,
	)
	if err != nil {
		return multiplex, err
	}

	// Read the GenesisDoc, errors can be ignored as they would have triggered
	// already in the above statement as well.
	icsGenesisDoc, _ := genesisDocProvider()
	genesisDoc, _ := icsGenesisDoc.DefaultGenesisDoc()

	// Legacy implementation runs only one node, as implemented in `node/node.go`.
	//
	// We store the instance in a multiplex map to allow this method to be used
	// as a fallback for when multiplex configuration is inconsistent or missing.
	multiplex[genesisDoc.ChainID] = helpers.NewChainInstance[*node.Node](genesisDoc.ChainID, readyNode)

	return multiplex, nil
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
		"chainId", state.ChainID,
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

// ----------------------------------------------------------------------------

// GetAddressForCometBFT returns a NetAddress for the CometBFT P2P messages.
func GetAddressForCometBFT(
	globalCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (cometbftAddr *p2p.NetAddress, err error) {
	promoteAddr := globalCfg.P2P.ExternalAddress
	if promoteAddr == "" {
		promoteAddr = globalCfg.P2P.ListenAddress
	}

	// P2P CometBFT Port is always: `discovery_port+1`
	p2pListenAddr := overwriteListenPort(
		promoteAddr,
		int(globalCfg.DiscoveryPort+1), // always DiscoveryPort+1
	)

	relayAddr, addrErr := helpers.NewRelayAddress(p2pListenAddr)
	if addrErr != nil {
		return nil, fmt.Errorf(
			"could not create relay address for P2P: %w", err)
	}

	relayAddr.SetID(nodeKey.ID())
	if cometbftAddr = relayAddr.NetAddress(); err != nil {
		return nil, fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	return // cometbftAddr
}
