package multiplex

import (
	"context"
	"errors"
	"fmt"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	bc "github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/service"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/indexer"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
)

// ScopedNode embeds a Node instance and adds a scope hash
type ScopedNode struct {
	ScopeHash  string
	ListenAddr string
	RPCAddress string
	*node.Node
}

// ----------------------------------------------------------------------------
// Builders

// NewMultiplexNode returns a new, ready to go, multiplex CometBFT Node.
// Multiplex-mode refers to the user-scoped replication strategy.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in
// a production environment. Please use this feature with
// caution as it is still being actively researched.
func NewMultiplexNode(ctx context.Context,
	config *cfg.Config,
	nodeKey *p2p.NodeKey,
	clientCreator proxy.ClientCreator,
	genesisDocProvider node.GenesisDocProvider,
	dbProvider cfg.DBProvider,
	metricsProvider node.MetricsProvider,
	logger log.Logger,
	options ...node.Option,
) (*NodeRegistry, error) {
	if config.BaseConfig.DBBackend == "boltdb" || config.BaseConfig.DBBackend == "cleveldb" {
		logger.Info("WARNING: BoltDB and GoLevelDB are deprecated and will be removed in a future release. Please switch to a different backend.")
	}

	// Fallback to legacy node implementation as soon as possible
	// The returned NodeRegistry contains only one entry and the
	// node implementation used is `node/node.go`, i.e. no multiplex.
	if config.Replication == cfg.SingularReplicationMode() {
		// Uses the default privValidator from config (FilePV)
		privValidator, err := privval.LoadOrGenFilePV(
			config.PrivValidatorKeyFile(),
			config.PrivValidatorStateFile(),
			func() (crypto.PrivKey, error) {
				return ed25519.GenPrivKey(), nil
			},
		)
		if err != nil {
			return nil, err
		}

		readyNode, err := node.NewNode(
			ctx,
			config,
			privValidator,
			nodeKey,
			clientCreator,
			genesisDocProvider,
			dbProvider,
			metricsProvider,
			logger,
			options...,
		)
		if err != nil {
			return nil, err
		}

		return &NodeRegistry{
			config:   config,
			nodeInfo: readyNode.NodeInfo(),
			nodeKey:  nodeKey,
			Scopes:   []string{},
			Nodes: MultiplexNode{"": &ScopedNode{
				ScopeHash: "",
				Node:      readyNode,
			}},
		}, nil
	}
	// End fallback to legacy node implementation

	// Initialize a multiplex reactor which handles the config
	// of multiple nodes, as many as there are replicated chains.
	// Create the reactor instance and safety-check genesis doc.
	reactor := NewReactor(
		nodeKey,
		config,
		logger,
		dbProvider,
		genesisDocProvider,
		GlobalMetricsProvider(config.Instrumentation),
		MultiplexMetricsProvider(config.Instrumentation),
	)

	// Warn the user about experimental status
	logger.Info("WARNING - EXPERIMENTAL: Starting a node multiplex", "nodeId", string(nodeKey.ID()))

	// ------------------------------------------------------------------------
	// Starts the MultiplexReactor

	// Start the multiplex reactor, this initializes the filesystem,
	// then the databases and stores.
	// This process creates concurrent goroutines to configure nodes.
	if err := reactor.Start(); err != nil {
		return nil, fmt.Errorf("could not start the multiplex reactor: %w", err)
	}

	// Read augmented user configuration (multiplex)
	mxUserConfig := reactor.GetUserConfig()

	// Service provider is used to retrieve proxyApp, eventBus, privVal
	serviceProvider := reactor.GetServiceProvider()

	replicatedChainsScopeHashes := mxUserConfig.GetScopeHashes()
	numReplicatedChains := len(replicatedChainsScopeHashes)

	multiplexMetricsProvider := MultiplexMetricsProvider(config.Instrumentation)
	dbMultiplexProvider := reactor.GetDBMultiplexProvider()

	// Select a limited number of listeners message updates from
	// the multiplex reactor channel. This loop limits duplicate
	// node initializations.
	//
	// TODO(midas): TBI whether loop can be removed to use `for select`.
	for i := 0; i < numReplicatedChains; i++ {
		// The multiplex reactor communicates scope hashes on a channel
		// to tell this bootstrapper about the readiness of a node config
		select {
		case userScopeHash := <-reactor.listenersStartedCh:
			scopeId := NewScopeIDFromHash(userScopeHash).Fingerprint()
			logger.With("scope", scopeId)

			// Retrieve replicated node config (concurrency-safe)
			nodeConfig := reactor.replConfig[userScopeHash]

			// Warn the user about experimental status
			logger.Info(
				"Started node listeners",
				"laddr", nodeConfig.P2P.ListenAddress,
			)

			// ------------------------------------------------------------------------
			// ENVIRONMENT

			// Retrieve genesis doc for current scope hash
			genesisProvider := reactor.GetGenesisProvider()
			replChainGenesisDoc, err := genesisProvider(userScopeHash)
			if err != nil {
				return nil, err
			}

			// Initialize metrics with user scope hash
			replicatedMetrics := multiplexMetricsProvider(replChainGenesisDoc.ChainID, userScopeHash)

			// Contains a DB, StateMachine, StateStore and BlockStore
			stateServices, err := reactor.GetStateServices(userScopeHash)
			if err != nil {
				return nil, err
			}

			stateMachine := stateServices.StateMachine

			// The genesis doc key will be deleted if it existed.
			// Not checking whether the key is there in case the genesis file was larger than
			// the max size of a value (in rocksDB for example), which would cause the check
			// to fail and prevent the node from booting.
			logger.Info("WARNING: deleting genesis file from database if present, the database stores a hash of the original genesis file now")

			err = stateServices.DB.Delete(genesisDocKey)
			if err != nil {
				logger.Error("Failed to delete genesis doc from DB ", err)
			}

			logger.Info("Blockstore version", "version", stateServices.BlockStore.GetVersion())

			// ------------------------------------------------------------------------
			// CONSENSUS

			eventBus := serviceProvider(userScopeHash, "eventBus").(*types.EventBus)
			proxyApp := serviceProvider(userScopeHash, "proxyApp").(proxy.AppConns)
			indexerService := serviceProvider(userScopeHash, "indexerService").(*txindex.IndexerService)
			privValidator := reactor.GetReplPrivValidator(userScopeHash)
			privValPubKey, err := privValidator.GetPubKey()
			if err != nil {
				logger.Error("Failed to get public key from priv validator: ", err)
			}

			// Determine whether we should attempt state sync.
			stateSync := nodeConfig.StateSync.Enable && !onlyValidatorIsUs(*stateServices.StateMachine, privValPubKey)
			if stateSync && stateServices.StateMachine.LastBlockHeight > 0 {
				logger.Info("Found local state with non-zero height, skipping state sync")
				stateSync = false
			}

			// Create the handshaker, which calls RequestInfo, sets the AppVersion on the state,
			// and replays any blocks as necessary to sync CometBFT with the app.
			consensusLogger := logger.With("module", "consensus")
			if !stateSync {
				err := doHandshake(ctx,
					*stateServices.StateStore,
					*stateServices.StateMachine,
					stateServices.BlockStore,
					replChainGenesisDoc,
					eventBus,
					proxyApp,
					consensusLogger,
				)
				if err != nil {
					return nil, err
				}

				// Reload the state. It will have the Version.Consensus.App set by the
				// Handshake, and may have other modifications as well (ie. depending on
				// what happened during block replay).

				reloadedState, err := stateServices.StateStore.Load()
				stateMachine = &ScopedState{
					ScopeHash: stateMachine.ScopeHash,
					State:     reloadedState,
				}
				if err != nil {
					return nil, sm.ErrCannotLoadState{Err: err}
				}
			}

			// Inform about the state machine block height
			logger.Info(
				"State machine loaded",
				"height", stateMachine.LastBlockHeight,
			)

			// Determine whether we should do block sync. This must happen after the handshake, since the
			// app may modify the validator set, specifying ourself as the only validator.
			blockSync := !onlyValidatorIsUs(*stateMachine, privValPubKey)
			waitSync := stateSync || blockSync

			logNodeStartupInfo(*stateMachine, privValPubKey, logger, consensusLogger)

			mempool, mempoolReactor := createMempoolAndMempoolReactor(
				nodeConfig,
				proxyApp,
				*stateMachine,
				waitSync,
				replicatedMetrics.MempoolMetrics,
				logger,
			)

			evidenceMultiplexDB := dbMultiplexProvider("evidence")
			evidenceReactor, evidencePool, err := createEvidenceReactor(
				nodeConfig,
				evidenceMultiplexDB,
				stateServices.StateStore,
				stateServices.BlockStore,
				logger,
				userScopeHash,
			)
			if err != nil {
				return nil, err
			}

			pruner, err := createPruner(
				nodeConfig,
				indexerService.GetTxIndexer(),
				indexerService.GetBlockIndexer(),
				stateServices.StateStore,
				stateServices.BlockStore,
				replicatedMetrics.StateMetrics,
				logger.With("module", "state"),
			)
			if err != nil {
				return nil, fmt.Errorf("failed to create pruner: %w", err)
			}

			// make block executor for consensus and blocksync reactors to execute blocks
			blockExec := NewMultiplexBlockExecutor(
				stateServices.StateStore,
				logger.With("module", "state"),
				proxyApp.Consensus(),
				mempool,
				evidencePool,
				stateServices.BlockStore,
				sm.BlockExecutorWithPruner(pruner),
				sm.BlockExecutorWithMetrics(replicatedMetrics.StateMetrics),
			)

			offlineStateSyncHeight := int64(0)
			if stateServices.BlockStore.Height() == 0 {
				offlineStateSyncHeight, err = blockExec.Store().GetOfflineStateSyncHeight()
				if err != nil && err.Error() != "value empty" {
					panic(fmt.Sprintf("failed to retrieve statesynced height from store %s with scope hash %s; expected state store height to be %v", err, userScopeHash, stateMachine.LastBlockHeight))
				}
			}

			// Don't start block sync if we're doing a state sync first.
			bcReactor, err := createBlocksyncReactor(
				nodeConfig,
				stateMachine,
				blockExec,
				stateServices.BlockStore,
				blockSync && !stateSync,
				privValPubKey.Address(),
				logger,
				replicatedMetrics.BlockSyncMetrics,
				offlineStateSyncHeight,
			)
			if err != nil {
				return nil, fmt.Errorf("could not create blocksync reactor: %w", err)
			}

			// consensusReactor, consensusState := createConsensusReactor(
			consensusReactor, _ := createConsensusReactor(
				nodeConfig,
				stateMachine,
				blockExec,
				stateServices.BlockStore,
				mempool,
				evidencePool,
				privValidator,
				replicatedMetrics.ConsensusMetrics,
				waitSync,
				eventBus,
				consensusLogger,
				offlineStateSyncHeight,
			)

			err = stateServices.StateStore.SetOfflineStateSyncHeight(0)
			if err != nil {
				panic(fmt.Sprintf("failed to reset the offline state sync height %s with scope hash %s", err, userScopeHash))
			}
			// Set up state sync reactor, and schedule a sync if requested.
			// FIXME The way we do phased startups (e.g. replay -> block sync -> consensus) is very messy,
			// we should clean this whole thing up. See:
			// https://github.com/tendermint/tendermint/issues/4644
			stateSyncReactor := statesync.NewReactor(
				*nodeConfig.StateSync,
				proxyApp.Snapshot(),
				proxyApp.Query(),
				replicatedMetrics.StateSyncMetrics,
			)
			stateSyncReactor.SetLogger(logger.With("module", "statesync"))

			// Inform about the state machine block height
			logger.Info("Consensus ready for replicated chain")

			// ------------------------------------------------------------------------
			// MULTIPLEX SERVICES READY
			// Multiplex services are now all ready and configured
			// The replicated (user scoped) chain is fully configured

			reactor.RegisterService(userScopeHash, "mempoolReactor", mempoolReactor.(*mempl.Reactor))
			reactor.RegisterService(userScopeHash, "blockSyncReactor", bcReactor)
			reactor.RegisterService(userScopeHash, "stateSyncReactor", stateSyncReactor)
			reactor.RegisterService(userScopeHash, "consensusReactor", consensusReactor)
			reactor.RegisterService(userScopeHash, "evidenceReactor", evidenceReactor)
			reactor.RegisterService(userScopeHash, "pruner", pruner)
		}
		// End of select
	}
	// End of for loop, following code is run *globally*

	logger = logger.With("scope", "global")

	// Global metrics are necessary to initialize stores and p2pMetrics is
	// registered globally because P2P implementation is not replicated.
	globalMetricsProvider := GlobalMetricsProvider(config.Instrumentation)
	sharedMetrics := globalMetricsProvider(string(nodeKey.ID()))

	// --------------------------------------------------------------------------
	// P2P / TRANSPORT / SWITCH

	nodeInfo, err := makeNodeInfo(config, nodeKey, reactor)
	if err != nil {
		return nil, err
	}

	err = createTransports(
		config,
		nodeInfo,
		nodeKey,
		reactor,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create p2p transports: %w", err)
	}

	p2pLogger := logger.With("module", "p2p")
	err = createSwitches(
		config,
		sharedMetrics.P2PMetrics,
		nodeInfo,
		nodeKey,
		p2pLogger,
		reactor,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create event switches: %w", err)
	}

	err = createAddressBooksAndSetOnSwitches(
		config,
		p2pLogger,
		nodeKey,
		reactor,
	)
	if err != nil {
		return nil, fmt.Errorf("could not create address books: %w", err)
	}

	// Optionally, start the pex reactors
	//
	// see TODO in node/node.go
	//
	// FIXME: in multiplex, this setting should be per-scope and creating
	// the PEX reactor can be moved to replicated services creation loop
	if config.P2P.PexReactor {
		// _, err = createPEXReactorsAndAddToSwitches(
		err = createPEXReactorsAndAddToSwitches(
			config,
			logger,
			reactor,
		)

		if err != nil {
			return nil, fmt.Errorf("could not PEX reactors: %w", err)
		}
	}

	nodeMultiplex := &NodeRegistry{
		config:   config,
		nodeInfo: nodeInfo,
		nodeKey:  nodeKey,
		Reactor:  reactor,

		Nodes:  make(map[string]*ScopedNode, len(replicatedChainsScopeHashes)),
		Scopes: replicatedChainsScopeHashes,
	}

	stateStoreProvider := reactor.GetStateStoreProvider()
	blockStoreProvider := reactor.GetBlockStoreProvider()
	statesProvider := reactor.GetStatesProvider()

	for _, userScopeHash := range replicatedChainsScopeHashes {
		// Retrieve genesis doc for current scope hash
		genesisProvider := reactor.GetGenesisProvider()
		// Error can be skipped because it would have exited before
		replChainGenesisDoc, _ := genesisProvider(userScopeHash)
		replChainNodeConfig := reactor.GetReplNodeConfig(userScopeHash)
		replChainEventSwitch := serviceProvider(userScopeHash, "eventSwitch").(*ScopedSwitch)

		node := node.NewNodeWithServices(
			replChainNodeConfig,
			replChainGenesisDoc,
			nodeInfo.GetReplNodeInfo(replChainGenesisDoc.ChainID, userScopeHash),
			nodeKey,

			reactor.GetReplPrivValidator(userScopeHash),
			replChainEventSwitch.GetAddrBook().(pex.AddrBook),
			serviceProvider(userScopeHash, "transport").(*p2p.MultiplexTransport),

			replChainEventSwitch.Switch,
			serviceProvider(userScopeHash, "eventBus").(*types.EventBus),
			serviceProvider(userScopeHash, "proxyApp").(proxy.AppConns),
			serviceProvider(userScopeHash, "mempoolReactor").(*mempl.Reactor).GetMempoolPtr(),
			serviceProvider(userScopeHash, "evidenceReactor").(*evidence.Reactor).GetPoolPtr(),

			serviceProvider(userScopeHash, "pruner").(*sm.Pruner),
			serviceProvider(userScopeHash, "indexerService").(*txindex.IndexerService),

			stateStoreProvider(userScopeHash).DBStore,
			blockStoreProvider(userScopeHash).BlockStore,

			serviceProvider(userScopeHash, "consensusReactor").(*cs.Reactor).GetState(),

			reactor.GetStateSync(userScopeHash),

			statesProvider(userScopeHash).GetState(),
		)

		// FIXME(midas): do we want "ScopedNode" here?
		node.BaseService = *service.NewBaseService(logger, "Node", node)

		for _, option := range options {
			option(node)
		}

		// stores this node instance in the multiplex
		nodeListenAddrs := replChainNodeConfig.GetListenAddresses()
		nodeMultiplex.Nodes[userScopeHash] = &ScopedNode{
			ScopeHash:  userScopeHash,
			ListenAddr: nodeListenAddrs["P2P"],
			RPCAddress: nodeListenAddrs["RPC"],
			Node:       node,
		}
	}

	return nodeMultiplex, nil
}

// ----------------------------------------------------------------------------
//

func makeNodeInfo(
	config *cfg.Config,
	nodeKey *p2p.NodeKey,
	multiplexReactor *Reactor,
) (MultiNetworkNodeInfo, error) {
	replicatedChains := multiplexReactor.GetScopeRegistry().GetScopeHashes()
	numReplicatedChains := len(replicatedChains)
	protocolVersions := make([]ScopedProtocolVersion, numReplicatedChains)
	supportedNetworks := make([]ScopedChainInfo, numReplicatedChains)
	listenAddrs := make([]ScopedListenAddr, numReplicatedChains)
	rpcAddresses := make([]ScopedListenAddr, numReplicatedChains)

	genesisDocProvider := multiplexReactor.GetGenesisProvider()
	statesProvider := multiplexReactor.GetStatesProvider()

	// Fill ProtocolVersions and Networks fields
	for i, userScopeHash := range replicatedChains {
		// Error can be skipped as it would have errored before
		genDoc, _ := genesisDocProvider(userScopeHash)
		nodeConfig := multiplexReactor.GetReplNodeConfig(userScopeHash)
		userState := statesProvider(userScopeHash)

		// TODO(midas): add builders for proto objects
		protocolVersions[i] = ScopedProtocolVersion{
			ScopeHash: userScopeHash,
			P2P:       version.P2PProtocol,
			Block:     userState.Version.Consensus.Block,
			App:       userState.Version.Consensus.App,
		}

		supportedNetworks[i] = ScopedChainInfo{
			ScopeHash: userScopeHash,
			ChainID:   genDoc.ChainID,
		}

		listenAddrs[i] = ScopedListenAddr{
			ScopeHash:  userScopeHash,
			ListenAddr: nodeConfig.P2P.ListenAddress,
		}

		rpcAddresses[i] = ScopedListenAddr{
			ScopeHash:  userScopeHash,
			ListenAddr: nodeConfig.RPC.ListenAddress,
		}

		i++
	}

	// FIXME: txIndexer can be disabled but multiplex *always* enables it for now
	txIndexerStatus := "on"
	// if _, ok := txIndexer.(*null.TxIndex); ok {
	// 	txIndexerStatus = "off"
	// }

	nodeInfo := MultiNetworkNodeInfo{
		DefaultNodeID:    nodeKey.ID(),
		Scopes:           replicatedChains,
		ProtocolVersions: protocolVersions,
		Networks:         supportedNetworks,
		ListenAddrs:      listenAddrs,
		RPCAddresses:     rpcAddresses,
		Version:          version.CMTSemVer,
		Channels: []byte{
			bc.BlocksyncChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
			statesync.SnapshotChannel, statesync.ChunkChannel,
		},
		Moniker: config.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			RPCAddress: rpcAddresses[0].ListenAddr,
		},
	}

	if config.P2P.PexReactor {
		nodeInfo.Channels = append(nodeInfo.Channels, pex.PexChannel)
	}

	nodeInfo.ListenAddr = nodeInfo.ListenAddrs[0].ListenAddr

	err := nodeInfo.Validate()
	return nodeInfo, err
}

func createPruner(
	config *cfg.Config,
	txIndexer txindex.TxIndexer,
	blockIndexer indexer.BlockIndexer,
	stateStore *ScopedStateStore,
	blockStore *ScopedBlockStore,
	metrics *sm.Metrics,
	logger log.Logger,
) (*sm.Pruner, error) {
	if err := initApplicationRetainHeight(stateStore); err != nil {
		return nil, err
	}

	prunerOpts := []sm.PrunerOption{
		sm.WithPrunerInterval(config.Storage.Pruning.Interval),
		sm.WithPrunerMetrics(metrics),
	}

	if config.Storage.Pruning.DataCompanion.Enabled {
		err := initCompanionRetainHeights(
			stateStore,
			config.Storage.Pruning.DataCompanion.InitialBlockRetainHeight,
			config.Storage.Pruning.DataCompanion.InitialBlockResultsRetainHeight,
		)
		if err != nil {
			return nil, err
		}
		prunerOpts = append(prunerOpts, sm.WithPrunerCompanionEnabled())
	}

	return sm.NewPruner(stateStore, blockStore, blockIndexer, txIndexer, logger, prunerOpts...), nil
}

// Set the initial application retain height to 0 to avoid the data companion
// pruning blocks before the application indicates it is OK. We set this to 0
// only if the retain height was not set before by the application.
func initApplicationRetainHeight(stateStore *ScopedStateStore) error {
	if _, err := stateStore.GetApplicationRetainHeight(); err != nil {
		if errors.Is(err, sm.ErrKeyNotFound) {
			return stateStore.SaveApplicationRetainHeight(0)
		}
		return err
	}
	return nil
}

// Sets the data companion retain heights if one of two possible conditions is
// met:
// 1. One or more of the retain heights has not yet been set.
// 2. One or more of the retain heights is currently 0.
func initCompanionRetainHeights(stateStore *ScopedStateStore, initBlockRH, initBlockResultsRH int64) error {
	curBlockRH, err := stateStore.GetCompanionBlockRetainHeight()
	if err != nil && !errors.Is(err, sm.ErrKeyNotFound) {
		return fmt.Errorf("failed to obtain companion block retain height: %w", err)
	}
	if curBlockRH == 0 {
		if err := stateStore.SaveCompanionBlockRetainHeight(initBlockRH); err != nil {
			return fmt.Errorf("failed to set initial data companion block retain height: %w", err)
		}
	}
	curBlockResultsRH, err := stateStore.GetABCIResRetainHeight()
	if err != nil && !errors.Is(err, sm.ErrKeyNotFound) {
		return fmt.Errorf("failed to obtain companion block results retain height: %w", err)
	}
	if curBlockResultsRH == 0 {
		if err := stateStore.SaveABCIResRetainHeight(initBlockResultsRH); err != nil {
			return fmt.Errorf("failed to set initial data companion block results retain height: %w", err)
		}
	}
	return nil
}
