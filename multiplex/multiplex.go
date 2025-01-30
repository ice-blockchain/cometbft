package multiplex

import (
	"context"

	"github.com/ice-blockchain/cometbft/config"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/state/txindex"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
)

// createMultiplexNodesWithServices creates the underlying [node.Node] instances
// that will be running consensus instances concurrently.
//
// Note that this method must be called after having fully configured all
// the required services for running a nodes multiplex. You should probably
// not have to call this method directly, see [NewNodesMultiplex] instead.
//
// Note also that this method does *not* call the Start() method for the
// created node instances. It is important to note that each node instance's
// Start() method must be called in a separate goroutine to permit concurrent
// consensus instances, blocks production and state machines replication.
//
// This method also registers services in the servicesRegistry:
// - `runtime/node`: The node instance or runtime service.
func (reactor *Reactor) createMultiplexNodesWithServices(
	_ context.Context,
	networks []string,
	options ...node.Option,
) MultiplexMap[*node.Node] {
	// Used to retrieve configuration and state per chain.
	genesisDocProvider := reactor.GetGenesisProvider()
	serviceProvider := reactor.GetServicesProvider()
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	statesProvider := reactor.GetInstanceProvider(InstanceKeyState)
	privvalProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)
	// switchProvider := reactor.GetInstanceProvider(InstanceKeyP2PSwitch)
	// transportProvider := reactor.GetInstanceProvider(InstanceKeyP2PTransport)
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyBlockStore)

	// Allocate return objects
	nodesMultiplex := MultiplexMap[*node.Node]{}
	eventSwitch := reactor.eventSwitch
	p2pTransport := reactor.transport

	// We iterate through an ordered list of known networks to create
	// one instance of [node.Node] for each replicated chain.
	//
	// This notably permits to keep backwards-compatibility with CometBFT.
	for _, chainID := range networks {
		// Config
		genesisDoc := genesisDocProvider(chainID)
		cfgOverwrite := configProvider(chainID).(*config.Config)
		privValidator := privvalProvider(chainID).(types.PrivValidator)

		// P2P
		// eventSwitch := switchProvider(chainID).(*p2p.Switch)
		// p2pTransport := transportProvider(chainID).(*p2p.MultiplexTransport)
		pexAddrBook := eventSwitch.GetAddrBook().(pex.AddrBook)

		// Consensus
		eventBus := serviceProvider(ServiceKeyEventBus, chainID).(*types.EventBus)
		proxyApp := reactor.abciClient.ToAppConns(chainID)
		memplReactor := serviceProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		consensusReactor := serviceProvider(ServiceKeyConsensusReactor, chainID).(*cs.Reactor)
		evidenceReactor := serviceProvider(ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor)
		indexerService := serviceProvider(ServiceKeyIndexers, chainID).(*txindex.IndexerService)
		pruner := serviceProvider(ServiceKeyPruner, chainID).(*sm.Pruner)

		// State/Blocks
		shouldStateSync := false // state-sync is disabled for nodes multiplexes
		stateMachine := statesProvider(chainID).(sm.State)
		stateStore := stateStoreProvider(chainID).(sm.Store)
		blockStore := blockStoreProvider(chainID).(*bs.BlockStore)

		nodeInstance := node.NewNodeWithServices(
			cfgOverwrite,
			genesisDoc,
			reactor.nodeInfo.GetNodeInfo(chainID),
			reactor.nodeKey,
			privValidator,
			pexAddrBook,
			p2pTransport,
			eventSwitch,
			eventBus,
			proxyApp,
			memplReactor.GetMempoolPtr(),
			evidenceReactor.GetPoolPtr(),
			pruner,
			indexerService,
			stateStore,
			blockStore,
			consensusReactor.GetState(), // cs.State
			shouldStateSync,
			stateMachine, // stateSyncGenesis (sm.State)
		)

		nodeInstance.BaseService = *service.NewBaseService(
			reactor.logger,
			"Node",
			nodeInstance,
		)

		// Apply custom node.Option configuration
		for _, option := range options {
			option(nodeInstance)
		}

		// Prepare registerable instances mapped to ChainID
		nodesMultiplex[chainID] = NewChainInstance(chainID, nodeInstance)

		// Add to service shutdown routine by registration
		reactor.RegisterService(ServiceKeyNodeRuntime, chainID, nodeInstance)
	}

	return nodesMultiplex
}
