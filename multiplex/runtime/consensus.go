package runtime

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ConsensusPool defines a transaction broadcast pool.
type ConsensusPool struct {
	service.BaseService

	mtx            *sync.Mutex
	nodeKey        *cmtp2p.NodeKey
	cometbftSwitch *cmtp2p.Switch

	acceptorImpl    client.Acceptor
	abciClient      proxy.ChainConns
	runtimeMgr      types.IdleManager
	runtimeComposer *runtimeComposer
	resourceMgr     types.ResourceManager

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.ConsensusHandler = (*ConsensusPool)(nil)

type ConsensusPoolOption func(*ConsensusPool)

// NewConsensusHandler creates a new consensus handler.
func NewConsensusHandler(
	ctx context.Context,
	nodeKey *cmtp2p.NodeKey,
	abciClient proxy.ChainConns,
	resourceMgr types.ResourceManager,
	composer types.RuntimeComposer,
	logger cmtlog.Logger,
	options ...ConsensusPoolOption,
) *ConsensusPool {
	pool := &ConsensusPool{
		mtx:     new(sync.Mutex),
		nodeKey: nodeKey,

		abciClient:      abciClient,
		resourceMgr:     resourceMgr,
		runtimeComposer: composer.(*runtimeComposer),

		// Provides a default acceptor implementation
		acceptorImpl: &client.DefaultAcceptor{},

		// Options
		logger: logger,
	}

	// Use option helpers
	pool.SetOptions(options...)

	pool.BaseService = *service.NewBaseService(ctx, logger, "ConsensusPool", pool)
	return pool
}

// ConsensusPoolWithLogger injects a custom logger instance.
func ConsensusPoolWithLogger(logger cmtlog.Logger) ConsensusPoolOption {
	return func(pool *ConsensusPool) {
		pool.logger = logger
	}
}

// ConsensusPoolWithAcceptor injects a custom acceptor implementation.
func ConsensusPoolWithAcceptor(
	acceptor client.Acceptor,
) ConsensusPoolOption {
	return func(pool *ConsensusPool) {
		pool.acceptorImpl = acceptor
	}
}

// ----------------------------------------------------------------------------
// ConsensusPool implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (pool *ConsensusPool) OnStart(ctx context.Context) (err error) {
	// TODO(midas): we may want the abciClient to be managed by this pool.
	return nil
}

// OnStop implements [service.Service] by closing the database.
func (pool *ConsensusPool) OnStop() {
	// Note: nothing to do with respect to the injected and executing ChainIDs
	// because this is *controlled* by the Registry in `Registry#StopRuntime`.
}

// OnReset implements [service.Service] by resetting the service.
func (pool *ConsensusPool) OnReset(ctx context.Context) error {
	// TODO(midas): we may want the abciClient to be managed by this pool.
	return nil
}

// ----------------------------------------------------------------------------
// ConsensusHandler API implementation

// SetSwitch is used to set a cmtp2p.Switch for CometBFT.
func (pool *ConsensusPool) SetSwitch(sw *cmtp2p.Switch) {
	pool.cometbftSwitch = sw
}

// Switch returns the cmtp2p.Switch instance for CometBFT.
func (pool *ConsensusPool) Switch() *cmtp2p.Switch {
	return pool.cometbftSwitch
}

// ABCI returns the "application-blockchain client interface".
func (pool *ConsensusPool) ABCI() proxy.ChainConns {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	return pool.abciClient
}

// Handshake executes the consensus/ABCI handshake to set the App version.
func (pool *ConsensusPool) Handshake(chainID string) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("ConsensusPool#Handshake", "chainId", chainID)

	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	eventBus := pool.runtimeComposer.EventBus(chainID)
	genesisDoc := pool.runtimeComposer.GenesisDoc(chainID)
	stateStore := pool.runtimeComposer.StateStore(chainID)
	blockStore := pool.runtimeComposer.BlockStore(chainID)
	stateMachine := pool.runtimeComposer.StateMachine(chainID)

	proxyApp := pool.abciClient.ToAppConns(chainID)
	handshaker := cs.NewHandshaker(
		stateStore,
		stateMachine,
		blockStore,
		&genesisDoc,
	)
	handshaker.SetLogger(pool.logger.With("module", "consensus"))
	handshaker.SetEventBus(eventBus)
	if err := handshaker.Handshake(pool.Context(), proxyApp); err != nil {
		return fmt.Errorf("error during consensus handshake for ChainID %s: %w", chainID, err)
	}

	// Reload the state after handshake succeeded.
	//
	// The state machine will have the Version.Consensus.App set by the Handshake,
	// and may have other modifications as well, ie. depending on what happened
	// during block replay.

	reloadedState, err := stateStore.Load()
	if err != nil {
		return sm.ErrCannotLoadState{Err: err}
	}

	pool.resourceMgr.Set(chainID, types.InstanceKeyStateMachine, reloadedState)
	return nil
}

// Inject injects mempool, blocksync, consensus and evidence reactors.
func (pool *ConsensusPool) Inject(chainID string) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("ConsensusPool#Inject", "chainId", chainID)

	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// Create a [pex.AddrBook] and [pex.Reactor].
	if err := pool.makeNetworkAddressBook(chainID); err != nil {
		return err
	}

	// Create a [mempl.CListMempool] and a [mempool.Reactor].
	if err := pool.makeNetworkMempoolReactor(chainID); err != nil {
		return err
	}

	// Create the [evidence.Reactor].
	if err := pool.makeNetworkEvidenceReactor(chainID); err != nil {
		return err
	}

	// Create the [blocksync.Reactor].
	if err := pool.makeNetworkBlocksyncReactor(chainID); err != nil {
		return err
	}

	// Create the [cs.Reactor].
	if err := pool.makeNetworkConsensusReactor(chainID); err != nil {
		return err
	}

	return nil
}

// Execute starts mempool, blocksync, consensus and evidence reactors.
func (pool *ConsensusPool) Execute(chainID string) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("ConsensusPool#Execute", "chainId", chainID)

	// (1) CAUTION:
	// Note that consensus reactors are not started here to prevent race
	// conditions between the replication routine and cometbft services.

	// Calls the Start method on the node.Node instance.
	nodeInstance := pool.runtimeComposer.Node(chainID)
	go func(network string, n *node.Node) {
		if n.IsStopped() {
			n.Reset(pool.Context()) // allows restart
		}

		pool.logger.Info("Starting new node", "chainId", network)
		pool.logger.Info("Using custom listen addresses",
			"p2p", n.Config().P2P.ListenAddress,
			"rpc", n.Config().RPC.ListenAddress,
		)

		if err := n.Start(); err != nil {
			pool.logger.Error("failed to start node",
				"err", err,
			)
		}

		pool.logger.Info("Started node",
			"nodeInfo", n.Switch().NodeInfo(),
		)
	}(chainID, nodeInstance)

	// (2) IMPORTANT:
	// We intentionally start reactors at the end to give the Node instance
	// some time to initialize and load services.

	var (
		blocksyncReactor *blocksync.Reactor
		consensusReactor *cs.Reactor
		evidenceReactor  *evidence.Reactor
		mempoolReactor   *mempl.Reactor
	)

	if bsR := pool.resourceMgr.Get(chainID, types.ServiceKeyBlockSyncReactor); bsR != nil {
		stateStore := pool.runtimeComposer.StateStore(chainID)
		blockStore := pool.runtimeComposer.BlockStore(chainID)

		// Update block executor state and blocks store.
		blocksyncReactor = bsR.(*blocksync.Reactor)
		blocksyncReactor.SetStateStore(stateStore)
		blocksyncReactor.SetBlockStore(blockStore)
	}
	if conR := pool.resourceMgr.Get(chainID, types.ServiceKeyConsensusReactor); conR != nil {
		consensusReactor = conR.(*cs.Reactor)
	}
	if evR := pool.resourceMgr.Get(chainID, types.ServiceKeyEvidenceReactor); evR != nil {
		evidenceReactor = evR.(*evidence.Reactor)
	}
	if memR := pool.resourceMgr.Get(chainID, types.ServiceKeyMempoolReactor); memR != nil {
		mempoolReactor = memR.(*mempl.Reactor)
	}

	// STOP if we don't have all required consensus reactors.
	if blocksyncReactor == nil || consensusReactor == nil || mempoolReactor == nil {
		pool.logger.Error("failed to start consensus reactors",
			"chainId", chainID,
			"bsR", blocksyncReactor,
			"conR", consensusReactor,
			"evR", evidenceReactor,
			"memR", mempoolReactor,
		)

		return fmt.Errorf(
			"error with consensus reactors; failed to get reactors for %s", chainID)
	}

	// Helper function to ensure reactors are reset if necessary before start.
	reactorStarterFn := func(name string, r p2p.Reactor) {
		if r.IsStopped() {
			r.Reset(pool.Context()) // allows re-start
		}

		if !r.IsRunning() {
			if err := r.Start(); err != nil {
				// Prevents erroring for concurrent calls to Start() method.
				if err != service.ErrAlreadyStarted {
					pool.logger.Error("Error starting reactor",
						"reactor", name,
						"err", err)
				}
			}
		}
	}

	// Start consensus reactors.
	reactorStarterFn("CONSENSUS", consensusReactor)
	reactorStarterFn("BLOCKSYNC", blocksyncReactor)
	reactorStarterFn("MEMPOOL", mempoolReactor)
	reactorStarterFn("EVIDENCE", evidenceReactor)

	// (3) CAUTION:
	// Note that for already existing peers, this has no effect, but for
	// freshly connected peers it enables the reactor channels.

	// Mark peers active in CONSENSUS and BLOCKSYNC reactors.
	peerSet := pool.cometbftSwitch.Peers(chainID)
	for _, peer := range peerSet.Copy() {
		pool.cometbftSwitch.InitPeerForScope(peer, chainID)
		pool.cometbftSwitch.AddPeerForScope(peer, chainID)
	}

	return nil
}

// Shutdown stops mempool, blocksync, consensus and evidence reactors.
func (pool *ConsensusPool) Shutdown(
	chainID string,
) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("ConsensusPool#Shutdown", "chainId", chainID)

	var (
		blocksyncReactor *blocksync.Reactor
		consensusReactor *cs.Reactor
		evidenceReactor  *evidence.Reactor
		mempoolReactor   *mempl.Reactor
	)

	if bsR := pool.resourceMgr.Get(chainID, types.ServiceKeyBlockSyncReactor); bsR != nil {
		blocksyncReactor = bsR.(*blocksync.Reactor)
		if blocksyncReactor.IsRunning() || blocksyncReactor.IsStarted() {
			blocksyncReactor.Stop()
		}
	}
	if conR := pool.resourceMgr.Get(chainID, types.ServiceKeyConsensusReactor); conR != nil {
		consensusReactor = conR.(*cs.Reactor)
		if consensusReactor.IsRunning() || consensusReactor.IsStarted() {
			consensusReactor.Stop()
		}
	}
	if evR := pool.resourceMgr.Get(chainID, types.ServiceKeyEvidenceReactor); evR != nil {
		evidenceReactor = evR.(*evidence.Reactor)
		if evidenceReactor.IsRunning() || evidenceReactor.IsStarted() {
			evidenceReactor.Stop()
		}
	}
	if memR := pool.resourceMgr.Get(chainID, types.ServiceKeyMempoolReactor); memR != nil {
		mempoolReactor = memR.(*mempl.Reactor)
		if mempoolReactor.IsRunning() || mempoolReactor.IsStarted() {
			mempoolReactor.Stop()
		}
	}

	if nodeR := pool.resourceMgr.Get(chainID, types.ServiceKeyNodeRuntime); nodeR != nil {
		nodeInstance := nodeR.(*node.Node)
		if nodeInstance.IsRunning() || nodeInstance.IsStarted() {
			nodeInstance.Stop()
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// Orchestration methods

func (pool *ConsensusPool) makeNetworkAddressBook(
	chainID string,
) error {
	if pool.resourceMgr.Has(chainID, types.ServiceKeyAddressesReactor) {
		return nil // Nothing to do
	}

	runtimeConfig := pool.runtimeComposer.Config(chainID)

	// Used to retrieve configuration and state per chain.
	addressBookPath := filepath.Join(runtimeConfig.RootDir, config.DefaultConfigDir)
	addrBookFile := filepath.Join(addressBookPath, config.DefaultAddrBookName)
	if _, err := os.Stat(addressBookPath); err != nil {
		return fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
	}

	addrBook := pex.NewAddrBook(pool.Context(), addrBookFile, false) // routabilityStrict=false
	addrBook.SetLogger(pool.logger.With("module", "p2p").With("book", addrBookFile))

	// Add ourselves to addrbook to prevent dialing ourselves
	if runtimeConfig.P2P.ExternalAddress != "" {
		externalAddress := p2p.IDAddressString(pool.nodeKey.ID(), runtimeConfig.P2P.ExternalAddress)
		addr, err := p2p.NewNetAddressString(externalAddress)
		if err != nil {
			return fmt.Errorf("p2p.external_address is incorrect: %w", err)
		}
		addrBook.AddOurAddress(addr)
	}
	if runtimeConfig.P2P.ListenAddress != "" {
		internalAddress := p2p.IDAddressString(pool.nodeKey.ID(), runtimeConfig.P2P.ListenAddress)
		addr, err := p2p.NewNetAddressString(internalAddress)
		if err != nil {
			return fmt.Errorf("p2p.laddr is incorrect: %w", err)
		}
		addrBook.AddOurAddress(addr)
	}

	seedNodes := splitAndTrimEmpty(runtimeConfig.P2P.Seeds, ",", " ")
	pexReactor := pex.NewReactor(pool.Context(),
		addrBook,
		&pex.ReactorConfig{
			Seeds:    seedNodes,
			SeedMode: runtimeConfig.P2P.SeedMode,
			// See consensus/reactor.go: blocksToContributeToBecomeGoodPeer 10000
			// blocks assuming 10s blocks ~ 28 hours.
			SeedDisconnectWaitPeriod:     28 * time.Hour,
			PersistentPeersMaxDialPeriod: runtimeConfig.P2P.PersistentPeersMaxDialPeriod,
		},
		pex.WithChainID(chainID),
	)
	pexReactor.SetLogger(pool.logger.With("module", "pex"))

	pool.resourceMgr.Set(chainID, types.ServiceKeyAddressesReactor, pexReactor)
	return nil
}

func (pool *ConsensusPool) makeNetworkMempoolReactor(
	chainID string,
) error {
	if pool.resourceMgr.Has(chainID, types.ServiceKeyMempoolReactor) {
		return nil // Nothing to do
	}

	extChainID := pool.runtimeComposer.UserChainID(chainID)
	privValidator := pool.runtimeComposer.Validator(chainID)
	runtimeConfig := pool.runtimeComposer.Config(chainID)
	stateMachine := pool.runtimeComposer.StateMachine(chainID)

	privValPubKey, _ := privValidator.GetPubKey()
	shouldBlockSync := !onlyValidatorIsUs(stateMachine.Copy(), privValPubKey) && !validatorsIncludesUs(stateMachine.Copy(), privValPubKey)

	var (
		mempool        *mempl.CListMempool
		mempoolReactor *mempl.Reactor
	)

	mempool = mempl.NewCListMempool(
		runtimeConfig.Mempool,
		pool.abciClient.Mempool(chainID),
		stateMachine.LastBlockHeight,
		mempl.WithPreCheck(sm.TxPreCheck(stateMachine.Copy())),
		mempl.WithPostCheck(sm.TxPostCheck(stateMachine.Copy())),
	)
	mempool.SetLogger(pool.logger.With("module", "mempool"))

	mempoolReactor = mempl.NewReactor(
		pool.Context(),
		runtimeConfig.Mempool,
		mempool,
		shouldBlockSync, // "waitSync"
		mempl.WithAcceptor(
			extChainID.GetUserAddress(),
			pool.acceptorImpl,
		),
		mempl.WithChainID(chainID),
		mempl.WithNodeKey(pool.nodeKey),
		mempl.WithIdleManager(pool.runtimeMgr),
		// XXX mempl.WithDialerFn
	)
	if runtimeConfig.Consensus.WaitForTxs() {
		mempool.EnableTxsAvailable()
	}
	mempoolReactor.SetLogger(pool.logger.With("module", "mempool"))
	mempoolReactor.SetSwitch(pool.Switch())

	pool.resourceMgr.Set(chainID, types.ServiceKeyMempoolReactor, mempoolReactor)
	return nil
}

func (pool *ConsensusPool) makeNetworkEvidenceReactor(
	chainID string,
) error {
	evidenceDBService := pool.resourceMgr.Get(chainID, types.ServiceKeyDatabaseEvidence).(service.Service)
	if err := helpers.EnsureStartDBService(pool.Context(), evidenceDBService); err != nil {
		return fmt.Errorf(
			"failed to open evidence database for %s: %w", chainID, err)
	}

	evidenceDB := evidenceDBService.(*helpers.DBService).DB()
	stateStore := pool.runtimeComposer.StateStore(chainID)
	blockStore := pool.runtimeComposer.BlockStore(chainID)
	runtimeConfig := pool.runtimeComposer.Config(chainID)

	// Create the [evidence.Reactor].
	if !pool.resourceMgr.Has(chainID, types.ServiceKeyEvidenceReactor) {
		evidencePool, err := evidence.NewPool(
			evidenceDB,
			stateStore,
			blockStore,
			evidence.WithDBKeyLayout(runtimeConfig.Storage.ExperimentalKeyLayout),
		)
		if err != nil {
			return fmt.Errorf("failed to create the evidence pool for %s: %w", chainID, err)
		}
		evidenceReactor := evidence.NewReactor(pool.Context(),
			evidencePool,
			evidence.WithChainID(chainID),
		)
		evidenceReactor.SetLogger(pool.logger.With("module", "evidence"))
		evidenceReactor.SetSwitch(pool.Switch())

		pool.resourceMgr.Set(chainID, types.ServiceKeyEvidenceReactor, evidenceReactor)
	}

	return nil
}

func (pool *ConsensusPool) makeNetworkBlocksyncReactor(
	chainID string,
) error {
	stateStore := pool.runtimeComposer.StateStore(chainID)
	blockStore := pool.runtimeComposer.BlockStore(chainID)
	mempoolPtr := pool.runtimeComposer.Mempool(chainID)
	evidencePtr := pool.runtimeComposer.EvidencePool(chainID)
	stateMachine := pool.runtimeComposer.StateMachine(chainID)
	privValidator := pool.runtimeComposer.Validator(chainID)

	privValPubKey, _ := privValidator.GetPubKey()
	shouldBlockSync := !onlyValidatorIsUs(stateMachine.Copy(), privValPubKey) && !validatorsIncludesUs(stateMachine.Copy(), privValPubKey)

	// Create the [sm.BlockExecutor] and [blocksync.Reactor].
	if !pool.resourceMgr.Has(chainID, types.ServiceKeyBlockSyncReactor) {
		blockExecutor := sm.NewBlockExecutor(
			stateStore,
			pool.logger.With("module", "state"),
			pool.abciClient.Consensus(chainID),
			mempoolPtr,
			evidencePtr,
			blockStore,
		)

		blockSyncReactor := blocksync.NewReactor(
			pool.Context(),
			stateMachine.Copy(),
			blockExecutor,
			blockStore,
			shouldBlockSync,
			privValPubKey.Address(),
			blocksync.NopMetrics(),
			0, // offlineStateSyncHeight (state-sync disabled)
			blocksync.WithChainID(chainID),
		)
		blockSyncReactor.SetLogger(pool.logger.With("module", "blocksync"))
		blockSyncReactor.SetSwitch(pool.Switch())

		pool.resourceMgr.Set(chainID, types.InstanceKeyBlockExecutor, blockExecutor)
		pool.resourceMgr.Set(chainID, types.ServiceKeyBlockSyncReactor, blockSyncReactor)
	}

	return nil
}

func (pool *ConsensusPool) makeNetworkConsensusReactor(
	chainID string,
) error {
	runtimeConfig := pool.runtimeComposer.Config(chainID)
	stateMachine := pool.runtimeComposer.StateMachine(chainID)
	blockStore := pool.runtimeComposer.BlockStore(chainID)
	blockExecutor := pool.runtimeComposer.BlockExecutor(chainID)
	mempoolPtr := pool.runtimeComposer.Mempool(chainID)
	evidencePtr := pool.runtimeComposer.EvidencePool(chainID)
	privValidator := pool.runtimeComposer.Validator(chainID)

	privValPubKey, _ := privValidator.GetPubKey()
	shouldBlockSync := !onlyValidatorIsUs(stateMachine.Copy(), privValPubKey) && !validatorsIncludesUs(stateMachine.Copy(), privValPubKey)

	// Create the [cs.State] and [cs.Reactor].
	if !pool.resourceMgr.Has(chainID, types.ServiceKeyConsensusReactor) {
		consensusState := cs.NewState(
			pool.Context(),
			runtimeConfig.Consensus, // contains overwrite of WAL
			stateMachine.Copy(),
			blockExecutor,
			blockStore,
			mempoolPtr,
			evidencePtr,
			cs.OfflineStateSyncHeight(0), // offlineStateSyncHeight (state-sync disabled)
		)
		consensusState.SetLogger(pool.logger.With("module", "consensus"))
		consensusState.SetPrivValidator(privValidator)

		consensusReactor := cs.NewReactor(
			pool.Context(),
			consensusState,
			shouldBlockSync, // "waitSync"
			cs.WithNodeKey(pool.nodeKey),
			cs.WithIdleManager(pool.runtimeMgr),
			cs.WithChainID(chainID),
		)
		consensusReactor.SetLogger(pool.logger.With("module", "consensus"))
		consensusReactor.SetSwitch(pool.Switch())

		// services which will be publishing and/or subscribing for messages (events)
		// consensusReactor will set it on consensusState and blockExecutor
		eventBus := pool.runtimeComposer.EventBus(chainID)
		consensusReactor.SetEventBus(eventBus)

		pool.resourceMgr.Set(chainID, types.ServiceKeyConsensusReactor, consensusReactor)
	}

	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (pool *ConsensusPool) SetOptions(options ...ConsensusPoolOption) {
	for _, option := range options {
		option(pool)
	}
}

// Logger returns the logger instance.
func (pool *ConsensusPool) Logger() cmtlog.Logger {
	return pool.logger
}

// SetIdleManager sets a custom idle manager instance.
func (pool *ConsensusPool) SetIdleManager(mgr types.IdleManager) {
	pool.runtimeMgr = mgr
}

// IdleManager returns the idle manager instance.
func (pool *ConsensusPool) IdleManager() types.IdleManager {
	return pool.runtimeMgr
}
