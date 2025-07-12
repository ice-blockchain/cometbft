package multiplex

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/p2p"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
)

// PrepareConsensusInstanceWithReactor initializes a consensus handshake.
//
// CAUTION: the consensus handshake (ABCI <> DB) must only be done when
// state sync does not execute. This handshake is intended to synchronize
// the database by executing blocks replay if necessary.
//
// After the handshake, this method will *re-load the state machine*.
func (reactor *Reactor) PrepareConsensusInstanceWithReactor(
	ctx context.Context,
	chainID string,
) error {
	clogger := reactor.logger.With("chainId", chainID)

	// Used for retrieving GenesisDoc instance by chain
	genesisDocProvider := reactor.GetGenesisProvider()
	servicesProvider := reactor.GetServicesProvider()

	// Used for retrieving state store instance by chain
	stateProvider := reactor.GetInstanceProvider(InstanceKeyState)
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyBlockStore)

	// Retrieve the correct instances/services by chain
	genesisDoc, genesisErr := genesisDocProvider(chainID)
	if genesisErr != nil {
		return genesisErr
	}

	stateMachine := stateProvider(chainID).(sm.State)
	stateStore := stateStoreProvider(chainID).(sm.Store)
	blockStore := blockStoreProvider(chainID).(*bs.BlockStore)
	eventBus, ok := servicesProvider(ServiceKeyEventBus, chainID).(*types.EventBus)
	if !ok {
		return fmt.Errorf(
			"could not get event bus in PrepareConsensusInstanceWithReactor with ChainID %s", chainID)
	}

	// First make sure the ABCI is setup correctly
	abciClient := reactor.GetABCIClient()
	if abciClient == nil {
		return errors.New("missing ABCI client (proxyApp) for consensus handshake")
	}

	// 1) Consensus handshake with ABCI
	proxyApp := abciClient.ToAppConns(chainID)
	handshaker := cs.NewHandshaker(
		stateStore,
		stateMachine,
		blockStore,
		genesisDoc,
	)
	handshaker.SetLogger(clogger.With("module", "consensus"))
	handshaker.SetEventBus(eventBus)
	if err := handshaker.Handshake(ctx, proxyApp); err != nil {
		return fmt.Errorf("error during consensus handshake: %v", err)
	}

	// 2) Reload the state after handshake succeeded.
	//
	// The state machine will have the Version.Consensus.App set by the Handshake,
	// and may have other modifications as well, ie. depending on what happened
	// during block replay.

	_, err := stateStore.Load()
	if err != nil {
		return sm.ErrCannotLoadState{Err: err}
	}

	return nil
}

// CreateConsensusInstanceReactors creates all reactors necessary to setup
// a node for being consensus-ready with a network.
//
// This method creates instances for the following services and reactors:
//
// 1) Create the mempool / mempool reactor
// 2) Create the evidence pool / evidence reactor
// 3) Create the block executor
// 4) Create block-sync reactor
// 5) Create consensus state / reactor
//
// Afterwards, pointers to the created instances are registered on the reactor.
//
// This method registers instances in the multiplexRegistry:
// - `flag/blockSync`: A flag that determines whether block-sync must run.
//
// This method also registers services in the servicesRegistry:
// - `reactor/mempool`: The mempool reactor with Mempool ABCI conn.
// - `reactor/blockSync`: The block-sync reactor with a block executor.
// - `reactor/consensus`: The consensus reactor with WAL file overwrite.
// - `reactor/evidence`: The evidence reactor around state- and block stores.
func (reactor *Reactor) CreateConsensusInstanceReactors(
	ctx context.Context,
	chainID string,
	blockSync bool,
	waitSync bool,
) (err error) {
	// First make sure the ABCI is setup correctly
	abciClient := reactor.GetABCIClient()
	if abciClient == nil {
		return errors.New(
			"missing ABCI client (proxyApp) for consensus execution")
	}

	extChainID, err := NewExtendedChainIDFromLegacy(chainID)
	if err != nil {
		return fmt.Errorf(
			"found incompatible multiplex ChainID %s: %w", chainID, err)
	}

	clogger := reactor.logger.With("chainId", chainID)

	// Used to retrieve configuration and state per chain.
	servicesProvider := reactor.GetServicesProvider()
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	statesProvider := reactor.GetInstanceProvider(InstanceKeyState)
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyBlockStore)
	privvalProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)

	evidenceDBService := servicesProvider(ServiceKeyDatabaseEvidence, chainID)
	if err := EnsureStartDBService(ctx, evidenceDBService); err != nil {
		return fmt.Errorf(
			"failed to open evidence database for %s: %w", chainID, err)
	}

	// The node config contains the configuration overwrite.
	cfgOverwrite := configProvider(chainID).(*config.Config)
	stateMachine := statesProvider(chainID).(sm.State)
	privValidator := privvalProvider(chainID).(types.PrivValidator)
	eventBus, ok := servicesProvider(ServiceKeyEventBus, chainID).(*types.EventBus)
	if !ok {
		err = fmt.Errorf(
			"could not get event bus in CreateConsensusInstanceReactors with ChainID %s", chainID)
		return // err
	}

	// Prometheus does not allow hyphens in metrics names, it must match
	// following regexp: [a-zA-Z_:][a-zA-Z0-9_:]*
	// see also: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
	metricsNames := cfgOverwrite.Instrumentation.Namespace + "_" + string(reactor.GetNodeKey().ID()) + ":" + strings.ReplaceAll(chainID, "-", "_")

	// We can safely ignore the error because it triggers before in Reactor.
	privValPubKey, _ := privValidator.GetPubKey()

	// 0) Retrieve prometheus metrics providers per module
	//
	// Metrics providers are also scoped per ChainID and the registration
	// permits to work with multiple node restarts without resetting metrics.
	memplMetricsProvider := reactor.RegisterMetrics("mempool", metricsNames, func() interface{} {
		return mempl.PrometheusMetrics(metricsNames, "chain_id", chainID)
	}).(*mempl.Metrics)

	stateMetricsProvider := reactor.RegisterMetrics("state", metricsNames, func() interface{} {
		return sm.PrometheusMetrics(metricsNames, "chain_id", chainID)
	}).(*sm.Metrics)

	bsyncMetricsProvider := reactor.RegisterMetrics("blocksync", metricsNames, func() interface{} {
		return blocksync.PrometheusMetrics(metricsNames, "chain_id", chainID)
	}).(*blocksync.Metrics)

	consensusMetricsProvider := reactor.RegisterMetrics("consensus", metricsNames, func() interface{} {
		return cs.PrometheusMetrics(metricsNames, "chain_id", chainID)
	}).(*cs.Metrics)

	// 1) Create the mempool / mempool reactor
	//
	// BREAKING: We do not permit using the NopMempool.
	var (
		mempool        *mempl.CListMempool
		mempoolReactor *mempl.Reactor
	)
	mempoolService := servicesProvider(ServiceKeyMempoolReactor, chainID)
	if mempoolService == nil {
		memplLogger := clogger.With("module", "mempool")
		mempool = mempl.NewCListMempool(
			cfgOverwrite.Mempool,
			abciClient.Mempool(chainID),
			stateMachine.LastBlockHeight,
			mempl.WithMetrics(memplMetricsProvider),
			mempl.WithPreCheck(sm.TxPreCheck(stateMachine.Copy())),
			mempl.WithPostCheck(sm.TxPostCheck(stateMachine.Copy())),
		)
		mempool.SetLogger(memplLogger)
		mempoolReactor = mempl.NewReactor(
			ctx,
			cfgOverwrite.Mempool,
			mempool,
			waitSync, // "waitSync"
			mempl.WithAcceptor(
				extChainID.GetUserAddress(),
				reactor.GetAcceptor(),
			),
			mempl.WithChainID(chainID),
			mempl.WithNodeKey(reactor.GetNodeKey()),
			mempl.WithDialerFn(reactor.GetRelayDialerForCometBFT()),
			mempl.WithRuntimeRegistry(reactor.GetRuntimeRegistry()),
		)
		if cfgOverwrite.Consensus.WaitForTxs() {
			mempool.EnableTxsAvailable()
		}
		mempoolReactor.SetLogger(memplLogger)

		// NOTE(midas): Set the switch instance early on so that the mempool
		// can start messaging right at Start and not wait for other reactors.
		mempoolReactor.SetSwitch(reactor.GetEventSwitchForCometBFT())

		reactor.RegisterService(ServiceKeyMempoolReactor, chainID, mempoolReactor)
	} else {
		mempoolReactor = mempoolService.(*mempl.Reactor)
		mempool = mempoolReactor.GetMempoolPtr()
	}

	// 2) Create the evidence pool / evidence reactor
	evidenceDB := evidenceDBService.(*DBService).DB()
	stateStore := stateStoreProvider(chainID).(sm.Store)
	blockStore := blockStoreProvider(chainID).(*bs.BlockStore)

	var (
		evidencePool    *evidence.Pool
		evidenceReactor *evidence.Reactor
	)
	evidenceService := servicesProvider(ServiceKeyEvidenceReactor, chainID)
	if evidenceService == nil {
		evidenceLogger := clogger.With("module", "evidence")
		evidencePool, err = evidence.NewPool(
			evidenceDB,
			stateStore,
			blockStore,
			evidence.WithDBKeyLayout(cfgOverwrite.Storage.ExperimentalKeyLayout),
		)
		if err != nil {
			err = fmt.Errorf("error creating the evidence pool: %w", err)
			return // err
		}
		evidenceReactor = evidence.NewReactor(ctx, evidencePool, evidence.WithChainID(chainID))
		evidenceReactor.SetLogger(evidenceLogger)

		reactor.RegisterService(ServiceKeyEvidenceReactor, chainID, evidenceReactor)
	} else {
		evidenceReactor = evidenceService.(*evidence.Reactor)
		evidencePool = evidenceReactor.GetPoolPtr()
	}

	// state-sync is disabled, so stays at 0!
	offlineStateSyncHeight := int64(0)

	// 3) Create the block sync reactor
	//
	// Make a block executor for consensus and blocksync reactors to execute
	// blocks - the block executor logs on the state module.
	var (
		blockExecutor    *sm.BlockExecutor
		blockSyncReactor *blocksync.Reactor
	)
	blocksyncService := servicesProvider(ServiceKeyBlockSyncReactor, chainID)
	if blocksyncService == nil {
		blockExecutor = sm.NewBlockExecutor(
			stateStore,
			clogger.With("module", "state"),
			abciClient.Consensus(chainID),
			mempool,
			evidencePool,
			blockStore,
			sm.BlockExecutorWithMetrics(stateMetricsProvider),
		)

		blockSyncReactor = blocksync.NewReactor(
			ctx,
			stateMachine.Copy(),
			blockExecutor,
			blockStore,
			blockSync,
			privValPubKey.Address(),
			bsyncMetricsProvider,
			offlineStateSyncHeight,
			blocksync.WithChainID(chainID),
		)
		blockSyncReactor.SetLogger(clogger.With("module", "blocksync"))

		// NOTE(midas): Set the switch instance early on so that the blocksync
		// can start messaging right at Start and not wait for other reactors.
		blockSyncReactor.SetSwitch(reactor.GetEventSwitchForCometBFT())

		reactor.RegisterService(ServiceKeyBlockSyncReactor, chainID, blockSyncReactor)
		reactor.RegisterInstance(InstanceKeyFlagBlockSync, chainID, blockSync)
	} else {
		blockSyncReactor = blocksyncService.(*blocksync.Reactor)
		blockExecutor = blockSyncReactor.BlockExecutor()
	}

	// 4) Create consensus state / reactor
	//
	// Note that using the config overwrite, we use a separate WAL-file
	// for every replicated chain.
	consensusService := servicesProvider(ServiceKeyConsensusReactor, chainID)
	if consensusService == nil {
		consensusLogger := clogger.With("module", "consensus")
		consensusState := cs.NewState(
			ctx,
			cfgOverwrite.Consensus, // contains overwrite of WAL
			stateMachine.Copy(),
			blockExecutor,
			blockStore,
			mempool,
			evidencePool,
			cs.StateMetrics(consensusMetricsProvider),
			cs.OfflineStateSyncHeight(offlineStateSyncHeight),
		)
		consensusState.SetLogger(consensusLogger)
		if privValidator != nil {
			consensusState.SetPrivValidator(privValidator)
		}
		consensusReactor := cs.NewReactor(
			ctx,
			consensusState,
			waitSync, // "waitSync"
			cs.ReactorMetrics(consensusMetricsProvider),
			cs.WithNodeKey(reactor.GetNodeKey()),
			cs.WithRuntimeRegistry(reactor.GetRuntimeRegistry()),
		)
		consensusReactor.SetLogger(consensusLogger)
		// services which will be publishing and/or subscribing for messages (events)
		// consensusReactor will set it on consensusState and blockExecutor
		consensusReactor.SetEventBus(eventBus)

		// NOTE(midas): Set the switch instance early on so that the consensus
		// reactor can respond with ChainReplicationComplete when necessary.
		consensusReactor.SetSwitch(reactor.GetEventSwitchForCometBFT())

		// Prepare registerable instances mapped to ChainID
		reactor.RegisterService(ServiceKeyConsensusReactor, chainID, consensusReactor)
	}

	return nil
}

// StartConsensusInstanceReactors starts the mempool, blocksync, consensus
// and evidence reactors. This must be called when the networks are injected
// into runtime and the p2p.Switch is already running.
func (reactor *Reactor) StartConsensusInstanceReactors(
	ctx context.Context,
	chainID string,
	sendStatusToPeers bool,
) error {
	cometbftSwitch := reactor.GetEventSwitchForCometBFT()
	reactorsAvailable := cometbftSwitch.Reactors(chainID)

	// TODO(midas): remove debug logs
	reactor.logger.Debug("StartConsensusInstanceReactors",
		"chainId", chainID,
		"sendStatus", sendStatusToPeers,
		"numActiveRuntimes", cometbftSwitch.NumActiveRuntimes(),
		"reactorsAvailable", reactorsAvailable,
	)

	// Add channels for the new ChainID to all existing peers
	// This prevents "unknown channel - missing ChainID" errors when peers
	// try to send messages for the new ChainID.
	if err := reactor.AddConnectionChannels(cometbftSwitch, []string{chainID}); err != nil {
		return fmt.Errorf(
			"error with consensus reactors; adding channels for ChainID %s: %w",
			chainID, err)
	}

	// Add active runtime for remote relays which don't have it yet.
	cometbftSwitch.AddActiveRuntime(chainID)

	// Make sure database connections are open for this ChainID.
	extChainID, _ := NewExtendedChainIDFromLegacy(chainID)
	if err := reactor.MakeNetworkDatabases(extChainID, []string{
		"blockstore",
		"state",
		"txindex",
		"evidence",
	}, true); err != nil {
		return fmt.Errorf(
			"error with consensus reactors; database unavailable for %s - %w",
			chainID, err)
	}

	reactor.envMutex.Lock()
	if reactor.abciClient.IsStopped() {
		reactor.abciClient.Reset(ctx)
	}
	if !reactor.abciClient.IsRunning() {
		if err := reactor.abciClient.Start(); err != nil {
			return fmt.Errorf(
				"error with consensus reactors; abci unavailable for %s - %w",
				chainID, err)
		}
	}
	reactor.envMutex.Unlock()

	servicesProvider := reactor.GetServicesProvider()

	// Given sendStatusToPeers, we should send completion updates to
	// all consensus peers, i.e. send a ChainReplicationComplete msg.
	//	consensusReactor := cometbftSwitch.Reactor(chainID, "CONSENSUS").(*cs.Reactor)
	consensusReactor := servicesProvider(ServiceKeyConsensusReactor, chainID).(*cs.Reactor)
	consensusReactor.SetSendStatusToPeers(sendStatusToPeers)
	consensusReactor.SetRuntimeRegistry(reactor.GetRuntimeRegistry())

	//	memplReactor := cometbftSwitch.Reactor(chainID, "MEMPOOL").(*mempl.Reactor)
	memplReactor := servicesProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
	memplReactor.SetRuntimeRegistry(reactor.GetRuntimeRegistry())

	blocksyncReactor := servicesProvider(ServiceKeyBlockSyncReactor, chainID).(*blocksync.Reactor)
	evidenceReactor := servicesProvider(ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor)

	reactorStarterFn := func(name string, r p2p.Reactor) {
		// Update the attached switch
		r.SetSwitch(cometbftSwitch)

		if r.IsStopped() {
			r.Reset(ctx) // allows re-start
		}

		if !r.IsRunning() {
			if err := r.Start(); err != nil {
				// Prevents erroring for concurrent calls to Start() method.
				if err != service.ErrAlreadyStarted {
					reactor.logger.Error("Error starting reactor",
						"reactor", name,
						"err", err)
				}
			}
		}
	}

	reactorStarterFn("CONSENSUS", consensusReactor)
	reactorStarterFn("BLOCKSYNC", blocksyncReactor)
	reactorStarterFn("MEMPOOL", memplReactor)
	reactorStarterFn("EVIDENCE", evidenceReactor)

	cometbftSwitch.AddReactor(chainID, "CONSENSUS", consensusReactor)
	cometbftSwitch.AddReactor(chainID, "BLOCKSYNC", blocksyncReactor)
	cometbftSwitch.AddReactor(chainID, "MEMPOOL", memplReactor)
	cometbftSwitch.AddReactor(chainID, "EVIDENCE", evidenceReactor)

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Done with StartConsensusInstanceReactors", "chainId", chainID)

	return nil
}

func (reactor *Reactor) StopConsensusInstanceReactors(
	ctx context.Context,
	chainID string,
) error {
	// TODO(midas): remove debug logs
	reactor.logger.Debug("StopConsensusInstanceReactors", "chainId", chainID)

	cometbftSwitch := reactor.GetEventSwitchForCometBFT()

	// Also remove this active runtime, so that in sw.addPeer()
	// we don't include it in relevantScopes anymore.
	cometbftSwitch.RemoveActiveRuntime(chainID)

	// Remove channels allocated for ChainID, from all existing peers.
	if err := reactor.RemoveConnectionChannels(cometbftSwitch, []string{chainID}); err != nil {
		reactor.logger.Error("error removing connection channels",
			"chainId", chainID,
			"err", err)
	}

	// Stop all the reactors available for this ChainID.
	reactorsForChain := cometbftSwitch.Reactors(chainID)
	for name, r := range reactorsForChain {
		if r.IsRunning() || r.IsStarted() {
			err := r.Stop()

			if err != nil && err != service.ErrAlreadyStopped {
				reactor.logger.Error("error stopping reactor",
					"chainId", chainID,
					"reactor", name,
					"err", err)
			}
		}
	}

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Done with StopConsensusInstanceReactors", "chainId", chainID)

	return nil
}
