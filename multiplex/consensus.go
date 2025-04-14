package multiplex

import (
	"context"
	"errors"
	"fmt"
	"strings"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	mempl "github.com/ice-blockchain/cometbft/mempool"
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
	// First make sure the ABCI is setup correctly
	abciClient := reactor.GetABCIClient()
	if abciClient == nil {
		return errors.New("missing ABCI client (proxyApp) for consensus handshake")
	}

	clogger := reactor.logger.With("chain_id", chainID)

	// Get this network's app connections for consensus
	proxyApp := abciClient.ToAppConns(chainID)

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

	// 1) Consensus handshake with ABCI
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
) error {
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

	clogger := reactor.logger.With("chain_id", chainID)

	// Used to retrieve configuration and state per chain.
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	statesProvider := reactor.GetInstanceProvider(InstanceKeyState)
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyBlockStore)
	evidenceDBProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseEvidence)
	privvalProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)
	servicesProvider := reactor.GetServicesProvider()

	// The node config contains the configuration overwrite.
	cfgOverwrite := configProvider(chainID).(*config.Config)
	stateMachine := statesProvider(chainID).(sm.State)
	privValidator := privvalProvider(chainID).(types.PrivValidator)
	eventBus, ok := servicesProvider(ServiceKeyEventBus, chainID).(*types.EventBus)
	if !ok {
		return fmt.Errorf(
			"could not get event bus in CreateConsensusInstanceReactors with ChainID %s", chainID)
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
	memplLogger := clogger.With("module", "mempool")
	mempool := mempl.NewCListMempool(
		cfgOverwrite.Mempool,
		abciClient.Mempool(chainID),
		stateMachine.LastBlockHeight,
		mempl.WithMetrics(memplMetricsProvider),
		mempl.WithPreCheck(sm.TxPreCheck(stateMachine.Copy())),
		mempl.WithPostCheck(sm.TxPostCheck(stateMachine.Copy())),
	)
	mempool.SetLogger(memplLogger)
	mempoolReactor := mempl.NewReactor(
		cfgOverwrite.Mempool,
		mempool,
		waitSync, // "waitSync"
		mempl.WithAcceptor(
			extChainID.GetUserAddress(),
			reactor.GetAcceptor(),
		),
		mempl.WithChainID(chainID),
		mempl.WithNodeKey(reactor.GetNodeKey()),
	)
	if cfgOverwrite.Consensus.WaitForTxs() {
		mempool.EnableTxsAvailable()
	}
	mempoolReactor.SetLogger(memplLogger)

	// 2) Create the evidence pool / evidence reactor
	evidenceDB := evidenceDBProvider(chainID).(dbm.DB)
	stateStore := stateStoreProvider(chainID).(sm.Store)
	blockStore := blockStoreProvider(chainID).(*bs.BlockStore)

	evidenceLogger := clogger.With("module", "evidence")
	evidencePool, err := evidence.NewPool(
		evidenceDB,
		stateStore,
		blockStore,
		evidence.WithDBKeyLayout(cfgOverwrite.Storage.ExperimentalKeyLayout),
	)
	if err != nil {
		return fmt.Errorf("error creating the evidence pool: %w", err)
	}
	evidenceReactor := evidence.NewReactor(evidencePool, evidence.WithChainID(chainID))
	evidenceReactor.SetLogger(evidenceLogger)

	// 3) Create the block executor
	//
	// Make a block executor for consensus and blocksync reactors to execute
	// blocks - the block execute logs on the state module.
	blockExecutor := sm.NewBlockExecutor(
		stateStore,
		clogger.With("module", "state"),
		abciClient.Consensus(chainID),
		mempool,
		evidencePool,
		blockStore,
		sm.BlockExecutorWithMetrics(stateMetricsProvider),
	)

	// state-sync is disabled, so stays at 0!
	offlineStateSyncHeight := int64(0)

	// 4) Create block-sync reactor
	//
	// Don't start block sync if we're doing a state sync first or if
	// we are the only validator on the network (caller sets blockSync).
	blockSyncReactor := blocksync.NewReactor(
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

	// 5) Create consensus state / reactor
	//
	// Note that using the config overwrite, we use a separate WAL-file
	// for every replicated chain.
	consensusLogger := clogger.With("module", "consensus")
	consensusState := cs.NewState(
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
		consensusState,
		waitSync, // "waitSync"
		cs.ReactorMetrics(consensusMetricsProvider),
	)
	consensusReactor.SetLogger(consensusLogger)
	// services which will be publishing and/or subscribing for messages (events)
	// consensusReactor will set it on consensusState and blockExecutor
	consensusReactor.SetEventBus(eventBus)

	// Prepare registerable instances mapped to ChainID
	reactor.RegisterService(ServiceKeyMempoolReactor, chainID, mempoolReactor)
	reactor.RegisterService(ServiceKeyEvidenceReactor, chainID, evidenceReactor)
	reactor.RegisterService(ServiceKeyBlockSyncReactor, chainID, blockSyncReactor)
	reactor.RegisterService(ServiceKeyConsensusReactor, chainID, consensusReactor)
	reactor.RegisterInstance(InstanceKeyFlagBlockSync, chainID, blockSync)

	return nil
}

// StartConsensusInstanceReactors starts the mempool, blocksync, consensus
// and evidence reactors. This must be called when the networks are injected
// into runtime and the p2p.Switch is already running.
func (reactor *Reactor) StartConsensusInstanceReactors(
	ctx context.Context,
	chainID string,
) error {
	servicesProvider := reactor.GetServicesProvider()

	if mempoolReactor, ok := servicesProvider(
		ServiceKeyMempoolReactor,
		chainID,
	).(*mempl.Reactor); ok && !mempoolReactor.IsRunning() {
		if err := mempoolReactor.Start(); err != nil {
			return fmt.Errorf(
				"error starting mempool reactor: %w", err)
		}
	}

	if blocksyncReactor, ok := servicesProvider(
		ServiceKeyBlockSyncReactor,
		chainID,
	).(*blocksync.Reactor); ok && !blocksyncReactor.IsRunning() {
		if err := blocksyncReactor.Start(); err != nil {
			return fmt.Errorf(
				"error starting blocksync reactor: %w", err)
		}
	}

	if consensusReactor, ok := servicesProvider(
		ServiceKeyConsensusReactor,
		chainID,
	).(*cs.Reactor); ok && !consensusReactor.IsRunning() {
		if err := consensusReactor.Start(); err != nil {
			return fmt.Errorf(
				"error starting consensus reactor: %w", err)
		}
	}

	if evidenceReactor, ok := servicesProvider(
		ServiceKeyEvidenceReactor,
		chainID,
	).(*evidence.Reactor); ok && !evidenceReactor.IsRunning() {
		if err := evidenceReactor.Start(); err != nil {
			return fmt.Errorf(
				"error starting evidence reactor: %w", err)
		}
	}

	return nil
}
