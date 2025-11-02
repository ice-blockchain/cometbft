package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/ice-blockchain/cometbft/abci/example/kvstore"
	"github.com/ice-blockchain/cometbft/config"
	bc "github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	mxruntime "github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeConsensusPoolNewConsensusHandler(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)
	composer := mxruntime.NewComposer(ctx, baseCfg, nil, resourceMgr, logger)

	nodeKey := &cmtp2p.NodeKey{}

	// TEST 1: Constructor with nil-ABCI client and simple parameters.
	pool := mxruntime.NewConsensusHandler(ctx, nodeKey, nil, resourceMgr, composer, logger)
	require.NotNil(t, pool, "expected non-nil consensus pool instance")
	assert.Equal(t, logger, pool.Logger())
	assert.Nil(t, pool.ABCI(), "expected ABCI connections to be nil")

	if got := consensusPoolNodeKey(t, pool); got != nodeKey {
		t.Fatalf("expected node key pointer to be preserved")
	}

	var wantComposer types.RuntimeComposer = composer
	var haveComposer types.RuntimeComposer = consensusPoolComposer(t, pool)
	assert.Equal(t, wantComposer, haveComposer,
		"expected runtime composer pointer to match constructor input")

	actualAcceptor := consensusPoolAcceptor(t, pool)
	assert.NotNil(t, actualAcceptor,
		"expected default acceptor implementation to be set")

	_, isDefaultAcceptor := actualAcceptor.(*client.DefaultAcceptor)
	assert.Equal(t, true, isDefaultAcceptor,
		fmt.Sprintf("expected default acceptor to be *client.DefaultAcceptor, got %T", actualAcceptor))

	customLogger := cmtlog.NewNopLogger()
	customAcceptor := &client.DefaultAcceptor{}

	// TEST 2: Constructor with nil-ABCI client and added optional parameters.
	poolWithOpts := mxruntime.NewConsensusHandler(
		ctx,
		nodeKey,
		nil,
		resourceMgr,
		composer,
		logger,
		mxruntime.ConsensusPoolWithLogger(customLogger),
		mxruntime.ConsensusPoolWithAcceptor(customAcceptor),
	)
	require.NotNil(t, poolWithOpts, "expected non-nil consensus pool instance with options")
	assert.Equal(t, customLogger, poolWithOpts.Logger(),
		"expected custom logger option to override default logger")

	actualAcceptor2 := consensusPoolAcceptor(t, poolWithOpts)
	assert.Equal(t, customAcceptor, actualAcceptor2)
}

func TestMultiplexRuntimeConsensusPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)
	composer := mxruntime.NewComposer(ctx, cfg, nil, resourceMgr, logger)

	nodeKey := &cmtp2p.NodeKey{}

	// TEST 1: Create a ConsensusPool and make sure it can be started.
	pool := mxruntime.NewConsensusHandler(ctx, nodeKey, nil, resourceMgr, composer, logger)
	assert.Equal(t, false, pool.IsRunning(),
		"expected pool to be stopped before Start")

	startErr := pool.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting ConsensusPool: %v", startErr))
	assert.Equal(t, true, pool.IsRunning(),
		"expected pool to report running state after Start")

	startErr = pool.Start()
	assert.Error(t, startErr)
	assert.Equal(t, true, errors.Is(startErr, service.ErrAlreadyStarted),
		fmt.Sprintf("expected second Start() to return ErrAlreadyStarted, got %v", startErr))

	stopErr := pool.Stop()
	assert.NoError(t, stopErr,
		fmt.Sprintf("unexpected error stopping ConsensusPool: %v", stopErr))
	assert.Equal(t, false, pool.IsRunning(),
		"expected pool not to be running after Stop")
	assert.Equal(t, true, pool.IsStopped(),
		"expected pool to report stopped state after Stop")
}

func TestMultiplexRuntimeConsensusPoolHandshake(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := mxruntime.NewResourceManager(t.Context(), logger)

	// Uses a RANDOM ChainID, only fingerprint is deterministic.
	withChainID := helpers.MakeChainID("ConsensusPool#Handshake")
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		baseCfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	// PREPARE: prepare and store a mutated state machine.
	stateStore := testConsensusPool.Composer().StateStore(withChainID)
	initialState := testConsensusPool.Composer().StateMachine(withChainID)
	mutatedState := initialState
	mutatedState.Version.Consensus.App = initialState.Version.Consensus.App + 1

	stateErr := stateStore.Save(mutatedState)
	require.NoError(t, stateErr,
		fmt.Sprintf("failed to save mutated state: %v", stateErr))

	resErr := resourceMgr.Set(withChainID, types.InstanceKeyStateMachine, initialState)
	require.NoError(t, resErr,
		fmt.Sprintf("unexpected error seeding state machine in resource manager: %v", resErr))

	// TEST 1: Execute an ABCI handshake by comparing the Consensus AppVersion
	// and the ABCI returned AppVersion; since we mutated the default (initial)
	// state machine, the returned AppVersion should be the one from mutatedState.
	handshakeErr := testConsensusPool.Handshake(withChainID)
	assert.NoError(t, handshakeErr,
		fmt.Sprintf("unexpected error executing handshake with ABCI: %v", handshakeErr))

	storedState := resourceMgr.Get(withChainID, types.InstanceKeyStateMachine)
	require.NotNil(t, storedState,
		"expected state machine to be stored after handshake")

	stateAfter, ok := storedState.(sm.State)
	require.Equal(t, true, ok,
		fmt.Sprintf("expected stored state to be sm.State, got %T", storedState))

	expectedAppVersion := mutatedState.Version.Consensus.App
	actualAppVersion := stateAfter.Version.Consensus.App

	assert.Equal(t, expectedAppVersion, actualAppVersion,
		fmt.Sprintf("expected mutated app version %d, got %d",
			expectedAppVersion, actualAppVersion))
}

func TestMultiplexRuntimeConsensusPoolInject(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := mxruntime.NewResourceManager(t.Context(), logger)

	// Uses a RANDOM ChainID, only fingerprint is deterministic.
	withChainID := helpers.MakeChainID("ConsensusPool#Inject")
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		baseCfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	// TEST 1:
	// Inject should setup the required reactors, notably mempool, blocksync,
	// consensus and evidence.
	injectErr := testConsensusPool.Inject(withChainID)
	require.NoError(t, injectErr,
		fmt.Sprintf("unexpected error injecting ChainID in consensus pool: %v", injectErr))

	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyAddressesReactor),
		"should inject address book and PEX reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyMempoolReactor),
		"should inject mempool reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyEvidenceReactor),
		"should inject evidence reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyBlockSyncReactor),
		"should inject blocksync reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyConsensusReactor),
		"should inject consensus reactor")

	actualPexReactor := resourceMgr.Get(withChainID, types.ServiceKeyAddressesReactor)
	assert.NotNil(t, actualPexReactor)

	actualMempoolReactor := resourceMgr.Get(withChainID, types.ServiceKeyMempoolReactor)
	assert.NotNil(t, actualMempoolReactor)

	actualEvidenceReactor := resourceMgr.Get(withChainID, types.ServiceKeyEvidenceReactor)
	assert.NotNil(t, actualEvidenceReactor)

	actualBlocksyncReactor := resourceMgr.Get(withChainID, types.ServiceKeyBlockSyncReactor)
	assert.NotNil(t, actualBlocksyncReactor)

	actualConsensusReactor := resourceMgr.Get(withChainID, types.ServiceKeyConsensusReactor)
	assert.NotNil(t, actualConsensusReactor)
}

func TestMultiplexRuntimeConsensusPoolInjectThenComposerBuild(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := mxruntime.NewResourceManager(t.Context(), logger)

	// Uses a RANDOM ChainID, only fingerprint is deterministic.
	withChainID := helpers.MakeChainID("ConsensusPool#InjectThenBuild")
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		baseCfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	// TEST 1:
	// Inject should setup the required reactors, notably mempool, blocksync,
	// consensus and evidence.
	injectErr := testConsensusPool.Inject(withChainID)
	require.NoError(t, injectErr,
		fmt.Sprintf("unexpected error injecting ChainID in consensus pool: %v", injectErr))

	// ...and runtimeComposer.Build() should package all services in a [node.Node].
	chainConns := testConsensusPool.ABCI()
	buildErr := testConsensusPool.Composer().Build(withChainID, chainConns)
	require.NoError(t, buildErr,
		fmt.Sprintf("unexpected error building chainID runtime in composer: %v", buildErr))

	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyAddressesReactor),
		"should inject address book and PEX reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyMempoolReactor),
		"should inject mempool reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyEvidenceReactor),
		"should inject evidence reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyBlockSyncReactor),
		"should inject blocksync reactor")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyConsensusReactor),
		"should inject consensus reactor")

	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyNodeRuntime),
		"should inject node.Node runtime service")

	actualNodeRuntime := resourceMgr.Get(withChainID, types.ServiceKeyNodeRuntime)
	assert.NotNil(t, actualNodeRuntime)
}

func TestMultiplexRuntimeConsensusPoolShutdown(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := mxruntime.NewResourceManager(t.Context(), logger)

	// Uses a RANDOM ChainID, only fingerprint is deterministic.
	withChainID := helpers.MakeChainID("ConsensusPool#InjectThenBuild")
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		baseCfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	injectErr := testConsensusPool.Inject(withChainID)
	require.NoError(t, injectErr,
		fmt.Sprintf("unexpected error injecting ChainID in consensus pool: %v", injectErr))

	// ...and runtimeComposer.Build() should package all services in a [node.Node].
	chainConns := testConsensusPool.ABCI()
	buildErr := testConsensusPool.Composer().Build(withChainID, chainConns)
	require.NoError(t, buildErr,
		fmt.Sprintf("unexpected error building chainID runtime in composer: %v", buildErr))

	// force services to run, without Execute.
	actualMempoolReactor := resourceMgr.Get(withChainID, types.ServiceKeyMempoolReactor)
	require.NotNil(t, actualMempoolReactor)
	actualEvidenceReactor := resourceMgr.Get(withChainID, types.ServiceKeyEvidenceReactor)
	require.NotNil(t, actualEvidenceReactor)
	actualBlocksyncReactor := resourceMgr.Get(withChainID, types.ServiceKeyBlockSyncReactor)
	require.NotNil(t, actualBlocksyncReactor)
	actualConsensusReactor := resourceMgr.Get(withChainID, types.ServiceKeyConsensusReactor)
	require.NotNil(t, actualConsensusReactor)
	actualNodeRuntime := resourceMgr.Get(withChainID, types.ServiceKeyNodeRuntime)
	require.NotNil(t, actualNodeRuntime)

	actualMempoolReactor.(*mempl.Reactor).Start()
	actualEvidenceReactor.(*evidence.Reactor).Start()
	actualBlocksyncReactor.(*bc.Reactor).Start()
	actualConsensusReactor.(*cs.Reactor).Start()
	actualNodeRuntime.(*cmtnode.Node).Start()
	defer func() {
		if actualMempoolReactor.(*mempl.Reactor).IsRunning() {
			actualMempoolReactor.(*mempl.Reactor).Stop()
		}
		if actualEvidenceReactor.(*evidence.Reactor).IsRunning() {
			actualEvidenceReactor.(*evidence.Reactor).Stop()
		}
		if actualBlocksyncReactor.(*bc.Reactor).IsRunning() {
			actualBlocksyncReactor.(*bc.Reactor).Stop()
		}
		if actualConsensusReactor.(*cs.Reactor).IsRunning() {
			actualConsensusReactor.(*cs.Reactor).Stop()
		}
		if actualNodeRuntime.(*cmtnode.Node).IsRunning() {
			actualNodeRuntime.(*cmtnode.Node).Stop()
		}
	}()

	// TEST 1:
	// Shutdown a consensus instance, should call node.Node#Stop, and should
	// call the services Stop method (mempool reactor, etc).
	shutdownErr := testConsensusPool.Shutdown(withChainID)
	require.NoError(t, shutdownErr,
		fmt.Sprintf("unexpected error stopping runtime for ChainID in consensus pool: %v", shutdownErr))

	assert.Equal(t, false, actualMempoolReactor.(*mempl.Reactor).IsRunning())
	assert.Equal(t, false, actualEvidenceReactor.(*evidence.Reactor).IsRunning())
	assert.Equal(t, false, actualBlocksyncReactor.(*bc.Reactor).IsRunning())
	assert.Equal(t, false, actualConsensusReactor.(*cs.Reactor).IsRunning())
	assert.Equal(t, false, actualNodeRuntime.(*cmtnode.Node).IsRunning())
}

func TestMultiplexRuntimeConsensusPoolExecute(t *testing.T) {
	defer goleak.VerifyNone(t)

	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := mxruntime.NewResourceManager(t.Context(), logger)

	// Uses a RANDOM ChainID, only fingerprint is deterministic.
	withChainID := helpers.MakeChainID("ConsensusPool#InjectThenBuild")
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		baseCfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	injectErr := testConsensusPool.Inject(withChainID)
	require.NoError(t, injectErr,
		fmt.Sprintf("unexpected error injecting ChainID in consensus pool: %v", injectErr))

	// ...and runtimeComposer.Build() should package all services in a [node.Node].
	chainConns := testConsensusPool.ABCI()
	buildErr := testConsensusPool.Composer().Build(withChainID, chainConns)
	require.NoError(t, buildErr,
		fmt.Sprintf("unexpected error building chainID runtime in composer: %v", buildErr))

	// TEST 1:
	// Start a consensus instance, should call node.Node#Start, and should
	// call the services Start method (mempool reactor, etc).
	executeErr := testConsensusPool.Execute(withChainID)
	require.NoError(t, executeErr,
		fmt.Sprintf("unexpected error starting runtime for ChainID in consensus pool: %v", executeErr))
	defer testConsensusPool.Shutdown(withChainID)

	actualNodeRuntime := resourceMgr.Get(withChainID, types.ServiceKeyNodeRuntime).(*cmtnode.Node)
	assert.NotNil(t, actualNodeRuntime)
	assert.Equal(t, true, actualNodeRuntime.IsRunning())
	assert.Equal(t, false, actualNodeRuntime.IsStopped())

	actualMempoolReactor := resourceMgr.Get(withChainID, types.ServiceKeyMempoolReactor)
	assert.NotNil(t, actualMempoolReactor)
	actualEvidenceReactor := resourceMgr.Get(withChainID, types.ServiceKeyEvidenceReactor)
	assert.NotNil(t, actualEvidenceReactor)
	actualBlocksyncReactor := resourceMgr.Get(withChainID, types.ServiceKeyBlockSyncReactor)
	assert.NotNil(t, actualBlocksyncReactor)
	actualConsensusReactor := resourceMgr.Get(withChainID, types.ServiceKeyConsensusReactor)
	assert.NotNil(t, actualConsensusReactor)

	assert.Equal(t, true, actualMempoolReactor.(*mempl.Reactor).IsRunning())
	assert.Equal(t, true, actualEvidenceReactor.(*evidence.Reactor).IsRunning())
	assert.Equal(t, true, actualBlocksyncReactor.(*bc.Reactor).IsRunning())
	assert.Equal(t, true, actualConsensusReactor.(*cs.Reactor).IsRunning())
}

// ----------------------------------------------------------------------------

// CAUTION: This helper injects a connection pool mock, a pre-configured
// runtimeComposer instance, and local ABCI clients for injectedChainIds.
// The [ConsensusPool#Stop] method is part of the returned shutdownFn.
func ResetTestMultiplexRuntimeConsensusPool(
	tb testing.TB,
	baseCfg *config.Config,
	resourceMgr *mxruntime.ResourceRegistry,
	customLogger cmtlog.Logger,
	withComposer *mxruntime.RuntimeComposer,
	injectedChainIds ...string,
) (testConsensusPool *mxruntime.ConsensusPool, shutdownFn func()) {
	tb.Helper()

	// creates a valid cmtp2p.NodeKey, required for Composer.Build().
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(baseCfg.RootDir, "node_key.json"))
	require.NotNil(tb, nodeKey)
	require.NoError(tb, keyErr)

	// create a mock types.ConnectionManager.
	mockConnPool := &mockConnectionPool{
		nodeKey:  nodeKey,
		nodeInfo: &p2p.MultiNetworkNodeInfo{DefaultNodeID: nodeKey.ID()},
	}
	// create a cmtp2p.Switch with nil-cmtp2p.Pool.
	eventSwitch := cmtp2p.NewSwitch(tb.Context(), baseCfg.P2P, mockConnPool)

	composer := withComposer
	if composer == nil {
		composer = mxruntime.NewComposer(tb.Context(),
			baseCfg,
			mockConnPool,
			resourceMgr,
			customLogger,
		)
		composer.SetSwitch(eventSwitch)
	}

	// create local multi-ChainID ABCI connector.
	chainConns := newMockChainConns()

	composerErr := composer.Start()
	require.NoError(tb, composerErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", composerErr))

	abciShutdownFns := make([]func(), 0, len(injectedChainIds))
	for i, chainID := range injectedChainIds {
		compositionErr := composer.Compose(chainID, []string{}, true)
		require.NoError(tb, compositionErr,
			fmt.Sprintf("unexpected error composing chainID in composer: %v", compositionErr))

		injectionErr := composer.Inject(chainID)
		require.NoError(tb, injectionErr,
			fmt.Sprintf("unexpected error injecting chainID in composer: %v", injectionErr))

		chainAppConns,
			abciShutdownFn := createLocalABCIClient(tb,
			tb.Name()+strconv.Itoa(i),
			chainID,
			baseCfg,
			customLogger,
		)

		abciShutdownFns = append(abciShutdownFns, abciShutdownFn)
		chainConns.set(chainID, chainAppConns)
	}

	testConsensusPool = mxruntime.NewConsensusHandler(tb.Context(),
		nodeKey,
		chainConns,
		resourceMgr,
		composer,
		customLogger,
	)
	testConsensusPool.SetSwitch(eventSwitch)

	startErr := testConsensusPool.Start()
	require.NoError(tb, startErr)

	shutdownFn = func() {
		testConsensusPool.Stop()
		composer.Stop()

		for _, abciShutdownFn := range abciShutdownFns {
			abciShutdownFn()
		}
	}

	return // testConsensusPool, shutdownFn
}

// ----------------------------------------------------------------------------

func createLocalABCIClient(
	tb testing.TB,
	dbName string,
	chainID string,
	baseCfg *config.Config,
	customLogger cmtlog.Logger,
) (proxy.AppConns, func()) {
	tb.Helper()

	databaseService := helpers.NewDBService(tb.Context(),
		dbName,
		baseCfg.DBDir(),
		baseCfg.DBBackend,
		cmtlog.NewNopLogger(),
	)
	dbStartErr := databaseService.Start()
	require.NoError(tb, dbStartErr,
		fmt.Sprintf("unexpected error starting database service: %v", dbStartErr))

	app := kvstore.NewApplication(databaseService.DB())
	appCreator := proxy.NewLocalClientCreator(app)
	appConns := proxy.NewAppConns(tb.Context(), appCreator, proxy.PrometheusMetrics(tb.Name()))
	appConns.SetLogger(customLogger)

	connStartErr := appConns.Start()
	require.NoError(tb, connStartErr,
		fmt.Sprintf("unexpected error starting ABCI service: %v", connStartErr))

	return appConns, func() {
		connStopErr := appConns.Stop()
		assert.NoError(tb, connStopErr,
			fmt.Sprintf("unexpected error stopping ABCI service: %v", connStopErr))

		stopDbErr := databaseService.Stop()
		assert.NoError(tb, stopDbErr,
			fmt.Sprintf("unexpected error stopping database service: %v", stopDbErr))
	}
}

// ----------------------------------------------------------------------------

func consensusPoolComposer(t *testing.T, pool *mxruntime.ConsensusPool) types.RuntimeComposer {
	t.Helper()

	field := reflect.ValueOf(pool).Elem().FieldByName("runtimeComposer")
	if !field.IsValid() {
		t.Fatal("runtimeComposer field not found")
	}
	if field.IsNil() {
		return nil
	}

	value := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
	return value.(types.RuntimeComposer)
}

func consensusPoolAcceptor(t *testing.T, pool *mxruntime.ConsensusPool) client.Acceptor {
	t.Helper()

	field := reflect.ValueOf(pool).Elem().FieldByName("acceptorImpl")
	if !field.IsValid() {
		t.Fatal("acceptorImpl field not found")
	}
	if field.IsNil() {
		return nil
	}

	value := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
	if value == nil {
		return nil
	}

	return value.(client.Acceptor)
}

func consensusPoolNodeKey(t *testing.T, pool *mxruntime.ConsensusPool) *cmtp2p.NodeKey {
	t.Helper()

	field := reflect.ValueOf(pool).Elem().FieldByName("nodeKey")
	if !field.IsValid() {
		t.Fatal("nodeKey field not found")
	}
	if field.IsNil() {
		return nil
	}

	value := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
	return value.(*cmtp2p.NodeKey)
}

// ----------------------------------------------------------------------------

type mockChainConns struct {
	service.BaseService

	mu    sync.RWMutex
	conns map[string]proxy.AppConns
}

// Ensure that our implementation satisfies interface.
var _ proxy.ChainConns = (*mockChainConns)(nil)

func newMockChainConns() *mockChainConns {
	return &mockChainConns{
		conns: make(map[string]proxy.AppConns),
	}
}

func (m *mockChainConns) set(chainID string, conns proxy.AppConns) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.conns[chainID] = conns
}

func (m *mockChainConns) get(chainID string) proxy.AppConns {
	m.mu.RLock()
	defer m.mu.RUnlock()

	return m.conns[chainID]
}

func (m *mockChainConns) AddNetwork(chainID string) {}

func (m *mockChainConns) ToAppConns(chainID string) proxy.AppConns {
	return m.get(chainID)
}

func (m *mockChainConns) Consensus(chainID string) proxy.AppConnConsensus {
	if conns := m.get(chainID); conns != nil {
		return conns.Consensus()
	}
	return nil
}

func (m *mockChainConns) Mempool(chainID string) proxy.AppConnMempool {
	if conns := m.get(chainID); conns != nil {
		return conns.Mempool()
	}
	return nil
}

func (m *mockChainConns) Query(chainID string) proxy.AppConnQuery {
	if conns := m.get(chainID); conns != nil {
		return conns.Query()
	}
	return nil
}

func (m *mockChainConns) Snapshot(chainID string) proxy.AppConnSnapshot {
	if conns := m.get(chainID); conns != nil {
		return conns.Snapshot()
	}
	return nil
}
