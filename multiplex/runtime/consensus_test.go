package runtime_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/ice-blockchain/cometbft/abci/example/kvstore"
	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
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

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()
	baseCfg.DBBackend = string(dbm.MemDBBackend)

	resourceMgr := newMockResourceManager()

	composer := mxruntime.NewComposer(ctx, baseCfg, nil, resourceMgr, logger)
	composerErr := composer.Start()
	require.NoError(t, composerErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", composerErr))

	defer func() {
		stopErr := composer.Stop()
		assert.NoError(t, stopErr,
			fmt.Sprintf("unexpected error stopping runtime composer: %v", stopErr))
	}()

	// TEST 1:
	// Compose and inject a ChainID with composer, then setup a local ABCI
	// client using `proxy.AppConns` and a `mockChainConns` in which we inject
	// a test ChainID as well.
	// Then save a mutated (testable) state machine.

	chainID := helpers.MakeChainID("Handshake")

	compositionErr := composer.Compose(chainID, nil, true)
	require.NoError(t, compositionErr,
		fmt.Sprintf("unexpected error composing chainID in composer: %v", compositionErr))

	injectionErr := composer.Inject(chainID)
	require.NoError(t, injectionErr,
		fmt.Sprintf("unexpected error injecting chainID in composer: %v", injectionErr))

	databaseService := helpers.NewDBService(t.Context(),
		"consensus-app-handshake",
		baseCfg.DBDir(),
		baseCfg.DBBackend,
		cmtlog.NewNopLogger(),
	)
	dbStartErr := databaseService.Start()
	require.NoError(t, dbStartErr,
		fmt.Sprintf("unexpected error starting database service: %v", dbStartErr))

	defer func() {
		stopDbErr := databaseService.Stop()
		assert.NoError(t, stopDbErr,
			fmt.Sprintf("unexected error stopping database service: %v", stopDbErr))
	}()

	app := kvstore.NewApplication(databaseService.DB())
	appCreator := proxy.NewLocalClientCreator(app)
	appConns := proxy.NewAppConns(t.Context(), appCreator, proxy.PrometheusMetrics("ConsensusHandshakeTest"))
	appConns.SetLogger(logger)

	connStartErr := appConns.Start()
	require.NoError(t, connStartErr,
		fmt.Sprintf("unexpected error starting ABCI service: %v", connStartErr))

	defer func() {
		connStopErr := appConns.Stop()
		assert.NoError(t, connStopErr,
			fmt.Sprintf("unexpected error stopping ABCI service: %v", connStopErr))
	}()

	// configure a chainConns with the ABCI connection
	chainConns := newMockChainConns()
	chainConns.set(chainID, appConns)

	stateStore := composer.StateStore(chainID)
	initialState := composer.StateMachine(chainID)
	mutatedState := initialState
	mutatedState.Version.Consensus.App = initialState.Version.Consensus.App + 1

	stateErr := stateStore.Save(mutatedState)
	require.NoError(t, stateErr,
		fmt.Sprintf("failed to save mutated state: %v", stateErr))

	resErr := resourceMgr.Set(chainID, types.InstanceKeyStateMachine, initialState)
	require.NoError(t, resErr,
		fmt.Sprintf("unexpected error seeding state machine in resource manager: %v", resErr))

	// TEST 2:
	// Execute a Consensus Handshake, i.e. a handshake that compares the
	// version of the app using the ABCI client and the state database.

	nodeKey := &cmtp2p.NodeKey{}
	pool := mxruntime.NewConsensusHandler(ctx, nodeKey, chainConns, resourceMgr, composer, logger)

	handshakeErr := pool.Handshake(chainID)
	assert.NoError(t, handshakeErr,
		fmt.Sprintf("unexpected error executing handshake with ABCI: %v", handshakeErr))

	storedState := resourceMgr.Get(chainID, types.InstanceKeyStateMachine)
	require.NotNil(t, storedState,
		"expected state machine to be stored after handshake")

	stateAfter, ok := storedState.(sm.State)
	require.Equal(t, true, ok,
		fmt.Sprintf("expected stored state to be sm.State, got %T", storedState))

	expectedAppVersion := mutatedState.Version.Consensus.App
	actualAppVersion := stateAfter.Version.Consensus.App

	assert.Equal(t, expectedAppVersion, actualAppVersion,
		fmt.Sprintf("expected app version %d, got %d",
			expectedAppVersion, actualAppVersion))
}

func TestMultiplexRuntimeConsensusPoolInject(t *testing.T) {

}

func TestMultiplexRuntimeConsensusPoolExecute(t *testing.T) {

}

func TestMultiplexRuntimeConsensusPoolShutdown(t *testing.T) {

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

// ----------------------------------------------------------------------------

type mockResourceManager struct {
	mu    sync.RWMutex
	store map[string]map[string]any
}

func newMockResourceManager() *mockResourceManager {
	return &mockResourceManager{
		store: make(map[string]map[string]any),
	}
}

func (m *mockResourceManager) Has(chainID, name string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if chainResources, ok := m.store[chainID]; ok {
		_, exists := chainResources[name]
		return exists
	}
	return false
}

func (m *mockResourceManager) Set(chainID, name string, resource any) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.store[chainID]; !ok {
		m.store[chainID] = make(map[string]any)
	}
	m.store[chainID][name] = resource
	return nil
}

func (m *mockResourceManager) Get(chainID, name string) any {
	m.mu.RLock()
	defer m.mu.RUnlock()

	if chainResources, ok := m.store[chainID]; ok {
		return chainResources[name]
	}
	return nil
}

func (m *mockResourceManager) Multiplex(name string) helpers.MultiplexMap[any] {
	m.mu.RLock()
	defer m.mu.RUnlock()

	result := make(helpers.MultiplexMap[any])
	for chainID, resources := range m.store {
		if val, ok := resources[name]; ok {
			result[chainID] = helpers.NewChainInstance(chainID, val)
		}
	}
	return result
}
