package runtime_test

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"reflect"
	"testing"
	"time"
	"unsafe"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	mxruntime "github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeComposerNewComposer(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	baseCfg := config.DefaultConfig()
	baseCfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	// creates a valid cmtp2p.NodeKey, required for Composer.Build().
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(baseCfg.RootDir, "node_key.json"))
	require.NotNil(t, nodeKey)
	require.NoError(t, keyErr)

	// create a mock types.ConnectionManager.
	mockConnPool := &mockConnectionPool{
		nodeKey:  nodeKey,
		nodeInfo: &p2p.MultiNetworkNodeInfo{DefaultNodeID: nodeKey.ID()},
	}

	// TEST 1: Constructor with nil-ConnectionPool and simple parameters.
	composer := mxruntime.NewComposer(ctx, baseCfg, nil, resourceMgr, logger)
	require.NotNil(t, composer, "expected non-nil composer instance")
	assert.Equal(t, logger, composer.Logger())
	assert.Nil(t, composerConnectionPool(t, composer))

	// TEST 2: Constructor with ConnectionPool and added optional parameters.
	customLogger := cmtlog.NewNopLogger()
	composerWithOpts := mxruntime.NewComposer(ctx,
		baseCfg,
		mockConnPool,
		resourceMgr,
		logger,
		mxruntime.ComposerWithLogger(customLogger),
	)
	require.NotNil(t, composerWithOpts, "expected non-nil composer instance with options")
	assert.Equal(t, customLogger, composerWithOpts.Logger(),
		"expected custom logger option to override default logger")
}

func TestMultiplexRuntimeComposerStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	// CAUTION: also sets a &mockConnectionPool.
	// CAUTION: we drop the shutdownFn to test it manually.
	testComposer, _ := ResetTestMultiplexRuntimeComposer(t,
		cfg,
		resourceMgr,
		logger,
	)
	defer func() {
		if testComposer.IsRunning() {
			testComposer.Stop()
		}
	}()

	require.NotNil(t, testComposer, "expected non-nil composer instance")
	assert.Equal(t, logger, testComposer.Logger())
	assert.NotNil(t, composerConnectionPool(t, testComposer))

	// TEST 1: Should not error starting the composer.
	startErr := testComposer.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", startErr))

	require.Equal(t, true, testComposer.IsRunning())

	// TEST 2: Should not error stopping the composer.
	stopErr := testComposer.Stop()
	require.NoError(t, stopErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", stopErr))
}

func TestMultiplexRuntimeComposerCompose(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	// CAUTION: also sets a &mockConnectionPool.
	// CAUTION: we drop the shutdownFn to test it manually.
	testComposer, shutdownFn := ResetTestMultiplexRuntimeComposer(t,
		cfg,
		resourceMgr,
		logger,
	)

	require.NotNil(t, testComposer, "expected non-nil composer instance")
	require.NotNil(t, shutdownFn)

	startErr := testComposer.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", startErr))
	defer shutdownFn()

	// TEST 1: Should orchestrate the runtime for a ChainID.
	withChainID := helpers.MakeChainID("RuntimeComposer#Compose")
	composeErr := testComposer.Compose(withChainID, []string{}, true)
	assert.NoError(t, composeErr)

	// ... filesystem / configuration
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyPathData),
		"should inject filesystem data path")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyPathConf),
		"should inject filesystem data path")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyConfig),
		"should inject runtime configuration")
	// ... privValidator
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyPrivValidator),
		"should inject a priv validator instance")
	// ... genesisDoc
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyGenesisDoc),
		"should inject a genesis docset instance")

	actualPrivValidator := resourceMgr.Get(withChainID, types.InstanceKeyPrivValidator)
	require.NotNil(t, actualPrivValidator)
}

func TestMultiplexRuntimeComposerUnload(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	// CAUTION: also sets a &mockConnectionPool.
	// CAUTION: we drop the shutdownFn to test it manually.
	testComposer, shutdownFn := ResetTestMultiplexRuntimeComposer(t,
		cfg,
		resourceMgr,
		logger,
	)

	require.NotNil(t, testComposer, "expected non-nil composer instance")
	require.NotNil(t, shutdownFn)

	startErr := testComposer.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", startErr))
	defer shutdownFn()

	withChainID := helpers.MakeChainID("RuntimeComposer#Unload")
	composeErr := testComposer.Compose(withChainID, []string{}, true)
	require.NoError(t, composeErr)
	injectErr := testComposer.Inject(withChainID)
	require.NoError(t, injectErr)

	actualStateDatabase := resourceMgr.Get(withChainID, types.ServiceKeyDatabaseState)
	require.NotNil(t, actualStateDatabase)
	actualStateDBService, ok := actualStateDatabase.(*helpers.DBService)
	require.Equal(t, true, ok)
	assert.Equal(t, true, actualStateDBService.IsRunning())

	// TEST 1: Should shutdown the databases (state machine) for ChainID
	unloadErr := testComposer.Unload(withChainID)
	assert.NoError(t, unloadErr)

	time.Sleep(300 * time.Millisecond)

	// State database should have been closed!
	assert.Equal(t, false, actualStateDBService.IsRunning())
}

func TestMultiplexRuntimeComposerInject(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	// CAUTION: also sets a &mockConnectionPool.
	// CAUTION: we drop the shutdownFn to test it manually.
	testComposer, shutdownFn := ResetTestMultiplexRuntimeComposer(t,
		cfg,
		resourceMgr,
		logger,
	)

	require.NotNil(t, testComposer, "expected non-nil composer instance")
	require.NotNil(t, shutdownFn)

	startErr := testComposer.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", startErr))
	defer shutdownFn()

	withChainID := helpers.MakeChainID("RuntimeComposer#Inject")
	composeErr := testComposer.Compose(withChainID, []string{}, true)
	require.NoError(t, composeErr)

	// TEST 1: Should orchestrate a state machine and block store for ChainID.
	injectErr := testComposer.Inject(withChainID)
	require.NoError(t, injectErr)
	defer testComposer.Unload(withChainID)

	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyStateMachine),
		"should inject a state machine instance")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.InstanceKeyBlockStore),
		"should inject a blocks store instance")
	require.Equal(t, true, resourceMgr.Has(withChainID, types.ServiceKeyDatabaseState),
		"should inject a state database")
}

func TestMultiplexRuntimeComposerBuild(t *testing.T) {
	defer goleak.VerifyNone(t)

	ctx := context.Background()
	logger := cmtlog.NewNopLogger()

	cfg := config.DefaultConfig()
	cfg.RootDir = t.TempDir()

	resourceMgr := mxruntime.NewResourceManager(ctx, logger)

	withChainID := helpers.MakeChainID("RuntimeComposer#Build")
	chainConns := newMockChainConns()
	chainAppConn, abciShutdownFn := createLocalABCIClient(t,
		t.Name(),
		withChainID,
		cfg,
		logger,
	)
	chainConns.set(withChainID, chainAppConn)
	defer abciShutdownFn()

	// CAUTION: also sets a &mockConnectionPool.
	// CAUTION: we drop the shutdownFn to test it manually.
	testComposer, shutdownFn := ResetTestMultiplexRuntimeComposer(t,
		cfg,
		resourceMgr,
		logger,
	)

	require.NotNil(t, testComposer, "expected non-nil composer instance")
	require.NotNil(t, shutdownFn)

	startErr := testComposer.Start()
	require.NoError(t, startErr,
		fmt.Sprintf("unexpected error starting runtime composer: %v", startErr))
	defer shutdownFn()

	composeErr := testComposer.Compose(withChainID, []string{}, true)
	require.NoError(t, composeErr)

	injectErr := testComposer.Inject(withChainID)
	require.NoError(t, injectErr)
	defer testComposer.Unload(withChainID)

	// CAUTION: we also need a ConsensusPool for mempool, blocksync, etc.
	testConsensusPool,
		poolShutdownFn := ResetTestMultiplexRuntimeConsensusPool(t,
		cfg,
		resourceMgr,
		logger,
		nil, // nil-Composer (auto-create)
		withChainID,
	)
	defer poolShutdownFn()

	consensusErr := testConsensusPool.Inject(withChainID)
	require.NoError(t, consensusErr,
		fmt.Sprintf("unexpected error injecting ChainID in consensus pool: %v", consensusErr))

	// TEST 1: Should configure a node.Node instance.
	buildErr := testComposer.Build(withChainID, chainConns)
	require.NoError(t, buildErr,
		fmt.Sprintf("unexpected error building chainID runtime in composer: %v", buildErr))

	actualNodeRuntime := resourceMgr.Get(withChainID, types.ServiceKeyNodeRuntime)
	require.NotNil(t, actualNodeRuntime)

	actualNode, isType := actualNodeRuntime.(*cmtnode.Node)
	assert.Equal(t, true, isType)
	assert.NotNil(t, actualNode)
	assert.Equal(t, false, actualNode.IsRunning()) // NOT started.
}

// ----------------------------------------------------------------------------

func ResetTestMultiplexRuntimeComposer(
	tb testing.TB,
	baseCfg *config.Config,
	resourceMgr *mxruntime.ResourceRegistry,
	customLogger cmtlog.Logger,
) (testComposer *mxruntime.RuntimeComposer, shutdownFn func()) {
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

	testComposer = mxruntime.NewComposer(tb.Context(),
		baseCfg,
		mockConnPool,
		resourceMgr,
		customLogger,
	)
	testComposer.SetSwitch(eventSwitch)

	shutdownFn = func() {
		if testComposer.IsRunning() {
			testComposer.Stop()
		}
	}

	return // testComposer, shutdownFn
}

// ----------------------------------------------------------------------------

func composerConnectionPool(tb testing.TB, cpr types.RuntimeComposer) *mockConnectionPool {
	tb.Helper()

	field := reflect.ValueOf(cpr).Elem().FieldByName("connectionPool")
	if !field.IsValid() {
		tb.Fatal("connectionPool field not found")
	}
	if field.IsNil() {
		return nil
	}

	value := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
	return value.(*mockConnectionPool)
}

type mockConnectionPool struct {
	service.BaseService

	nodeInfo *p2p.MultiNetworkNodeInfo
	nodeKey  *cmtp2p.NodeKey
}

var _ types.ConnectionManager = (*mockConnectionPool)(nil)
var _ cmtp2p.Pool = (*mockConnectionPool)(nil)

func (m *mockConnectionPool) NodeKey() *cmtp2p.NodeKey  { return m.nodeKey }
func (m *mockConnectionPool) NodeInfo() cmtp2p.NodeInfo { return m.nodeInfo }
func (*mockConnectionPool) Transport() *cmtp2p.MultiplexTransport {
	return &cmtp2p.MultiplexTransport{}
}
func (*mockConnectionPool) Dispatcher() cmtp2p.Dispatcher { return &cmtp2p.MockDispatcherImpl{} }
func (*mockConnectionPool) Connector() cmtp2p.Connector   { return nil }
func (*mockConnectionPool) Handshaker() cmtp2p.Handshaker { return nil }

func (*mockConnectionPool) NumPeers(chainIds ...string) (inbound, outbound, dialing int) {
	return 0, 0, 0
}
func (*mockConnectionPool) Peers(chainIds ...string) *cmtp2p.PeerSet { return cmtp2p.NewPeerSet() }
func (*mockConnectionPool) AddPeer(peer *cmtp2p.PeerImpl) error      { return nil }
func (*mockConnectionPool) RemovePeer(peerID cmtp2p.ID) error        { return nil }
func (*mockConnectionPool) HasPeer(peer *cmtp2p.PeerImpl) bool       { return false }
func (*mockConnectionPool) HasPeerID(id cmtp2p.ID) bool              { return false }
func (*mockConnectionPool) HasPeerIP(ip net.IP) bool                 { return false }

func (*mockConnectionPool) Broadcast(e cmtp2p.Envelope) error    { return nil }
func (*mockConnectionPool) TryBroadcast(e cmtp2p.Envelope) error { return nil }

func (*mockConnectionPool) HasConnection(peerID cmtp2p.ID) bool                     { return false }
func (*mockConnectionPool) Connection(peerID cmtp2p.ID) *conn.MConnection           { return nil }
func (*mockConnectionPool) HasPeerForChainID(peerID cmtp2p.ID, chainID string) bool { return false }
func (*mockConnectionPool) SetPeerForChainID(peerID cmtp2p.ID, chainID string) int  { return 0 }
func (*mockConnectionPool) InitPeerForChainID(peerID cmtp2p.ID, chainID string) *cmtp2p.PeerImpl {
	return nil
}
func (*mockConnectionPool) AddPeerForChainID(peerID cmtp2p.ID, chainID string) bool { return false }
