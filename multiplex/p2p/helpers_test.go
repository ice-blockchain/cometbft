package p2p_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

func ResetTestMultiplexConnectionPool(
	tb testing.TB,
	customLogger cmtlog.Logger,
	withOptions ...p2p.ConnectionPoolOption,
) (*p2p.ConnectionPool, func()) {
	tb.Helper()

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	resourceMgr := runtime.NewResourceManager(tb.Context(), customLogger)
	require.NotNil(tb, resourceMgr)

	nodeInfo := p2p.NewMultiNetworkNodeInfo()
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(tmpRootDir, "node_key.json"))
	require.NotNil(tb, nodeKey)
	require.NoError(tb, keyErr)

	transport := cmtp2p.NewMultiplexTransport(tb.Context(), nodeInfo, *nodeKey)
	require.NotNil(tb, transport)

	// Test the pool constructor.
	testPool := p2p.NewConnectionManager(tb.Context(),
		nodeKey,
		transport,
		resourceMgr,
		customLogger,
	)
	assert.NotNil(tb, testPool)

	return testPool, func() {
		defer os.RemoveAll(tmpRootDir)
	}
}

func ResetTestMultiplexPeerConnector(
	tb testing.TB,
	customLogger cmtlog.Logger,
	withOptions ...p2p.ConnectorOption,
) (*p2p.PeerConnector, func()) {
	tb.Helper()

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	resourceMgr := runtime.NewResourceManager(tb.Context(), customLogger)
	require.NotNil(tb, resourceMgr)

	nodeInfo := p2p.NewMultiNetworkNodeInfo()
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(tmpRootDir, "node_key.json"))
	require.NotNil(tb, nodeKey)
	require.NoError(tb, keyErr)

	transport := cmtp2p.NewMultiplexTransport(tb.Context(), nodeInfo, *nodeKey)
	require.NotNil(tb, transport)

	// CAUTION: a connection pool is mandatory for the connector.
	testPool, poolShutdownFn := ResetTestMultiplexConnectionPool(tb, cmtlog.TestingLogger())
	require.NotNil(tb, testPool)
	require.NotNil(tb, poolShutdownFn)

	// CAUTION: overwrites the options to force a connection pool.
	withOptions = append(withOptions, p2p.ConnectorWithPool(testPool))

	// CAUTION: a multiplex Reactor is mandatory for the dispatcher.
	testMultiplexReactor := cmtp2p.NewBaseReactor(tb.Context(), "MULTIPLEX", nil)
	testDispatcher := testPool.Dispatcher()
	testDispatcher.SetMultiplexReactor(testMultiplexReactor)

	// Test the instance constructor.
	testConnector := p2p.NewConnector(tb.Context(),
		transport,
		testDispatcher,
		customLogger,
		withOptions...,
	)
	require.NotNil(tb, testConnector)

	return testConnector, func() {
		defer os.RemoveAll(tmpRootDir)
		defer poolShutdownFn()
	}
}
