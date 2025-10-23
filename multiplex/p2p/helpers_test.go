package p2p_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/e2e"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

func ResetTestMultiplexConnectionPool(
	tb testing.TB,
	listenPort uint16,
	nodeInfo *p2p.MultiNetworkNodeInfo, // nil-able
	nodeKey *cmtp2p.NodeKey, // nil-able
	customLogger cmtlog.Logger,
	withOptions ...p2p.ConnectionPoolOption,
) (*p2p.ConnectionPool, func()) {
	tb.Helper()

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	// creates config.Config
	testConf := e2e.MakeConfig(tb, tmpRootDir)
	testConf.DiscoveryPort = listenPort
	testConf.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", listenPort+1)

	// creates runtime.ResourceRegistry
	resourceMgr := runtime.NewResourceManager(tb.Context(), customLogger)
	require.NotNil(tb, resourceMgr)

	// creates or reuses cmtp2p.NodeKey
	if nodeKey == nil {
		var keyErr error
		nodeKey, keyErr = cmtp2p.LoadOrGenNodeKey(filepath.Join(tmpRootDir, "node_key.json"))
		require.NotNil(tb, nodeKey)
		require.NoError(tb, keyErr)
	}

	netListenAddr, addrErr := cmtp2p.NewNetAddressString(
		string(nodeKey.ID()) + "@0.0.0.0:" + strconv.Itoa(int(listenPort)),
	)
	require.NoError(tb, addrErr)

	// creates or reuses p2p.MultiNetworkNodeInfo
	if nodeInfo == nil {
		nodeInfo = p2p.NewMultiNetworkNodeInfoWithConfig(
			testConf,
			nodeKey,
			netListenAddr,
			[]byte{},
		)
	}

	// creates cmtp2p.MultiplexTransport
	transport := cmtp2p.NewMultiplexTransport(tb.Context(), nodeInfo, *nodeKey)
	require.NotNil(tb, transport)

	// creates p2p.ConnectionPool
	testPool := p2p.NewConnectionManager(tb.Context(),
		nodeKey,
		transport,
		resourceMgr,
		customLogger,
	)
	require.NotNil(tb, testPool)

	// CAUTION: a multiplex Reactor is mandatory for the dispatcher.
	testMultiplexReactor := cmtp2p.NewBaseReactor(tb.Context(), "MULTIPLEX", nil)
	testDispatcher := testPool.Dispatcher()
	testDispatcher.SetMultiplexReactor(testMultiplexReactor)

	return testPool, func() {
		defer os.RemoveAll(tmpRootDir)
	}
}

func ResetTestMultiplexPeerConnector(
	tb testing.TB,
	listenPort uint16,
	customLogger cmtlog.Logger,
	withOptions ...p2p.ConnectorOption,
) (*p2p.PeerConnector, func()) {
	tb.Helper()

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	// creates config.Config
	testConf := e2e.MakeConfig(tb, tmpRootDir)
	testConf.DiscoveryPort = listenPort
	testConf.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", listenPort+1)

	// creates runtime.ResourceRegistry
	resourceMgr := runtime.NewResourceManager(tb.Context(), customLogger)
	require.NotNil(tb, resourceMgr)

	// creates cmtp2p.NodeKey
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(tmpRootDir, "node_key.json"))
	require.NotNil(tb, nodeKey)
	require.NoError(tb, keyErr)

	netListenAddr, addrErr := cmtp2p.NewNetAddressString(
		string(nodeKey.ID()) + "@0.0.0.0:" + strconv.Itoa(int(listenPort)),
	)
	require.NoError(tb, addrErr)

	// creates p2p.MultiNetworkNodeInfo
	nodeInfo := p2p.NewMultiNetworkNodeInfoWithConfig(testConf, nodeKey, netListenAddr, []byte{})

	// creates p2p.Hanshaker
	handshaker := p2p.NewHandshaker(tb.Context(), nodeInfo, customLogger)

	// creates cmtp2p.MultiplexTransport
	transport := cmtp2p.NewMultiplexTransportWithCustomHandshake(
		tb.Context(),
		nodeInfo,
		*nodeKey,
		func(c net.Conn, timeout time.Duration, ni cmtp2p.NodeInfo) (cmtp2p.NodeInfo, error) {
			return handshaker.Handshake(c, timeout)
		},
	)
	require.NotNil(tb, transport)
	transport.SetLogger(customLogger)

	// CAUTION: starts listening on listenPort with node ID.
	listenErr := transport.Listen(*netListenAddr)
	require.NoError(tb, listenErr)
	transportShutdownFn := func() {
		transport.Close()
	}

	// CAUTION: a connection pool is mandatory for the connector.
	testPool, poolShutdownFn := ResetTestMultiplexConnectionPool(tb, listenPort, nodeInfo, nodeKey, customLogger)
	require.NotNil(tb, testPool)
	require.NotNil(tb, poolShutdownFn)

	// CAUTION: overwrites the options to force a connection pool.
	withOptions = append(withOptions, p2p.ConnectorWithPool(testPool))

	// CAUTION: a multiplex Reactor is mandatory for the dispatcher.
	testMultiplexReactor := cmtp2p.NewBaseReactor(tb.Context(), "MULTIPLEX", nil)
	testDispatcher := testPool.Dispatcher()
	testDispatcher.SetMultiplexReactor(testMultiplexReactor)

	// creates p2p.PeerConnector
	testConnector := p2p.NewConnector(tb.Context(),
		transport,
		testDispatcher,
		customLogger,
		withOptions...,
	)
	require.NotNil(tb, testConnector)

	// CAUTION:
	// In tests we must overwrite the pool's connector because it creates
	// one internally that unit tests *don't use* to permit more flexibility
	// on testing the PeerConnector implementation.
	p2p.ConnectionPoolWithConnector(testConnector)(testPool)

	return testConnector, func() {
		defer os.RemoveAll(tmpRootDir)
		defer poolShutdownFn()
		defer transportShutdownFn()
	}
}
