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
	transport *cmtp2p.MultiplexTransport, // nil-able
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
	if transport == nil {
		transport = cmtp2p.NewMultiplexTransport(tb.Context(), nodeInfo, *nodeKey)
		require.NotNil(tb, transport)
	}

	// CAUTION: starts listening on listenPort with node ID.
	// We must do this here because in the e2e scenario, this is done by the
	// methods StartP2PServerDiscovery and StartP2PServerCometBFT in MultiplexBackend
	// which are executed on Init(), before the connection pool is started.
	listenErr := transport.Listen(*netListenAddr)
	require.NoError(tb, listenErr)
	transportShutdownFn := func() {
		transport.Close()
	}

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
		defer transportShutdownFn()
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

	// CAUTION: a connection pool is mandatory for the connector.
	// Note that this also starts the Listen() method of MultiplexTransport.
	testPool, poolShutdownFn := ResetTestMultiplexConnectionPool(tb,
		listenPort,
		nodeInfo,
		nodeKey,
		transport,
		customLogger,
	)
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
	}
}

// ----------------------------------------------------------------------------

func createPeerConnectors(
	tb testing.TB,
	numPeers int,
	startPort int,
	customLogger cmtlog.Logger,
	withOptions ...p2p.ConnectorOption,
) (
	connectors []*p2p.PeerConnector,
	addresses []*cmtp2p.NetAddress,
	shutdownFns []func(),
) {
	tb.Helper()

	connectors = make([]*p2p.PeerConnector, numPeers)
	addresses = make([]*cmtp2p.NetAddress, numPeers)
	shutdownFns = make([]func(), numPeers)

	// create and start PeerConnector instances
	for i := 0; i < numPeers; i++ {
		testConnector, connShutdownFn := ResetTestMultiplexPeerConnector(tb,
			uint16(1000*i+startPort), // e.g. 1000 * 2 + 30001
			customLogger.With("process", "peer-"+strconv.Itoa(i+1)),
			withOptions...,
		)
		require.NotNil(tb, testConnector)
		require.NotNil(tb, connShutdownFn)

		shouldStartErr := testConnector.Start()
		require.NoError(tb, shouldStartErr, fmt.Sprintf("peer at %d should start", i))

		// give both some time to start listening correctly.
		time.Sleep(300 * time.Millisecond)
		require.Equal(tb, true, testConnector.Transport().IsListening())

		// create and store addresses for return.
		peerNodeID := testConnector.Transport().NodeInfo().ID()
		peerUsePort := startPort + (i * 1000) // see startPort
		peerAddress, addrErr := cmtp2p.NewNetAddressString(
			"tcp://" + string(peerNodeID) + "@127.0.0.1:" + strconv.Itoa(peerUsePort),
		)
		require.NoError(tb, addrErr)
		require.NotNil(tb, peerAddress)

		connectors[i] = testConnector
		addresses[i] = peerAddress
		shutdownFns[i] = func() {
			defer connShutdownFn()
			defer testConnector.Stop()

			// ... and we must correctly cleanup afterwards (disconnect from all).
			connPool := testConnector.Pool()
			cPeerSet := connPool.Peers().Copy()
			for i := 0; i < len(cPeerSet); i++ {
				connPool.RemovePeer(cPeerSet[i].ID())
			}
		}
	}

	return // connectors, addresses, shutdownFns
}

func createConnectionPoolWithOtherPeers(
	tb testing.TB,
	numDialedPeers int, // total=1+numDialedPeers
	startPort int,
	customLogger cmtlog.Logger,
	withPoolOptions []p2p.ConnectionPoolOption,
	withConnOptions []p2p.ConnectorOption,
) (
	*p2p.ConnectionPool,
	[]*p2p.PeerConnector,
	[]*cmtp2p.NetAddress,
	func(),
) {
	tb.Helper()

	// CAUTION: we use peer-0 to test ConnectionPool, and other peers
	// are created with the createPeerConnectors helper from PeerConnector.
	testPool1, poolShutdownFn := ResetTestMultiplexConnectionPool(tb,
		uint16(startPort),
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		customLogger.With("process", "peer-0"),
		withPoolOptions...,
	)
	require.NotNil(tb, testPool1)
	require.NotNil(tb, poolShutdownFn)

	shouldNotErrStart := testPool1.Start()
	require.NoError(tb, shouldNotErrStart)

	// Creates numDialedPeers additional peer connectors that we can dial.
	remoteStartPort := startPort + 1000
	testConnectors,
		peerAddresses,
		shutdownFns := createPeerConnectors(tb,
		numDialedPeers,
		remoteStartPort,
		customLogger,
		withConnOptions...,
	)
	require.NotEmpty(tb, testConnectors)
	require.NotEmpty(tb, shutdownFns)

	return testPool1, testConnectors, peerAddresses, func() {
		defer poolShutdownFn()
		defer testPool1.Stop()

		// ... and shutdown other peers.
		defer func() {
			for _, shutdownFn := range shutdownFns {
				shutdownFn()
			}
		}()
	}
}
