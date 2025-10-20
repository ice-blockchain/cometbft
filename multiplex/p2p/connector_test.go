package p2p_test

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cosmos/gogoproto/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	memp2p "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexP2PPeerConnectorNewConnector(t *testing.T) {
	defer goleak.VerifyNone(t)

	tmpRootDir, err := os.MkdirTemp("", t.Name()+"-1")
	require.NoError(t, err)
	defer os.RemoveAll(tmpRootDir)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	require.NotNil(t, resourceMgr)

	nodeInfo := p2p.NewMultiNetworkNodeInfo()
	nodeKey, keyErr := cmtp2p.LoadOrGenNodeKey(filepath.Join(tmpRootDir, "node_key.json"))
	require.NotNil(t, nodeKey)
	require.NoError(t, keyErr)

	transport := cmtp2p.NewMultiplexTransport(t.Context(), nodeInfo, *nodeKey)
	require.NotNil(t, transport)

	dispatcher := p2p.NewDispatcher(t.Context(), nodeInfo, resourceMgr, cmtlog.NewNopLogger())
	require.NotNil(t, dispatcher)

	// Test the instance constructor.
	testConnector := p2p.NewConnector(t.Context(),
		transport,
		dispatcher,
		cmtlog.NewNopLogger(),
	)
	assert.NotNil(t, testConnector)

	// Test the assignment of connector services.
	assert.NotNil(t, testConnector.Transport())
	assert.NotNil(t, testConnector.Dispatcher())
}

func TestMultiplexP2PPeerConnectorNewConnectorHelper(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnector, shutdownFn := ResetTestMultiplexPeerConnector(t, 30001, cmtlog.NewNopLogger())
	require.NotNil(t, testConnector)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	// Test the assignment of connector services.
	assert.NotNil(t, testConnector.Transport())
	assert.NotNil(t, testConnector.Dispatcher())
}

func TestMultiplexP2PPeerConnectorStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test that the connector errors if no connection pool is set.
	testConnector1, shutdownFn1 := ResetTestMultiplexPeerConnector(t,
		30001,
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testConnector1)
	require.NotNil(t, shutdownFn1)

	// CAUTION: Set nil pool errors on Start().
	p2p.ConnectorWithPool(nil)(testConnector1)

	shouldErr := testConnector1.Start()
	require.Error(t, shouldErr) // Must not start.
	// force-shutdown here to stop listening.
	shutdownFn1()

	// Test the instance start/stop implementation.
	testConnector2, shutdownFn2 := ResetTestMultiplexPeerConnector(t,
		30001,
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testConnector2)
	require.NotNil(t, shutdownFn2)
	defer shutdownFn2()

	shouldNotErr := testConnector2.Start()
	assert.NoError(t, shouldNotErr)
	defer testConnector2.Stop()

	assert.Equal(t, true, testConnector2.Transport().IsListening())
}

func TestMultiplexP2PPeerConnectorCreateHelper(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnectors, shutdownFns := createPeerConnectors(t, 3, 30001, cmtlog.NewNopLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	require.Len(t, testConnectors, 3)

	// TEST 1: peer-1 connects to peer-2
	testPeer1NodeID := testConnectors[0].Transport().NodeInfo().ID()
	testPeer2NodeID := testConnectors[1].Transport().NodeInfo().ID()
	testPeer3NodeID := testConnectors[2].Transport().NodeInfo().ID()

	testAddrConn2, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(testPeer2NodeID) + "@127.0.0.1:31001", // peer-2
	)
	assert.NoError(t, addrErr)
	require.NotNil(t, testAddrConn2)

	testPeerConn2, dialErr := testConnectors[0].Dial(testAddrConn2)
	assert.NoError(t, dialErr, "peer-1 should connect to peer-2")
	assert.NotNil(t, testPeerConn2)

	// TEST 2: peer-1 connects to peer-3
	testAddrConn3, addrErr3 := cmtp2p.NewNetAddressString(
		"tcp://" + string(testPeer3NodeID) + "@127.0.0.1:32001", // peer-3
	)
	assert.NoError(t, addrErr3)
	require.NotNil(t, testAddrConn3)

	testPeerConn3, dialErr3 := testConnectors[0].Dial(testAddrConn3)
	assert.NoError(t, dialErr3, "peer-1 should connect to peer-3")
	assert.NotNil(t, testPeerConn3)

	// TEST 3: peer-3 connects to peer-2
	testPeerConn32, dialErr32 := testConnectors[2].Dial(testAddrConn2)
	assert.NoError(t, dialErr32, "peer-3 should connect to peer-2")
	assert.NotNil(t, testPeerConn32)

	// ... and we must correctly cleanup afterwards.
	testConnectors[0].Pool().RemovePeer(testPeer2NodeID) // peer1 -> peer2
	testConnectors[0].Pool().RemovePeer(testPeer3NodeID) // peer1 -> peer3
	testConnectors[1].Pool().RemovePeer(testPeer1NodeID) // peer2 <- peer1
	testConnectors[1].Pool().RemovePeer(testPeer3NodeID) // peer2 <- peer3
	testConnectors[2].Pool().RemovePeer(testPeer1NodeID) // peer3 <- peer1
	testConnectors[2].Pool().RemovePeer(testPeer2NodeID) // peer3 -> peer2
}

func TestMultiplexP2PPeerConnectorDialErrors(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnector, shutdownFn := ResetTestMultiplexPeerConnector(t,
		30001,
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testConnector)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	shouldStartErr := testConnector.Start()
	require.NoError(t, shouldStartErr)
	defer testConnector.Stop()

	// give some time to start listening correctly.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, true, testConnector.Transport().IsListening())

	selfNodeId := testConnector.Transport().NodeInfo().ID()
	testAddress, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(selfNodeId) + "@127.0.0.1:30001",
	)
	assert.NoError(t, addrErr)
	require.NotNil(t, testAddress)

	// Test that we error about connecting to SELF.
	testPeer, dialErr := testConnector.Dial(testAddress)
	assert.Error(t, dialErr)
	assert.Contains(t, dialErr.Error(), "self ID<")
	assert.Nil(t, testPeer, "rejected dialing should not add peer")

	// Test that we also error about connecting to invalid relay.
	randPrivKey := ed25519.GenPrivKey()
	randNodeKey := &cmtp2p.NodeKey{PrivKey: randPrivKey}
	testAddress2, addrErr2 := cmtp2p.NewNetAddressString(
		"tcp://" + string(randNodeKey.ID()) + "@127.0.0.1:31234", // wrong port
	)
	assert.NoError(t, addrErr2)
	require.NotNil(t, testAddress2)

	testPeer2, dialErr2 := testConnector.Dial(testAddress2)
	assert.Error(t, dialErr2)
	assert.Contains(t, dialErr2.Error(), "connection refused")
	assert.Nil(t, testPeer2, "rejected dialing should not add peer")
}

func TestMultiplexP2PPeerConnectorDialSuccess(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnector1, shutdownFn1 := ResetTestMultiplexPeerConnector(t,
		30001,
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testConnector1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	shouldStartErr := testConnector1.Start()
	require.NoError(t, shouldStartErr, "first peer should start")
	defer testConnector1.Stop()

	testConnector2, shutdownFn2 := ResetTestMultiplexPeerConnector(t,
		40001,
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testConnector2)
	require.NotNil(t, shutdownFn2)
	defer shutdownFn2()

	shouldStartErr2 := testConnector2.Start()
	require.NoError(t, shouldStartErr2, "second peer should start")
	defer testConnector2.Stop()

	// give both some time to start listening correctly.
	time.Sleep(300 * time.Millisecond)
	require.Equal(t, true, testConnector1.Transport().IsListening())
	require.Equal(t, true, testConnector2.Transport().IsListening())

	// TEST 1: peer-1 connects to peer-2
	peer1NodeId := testConnector1.Transport().NodeInfo().ID()
	peer2NodeId := testConnector2.Transport().NodeInfo().ID()
	testAddress, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(peer2NodeId) + "@127.0.0.1:40001", // peer-2
	)
	assert.NoError(t, addrErr)
	require.NotNil(t, testAddress)

	// Test that we succeed in connecting to a valid relay.
	testPeer, dialErr := testConnector1.Dial(testAddress)
	assert.NoError(t, dialErr, "peer-1 should connect to peer-2")
	assert.NotNil(t, testPeer)

	// TEST 2: peer-2 connects to peer-1
	testAddress2, addrErr2 := cmtp2p.NewNetAddressString(
		"tcp://" + string(peer1NodeId) + "@127.0.0.1:30001", // peer-1
	)
	assert.NoError(t, addrErr2)
	require.NotNil(t, testAddress2)

	// Test that we succeed in connecting back to the peer-1 relay.
	testPeer2, dialErr2 := testConnector2.Dial(testAddress2)
	assert.NoError(t, dialErr2, "peer-2 should connect to peer-1")
	assert.NotNil(t, testPeer2)

	// ... and we must correctly cleanup afterwards.
	testConnector1.Pool().RemovePeer(peer2NodeId)
	testConnector2.Pool().RemovePeer(peer1NodeId)
}

// TODO(midas): add TestMultiplexP2PPeerConnectorDialConcurrent

func TestMultiplexP2PPeerConnectorSend(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnectors, shutdownFns := createPeerConnectors(t, 2, 30001, cmtlog.NewNopLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	require.Len(t, testConnectors, 2)

	testPeer1NodeID := testConnectors[0].Transport().NodeInfo().ID()
	testPeer2NodeID := testConnectors[1].Transport().NodeInfo().ID()

	// first we dial peer-2 from peer-1
	testAddrConn2, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(testPeer2NodeID) + "@127.0.0.1:31001", // peer-2
	)
	assert.NoError(t, addrErr)
	require.NotNil(t, testAddrConn2)

	testPeerConn2, dialErr := testConnectors[0].Dial(testAddrConn2)
	require.NoError(t, dialErr, "peer-1 should connect to peer-2")
	require.NotNil(t, testPeerConn2)

	testTransactions := make([][]byte, 1)
	testTransactions[0] = []byte{1, 2, 3}

	testTxMessage := cmtp2p.Envelope{
		ChainID:   "test-chain-1",
		ChannelID: mempl.MempoolChannel,
		Message:   &memp2p.Txs{Txs: testTransactions},
	}

	// TEST 1: sending a message to unknown peer must error.
	// Note that "self" is invalid for Send.
	shouldErr1 := testConnectors[0].Send(testPeer1NodeID, testTxMessage)
	assert.Error(t, shouldErr1, "sending to invalid peer ID must error")
	assert.Contains(t, shouldErr1.Error(), "missing MConnection")
	assert.Contains(t, shouldErr1.Error(), testPeer1NodeID)

	// TEST 2: sending an empty message must error.
	var emptyMsg proto.Message
	shouldErr2 := testConnectors[0].Send(testPeer2NodeID, cmtp2p.Envelope{
		Message: emptyMsg,
	})
	assert.Error(t, shouldErr2, "sending an empty message must error")
	assert.Contains(t, shouldErr2.Error(), "Message may not be empty")
	assert.Contains(t, shouldErr2.Error(), testPeer2NodeID)

	// TEST 3: sending an actual well-formed transaction must succeed.
	// peer-1 sends to previously dialed peer-2
	shouldNotErr := testConnectors[0].Send(testPeer2NodeID, testTxMessage)
	assert.NoError(t, shouldNotErr, "sending a valid message should not error")

	actualHasPeer := testConnectors[0].Pool().HasPeerForChainID(testPeer2NodeID, "test-chain-1")
	shouldNotFindSelf := testConnectors[0].Pool().HasPeerForChainID(testPeer1NodeID, "test-chain-1")
	assert.Equal(t, true, actualHasPeer)
	assert.Equal(t, false, shouldNotFindSelf)

	// ... and we must correctly cleanup afterwards.
	testConnectors[0].Pool().RemovePeer(testPeer2NodeID)
	testConnectors[1].Pool().RemovePeer(testPeer1NodeID)
}

func TestMultiplexP2PPeerConnectorSendConcurrent(t *testing.T) {
	defer goleak.VerifyNone(t)

	numPeers := 3
	testConnectors, shutdownFns := createPeerConnectors(t, numPeers, 30001, cmtlog.TestingLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	require.Len(t, testConnectors, numPeers)

	peerAddresses := make([]*cmtp2p.NetAddress, numPeers)
	peerNodeIds := map[string]cmtp2p.ID{
		"peer-1": testConnectors[0].Transport().NodeInfo().ID(),
		"peer-2": testConnectors[1].Transport().NodeInfo().ID(),
		"peer-3": testConnectors[2].Transport().NodeInfo().ID(),
	}

	// fill peerAddresses
	for i := 1; i <= numPeers; i++ {
		name := "peer-" + strconv.Itoa(i)
		peerId := peerNodeIds[name]

		testPort := 30001 + ((i - 1) * 1000) // see createPeerConnectors()
		testAddr, addrErr := cmtp2p.NewNetAddressString(
			"tcp://" + string(peerId) + "@127.0.0.1:" + strconv.Itoa(testPort),
		)
		require.NoError(t, addrErr)
		require.NotNil(t, testAddr)

		peerAddresses[i-1] = testAddr
	}

	// CAUTION: we connect to all peers from peer-1
	// peer-1 connects to peer-2
	testPeer2, dialErrPeer2 := testConnectors[0].Dial(peerAddresses[1]) // peer-2
	require.NoError(t, dialErrPeer2, "peer-1 should connect to peer-2")
	require.NotNil(t, testPeer2)
	// peer-1 connects to peer-3
	testPeer3, dialErrPeer3 := testConnectors[0].Dial(peerAddresses[2]) // peer-3
	require.NoError(t, dialErrPeer3, "peer-1 should connect to peer-3")
	require.NotNil(t, testPeer3)

	// prepare some test data
	testTransactions := make([][]byte, 1)
	testTransactions[0] = []byte{1, 2, 3}

	testTxMessage := cmtp2p.Envelope{
		ChainID:   "test-chain-1",
		ChannelID: mempl.MempoolChannel,
		Message:   &memp2p.Txs{Txs: testTransactions},
	}

	// TEST 1: sending messages concurrently should not error.
	// Note that half of the messages are sent to each peer.
	var actualNumMessages atomic.Uint64
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			withPeerID := testPeer2.ID()
			if c%2 == 0 {
				withPeerID = testPeer3.ID()
			}

			shouldNotErr := testConnectors[0].Send(withPeerID, testTxMessage)
			require.NoError(t, shouldNotErr, "sending a valid message should not error")

			actualNumMessages.Add(1)
		}(i + 1)
	}
	waitAll.Wait()

	assert.Equal(t, uint64(100), actualNumMessages.Load())
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
	shutdownFns []func(),
) {
	tb.Helper()

	connectors = make([]*p2p.PeerConnector, numPeers)
	shutdownFns = make([]func(), numPeers)

	// create and start PeerConnector instances
	for i := 0; i < numPeers; i++ {
		testConnector, connShutdownFn := ResetTestMultiplexPeerConnector(tb,
			uint16(1000*i+startPort),
			customLogger,
			withOptions...,
		)
		require.NotNil(tb, testConnector)
		require.NotNil(tb, connShutdownFn)

		shouldStartErr := testConnector.Start()
		require.NoError(tb, shouldStartErr, fmt.Sprintf("peer at %d should start", i))

		// give both some time to start listening correctly.
		time.Sleep(300 * time.Millisecond)
		require.Equal(tb, true, testConnector.Transport().IsListening())

		connectors[i] = testConnector
		shutdownFns[i] = func() {
			defer connShutdownFn()
			defer testConnector.Stop()
		}
	}

	return // connectors, shutdownFns
}
