package p2p_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexP2PConnectionPoolNewConnectionManager(t *testing.T) {
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

	// Test the pool constructor.
	testPool := p2p.NewConnectionManager(t.Context(),
		nodeKey,
		transport,
		resourceMgr,
		cmtlog.TestingLogger(),
	)
	assert.NotNil(t, testPool)

	// Test resources assignments.
	expectedNodeID := string(nodeKey.ID())
	assert.Equal(t, expectedNodeID, string(testPool.NodeKey().ID()))

	// Test the assignment of connection services.
	assert.NotNil(t, testPool.Transport())
	assert.NotNil(t, testPool.Dispatcher())
	assert.NotNil(t, testPool.Connector())
	assert.NotNil(t, testPool.Handshaker())
}

func TestMultiplexP2PConnectionPoolNewConnectionManagerHelper(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool, shutdownFn := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	// Test the assignment of connection services.
	assert.NotNil(t, testPool.Transport())
	assert.NotNil(t, testPool.Dispatcher())
	assert.NotNil(t, testPool.Connector())
	assert.NotNil(t, testPool.Handshaker())
}

func TestMultiplexP2PConnectionPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool1, shutdownFn1 := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	// TEST 1: Make sure that we can start the pool.
	shouldNotErrStart1 := testPool1.Start()
	require.NoError(t, shouldNotErrStart1)

	assert.Equal(t, true, testPool1.Connector().IsRunning())

	// ... and also stop it
	shouldNotErrStop1 := testPool1.Stop()
	assert.NoError(t, shouldNotErrStop1)

	assert.Equal(t, false, testPool1.Connector().IsRunning())

	testPool2, shutdownFn2 := ResetTestMultiplexConnectionPool(t,
		30001, // SAME
		nil,   // nil-NodeKey
		nil,   // nil-NodeInfo
		nil,   // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool2)
	require.NotNil(t, shutdownFn2)
	defer shutdownFn1()

	// TEST 2: Start another pool on same port as the stopped one.
	shouldNotErrStart2 := testPool2.Start()
	require.NoError(t, shouldNotErrStart2)
	defer testPool2.Stop()
}

func TestMultiplexP2PConnectionPoolSetPeerForChainID(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool1, shutdownFn1 := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	randomPeerIds := []cmtp2p.ID{}
	for i := 0; i < 10; i++ {
		randPrivKey := ed25519.GenPrivKey()
		randNodeKey := &cmtp2p.NodeKey{PrivKey: randPrivKey}
		randomPeerIds = append(randomPeerIds, randNodeKey.ID())
	}

	expectedPeerIdsByChainID := map[string][]cmtp2p.ID{}
	for i := 0; i < 100; i++ {
		randPeerID := randomPeerIds[rand.Intn(len(randomPeerIds))]
		randChainID := "test-chain-" + strconv.Itoa(rand.Intn(20))

		if _, ok := expectedPeerIdsByChainID[randChainID]; !ok {
			expectedPeerIdsByChainID[randChainID] = []cmtp2p.ID{}
		}
		expectedPeerIdsByChainID[randChainID] = append(expectedPeerIdsByChainID[randChainID], randPeerID)

		actualCntPeersPerChainID := testPool1.SetPeerForChainID(randPeerID, randChainID)
		assert.Greater(t, actualCntPeersPerChainID, 0)
	}

	assert.NotEmpty(t, expectedPeerIdsByChainID)

	// TEST 1: Make sure that we registered all peer IDs by ChainID
	for expectedChainID, expectedPeerIds := range expectedPeerIdsByChainID {
		for _, mustHavePeerID := range expectedPeerIds {
			actualHasPeer := testPool1.HasPeerForChainID(mustHavePeerID, expectedChainID)
			assert.Equal(t, true, actualHasPeer)
		}
	}
}

func TestMultiplexP2PConnectionPoolInitPeerForChainID(t *testing.T) {
	defer goleak.VerifyNone(t)

	// CAUTION: we use peer-0 to test ConnectionPool, and other peers
	// are created with the createPeerConnectors helper from PeerConnector.
	testPool1, shutdownFn1 := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	shouldNotErrStart := testPool1.Start()
	require.NoError(t, shouldNotErrStart)
	defer testPool1.Stop()

	// TEST 1: Initialize a random peerID for ChainID, should not error.
	randPrivKey := ed25519.GenPrivKey()
	randNodeKey := &cmtp2p.NodeKey{PrivKey: randPrivKey}
	actualPeerForReactor := testPool1.InitPeerForChainID(randNodeKey.ID(), "test-chain-1")
	assert.Nil(t, actualPeerForReactor) // should be nil because we used a random peer ID.

	// Creates an additional peer connector that we can dial.
	testConnectors,
		peerAddresses,
		shutdownFns := createPeerConnectors(t, 1, 31001, cmtlog.NewNopLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	// peer-0 connects to peer-1
	testPeer1, dialErrPeer1 := testPool1.Connector().Dial(peerAddresses[0]) // peer-1
	require.NoError(t, dialErrPeer1, "peer-0 should connect to peer-1")
	require.NotNil(t, testPeer1)
	assert.Equal(t, string(peerAddresses[0].ID), string(testPeer1.ID()))

	// TEST 2: Initialize a known peerID for ChainID, should return peer.
	actualPeer1ForReactor := testPool1.InitPeerForChainID(testPeer1.ID(), "test-chain-1")
	assert.NotNil(t, actualPeer1ForReactor) // should NOT be nil.
	assert.Equal(t, string(peerAddresses[0].ID), string(actualPeer1ForReactor.ID()))
}

func TestMultiplexP2PConnectionPoolAddPeerForChainID(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool1, shutdownFn1 := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	shouldNotErrStart := testPool1.Start()
	require.NoError(t, shouldNotErrStart)
	defer testPool1.Stop()

	// Creates an additional peer connector that we can dial.
	testConnectors,
		peerAddresses,
		shutdownFns := createPeerConnectors(t, 1, 31001, cmtlog.NewNopLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	// peer-1 connects to peer-2
	testPeer2, dialErrPeer2 := testPool1.Connector().Dial(peerAddresses[0]) // peer-2
	require.NoError(t, dialErrPeer2, "peer-1 should connect to peer-2")
	require.NotNil(t, testPeer2)
	assert.Equal(t, string(peerAddresses[0].ID), string(testPeer2.ID()))

	// CAUTION: we inject a custom Mempool reactor so that we can test the actual
	// calls to memR.AddPeer(). Otherwise the dispatcher returns an empty map.
	testMempoolReactor := mempl.NewEmptyReactor(t.Context())
	testPool1.Dispatcher().SetReactor(
		"test-chain-1",
		"MEMPOOL",
		testMempoolReactor,
	)

	// TEST 1: Adding an uninitialized peerID for ChainID, should add it after init.
	actualAddedPeer2 := testPool1.AddPeerForChainID(testPeer2.ID(), "test-chain-1")
	assert.Equal(t, true, actualAddedPeer2)
}

func TestMultiplexP2PConnectionPoolNumPeers(t *testing.T) {
	defer goleak.VerifyNone(t)

	// CAUTION: we use peer-0 to test ConnectionPool, and other peers
	// are created with the createPeerConnectors helper from PeerConnector.
	testPool1, shutdownFn1 := ResetTestMultiplexConnectionPool(t,
		30001,
		nil, // nil-NodeKey
		nil, // nil-NodeInfo
		nil, // nil-Transport
		cmtlog.NewNopLogger(),
	)
	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	shouldNotErrStart := testPool1.Start()
	require.NoError(t, shouldNotErrStart)
	defer testPool1.Stop()

	require.Equal(t, true, testPool1.IsRunning())
	require.Equal(t, true, testPool1.Connector().IsRunning())
	require.Equal(t, true, testPool1.Transport().IsListening())

	// Creates 6 additional peer connectors that we use for inbound/outbound.
	// Creates peer-1 to peer-6, i.e. :31001 to :36001.
	testConnectors,
		peerAddresses,
		shutdownFns := createPeerConnectors(t, 6, 31001, cmtlog.NewNopLogger())
	require.NotEmpty(t, testConnectors)
	require.NotEmpty(t, shutdownFns)
	defer func() {
		for _, shutdownFn := range shutdownFns {
			shutdownFn()
		}
	}()

	expectedInboundPeers := 0
	expectedOutboundPeers := 0

	// peer-0 connects to peer-1
	testPeer1, dialErrPeer1 := testPool1.Connector().Dial(peerAddresses[0]) // peer-1
	require.NoError(t, dialErrPeer1, "peer-0 should connect to peer-1")
	expectedOutboundPeers++
	// peer-0 connects to peer-2
	testPeer2, dialErrPeer2 := testPool1.Connector().Dial(peerAddresses[1]) // peer-2
	require.NoError(t, dialErrPeer2, "peer-0 should connect to peer-2")
	expectedOutboundPeers++
	// peer-0 connects to peer-3
	testPeer3, dialErrPeer3 := testPool1.Connector().Dial(peerAddresses[2]) // peer-3
	require.NoError(t, dialErrPeer3, "peer-0 should connect to peer-3")
	expectedOutboundPeers++
	// peer-0 connects to peer-4
	testPeer4, dialErrPeer4 := testPool1.Connector().Dial(peerAddresses[3]) // peer-4
	require.NoError(t, dialErrPeer4, "peer-0 should connect to peer-4")
	expectedOutboundPeers++
	// peer-0 connects to peer-5
	testPeer5, dialErrPeer5 := testPool1.Connector().Dial(peerAddresses[4]) // peer-5
	require.NoError(t, dialErrPeer5, "peer-0 should connect to peer-5")
	expectedOutboundPeers++

	require.NotNil(t, testPeer1)
	require.NotNil(t, testPeer2)
	require.NotNil(t, testPeer3)
	require.NotNil(t, testPeer4)
	require.NotNil(t, testPeer5)

	// TEST 1: do we have the correct number of peers? (all outbound up to here)
	actualInbound, actualOutbound, actualDialing := testPool1.NumPeers()
	assert.Equal(t, 0, actualInbound)
	assert.Equal(t, 0, actualDialing)
	assert.Equal(t, expectedOutboundPeers, actualOutbound)

	// CAUTION: this one will be INBOUND for peer-0, so we first make sure
	// that peer-0 is actively listening for connections and can be dialed.
	require.Equal(t, true, testPool1.Transport().IsListening())
	peer1ListenAddr, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(testPool1.NodeKey().ID()) + "@127.0.0.1:30001",
	)
	require.NoError(t, addrErr)

	// peer-6 connects to peer-0
	testPeer0, dialErrPeer0 := testConnectors[5].Dial(peer1ListenAddr) // peer-0
	require.NoError(t, dialErrPeer0, "peer-6 should connect to peer-0")
	expectedInboundPeers++

	require.NotNil(t, testPeer0)
}

func TestMultiplexP2PConnectionPoolPeers(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolAddPeer(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolRemovePeer(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolBroadcast(t *testing.T) {

}
