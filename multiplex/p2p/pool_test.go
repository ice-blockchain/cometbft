package p2p_test

import (
	"math/rand"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

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

	// CAUTION: we inject a custom Mempool reactor so that we can test the actual
	// calls to memR.AddPeer(). Otherwise the dispatcher returns an empty map.
	testMempoolReactor := mempl.NewEmptyReactor(t.Context())
	testPool1.Dispatcher().SetReactor(
		"test-chain-1",
		"MEMPOOL",
		testMempoolReactor,
	)

	// TEST 1: Adding an uninitialized peerID for ChainID, should add it after init.
	actualAddedPeer1 := testPool1.AddPeerForChainID(testPeer1.ID(), "test-chain-1")
	assert.Equal(t, true, actualAddedPeer1)
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

	// TEST 2: do we have the correct number of peers after adding inbound.
	actualInbound2, actualOutbound2, actualDialing2 := testPool1.NumPeers()
	assert.Equal(t, expectedInboundPeers, actualInbound2)
	assert.Equal(t, 0, actualDialing2)
	assert.Equal(t, expectedOutboundPeers, actualOutbound2)
}

func TestMultiplexP2PConnectionPoolPeers(t *testing.T) {
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

	// Creates 3 additional peer connectors that we use for inbound/outbound.
	// Creates peer-1 to peer-3, i.e. :31001 to :33001.
	numRemotes := 3
	testConnectors,
		peerAddresses,
		shutdownFns := createPeerConnectors(t, numRemotes, 31001, cmtlog.NewNopLogger())
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

	require.NotNil(t, testPeer1)
	require.NotNil(t, testPeer2)

	// CAUTION: this one will be INBOUND for peer-0, so we first make sure
	// that peer-0 is actively listening for connections and can be dialed.
	require.Equal(t, true, testPool1.Transport().IsListening())
	peer1ListenAddr, addrErr := cmtp2p.NewNetAddressString(
		"tcp://" + string(testPool1.NodeKey().ID()) + "@127.0.0.1:30001",
	)
	require.NoError(t, addrErr)

	// peer-3 connects to peer-0
	testPeer3NodeID := testConnectors[2].Transport().NodeInfo().ID()
	testPeer0, dialErrPeer0 := testConnectors[2].Dial(peer1ListenAddr) // peer-0
	require.NoError(t, dialErrPeer0, "peer-3 should connect to peer-0")
	expectedInboundPeers++

	require.NotNil(t, testPeer0)
	time.Sleep(300 * time.Millisecond)

	// This call is issued from cometbft reactors with switch.InitPeerForScope().
	testPool1.SetPeerForChainID(testPeer1.ID(), "test-chain-1")
	testPool1.SetPeerForChainID(testPeer2.ID(), "test-chain-1")
	testPool1.SetPeerForChainID(testPeer3NodeID, "test-chain-1")
	testPool1.SetPeerForChainID(testPeer3NodeID, "test-chain-2")

	numPeersChain1 := 3
	numPeersChain2 := 1
	numPeersChain3 := 0

	// TEST 1: Make sure the pool tracks the correct peers per ChainID
	// and that it returns a cmtp2p.PeerSet instance.
	actualAllPeers := testPool1.Peers()
	actualPeersChain1 := testPool1.Peers("test-chain-1")
	actualPeersChain2 := testPool1.Peers("test-chain-2")
	actualPeersEmpty := testPool1.Peers("test-chain-3") // empty!

	require.NotNil(t, actualAllPeers, "should return a peerset given no ChainID")
	require.NotNil(t, actualPeersChain1, "should return a peerset given test-chain-1")
	require.NotNil(t, actualPeersChain2, "should return a peerset given test-chain-2")
	require.NotNil(t, actualPeersEmpty, "should return a peerset given test-chain-3")

	// TEST 2: Do we get the correct peerset contents?
	assert.Equal(t, numRemotes, actualAllPeers.Size())
	assert.Equal(t, numPeersChain1, actualPeersChain1.Size())
	assert.Equal(t, numPeersChain2, actualPeersChain2.Size())
	assert.Equal(t, numPeersChain3, actualPeersEmpty.Size()) // empty!

	// TEST 3: Make sure that the correct peer IDs have been added.
	assert.Equal(t, true, actualAllPeers.Has(testPeer1.ID()))
	assert.Equal(t, true, actualAllPeers.Has(testPeer2.ID()))
	assert.Equal(t, true, actualAllPeers.Has(testPeer3NodeID))
	// test-chain-1 also has all peers
	assert.Equal(t, true, actualPeersChain1.Has(testPeer1.ID()))
	assert.Equal(t, true, actualPeersChain1.Has(testPeer2.ID()))
	assert.Equal(t, true, actualPeersChain1.Has(testPeer3NodeID))
	// test-chain-2 only has peer-3
	assert.Equal(t, false, actualPeersChain2.Has(testPeer1.ID()))
	assert.Equal(t, false, actualPeersChain2.Has(testPeer2.ID()))
	assert.Equal(t, true, actualPeersChain2.Has(testPeer3NodeID))
	// test-chain-3 has none
	assert.Equal(t, false, actualPeersEmpty.Has(testPeer1.ID()))
	assert.Equal(t, false, actualPeersEmpty.Has(testPeer2.ID()))
	assert.Equal(t, false, actualPeersEmpty.Has(testPeer3NodeID))
}

func TestMultiplexP2PConnectionPoolAddPeer(t *testing.T) {
	defer goleak.VerifyNone(t)

	// CAUTION: we create peer-0 for testing ConnectionPool and we additionally
	// dial peer-1 such that the peers are connected.
	numRemotes := 1
	testPool1,
		testConnectors,
		peerAddresses,
		shutdownFn := createConnectionPoolWithOtherPeers(t,
		numRemotes,
		30001,
		cmtlog.NewNopLogger(),
		[]p2p.ConnectionPoolOption{},
		[]p2p.ConnectorOption{},
	)

	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	require.NotEmpty(t, testConnectors)
	require.NotNil(t, testConnectors[0])
	require.NotNil(t, testConnectors[0].Transport())

	testPeer1 := testConnectors[0].Transport().NodeInfo()

	// peer-0 connects to all other peers.
	dialWg := sync.WaitGroup{}
	dialWg.Add(numRemotes)
	for i := 0; i < numRemotes; i++ {
		// peer-0 connects to peer-i.
		go func() {
			defer dialWg.Done()

			testPeerI, dialErrPeerI := testPool1.Connector().Dial(peerAddresses[i]) // peer-i
			require.NoError(t, dialErrPeerI, "peer-0 should connect to peer-"+strconv.Itoa(i+1))
			require.NotNil(t, testPeerI)
			require.Equal(t, string(peerAddresses[i].ID), string(testPeerI.ID()))
		}()
	}
	dialWg.Wait()

	// TEST 1: After successful dialing, testPeer1 should appear in the peerset
	// and the MConnection instance should be running.
	actualPeers := testPool1.Peers()
	require.Equal(t, 1, actualPeers.Size())
	require.Equal(t, true, actualPeers.Has(testPeer1.ID()))
	require.Equal(t, true, testPool1.HasConnection(testPeer1.ID()))

	actualMConnectionPeer1 := testPool1.Connection(testPeer1.ID())
	assert.Equal(t, true, actualMConnectionPeer1.IsRunning())

	// TEST 2: Adding the peer a second time should have no effect on the peerset.
	testSecondPeer1, dialErrSecondPeer1 := testPool1.Connector().Dial(peerAddresses[0]) // peer-1
	require.NoError(t, dialErrSecondPeer1, "peer-0 identify already dialed peer-1")
	require.NotNil(t, testSecondPeer1)
	// makes sure it returns the same and correct already-dialed peer instance.
	assert.Equal(t, string(testPeer1.ID()), string(testSecondPeer1.ID()))
	actualPeers = testPool1.Peers()
	assert.Equal(t, 1, actualPeers.Size()) // didn't move.
}

func TestMultiplexP2PConnectionPoolRemovePeer(t *testing.T) {
	defer goleak.VerifyNone(t)

	// CAUTION: we create peer-0 for testing ConnectionPool and we additionally
	// dial peer-1, peer-2 and peer-3 such that the peers are connected.
	numRemotes := 3
	testPool1,
		testConnectors,
		peerAddresses,
		shutdownFn := createConnectionPoolWithOtherPeers(t,
		numRemotes,
		30001,
		cmtlog.NewNopLogger(),
		[]p2p.ConnectionPoolOption{},
		[]p2p.ConnectorOption{},
	)

	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	require.NotEmpty(t, testConnectors)
	require.Len(t, testConnectors, numRemotes)
	require.Len(t, peerAddresses, numRemotes)

	testPeer0 := testPool1.NodeInfo()
	testPeer1 := testConnectors[0].Transport().NodeInfo()
	testPeer2 := testConnectors[1].Transport().NodeInfo()
	testPeer3 := testConnectors[2].Transport().NodeInfo()

	require.NotNil(t, testPeer0)
	require.NotNil(t, testPeer1)
	require.NotNil(t, testPeer2)
	require.NotNil(t, testPeer3)

	// peer-0 connects to all other peers.
	dialWg := sync.WaitGroup{}
	dialWg.Add(numRemotes)
	for i := 0; i < numRemotes; i++ {
		// peer-0 connects to peer-i.
		go func() {
			defer dialWg.Done()

			testPeerI, dialErrPeerI := testPool1.Connector().Dial(peerAddresses[i]) // peer-i
			require.NoError(t, dialErrPeerI, "peer-0 should connect to peer-"+strconv.Itoa(i+1))
			require.NotNil(t, testPeerI)
			require.Equal(t, string(peerAddresses[i].ID), string(testPeerI.ID()))
		}()
	}
	dialWg.Wait()

	actualPeersOfPeer0 := testPool1.Peers()
	actualPeersOfPeer1 := testConnectors[0].Pool().Peers()
	actualPeersOfPeer2 := testConnectors[1].Pool().Peers()
	actualPeersOfPeer3 := testConnectors[2].Pool().Peers()

	// TEST 1: Sanity checks, peer-0 is connected to all, and each of the
	// remote peers is only connected to peer-0.
	assert.Equal(t, numRemotes, actualPeersOfPeer0.Size())
	assert.Equal(t, 1, actualPeersOfPeer1.Size()) // connected only to peer-0
	assert.Equal(t, 1, actualPeersOfPeer2.Size()) // connected only to peer-0
	assert.Equal(t, 1, actualPeersOfPeer3.Size()) // connected only to peer-0

	// TEST 2: Now make sure about removing peers with RemovePeer().
	shouldNotErr := testPool1.RemovePeer(testPeer3.ID())
	assert.NoError(t, shouldNotErr)

	actualPeersOfPeer0 = testPool1.Peers()

	assert.Equal(t, true, testPool1.HasPeerID(testPeer2.ID())) // we did not remove peer-2
	assert.Equal(t, numRemotes-1, actualPeersOfPeer0.Size())
	assert.Equal(t, false, testPool1.HasPeerID(testPeer3.ID()))

	// TEST 3: Wait 1 second to give peer-3 time to find out that peer-0 disconnected.
	time.Sleep(1000 * time.Millisecond)
	actualPeersOfPeer3 = testConnectors[2].Pool().Peers()
	assert.Equal(t, 0, actualPeersOfPeer3.Size()) // peer-3 has no more connections.
}

func TestMultiplexP2PConnectionPoolBroadcast(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRemotes := 3

	// CAUTION: we create peer-0 for testing ConnectionPool and we additionally
	// dial peer-1, peer-2 and peer-3 such that the peers are connected.
	testPool1,
		testConnectors,
		peerAddresses,
		shutdownFn := createConnectionPoolWithOtherPeers(t,
		numRemotes,
		30001,
		cmtlog.TestingLogger(),
		[]p2p.ConnectionPoolOption{},
		[]p2p.ConnectorOption{},
	)

	require.NotNil(t, testPool1)
	require.NotNil(t, shutdownFn)
	defer shutdownFn()

	require.NotEmpty(t, testConnectors)
	require.Len(t, testConnectors, numRemotes)
	require.Len(t, peerAddresses, numRemotes)

	testPeer0 := testPool1.NodeInfo()
	testPeer1 := testConnectors[0].Transport().NodeInfo()
	testPeer2 := testConnectors[1].Transport().NodeInfo()
	testPeer3 := testConnectors[2].Transport().NodeInfo()

	require.NotNil(t, testPeer0)
	require.NotNil(t, testPeer1)
	require.NotNil(t, testPeer2)
	require.NotNil(t, testPeer3)

	// CAUTION: we inject custom MockDispatcherImpl to keep track of actually
	// dispatched (and received) messages on remote peers.
	mockDispatcher1 := &cmtp2p.MockDispatcherImpl{Logger: cmtlog.TestingLogger()}
	p2p.ConnectorWithDispatcher(mockDispatcher1)(testConnectors[0]) // peer-1
	mockDispatcher2 := &cmtp2p.MockDispatcherImpl{Logger: cmtlog.TestingLogger()}
	p2p.ConnectorWithDispatcher(mockDispatcher2)(testConnectors[1]) // peer-2
	mockDispatcher3 := &cmtp2p.MockDispatcherImpl{Logger: cmtlog.TestingLogger()}
	p2p.ConnectorWithDispatcher(mockDispatcher3)(testConnectors[2]) // peer-3

	// peer-0 connects to all other peers.
	dialWg := sync.WaitGroup{}
	dialWg.Add(numRemotes)
	for i := 0; i < numRemotes; i++ {
		// peer-0 connects to peer-i.
		go func() {
			defer dialWg.Done()

			testPeerI, dialErrPeerI := testPool1.Connector().Dial(peerAddresses[i]) // peer-i
			require.NoError(t, dialErrPeerI, "peer-0 should connect to peer-"+strconv.Itoa(i+1))
			require.NotNil(t, testPeerI)
			require.Equal(t, string(peerAddresses[i].ID), string(testPeerI.ID()))
		}()
	}
	dialWg.Wait()

	// This call is issued from cometbft reactors with switch.InitPeerForScope().
	testPool1.SetPeerForChainID(testPeer1.ID(), "test-chain-1")
	testPool1.SetPeerForChainID(testPeer2.ID(), "test-chain-1")
	testPool1.SetPeerForChainID(testPeer3.ID(), "test-chain-1")

	testTransactions := make([][]byte, 1)
	testTransactions[0] = []byte{1, 2, 3}

	testTxMessage := cmtp2p.Envelope{
		ChannelID: mempl.MempoolChannel,
		Message:   &memp2p.Txs{Txs: testTransactions},
	}

	// TEST 1: Using a ChainID that has no peers should error.
	testTxMessage.ChainID = "test-chain-2" // test-chain-2 has no peers
	shouldErr := testPool1.Broadcast(testTxMessage)
	assert.Error(t, shouldErr)
	assert.Contains(t, shouldErr.Error(), "peerset is empty")
	assert.Contains(t, shouldErr.Error(), "test-chain-2")

	// TEST 2: Using a correct ChainID, it should send to the peers.
	testTxMessage.ChainID = "test-chain-1"
	shouldNotErr := testPool1.Broadcast(testTxMessage)
	assert.NoError(t, shouldNotErr)

	// give the messages some time to be dispatched.
	t.Logf("Waiting 500ms to evaluate dispatch...")
	time.Sleep(500 * time.Millisecond)

	expectedDispatchedPackets := uint64(1)
	require.Equal(t, expectedDispatchedPackets, mockDispatcher1.NumDispatchedPackets.Load())
	require.Equal(t, expectedDispatchedPackets, mockDispatcher2.NumDispatchedPackets.Load())
	require.Equal(t, expectedDispatchedPackets, mockDispatcher3.NumDispatchedPackets.Load())

	testTransactions2 := make([][]byte, 1)
	testTransactions2[0] = []byte{4, 5, 6}

	testTxMessage2 := cmtp2p.Envelope{
		ChainID:   "test-chain-1",
		ChannelID: mempl.MempoolChannel,
		Message:   &memp2p.Txs{Txs: testTransactions2},
	}

	// TEST 4: Using a correct ChainID, it should send to the peers again.
	shouldNotErr2 := testPool1.Broadcast(testTxMessage2)
	assert.NoError(t, shouldNotErr2)

	// give the messages some time to be dispatched.
	t.Logf("Waiting 500ms to evaluate dispatch...")
	time.Sleep(500 * time.Millisecond)

	expectedDispatchedPackets = uint64(2)
	require.Equal(t, expectedDispatchedPackets, mockDispatcher1.NumDispatchedPackets.Load())
	require.Equal(t, expectedDispatchedPackets, mockDispatcher2.NumDispatchedPackets.Load())
	require.Equal(t, expectedDispatchedPackets, mockDispatcher3.NumDispatchedPackets.Load())
}
