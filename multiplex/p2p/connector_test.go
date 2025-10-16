package p2p_test

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
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

	testConnector, shutdownFn := ResetTestMultiplexPeerConnector(t, cmtlog.NewNopLogger(), 30001)
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
		cmtlog.NewNopLogger(),
		30001,
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
		cmtlog.NewNopLogger(),
		30001,
	)
	require.NotNil(t, testConnector2)
	require.NotNil(t, shutdownFn2)
	defer shutdownFn2()

	shouldNotErr := testConnector2.Start()
	assert.NoError(t, shouldNotErr)
	defer testConnector2.Stop()

	assert.Equal(t, true, testConnector2.Transport().IsListening())
}

func TestMultiplexP2PPeerConnectorDialErrors(t *testing.T) {
	defer goleak.VerifyNone(t)

	testConnector, shutdownFn := ResetTestMultiplexPeerConnector(t,
		cmtlog.NewNopLogger(),
		30001,
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
		cmtlog.TestingLogger(),
		30001,
	)
	require.NotNil(t, testConnector1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	shouldStartErr := testConnector1.Start()
	require.NoError(t, shouldStartErr, "first peer should start")
	defer testConnector1.Stop()

	testConnector2, shutdownFn2 := ResetTestMultiplexPeerConnector(t,
		cmtlog.TestingLogger(),
		40001,
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

	// peer-1 connects to peer-2
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

	// ... and we must correctly cleanup afterwards.
	testConnector1.Pool().RemovePeer(peer2NodeId)
	testConnector2.Pool().RemovePeer(peer1NodeId)
}
