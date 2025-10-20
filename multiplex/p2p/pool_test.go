package p2p_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
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

	testPool, shutdownFn := ResetTestMultiplexConnectionPool(t, 30001, nil, nil, cmtlog.NewNopLogger())
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

}

func TestMultiplexP2PConnectionPoolSetPeerForChainID(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolInitPeerForChainID(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolAddPeerForChainID(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolNumPeers(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolPeers(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolAddPeer(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolRemovePeer(t *testing.T) {

}

func TestMultiplexP2PConnectionPoolBroadcast(t *testing.T) {

}
