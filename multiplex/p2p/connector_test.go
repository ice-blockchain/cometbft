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

	testConnector, shutdownFn := ResetTestMultiplexPeerConnector(t, cmtlog.NewNopLogger())
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
		cmtlog.TestingLogger(),
	)
	require.NotNil(t, testConnector1)
	require.NotNil(t, shutdownFn1)
	defer shutdownFn1()

	// CAUTION: Set nil pool errors on Start().
	p2p.ConnectorWithPool(nil)(testConnector1)

	shouldErr := testConnector1.Start()
	assert.Error(t, shouldErr)

	// Test the instance start/stop implementation.
	testConnector2, shutdownFn2 := ResetTestMultiplexPeerConnector(t,
		cmtlog.TestingLogger(),
	)
	require.NotNil(t, testConnector2)
	require.NotNil(t, shutdownFn2)
	defer shutdownFn2()

	shouldNotErr := testConnector2.Start()
	assert.NoError(t, shouldNotErr)

	shouldNotErr = testConnector2.Stop()
	assert.NoError(t, shouldNotErr)
}
