package e2e

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
)

// ----------------------------------------------------------------------------
// TestMultiplexBackend

func TestMultiplexBackendNewServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 1
	backends := ResetTestMultiplexRelays(t, numRelays, cmtlog.NewNopLogger())
	require.NotEmpty(t, backends)
	defer shutdownBackends(t, backends...)

	assert.NotNil(t, backends[0])

	testBackend := backends[0]
	assert.NotNil(t, testBackend.Config())
	assert.NotNil(t, testBackend.Acceptor())
	assert.NotNil(t, testBackend.RuntimeManager())
	assert.NotNil(t, testBackend.IdleManager())
	assert.NotNil(t, testBackend.Discovery())
	assert.NotNil(t, testBackend.CometBFT())
	assert.NotNil(t, testBackend.SnapsApp())
	assert.NotNil(t, testBackend.ChainConns())
}

func TestMultiplexBackendOnStart(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 1
	backends := ResetTestMultiplexRelays(t, numRelays, cmtlog.TestingLogger().With("process", "relay-1"))
	require.NotEmpty(t, backends)
	defer shutdownBackends(t, backends...)

	testBackend := backends[0]
	require.NotNil(t, testBackend)

	startErr := testBackend.Start()
	assert.NoError(t, startErr)

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to evaluate OnStart...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// Discovery P2P must be listening
	testDiscovery := testBackend.Discovery().Transport()
	assert.NotNil(t, testDiscovery)
	assert.Equal(t, true, testDiscovery.IsListening()) // LISTEN

	// CometBFT P2P must be listening
	testCometBFT := testBackend.CometBFT().Transport()
	assert.NotNil(t, testCometBFT)
	assert.Equal(t, true, testCometBFT.IsListening()) // LISTEN
}
