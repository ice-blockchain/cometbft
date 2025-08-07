package e2e

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
)

// ----------------------------------------------------------------------------
// TestMultiplexBackend

func TestMultiplexBackendNewServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	backends := ResetTestMultiplexRelays(t, 1, cmtlog.TestingLogger())
	require.NotEmpty(t, backends)
	assert.NotNil(t, backends[0])

	defer func() {
		for i := 0; i < len(backends); i++ {
			backend := backends[i]
			rootDir := backend.Config().RootDir
			go closeAndRemoveAll(t, rootDir, backend)
		}
	}()

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
