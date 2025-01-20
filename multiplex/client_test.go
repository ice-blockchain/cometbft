package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func TestMultiplexClientNewClient(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexClient(t, 0, cmtlog.NewNopLogger())
	require.NotNil(t, server)

	defer func() {
		defer os.RemoveAll(rootDir)
		if server != nil {
			err := server.Close()
			assert.NoError(t, err, "should shutdown gracefully")
		}
	}()
}

func TestMultiplexClientBroadcastTx(t *testing.T) {

}

func TestMultiplexClientBroadcastTxRemoval(t *testing.T) {

}

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: This helper uses a random multiplex config.
func ResetTestMultiplexClient(
	tb testing.TB,
	numChains int,
	customLogger cmtlog.Logger,
) (string, *mx.MultiplexBackend) {
	tb.Helper()

	// Uses config.TestConfig() and random MultiplexConfig
	rootDir,
		globalCfg := ResetTestMultiplexNode(tb, numChains)

	server, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfg,
		customLogger,
	)
	require.NoError(tb, err, "should create server instance")

	// Start the node backend
	server.MustStart()

	return rootDir, server
}
