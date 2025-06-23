package multiplex_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func TestMultiplexClientNewClient(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexClient(t, 0, cmtlog.NewNopLogger())
	require.NotNil(t, server)

	defer func() {
		closeAndRemoveAll(t, rootDir, server)
	}()

	cli := mx.NewClient()
	cli.SetBackend(server)

	assert.NotNil(t, cli.GetBackend())
	assert.NotNil(t, cli.GetRuntimeRegistry())
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
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfg,
		customLogger,
	)
	require.NoError(tb, err, "should create server instance")

	// Start the node backend
	server.Start()

	return rootDir, server
}
