package e2e

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// CAUTION: This helper uses an empty multiplex config on multiple relays.
func ResetTestMultiplexRelays(
	tb testing.TB,
	numRelays int,
	customLoggers ...cmtlog.Logger,
) []*mx.MultiplexBackend {
	tb.Helper()

	require.Len(tb, customLoggers, numRelays,
		"count of loggers passed should be equal numRelays")

	backends := make([]*mx.MultiplexBackend, numRelays)

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	relayConf1 := config.TestConfig()
	relayConf1.SetRoot(tmpRootDir)

	serverRelay1, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		relayConf1,
		customLoggers[0],
	)
	require.NoError(tb, err, "should create first server instance")

	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-"+strconv.Itoa(r+1))

		relayConfX := config.TestConfig()
		relayConfX.SetRoot(tmpRootDir)

		serverRelayX, err := mx.NewServer(
			tb.Context(),
			&client.DefaultAcceptor{},
			relayConfX,
			customLoggers[r],
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		backends[r] = serverRelayX
	}

	return backends
}

// -----------------------------------------------------------------------------

func closeAndRemoveAll(tb testing.TB, rootDir string, backend *mx.MultiplexBackend) {
	tb.Helper()

	defer os.RemoveAll(rootDir)

	if backend.IsRunning() {
		err := backend.Stop()
		assert.NoError(tb, err, "should shutdown backend gracefully")
	}
}
