package runtime_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeConfigNewConfig(t *testing.T) {
	tmpRootDir, err := os.MkdirTemp("", t.Name()+"-1")
	require.NoError(t, err)

	baseConfig := config.TestConfig()
	baseConfig.SetRoot(tmpRootDir)

	testChainID := helpers.MakeChainID("test-chain-1")
	discoveryPort := 10001 // different from default (30001)
	extendChainID := helpers.NewExtendedChainIDFromString(testChainID)

	testConfig := runtime.NewConfig(
		baseConfig,
		testChainID,
		"", // empty seed nodes (always)
		config.DefaultStateSyncConfig(),
		discoveryPort,
	)
	require.NotNil(t, testConfig)
	assert.Equal(t, int(discoveryPort), int(testConfig.DiscoveryPort))

	// Test that we return a deep copy and didn't touch baseConfig
	assert.NotEqual(t, int(discoveryPort), int(baseConfig.DiscoveryPort))

	// Test cometbft listen addresses
	expectedP2PPort := strconv.Itoa(discoveryPort + 1)
	expectedRPCPort := strconv.Itoa(discoveryPort + 2)
	assert.Contains(t, testConfig.P2P.ListenAddress, expectedP2PPort)
	assert.Contains(t, testConfig.RPC.ListenAddress, expectedRPCPort)

	// Test filesystem structure for WAL
	expectedWALPath := filepath.Join(tmpRootDir,
		config.DefaultDataDir,
		extendChainID.GetUserAddress(),
		testChainID, "wal",
	)
	assert.Equal(t, expectedWALPath, testConfig.Consensus.WalFile())

	// Test that explicitely modifying testConfig doesn't affect baseConfig
	testConfig.DiscoveryPort = uint16(50001)
	assert.Equal(t, int(50001), int(testConfig.DiscoveryPort))
	assert.NotEqual(t, int(50001), int(baseConfig.DiscoveryPort))
}
