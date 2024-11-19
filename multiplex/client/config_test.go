package client_test

import (
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func makeDeterministicTrustHash(input string) string {
	return strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(input)),
	))
}

func TestMultiplexClientInjectSyncConfig(t *testing.T) {
	expectedAddress := "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	expectedChainID := "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB"
	baseTrustHeight := int64(123)

	// test some valid values
	baseSyncConf := config.DefaultStateSyncConfig()
	baseSyncConf.Enable = true
	baseSyncConf.TrustPeriod = 123 * time.Hour // by default TrustPeriod=168h
	baseSyncConf.TrustHeight = baseTrustHeight
	baseSyncConf.TrustHash = makeDeterministicTrustHash("trust me!")

	multiplexConf := config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{expectedChainID: baseSyncConf},
		map[string]string{},
		map[string][]string{expectedAddress: {expectedChainID}})

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectSyncConf map[string]*config.StateSyncConfig //nolint:gosimple
	injectSyncConf = client.InjectSyncConfig(
		&multiplexConf.MultiplexConfig,
		mockSyncConfigExtensionMutatesHeight, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectSyncConf, "SyncConfigExtensionFn may not return nil")

	// Must return a map[string]*config.StateSyncConfig
	assert.Contains(t, injectSyncConf, expectedChainID)
	assert.NotNil(t, injectSyncConf[expectedChainID], "SyncConfigExtensionFn may not return nil entries")
	assert.IsType(t, &config.StateSyncConfig{}, injectSyncConf[expectedChainID])

	// The extension should have mutated the TrustHeight field
	assert.Equal(t, baseTrustHeight+1, injectSyncConf[expectedChainID].TrustHeight)

	// But it should not have touched the input config object
	assert.Equal(t, baseTrustHeight, baseSyncConf.TrustHeight)
	assert.Equal(t, baseTrustHeight, multiplexConf.SyncConfig[expectedChainID].TrustHeight)
}

func TestMultiplexClientInjectChainSeeds(t *testing.T) {
	expectedAddress := "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	expectedChainID := "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB"

	// test some valid values
	baseSeeds := "testNodeId2@192.168.1.1:30001,testNodeId3@192.168.1.2:30001"

	multiplexConf := config.MultiplexTestBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{expectedChainID: baseSeeds},
		map[string][]string{expectedAddress: {expectedChainID}})

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectSeedsConf map[string]string //nolint:gosimple
	injectSeedsConf = client.InjectChainSeeds(
		&multiplexConf.MultiplexConfig,
		mockSeedConfigExtensionPrefixOneSeed, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectSeedsConf, "SeedConfigExtensionFn may not return nil")

	// Must return a map[string]string
	assert.Contains(t, injectSeedsConf, expectedChainID)
	assert.NotNil(t, injectSeedsConf[expectedChainID], "SeedConfigExtensionFn may not return nil entries")
	assert.NotEmpty(t, injectSeedsConf[expectedChainID])

	// The extension should have prefixed the testSeedNodesExample
	assert.Contains(t, injectSeedsConf[expectedChainID], testSeedNodesExample)

	// But it should not have touched the input config object
	assert.Equal(t, baseSeeds, multiplexConf.ChainSeeds[expectedChainID])
}
