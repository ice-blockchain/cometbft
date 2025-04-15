package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
)

func TestMultiplexChainRegistryLoadSeedsFromFile(t *testing.T) {
	// ----------------
	// Errors
	// create temporary file
	errfile, err := os.CreateTemp("", "errors.seeds.json")
	require.NoError(t, err)
	defer os.Remove(errfile.Name())

	// Should error if file doesn't exist
	_, err = mx.LoadSeedsFromFile("invalid.file")
	assert.Error(t, err, "should error given unknown file")

	failCases := [][]byte{
		[]byte(`1234`),
		[]byte(`{}`),
		[]byte(`{"abc": 1}`),
		[]byte(`{"def": null}`),
		[]byte(`{"": ""}`),
	}

	for _, failCaseSeedsBytes := range failCases {
		err := cmtos.WriteFile(errfile.Name(), failCaseSeedsBytes, 0o644)
		require.NoError(t, err)

		// Should drop empty seed configurations
		seeds, _ := mx.LoadSeedsFromFile(errfile.Name())
		assert.Empty(t, seeds)
	}

	// ----------------
	// Successes
	// create temporary file
	tmpfile, err := os.CreateTemp("", "seeds.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	seedsBytes := []byte(`{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001,seed2@127.0.0.1:30002"
	}`)
	err = cmtos.WriteFile(tmpfile.Name(), seedsBytes, 0o644)
	require.NoError(t, err)

	seeds, err := mx.LoadSeedsFromFile(tmpfile.Name())
	assert.NoError(t, err)
	assert.Len(t, seeds, 1) // one key (user address)

	seedsBytes = []byte(`{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC": "seed2@127.0.0.1:30002",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC": "seed3@127.0.0.1:30003"
	}`)
	err = cmtos.WriteFile(tmpfile.Name(), seedsBytes, 0o644)
	require.NoError(t, err)

	seeds, err = mx.LoadSeedsFromFile(tmpfile.Name())
	assert.NoError(t, err)
	assert.Len(t, seeds, 3) // three key (user address)
	assert.Contains(t, seeds, "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB")
}

func TestMultiplexChainRegistryLoadChainsFromGenesisFile(t *testing.T) {
	// ----------------
	// Errors
	// create temporary file
	errfile, err := os.CreateTemp("", "errors.genesis.json")
	require.NoError(t, err)
	defer os.Remove(errfile.Name())

	// Should error if file doesn't exist
	_, err = mx.LoadChainsFromGenesisFile("invalid.file")
	assert.Error(t, err, "should error given unknown file")

	// ----------------
	// Successes
	// create temporary file
	tmpfile, err := os.CreateTemp("", "seeds.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	// save genesis doc set in temp file
	numChains := 3
	genesisDocSet := randomGenesisDocSet(numChains)
	require.Len(t, genesisDocSet, numChains)
	err = genesisDocSet.SaveAs(tmpfile.Name())
	require.NoError(t, err)

	userChains, err := mx.LoadChainsFromGenesisFile(tmpfile.Name())
	assert.NoError(t, err, "should not error given valid GenesisDocSet")

	// In this test, we use the validator address as the chain owner by
	// force-including the validator address in the ChainID.
	// see also: randomGenesisDocSet()
	for _, testGenesisDoc := range genesisDocSet {
		expectedOwner := testGenesisDoc.Validators[0].Address.String()
		expectedChainID := testGenesisDoc.ChainID

		// Must map ChainIDs to user addresses
		assert.Contains(t, userChains, expectedOwner)

		// Must contain correct ChainID
		assert.Contains(t, userChains[expectedOwner], expectedChainID)
	}
}

func TestMultiplexChainRegistryNewChainRegistry(t *testing.T) {
	// ----------------
	// Errors
	// Should not do anything given "Disabled" strategy
	stopConf := config.EmptyMultiplexConfig() // contains ReplicationStrategy{"Disabled"}
	stopRegistry, err := mx.NewChainRegistry(&stopConf, "")
	assert.NoError(t, err, "should not error given disabled multiplex configuration")
	assert.Empty(t, stopRegistry.GetChains())

	// ----------------
	// Successes

	// Should not error given empty chains with "Network" strategy
	misConf := config.MultiplexTestBaseConfig( // contains ReplicationStrategy("Network")
		map[string]string{},
		map[string][]string{},
	)
	_, err = mx.NewChainRegistry(&misConf.MultiplexConfig, "")
	assert.NoError(t, err, "should not error given empty replicated chains")

	minimalConf := config.MultiplexTestBaseConfig(
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		}},
	)
	minimalRegistry, err := mx.NewChainRegistry(&minimalConf.MultiplexConfig, "")
	assert.NoError(t, err, "should not error given valid minimal multiplex configuration")
	assert.Len(t, minimalRegistry.GetChains(), 1)

	exampleConf := config.MultiplexTestBaseConfig(
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "seed1@127.0.0.1:30001",
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC": "seed1@127.0.0.1:30002",
			"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC": "seed1@127.0.0.1:30003",
		},
		map[string][]string{
			// The order here doesn't matter, so we intentionally force a sorting operation.
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {
				"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
			},
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
			},
		},
	)
	chainRegistry, err := mx.NewChainRegistry(&exampleConf.MultiplexConfig, "")
	assert.NoError(t, err, "should not error given valid example multiplex configuration")
	assert.Len(t, chainRegistry.GetChains(), 3)

	// Should be ordered even though the above configuration intentionally passes unordered ChainIDs
	assert.Equal(t, chainRegistry.GetChains()[0], "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB")
	assert.Equal(t, chainRegistry.GetChains()[1], "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC")
	assert.Equal(t, chainRegistry.GetChains()[2], "mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC")
}

func TestMultiplexChainRegistryGetStateSyncConfig(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	testChainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(t, err, "should create chain registry from random multiplex config")

	// We can safely iterate the networks list from registry
	// because state-sync config is not ordered specifically
	for _, chainID := range testChainRegistry.GetChains() {
		actualStateSyncConf, err := testChainRegistry.GetStateSyncConfig(chainID)
		assert.NoError(t, err, "should not error given existing ChainID")
		assert.NotNil(t, actualStateSyncConf)
		assert.IsType(t, &config.StateSyncConfig{}, actualStateSyncConf)
	}

	// Make sure invalid ChainID produce errors
	testFailCases := []string{
		"this-chainid-doesnt-exist",
		"nor-does-this-one",
		"or also  this one",
	}
	for _, failCaseChainID := range testFailCases {
		_, err := testChainRegistry.GetAddress(failCaseChainID)
		assert.Error(t, err, "should error given unknown or invalid ChainID")
	}
}

func TestMultiplexChainRegistryGetSeeds(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	testChainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(t, err, "should create chain registry from random multiplex config")

	// We can safely iterate the networks list from registry
	// because seed nodes config is not ordered specifically
	for _, chainID := range testChainRegistry.GetChains() {
		actualSeeds, err := testChainRegistry.GetSeeds(chainID)
		assert.NoError(t, err, "should not error given existing ChainID")
		assert.NotNil(t, actualSeeds)
		assert.NotEmpty(t, actualSeeds)
	}

	// Make sure invalid ChainID produce errors
	testFailCases := []string{
		"this-chainid-doesnt-exist",
		"nor-does-this-one",
		"or also  this one",
	}
	for _, failChainID := range testFailCases {
		_, err := testChainRegistry.GetAddress(failChainID)
		assert.Error(t, err, "should error given unknown or invalid ChainID")
	}
}

func TestMultiplexChainRegistryGetAddress(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	testChainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(t, err, "should create chain registry from random multiplex config")

	// Iterate the CONFIG to test for correct addresses retrievals
	for userAddress, chainIds := range nodeCfg.UserChains {
		for _, chainID := range chainIds {
			actualAddress, err := testChainRegistry.GetAddress(chainID)
			assert.NoError(t, err, "should not error given existing ChainID")
			assert.Equal(t, userAddress, actualAddress)
		}
	}

	// Make sure invalid ChainID produce errors
	testFailCases := []string{
		"this-chainid-doesnt-exist",
		"nor-does-this-one",
		"or also  this one",
	}
	for _, failChainID := range testFailCases {
		_, err := testChainRegistry.GetAddress(failChainID)
		assert.Error(t, err, "should error given unknown or invalid ChainID")
	}
}

func TestMultiplexChainRegistryFindChain(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	testChainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(t, err, "should create chain registry from random multiplex config")

	// GetChains is tested to return an alphabetically ordered slice of ChainID
	for index, chainID := range testChainRegistry.GetChains() {
		// Should return the correct index in ordered slice
		actualIndex, err := testChainRegistry.FindChain(chainID)
		assert.NoError(t, err)
		assert.Equal(t, index, actualIndex)
	}

	// Make sure invalid ChainID produce errors and return -1
	testFailCases := []string{
		"this-chainid-doesnt-exist",
		"nor-does-this-one",
		"or also  this one",
	}
	for _, failChainID := range testFailCases {
		errIndex, err := testChainRegistry.FindChain(failChainID)
		assert.Error(t, err, "should error given unknown or invalid ChainID")
		assert.Equal(t, -1, errIndex, "should return -1 given unknown or invalid ChainID")
	}
}

func TestMultiplexChainRegistryUsingGenesisDocSet(t *testing.T) {
	numChains := 3

	// STEP 1
	// Create a random genesis doc set.
	tmpfile, err := os.CreateTemp("", "genesisdocset.json")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	genDocSet := randomGenesisDocSet(numChains)
	require.Len(t, genDocSet, numChains)
	err = genDocSet.SaveAs(tmpfile.Name())
	require.NoError(t, err)

	err = tmpfile.Close()
	require.NoError(t, err)

	// TEST 1
	// Initialize a chain registry using genesis doc set file.

	emptyConf := config.MultiplexTestBaseConfig(
		map[string]string{},
		map[string][]string{},
	)
	testChainRegistry, err := mx.NewChainRegistry(&emptyConf.MultiplexConfig, tmpfile.Name())
	assert.NoError(t, err, "should not error given genesis doc set file")
	assert.Len(t, testChainRegistry.GetChains(), numChains)

	for i := 0; i < numChains; i++ {
		assert.Equal(t, true, testChainRegistry.HasChain(genDocSet[i].ChainID),
			"should contain ChainID: "+genDocSet[i].ChainID)
	}

	// TEST 2
	// Initialize a chain registry using config AND genesis doc set file.

	nonEmptyConf := config.MultiplexTestBaseConfig(
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			"test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		}},
	)
	mergedNumChains := numChains + 1

	testMergeRegistry, err := mx.NewChainRegistry(&nonEmptyConf.MultiplexConfig, tmpfile.Name())
	assert.NoError(t, err, "should not error given config and genesis doc set file")
	assert.Len(t, testMergeRegistry.GetChains(), mergedNumChains)

	for _, userChainIds := range nonEmptyConf.UserChains {
		for _, testChainID := range userChainIds {
			assert.Equal(t, true, testMergeRegistry.HasChain(testChainID),
				"missing config ChainID, should contain ChainID: "+testChainID)
		}
	}

	for i := 0; i < len(genDocSet); i++ {
		assert.Equal(t, true, testMergeRegistry.HasChain(genDocSet[i].ChainID),
			"missing genesis doc set ChainID, should contain ChainID: "+genDocSet[i].ChainID)
	}
}

func makeChainRegistryFromConfig(tb testing.TB, conf config.MultiplexConfig) mx.ChainRegistry {
	tb.Helper()

	chainRegistry, err := mx.NewChainRegistry(&conf, "")
	require.NoError(tb, err, "should create chain registry from config")

	return chainRegistry
}

func makeRandomMultiplexConfig(tb testing.TB, numChains int, discoveryPort int) config.MultiplexConfig {
	tb.Helper()

	if numChains == 0 {
		return config.MultiplexBaseConfig(
			map[string]string{},
			map[string][]string{},
		).MultiplexConfig
	}

	randomChainIDs := make([]string, numChains)
	randChainSeeds := make(map[string]string, numChains)
	randUserChains := make(map[string][]string, numChains)

	for i := 0; i < numChains; i++ {
		userPubKey := ed25519.GenPrivKey().PubKey()
		userAddress := userPubKey.Address().String()
		fingerprint := makeFingerprint("Posts") // This is the "scope"

		chainID, err := mx.NewExtendedChainID(userAddress, fingerprint)
		require.NoError(tb, err, "should create random ChainID")

		randUserChains[userAddress] = make([]string, 1)
		randUserChains[userAddress][0] = chainID.String()

		// Uses a fake (random) seed node ID
		testSeedNodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
		testSeedNodeID := testSeedNodeKey.ID()

		randomChainIDs[i] = chainID.String()
		randChainSeeds[chainID.String()] = string(testSeedNodeID) + "@127.0.0.1:30001"
	}

	return config.MultiplexConfig{
		Strategy:      mx.NetworkReplicationStrategy(),
		ChainSeeds:    randChainSeeds,
		UserChains:    randUserChains,
		DiscoveryPort: uint16(discoveryPort),
	}
}
