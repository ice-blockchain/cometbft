package multiplex_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmtrand "github.com/ice-blockchain/cometbft/internal/rand"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/protoio"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/node"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	mx "github.com/ice-blockchain/cometbft/multiplex"
)

// -----------------------------------------------------------------------------
// TestMultiplexChainRegistry

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
	chainIds := testChainRegistry.GetChains()
	for _, chainID := range chainIds {
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
	chainIds := testChainRegistry.GetChains()
	for _, chainID := range chainIds {
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
	chainIds := testChainRegistry.GetChains()
	for index, chainID := range chainIds {
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

// TODO(midas): TestMultiplexChainRegistryAddChain

// -----------------------------------------------------------------------------
// TestMultiplexConfig

func TestMultiplexConfigDefaultLegacyFallback(t *testing.T) {
	// Must use EmptyMultiplexConfig()
	conf := config.TestConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), conf.Strategy)

	// Must use EmptyMultiplexConfig()
	baseConf := config.DefaultBaseConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), baseConf.Strategy)
}

func TestMultiplexConfigMultiplexBaseConfig(t *testing.T) {
	// Must accept empty multiplex config
	conf := config.MultiplexBaseConfig(
		map[string]string{},
		map[string][]string{},
	)
	assert.Equal(t, mx.NetworkReplicationStrategy(), conf.Strategy)

	// Must accept chainSeeds
	conf = config.MultiplexBaseConfig(
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "id@host:port",
		},
		map[string][]string{},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.ChainSeeds)

	// Must accept userChains
	conf = config.MultiplexBaseConfig(
		map[string]string{},
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
			},
		},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.UserChains)
}

func TestMultiplexConfigNewConfigOverwrite(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err, "should create rootDir for tests")
	defer os.RemoveAll(rootDir)

	conf := config.TestConfig()
	conf.MultiplexConfig = makeRandomMultiplexConfig(t, 3, 30001)
	conf.SetRoot(rootDir)

	chainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)

	// first ChainID has start ports
	chainID1 := chainRegistry.GetChains()[0]
	address1, err := chainRegistry.GetAddress(chainID1)
	expectWal1 := makeWalPath(rootDir, address1, chainID1)
	require.NoError(t, err)
	cfgOverwrite1, err := mx.NewConfigOverwrite(conf, chainRegistry, chainID1)
	assert.NoError(t, err)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite1.P2P.ListenAddress)
	assert.Contains(t, cfgOverwrite1.P2P.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+1)) // :30002
	assert.Contains(t, cfgOverwrite1.RPC.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+2)) // :30003
	assert.Equal(t, expectWal1, cfgOverwrite1.Consensus.WalFile())

	// second ChainID has starts ports + 1
	chainID2 := chainRegistry.GetChains()[1]
	address2, err := chainRegistry.GetAddress(chainID2)
	assert.NoError(t, err)

	expectWal2 := makeWalPath(rootDir, address2, chainID2)
	cfgOverwrite2, err := mx.NewConfigOverwrite(conf, chainRegistry, chainID2)
	assert.NoError(t, err)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite2.P2P.ListenAddress)
	assert.Equal(t, cfgOverwrite1.P2P.ListenAddress, cfgOverwrite2.P2P.ListenAddress)
	assert.Equal(t, cfgOverwrite1.RPC.ListenAddress, cfgOverwrite2.RPC.ListenAddress)
	assert.Contains(t, cfgOverwrite2.P2P.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+1)) // :30002
	assert.Contains(t, cfgOverwrite2.RPC.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+2)) // :30003
	assert.Equal(t, expectWal2, cfgOverwrite2.Consensus.WalFile())

	// third ChainID has starts ports + 2
	chainID3 := chainRegistry.GetChains()[2]
	address3, err := chainRegistry.GetAddress(chainID3)
	assert.NoError(t, err)

	expectWal3 := makeWalPath(rootDir, address3, chainID3)
	cfgOverwrite3, err := mx.NewConfigOverwrite(conf, chainRegistry, chainID3)
	assert.NoError(t, err)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite3.P2P.ListenAddress)
	assert.Equal(t, cfgOverwrite2.P2P.ListenAddress, cfgOverwrite3.P2P.ListenAddress)
	assert.Equal(t, cfgOverwrite2.RPC.ListenAddress, cfgOverwrite3.RPC.ListenAddress)
	assert.Contains(t, cfgOverwrite3.P2P.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+1)) // :30002
	assert.Contains(t, cfgOverwrite3.RPC.ListenAddress, strconv.Itoa(int(conf.DiscoveryPort)+2)) // :30003
	assert.Equal(t, expectWal3, cfgOverwrite3.Consensus.WalFile())
}

// -----------------------------------------------------------------------------
// TestMultiplexDB

func TestMultiplexDBChainID(t *testing.T) {
	rootDir, multiplexDB := ResetMultiplexDBTestRoot(t, "test-mx-db-chain-id", 1000)
	defer os.RemoveAll(rootDir)

	assert.Equal(t, true, cmtos.FileExists(rootDir))

	for chainID, chainDB := range multiplexDB {
		assert.NotEmpty(t, chainID)
		assert.Equal(t, chainID, chainDB.ChainID)
		assert.NotNil(t, chainDB.DB)
	}
}

func TestMultiplexDBSimpleSetGet(t *testing.T) {
	numDatabases := 1000

	// Reset test fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(t, "test-multi-db-set-get-key", numDatabases)
	defer os.RemoveAll(rootDir)

	// Mocks a key-value pair
	key := []byte(`key`)
	data := []byte(`value`)

	randomizer := cmtrand.NewRand()
	dbIdx := randomizer.Intn(numDatabases)
	err := dbPtrs[dbIdx].Set(key, data)
	require.NoError(t, err)

	actual, err := dbPtrs[dbIdx].Get(key)
	require.NoError(t, err)
	assert.Equal(t, data, actual)
}

func TestMultiplexDBParallelSetGet(t *testing.T) {
	numDatabases := 1000
	rootDir, multiplexDB := ResetMultiplexDBTestRoot(t, "test-mx-db-parallel-set-get", numDatabases)
	defer os.RemoveAll(rootDir)

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	// Mocks a key-value pair
	key := []byte(`key`)
	data := []byte(`value`)

	// Use an array, not user-indexed here
	dbs := make([]*mx.ChainDB, numDatabases)
	i := 0
	for _, db := range multiplexDB {
		dbs[i] = db
		i++
	}

	t.Run("parallel", func(t *testing.T) {
		t.Parallel()

		dbIdx := randomizer.Intn(numDatabases)
		err := dbs[dbIdx].Set(key, data)
		assert.NoError(t, err)

		actual, err := dbs[dbIdx].Get(key)
		require.NoError(t, err)
		assert.Equal(t, data, actual)
	})
}

func TestMultiplexDBNewMultiplexDB(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)

	conf := config.TestConfig()

	// ----------------
	// Errors
	failCases := []string{
		"invalid-chain-id",
		"cosmoshub-4",
		"mx-chain-1234-5678",
		// invalid addresses
		"mx-chain-#000000000000000000000000000000000000000-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC4-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43AAB-1A63C0E60122F9BB",
		// invalid fingerprints
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-#000000000000000",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BBCC",
	}

	for _, failCaseChainID := range failCases {
		conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {failCaseChainID},
		})
		conf.SetRoot(rootDir)

		failChainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)
		_, err := mx.NewMultiplexDB(&mx.ChainDBContext{
			DBContext: config.DBContext{ID: "state", Config: conf},
		}, failChainRegistry.GetChains())
		assert.Error(t, err, "should forward error given invalid ChainID: "+failCaseChainID)
	}

	// ----------------
	// Successes
	exampleChains := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
	}

	conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {exampleChains[0], exampleChains[1]},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {exampleChains[2]},
	})
	conf.SetRoot(rootDir)

	testChainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)
	multiplex, err := mx.NewMultiplexDB(&mx.ChainDBContext{
		DBContext: config.DBContext{ID: "state", Config: conf},
	}, testChainRegistry.GetChains())

	assert.NoError(t, err, "should not error given valid configuration")
	assert.Len(t, multiplex, len(exampleChains), "should create correct number of databases")
	assert.Contains(t, multiplex, exampleChains[0])
	assert.Contains(t, multiplex, exampleChains[1])
	assert.Contains(t, multiplex, exampleChains[2])
	assert.Equal(t, exampleChains[0], multiplex[exampleChains[0]].ChainID)
	assert.Equal(t, exampleChains[1], multiplex[exampleChains[1]].ChainID)
	assert.Equal(t, exampleChains[2], multiplex[exampleChains[2]].ChainID)
	assert.NotNil(t, multiplex[exampleChains[0]].DB)
	assert.NotNil(t, multiplex[exampleChains[1]].DB)
	assert.NotNil(t, multiplex[exampleChains[2]].DB)
}

// -----------------------------------------------------------------------------
// TestMultiplexFS

func TestMultiplexFSDisabled(t *testing.T) {
	rootDir, conf := ResetMultiplexFSTestRoot(t, "test-mx-fs-disabled")
	defer os.RemoveAll(rootDir)

	emptyChainRegistry := makeChainRegistryFromConfig(t, config.EmptyMultiplexConfig())
	multiplex, err := mx.NewMultiplexFS(conf, emptyChainRegistry.GetChains())
	assert.NoError(t, err)
	assert.Len(t, multiplex, 1) // default disables multiplex

	// Sets empty string key to default data dir
	assert.Equal(t, config.DefaultDataDir, multiplex[""])
}

func TestMultiplexFSNewMultiplexFS(t *testing.T) {
	rootDir, conf := ResetMultiplexFSTestRoot(t, "test-mx-fs-new-multiplex-fs")
	defer os.RemoveAll(rootDir)

	// ----------------
	// Errors
	failCases := []string{
		"invalid-chain-id",
		"cosmoshub-4",
		"mx-chain-1234-5678",
		// invalid addresses
		"mx-chain-#000000000000000000000000000000000000000-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC4-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43AAB-1A63C0E60122F9BB",
		// invalid fingerprints
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-#000000000000000",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BBCC",
	}

	for _, failCaseChainID := range failCases {
		conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {failCaseChainID},
		})
		conf.SetRoot(rootDir)

		failChainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)
		_, err := mx.NewMultiplexFS(conf, failChainRegistry.GetChains())
		assert.Error(t, err, "should forward error given invalid configuration")
	}

	// ----------------
	// Successes
	exampleChains := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
	}

	conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {exampleChains[0], exampleChains[1]},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {exampleChains[2]},
	})
	conf.SetRoot(rootDir)

	testChainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)
	multiplex, err := mx.NewMultiplexFS(conf, testChainRegistry.GetChains())

	assert.NoError(t, err, "should not error given valid configuration")
	assert.Len(t, multiplex, len(exampleChains), "should create correct number of paths")
	assert.Contains(t, multiplex, exampleChains[0])
	assert.Contains(t, multiplex, exampleChains[1])
	assert.Contains(t, multiplex, exampleChains[2])
	assert.NotEmpty(t, multiplex[exampleChains[0]])
	assert.NotEmpty(t, multiplex[exampleChains[1]])
	assert.NotEmpty(t, multiplex[exampleChains[2]])
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[0]]))
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[1]]))
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[2]]))
}

// -----------------------------------------------------------------------------
// TestMultiplexGenesis

func TestMultiplexGenesisDocSetBad(t *testing.T) {
	// test some bad ones from raw json
	testCases := [][]byte{
		{},               // empty
		{1},              // junk
		[]byte(`{null}`), // invalid GenesisDocs
		[]byte(`[{}]`),   // invalid GenesisDocs
		[]byte(`[{"chain_id": "", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}]`),   // empty chain_id
		[]byte(`[{"chain_id": null, "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}]`), // nil chain_id
		[]byte(`[
			{"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}},
			{"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}
		]`), // ChainID not unique
	}

	for i, testCase := range testCases {
		_, err := mx.GenesisDocSetFromJSON(testCase)

		assert.Error(t, err, "expected error for invalid genDocSet json at "+strconv.Itoa(i))
	}

	emptyGenesisDocSet := []byte(`[]`)
	_, actualErr := mx.GenesisDocSetFromJSON(emptyGenesisDocSet)
	assert.NoError(t, actualErr, "should not error with empty genesis doc set")
}

func TestMultiplexGenesisDocSetGood(t *testing.T) {
	// test one genesis doc by raw json
	genDocSetBytes := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		}
	]`)
	_, err := mx.GenesisDocSetFromJSON(genDocSetBytes)
	assert.NoError(t, err, "expected no error for correct genesis docs set with single user from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultiple := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		},
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`)
	_, err = mx.GenesisDocSetFromJSON(genDocSetBytesMultiple)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple users from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultipleChains := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		},
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB-D1ED2B487F2E93CC",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`)
	_, err = mx.GenesisDocSetFromJSON(genDocSetBytesMultipleChains)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple chains from json")

	// create a GenesisDocSet from struct
	valPubKey := ed25519.GenPrivKey().PubKey()
	baseGenDocSet := mx.GenesisDocSet{
		{
			ChainID: "abc",
			Validators: []types.GenesisValidator{{
				Address: valPubKey.Address(),
				PubKey:  valPubKey,
				Power:   10,
				Name:    "myval",
			}},
		},
	}
	_, err = cmtjson.Marshal(baseGenDocSet)
	assert.NoError(t, err, "error marshaling genDocSet")

	// create multiple types.GenesisDoc from struct
	genDocSetDefault := randomGenesisDocSet(3)
	err = genDocSetDefault.ValidateAndComplete()
	assert.NoError(t, err, "expected no error from random correct genDocSet struct")
}

func TestMultiplexGenesisDocSetSaveAs(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "genesis")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	genDocSet := randomGenesisDocSet()

	// save
	err = genDocSet.SaveAs(tmpfile.Name())
	require.NoError(t, err)
	stat, err := tmpfile.Stat()
	require.NoError(t, err)
	if err != nil && stat.Size() <= 0 {
		t.Fatalf("SaveAs failed to write any bytes to %v", tmpfile.Name())
	}

	err = tmpfile.Close()
	require.NoError(t, err)

	// load
	genDocSet2, err := mx.GenesisDocSetFromFile(tmpfile.Name())
	require.NoError(t, err)
	assert.EqualValues(t, genDocSet2, genDocSet)
	assert.Equal(t, genDocSet2, genDocSet)
}

func TestMultiplexGenesisDocSetValidatorHash(t *testing.T) {
	genDocSet := randomGenesisDocSet()
	assert.NotEmpty(t, genDocSet.ValidatorHash())
}

func TestMultiplexGenesisDocSetSearchGenesisDocByChainID(t *testing.T) {
	// defines a correct genesis doc
	genDocSetFmt := `[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "%s",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`

	// searching for an existing ChainID
	testCases := []string{
		"abc1",
		"abc2",
	}

	for i, testChainID := range testCases {
		testCaseGenesis := fmt.Sprintf(genDocSetFmt, testChainID)
		testCaseDocSet, err := mx.GenesisDocSetFromJSON([]byte(testCaseGenesis))
		require.NoError(t, err)
		assert.Equal(t, 1, len(testCaseDocSet))

		doc, ok, err := testCaseDocSet.SearchGenesisDocByChainID(testChainID)
		assert.NoError(t, err)
		assert.Equal(t, true, ok, "ChainID should be found at "+strconv.Itoa(i))
		assert.Equal(t, testChainID, doc.ChainID)
	}

	// searching for non-existing ChainID
	testCases = []string{
		"abc3",
		"abc4",
	}

	failCaseGenesis := fmt.Sprintf(genDocSetFmt, "not-abc")
	failCaseDocSet, err := mx.GenesisDocSetFromJSON([]byte(failCaseGenesis))
	require.NoError(t, err)

	for _, failCaseChainID := range testCases {
		doc, ok, err := failCaseDocSet.SearchGenesisDocByChainID(failCaseChainID)
		assert.NoError(t, err)
		assert.Equal(t, false, ok)
		assert.Empty(t, doc)
	}
}

func TestMultiplexGenesisDocSetValidateGenesisDocChecksum(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	genesisDocSet := randomGenesisDocSet(3)
	for i, genesisDoc := range genesisDocSet {
		dbName := "genesis-" + strconv.Itoa(i)
		db, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)

		genesisDocJSON, err := cmtjson.Marshal(genesisDoc)
		require.NoError(t, err)
		expectedSha256 := tmhash.Sum(genesisDocJSON)

		err = mx.ValidateGenesisDocChecksum(
			&mx.ChainDB{
				ChainID: "test",
				DB:      db,
			},
			&genesisDoc,
		)
		assert.NoError(t, err)

		// Should save the genesis doc hash in db
		actualSha256, err := db.Get(genesisDocHashKey)
		assert.NoError(t, err)
		assert.Equal(t, true, bytes.Equal(expectedSha256, actualSha256))
	}
}

func TestMultiplexGenesisDocFromChainParams(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Test with one validator
	genValidator := ed25519.GenPrivKey()
	cmtConsensus := types.DefaultConsensusParams().ToProto()

	cmtValidators, err := types.NewValidatorSet([]*types.Validator{
		types.NewValidator(genValidator.PubKey(), 10),
	}).ToProto()
	assert.NoError(t, err)
	assert.NotNil(t, cmtValidators)

	chainParams := &mxp2p.ChainParams{
		GenesisTime:     cmttime.Now(),
		ChainID:         "genesis-test1",
		InitialHeight:   1,
		ConsensusParams: &cmtConsensus,
		Validators:      *cmtValidators,
		AppHash:         []byte{1, 2, 3},
		AppStateBytes:   []byte("data"),
	}

	actualGenesisDoc, err := mx.GenesisDocFromChainParams(chainParams)
	assert.NoError(t, err, "should format ChainParams as GenesisDoc")
	assert.NotNil(t, actualGenesisDoc.GenesisTime)
	assert.Equal(t, "genesis-test1", actualGenesisDoc.ChainID)
	assert.NotEmpty(t, actualGenesisDoc.AppHash)
	assert.NotEmpty(t, actualGenesisDoc.AppState)

	// Test with many validators and mutate consensus params
	numValidators := 7
	validatorSetMany := types.NewValidatorSet([]*types.Validator{
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
	})
	cmtValidatorsMany, err := validatorSetMany.ToProto()
	assert.NoError(t, err)
	assert.NotNil(t, cmtValidatorsMany)
	assert.Len(t, cmtValidatorsMany.Validators, numValidators)

	expectedConsensusChange := int64(123456789)
	expectedInitialHeight := int64(101)
	expectedAppVersion := uint64(123)

	newConsensus := types.DefaultConsensusParams()
	newConsensus.Block.MaxBytes = expectedConsensusChange
	newConsensus.Block.MaxGas = expectedConsensusChange
	newConsensus.Version.App = expectedAppVersion
	cmtNewConsensus := newConsensus.ToProto()

	chainParamsMany := &mxp2p.ChainParams{
		GenesisTime:     cmttime.Now(),
		ChainID:         "genesis-test2",
		InitialHeight:   expectedInitialHeight,
		ConsensusParams: &cmtNewConsensus,
		Validators:      *cmtValidatorsMany,
		AppHash:         []byte{3, 2, 1},
		AppStateBytes:   []byte("data2"),
	}

	actualGenesisDoc2, err := mx.GenesisDocFromChainParams(chainParamsMany)
	assert.NoError(t, err, "should format ChainParams as GenesisDoc")
	assert.NotNil(t, actualGenesisDoc2.GenesisTime)
	assert.Equal(t, "genesis-test2", actualGenesisDoc2.ChainID)
	assert.Equal(t, expectedInitialHeight, actualGenesisDoc2.InitialHeight)
	assert.Len(t, actualGenesisDoc2.Validators, numValidators)
	assert.NotEmpty(t, actualGenesisDoc2.AppHash)
	assert.NotEmpty(t, actualGenesisDoc2.AppState)

	actualConsensusParams := actualGenesisDoc2.ConsensusParams
	assert.Equal(t, expectedConsensusChange, actualConsensusParams.Block.MaxBytes)
	assert.Equal(t, expectedConsensusChange, actualConsensusParams.Block.MaxGas)
	assert.Equal(t, expectedAppVersion, actualConsensusParams.Version.App)
}

// -----------------------------------------------------------------------------
// TestMultiplexReactor

func TestMultiplexReactorNewReactor(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeKey := makeRandomNodeKey()
	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(t, err, "should create chain registry instance")

	// ----------------
	// Errors
	// Should panic given an error when executing the GenesisDocProvider.
	assert.Panics(t, func() {
		mx.NewReactor(
			t.Context(),
			nodeKey,
			nodeCfg,
			cmtlog.NewNopLogger(),
			chainRegistry,
			mockErrorGenesisDocSetProviderFunc(), // error!
		)
	}, "should panic given an error with GenesisDocProvider")

	// ----------------
	// Successes

	// Test a successful configuration of a Reactor
	reactor := mx.NewReactor(
		t.Context(),
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		mockGenesisDocSetProviderFunc(),
	)

	// NewReactor may not return nil
	assert.NotNil(t, reactor)

	defer setReactorNodesStopsServers(reactor)

	testChainIds := reactor.GetNetworks()

	// Networks must be ordered
	// ChainRegistry.GetChains() is tested to produce an ordered slice.
	assert.NotEmpty(t, testChainIds)
	regChainIds := chainRegistry.GetChains()
	for i, testChainID := range regChainIds {
		actualChainID := testChainIds[i]
		assert.Equal(t, testChainID, actualChainID)
	}

	// NewReactor must initialize providers
	assert.NotNil(t, reactor.GetGenesisProvider())
	assert.NotNil(t, reactor.GetServicesProvider())
	assert.NotNil(t, reactor.GetMultiplexProvider())
}

func TestMultiplexReactorRegisterService(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	// Create a test reactor
	reactor := makeTestReactor(t, nodeCfg)
	defer setReactorNodesStopsServers(reactor)

	// Create services per chain
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			// Test registering a valid service
			eventBus := types.NewEventBus(t.Context())
			reactor.RegisterService(mx.ServiceKeyEventBus, chainID, eventBus)
		}
	}

	// And retrieve to assert
	servicesProvider := reactor.GetServicesProvider()
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			// Type-assertion to cast back to actual service structure
			eventBus := servicesProvider(mx.ServiceKeyEventBus, chainID).(*types.EventBus)

			assert.NotNil(t, eventBus, "service provider should return service instance")
			assert.IsType(t, &types.EventBus{}, eventBus)
		}
	}

	// ------------------------------------------------
	// RESET reactor
	otherReactor := makeTestReactor(t, nodeCfg)
	defer setReactorNodesStopsServers(otherReactor)

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			wg.Add(1)
			go func(concurrentChainID string) {
				// Test registering a valid service in parallel goroutine
				eventBus := types.NewEventBus(t.Context())
				otherReactor.RegisterService(mx.ServiceKeyEventBus, concurrentChainID, eventBus)

				wg.Done()
			}(chainID)
		}
	}

	wg.Wait()

	// And retrieve to assert
	otherServicesProvider := otherReactor.GetServicesProvider()
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			// Type-assertion to cast back to actual service structure
			eventBus := otherServicesProvider(mx.ServiceKeyEventBus, chainID).(*types.EventBus)

			assert.NotNil(t, eventBus, "service provider should return service instance")
			assert.IsType(t, &types.EventBus{}, eventBus)
		}
	}
}

func TestMultiplexReactorRegisterInstance(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 5, 30001) // 5 distinct networks

	// Create a test reactor
	reactor := makeTestReactor(t, nodeCfg)
	defer setReactorNodesStopsServers(reactor)

	testDBKey := []byte(`testChainId`)

	// Create instances per chain
	// Using ORDERED networks because of ports overwrite content test
	testChainIds := reactor.GetNetworks()
	for index, chainID := range testChainIds {
		// 1. We create a mutated config per chain
		perChainCfg, err := mx.NewConfigOverwrite(
			nodeCfg,
			reactor.GetChainRegistry(),
			chainID,
		)
		require.NoError(t, err)

		// 2. We create a database instance per chain
		dbName := "chaindb-" + strconv.Itoa(index)
		perChainDB, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)
		// .. and add some data to it
		err = perChainDB.SetSync(testDBKey, []byte(chainID))
		require.NoError(t, err)

		// Test registering a valid instance
		reactor.RegisterInstance(mx.InstanceKeyConfig, chainID, perChainCfg) // "config"
		reactor.RegisterInstance(mx.ServiceKeyDatabaseState, chainID, &mx.ChainDB{
			ChainID: chainID,
			DB:      perChainDB,
		}) // "database/state"
	}

	// And retrieve to assert
	configProvider := reactor.GetInstanceProvider(mx.InstanceKeyConfig)
	assert.NotNil(t, configProvider, "should return multiplex map of config instances")

	databaseProvider := reactor.GetInstanceProvider(mx.ServiceKeyDatabaseState)
	assert.NotNil(t, databaseProvider, "should return multiplex map of database instances")

	// Using ORDERED networks because of ports overwrite content test
	for _, chainID := range testChainIds {
		// 1. Type-assertion to cast back to actual instance
		perChainCfg := configProvider(chainID).(*config.Config)

		assert.NotNil(t, perChainCfg, "instance provider should return instance")
		assert.IsType(t, &config.Config{}, perChainCfg)

		// 2. Also do some asserts about the DB instance stored
		perChainDB := databaseProvider(chainID).(*mx.ChainDB)

		assert.NotNil(t, perChainDB, "instance provide should return instance")
		assert.IsType(t, &mx.ChainDB{}, perChainDB)

		actualChainID, err := perChainDB.DB.Get(testDBKey)
		assert.NoError(t, err, "should retrieve key from database")
		assert.Equal(t, []byte(chainID), actualChainID)
	}

	// ------------------------------------------------
	// RESET reactor
	otherReactor := makeTestReactor(t, nodeCfg)
	defer setReactorNodesStopsServers(otherReactor)

	otherChainIds := otherReactor.GetNetworks()

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for index, chainID := range otherChainIds {
		wg.Add(1)
		go func(idx int, concurrentChainID string) {
			// 1. We create a mutated config per chain
			perChainCfg, err := mx.NewConfigOverwrite(
				nodeCfg,
				otherReactor.GetChainRegistry(),
				concurrentChainID,
			)
			require.NoError(t, err)

			// 2. We create a database instance per chain
			dbName := "chaindb-" + strconv.Itoa(idx)
			perChainDB, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
			require.NoError(t, err)
			// .. and add some data to it
			err = perChainDB.SetSync(testDBKey, []byte(concurrentChainID))
			require.NoError(t, err)

			// Test registering a valid instance in parallel goroutines
			otherReactor.RegisterInstance(mx.InstanceKeyConfig, concurrentChainID, perChainCfg) // "config"
			otherReactor.RegisterInstance(mx.ServiceKeyDatabaseState, concurrentChainID, &mx.ChainDB{
				ChainID: concurrentChainID,
				DB:      perChainDB,
			}) // "database/state"

			wg.Done()
		}(index, chainID)
	}

	wg.Wait()

	// And retrieve to assert
	otherConfigProvider := reactor.GetInstanceProvider(mx.InstanceKeyConfig)
	assert.NotNil(t, otherConfigProvider, "should return multiplex map of config instances")

	otherDatabaseProvider := reactor.GetInstanceProvider(mx.ServiceKeyDatabaseState)
	assert.NotNil(t, otherDatabaseProvider, "should return multiplex map of database instances")
	for _, chainID := range otherChainIds {
		// 1. Type-assertion to cast back to actual instance
		perChainCfg := otherConfigProvider(chainID).(*config.Config)

		assert.NotNil(t, perChainCfg, "instance provider should return instance")
		assert.IsType(t, &config.Config{}, perChainCfg)

		// 2. Also do some asserts about the DB instance stored
		perChainDB := otherDatabaseProvider(chainID).(*mx.ChainDB)

		assert.NotNil(t, perChainDB, "instance provide should return instance")
		assert.IsType(t, &mx.ChainDB{}, perChainDB)

		actualChainID, err := perChainDB.DB.Get(testDBKey)
		assert.NoError(t, err, "should retrieve key from database")
		assert.Equal(t, []byte(chainID), actualChainID)
	}
}

func TestMultiplexReactorRegisterNetwork(t *testing.T) {

	numNetworks := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	testExtChainID,
		testReactor,
		shutdownFn := ResetTestMultiplexReactorRuntimeWithInjection(t, numNetworks, cmtlog.NewNopLogger())
	defer func() {
		shutdownFn(testReactor)
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()
	testUserAddress := testExtChainID.GetUserAddress()
	testChainID := testExtChainID.String()

	// Act
	registerErr := testReactor.RegisterNetwork(testUserAddress, testChainID)
	assert.NoError(t, registerErr, "should register network in running reactor")

	// Test that we injected the ChainID
	testChainIds := testReactor.GetNetworks()
	assert.Len(t, testChainIds, numNetworks+1) // Injected 1
	assert.Equal(t, true, testReactor.HasNetwork(testChainID))

	// Also test that we updated MultiNetworkNodeInfo
	testMultiNetNodeInfo := testReactor.GetMultiNetworkNodeInfo()
	assert.NotNil(t, testMultiNetNodeInfo)

	actualChainNodeInfo, err := testMultiNetNodeInfo.GetNodeInfo(testChainID)
	assert.NoError(t, err)
	assert.NotNil(t, actualChainNodeInfo)
	assert.Equal(t, testChainID, actualChainNodeInfo.Network)
}

func TestMultiplexReactorUpdatedGenesisDocProvider(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChainsToInject := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	testExtChainID,
		testReactor,
		shutdownFn := ResetTestMultiplexReactorRuntimeWithInjection(t, numChainsToInject, cmtlog.NewNopLogger())

	// Shutdown routine
	defer shutdownFn(testReactor)

	newTestChainID := testExtChainID.String()

	// Make sure that calling the GenesisProvider returns an up-to-date
	// GenesisDocSet that contains the injected network
	testGenesisDocsProvider := testReactor.GetGenesisProvider()
	assert.NotNil(t, testGenesisDocsProvider)

	// Tests that it includes the newly injected ChainID
	testGenesisDoc, providerErr := testGenesisDocsProvider(newTestChainID)
	assert.NoError(t, providerErr)
	assert.NotNil(t, testGenesisDoc)
	assert.Equal(t, newTestChainID, testGenesisDoc.ChainID)

	// Sanity check, do we still error for unknown ChainID?!
	errGenesisDoc, testProviderErr := testGenesisDocsProvider("test-chain-1")
	assert.Error(t, testProviderErr)
	assert.Nil(t, errGenesisDoc)
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorConsensus

func TestMultiplexReactorConsensusPrepareConsensusInstanceWithReactor(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		assert.NoError(t, err)
	}()

	testChainIds := reactor.GetNetworks()

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		t.Context(),
		testChainIds,
		proxy.DefaultClientCreator(t.Context(), globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.PrometheusMetrics(globalCfg.Instrumentation.Namespace+"_"+string(reactor.GetNodeKey().ID())),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Also setup node listeners, this mimics a runtime allocation.
	for _, chainID := range testChainIds {
		allocErr := reactor.AllocateNetwork(chainID)
		require.NoError(t, allocErr, "should initialize network")

		createErr := reactor.InjectNewNetwork(chainID, []string{})
		require.NoError(t, createErr, "should inject network")

		injectErr := reactor.InjectNewRuntime(t.Context(), chainID)
		require.NoError(t, injectErr, "should inject runtime")
	}

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range testChainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(t.Context(), chainID)
		assert.NoError(t, err, "should not error for consensus handshake")
	}
}

func TestMultiplexReactorConsensusCreateConsensusInstanceReactors(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		assert.NoError(t, err)
	}()

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	// Start an ABCI client
	testChainIds := reactor.GetNetworks()
	abciClient := proxy.NewMultiplexAppConn(
		t.Context(),
		testChainIds,
		proxy.DefaultClientCreator(t.Context(), globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Also setup node listeners, this mimics a runtime allocation.
	for _, chainID := range testChainIds {
		allocErr := reactor.AllocateNetwork(chainID)
		require.NoError(t, allocErr, "should initialize network")

		createErr := reactor.InjectNewNetwork(chainID, []string{})
		require.NoError(t, createErr, "should inject network")

		injectErr := reactor.InjectNewRuntime(context.Background(), chainID)
		require.NoError(t, injectErr, "should inject runtime")
	}

	// Uses to retrieve reactors per network
	servicesProvider := reactor.GetServicesProvider()
	require.NotNil(t, servicesProvider, "services provider must not be nil")

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range testChainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		assert.NoError(t, err, "should not error for consensus handshake")

		// Test with blockSync=true
		blockSync := true
		err = reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainID,
			blockSync,
			false, // waitSync
		)
		assert.NoError(t, err, "should not error creating consensus reactors")

		// Type-assertions make sure we have correct reactors set.
		testMempoolReactor := servicesProvider(mx.ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		testBlockSyncReactor := servicesProvider(mx.ServiceKeyBlockSyncReactor, chainID).(*blocksync.Reactor)
		testConsensusReactor := servicesProvider(mx.ServiceKeyConsensusReactor, chainID).(*cs.Reactor)
		testEvidenceReactor := servicesProvider(mx.ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor)

		// Also make sure we have actual instances, not nil
		assert.NotNil(t, testMempoolReactor, "mempool reactor must not be nil")
		assert.NotNil(t, testBlockSyncReactor, "blockSync reactor must not be nil")
		assert.NotNil(t, testConsensusReactor, "consensus reactor must not be nil")
		assert.NotNil(t, testEvidenceReactor, "evidence reactor must not be nil")
	}
}

// TODO(midas): TestMultiplexReactorConsensusStartConsensusInstanceReactors
// TODO(midas): TestMultiplexReactorConsensusStopConsensusInstanceReactors

// -----------------------------------------------------------------------------
// TestMultiplexReactorState

func TestMultiplexReactorStateInitMultiplexStatesEmptyState(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.ServiceKeyDatabaseState) // Uses database/state
	defer os.RemoveAll(rootDir)

	// This test does not start the reactor, we must stop dbs manually.
	defer func() {
		servicesProvider := reactor.GetServicesProvider()
		for _, chainID := range reactor.GetNetworks() {
			dbService := servicesProvider(mx.ServiceKeyDatabaseState, chainID)
			dbService.Stop()
		}
	}()

	// InitMultiplexStates() uses `ValidateGenesisDocChecksum()` which reads
	// a genesisDocHashKey from the database. This following loop sets up a
	// mock environment, where odd chain indexes *do not* have a hash in db
	// and where even chain indexes *do* have a *valid* hash in db.
	// This permits to test the Checksum validation feature.
	testChainIds := reactor.GetNetworks()
	servicesProvider := reactor.GetServicesProvider()
	for index, chainID := range testChainIds {
		// odd indexes do NOT have genesisDocHashKey set
		// this means that InitMultiplexStates will set it
		if index%2 != 0 {
			continue
		}

		databaseService := servicesProvider(mx.ServiceKeyDatabaseState, chainID)
		assert.NotNil(t, databaseService, "should return multiplex map of database instances")

		stateDB := databaseService.(*mx.DBService).DB()
		assert.NotNil(t, stateDB, "should return valid ChainDB per ChainID")

		// even indexes do have genesisDocHashKey set
		// it must match the genesisDoc`s SHA256 hash
		genesisDocProvider := reactor.GetGenesisProvider()
		genesisDoc, err := genesisDocProvider(chainID)
		assert.NoError(t, err, "should return genesis doc per ChainID")
		genesisDocJSON, err := cmtjson.Marshal(genesisDoc)
		assert.NoError(t, err, "should marshal genesis doc to JSON")
		genesisDocHash := tmhash.Sum(genesisDocJSON)
		err = stateDB.SetSync(genesisDocHashKey, genesisDocHash)
		assert.NoError(t, err, "should store genesis doc hash in db")
	}

	// Execute the method being tested
	err := reactor.InitMultiplexStates(testChainIds)
	assert.NoError(t, err, "should not error given empty state in database")

	statesProvider := reactor.GetInstanceProvider(mx.InstanceKeyState)
	assert.NotNil(t, statesProvider, "should not error getting states provider")

	// Do we have all state instances, with correct ChainID?
	for _, chainID := range testChainIds {
		chainState := statesProvider(chainID).(sm.State)
		assert.NotNil(t, chainState, "state instance per chain must not be nil")

		// And validate the ChainID
		assert.Equal(t, chainID, chainState.ChainID)

		// Test that we also have a genesisDocHashKey filled now
		databaseService := servicesProvider(mx.ServiceKeyDatabaseState, chainID)
		assert.NotNil(t, databaseService, "should return multiplex map of database instances")
		stateDB := databaseService.(*mx.DBService).DB()
		assert.NotNil(t, stateDB, "should return valid ChainDB per ChainID")

		genesisDocHash, err := stateDB.Get(genesisDocHashKey)
		assert.NoError(t, err)
		assert.NotEmpty(t, genesisDocHash)
		assert.Len(t, genesisDocHash, tmhash.Size)
	}
}

func TestMultiplexReactorStateInitMultiplexStatesFilledState(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.ServiceKeyDatabaseState) // Uses database/state
	defer os.RemoveAll(rootDir)

	// This test does not start the reactor, we must stop dbs manually.
	defer func() {
		servicesProvider := reactor.GetServicesProvider()
		for _, chainID := range reactor.GetNetworks() {
			dbService := servicesProvider(mx.ServiceKeyDatabaseState, chainID)
			dbService.Stop()
		}
	}()

	genesisDocProvider := reactor.GetGenesisProvider()
	servicesProvider := reactor.GetServicesProvider()
	require.NotNil(t, servicesProvider, "should return multiplex map of database instances")

	testChainBlockHeight := int64(123)

	// Pre-populate the State store with some testable data
	testChainIds := reactor.GetNetworks()
	for _, chainID := range testChainIds {
		genesisDoc, err := genesisDocProvider(chainID)
		require.NoError(t, err, "should load genesis doc by ChainID")
		stateDB := servicesProvider(mx.ServiceKeyDatabaseState, chainID).(*mx.DBService).DB()
		require.NotNil(t, stateDB, "should return valid ChainDB per ChainID")

		customState, err := sm.MakeGenesisState(genesisDoc)
		require.NoError(t, err, "should create state from genesis doc")

		// Validators are required for "sm.State" post block height 1
		validators := make([]*types.Validator, len(genesisDoc.Validators))
		for i, genDocValidator := range genesisDoc.Validators {
			validators[i] = types.NewValidator(genDocValidator.PubKey, genDocValidator.Power)
		}

		customState.Validators = types.NewValidatorSet(validators)
		customState.LastValidators = customState.Validators
		customState.NextValidators = customState.Validators

		// mutate state with testable data
		customState.LastBlockHeight = testChainBlockHeight // mutating Height
		customState.LastBlockID = types.BlockID{}
		customState.AppHash = tmhash.Sum([]byte(chainID)) // mutating AppHash
		// TODO(midas): Fill customState.Data with custom data

		// CAUTION: we inject a custom State here
		err = stateDB.SetSync(stateKey, customState.Bytes())
		require.NoError(t, err, "should update state instance in database")
	}

	// Execute the method being tested
	err := reactor.InitMultiplexStates(testChainIds)
	assert.NoError(t, err, "should not error given filled state in database")

	statesProvider := reactor.GetInstanceProvider(mx.InstanceKeyState)
	assert.NotNil(t, statesProvider, "should not error getting states provider")

	// Do we have all state instances, with correct ChainID?
	for _, chainID := range testChainIds {
		chainState := statesProvider(chainID).(sm.State)
		assert.NotNil(t, chainState, "state instance per chain must not be nil")

		// And validate the loaded state instance contains our
		// mutated values with correct correspondence.
		assert.Equal(t, chainID, chainState.ChainID)
		assert.Equal(t, testChainBlockHeight, chainState.LastBlockHeight) // mutated height

		expectedSha256 := tmhash.Sum([]byte(chainID))
		assert.Equal(t, expectedSha256, chainState.AppHash) // mutated AppHash
	}
}

func TestMultiplexReactorStateInitMultiplexBlockStores(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexState(t, numChains, mx.ServiceKeyDatabaseBlock) // Uses database/blockStore
	defer os.RemoveAll(rootDir)

	// This test does not start the reactor, we must stop dbs manually.
	defer func() {
		servicesProvider := reactor.GetServicesProvider()
		for _, chainID := range reactor.GetNetworks() {
			dbService := servicesProvider(mx.ServiceKeyDatabaseBlock, chainID)
			dbService.Stop()
		}
	}()

	testChainIds := reactor.GetNetworks()

	// Execute the method being tested
	err := reactor.InitMultiplexBlockStores(testChainIds)
	assert.NoError(t, err, "should not error given empty state in database")

	blockStoresProvider := reactor.GetInstanceProvider(mx.InstanceKeyBlockStore)
	assert.NotNil(t, blockStoresProvider, "should not error getting states provider")

	// Do we have all blockStore databases?
	for _, chainID := range testChainIds {
		// Type-assertion makes sure we have correct type
		chainBlockStore := blockStoresProvider(chainID).(*bs.BlockStore)
		assert.NotNil(t, chainBlockStore, "blockStore instance per chain must not be nil")
		assert.IsType(t, &bs.BlockStore{}, chainBlockStore)
	}
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorP2P

func TestMultiplexReactorP2PCreateTransportSwitchesWithReactors(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexP2P(t, numChains)
	defer func() {
		defer os.RemoveAll(rootDir)

		// Since we are not using Backend, we must shutdown servers.
		setReactorNodesStopsServers(reactor)

		err := reactor.Stop()
		require.NoError(t, err)
	}()

	testChainIds := reactor.GetNetworks()

	// Requires correct MultiNetworkNodeInfo
	_, infoErr := reactor.MakeMultiNetworkNodeInfo()
	require.NoError(t, infoErr)

	// Should create [p2p.MultiplexTransport] instances
	err := reactor.CreateTransportSwitchesWithReactors(context.TODO(), testChainIds)
	assert.NoError(t, err, "should not error creating transports and switches")

	assert.NotNil(t, reactor.GetEventSwitchForCometBFT())
	assert.NotNil(t, reactor.GetTransportForCometBFT())

	testSwitch := reactor.GetEventSwitchForCometBFT()
	testTransport := reactor.GetTransportForCometBFT()
	assert.NotNil(t, testTransport.NetAddress())

	for _, chainID := range testChainIds {
		testReactors := testSwitch.Reactors(chainID)
		assert.Len(t, testReactors, 5) // mempool, blocksync, consensus, evidence, pex
		assert.Contains(t, testReactors, "MEMPOOL")
		assert.Contains(t, testReactors, "BLOCKSYNC")
		assert.Contains(t, testReactors, "CONSENSUS")
		assert.Contains(t, testReactors, "EVIDENCE")
		assert.Contains(t, testReactors, "PEX")

		assert.NotNil(t, testSwitch.Reactor(chainID, "MEMPOOL"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "BLOCKSYNC"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "CONSENSUS"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "EVIDENCE"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "PEX"))
	}
}

func TestMultiplexReactorP2PCreateAddressBooks(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, _, reactor := ResetTestMultiplexP2P(t, numChains)
	defer func() {
		defer os.RemoveAll(rootDir)

		// Since we are not using Backend, we must shutdown servers.
		setReactorNodesStopsServers(reactor)

		err := reactor.Stop()
		require.NoError(t, err)
	}()

	testChainIds := reactor.GetNetworks()

	// Requires correct MultiNetworkNodeInfo
	_, infoErr := reactor.MakeMultiNetworkNodeInfo()
	require.NoError(t, infoErr)

	err := reactor.CreateTransportSwitchesWithReactors(context.TODO(), testChainIds)
	require.NoError(t, err, "should not error creating transports and switches")

	// Should create [p2p.pex.AddrBook] instances
	err = reactor.CreateAddressBooks(context.TODO(), testChainIds)
	assert.NoError(t, err, "should not error creating pex address books")

	// Should set the AddrBook on [p2p.Switch]
	// switchesProvider := reactor.GetInstanceProvider(mx.InstanceKeyP2PSwitch)
	// assert.NotNil(t, switchesProvider, "event switch provider must not be nil")

	assert.NotNil(t, reactor.GetEventSwitchForCometBFT())
	testSwitch := reactor.GetEventSwitchForCometBFT()

	for _, chainID := range testChainIds {
		// eventSwitch := switchesProvider(chainID).(*p2p.Switch)
		// assert.NotNil(t, eventSwitch)

		// Do we have the PEX and AddrBook?
		testReactors := testSwitch.Reactors(chainID)
		assert.Contains(t, testReactors, "PEX")
		assert.NotNil(t, testSwitch.GetAddrBook())
	}
}

func TestMultiplexReactorP2PHandshake(t *testing.T) {
	defer goleak.VerifyNone(t)

	ln, err := net.Listen("tcp", "127.0.0.1:0") // with 0, OS picks free port
	if err != nil {
		t.Fatal(err)
	}

	var (
		peerPV       = ed25519.GenPrivKey()
		peerNodeInfo = testNodeInfo(p2p.PubKeyToID(peerPV.PubKey()), defaultNodeName).(*mx.MultiNetworkNodeInfo)
	)

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}

		go func(c net.Conn) {
			niProto, err := peerNodeInfo.ToProto()
			if err != nil {
				t.Error(err)
				return
			}
			_, err = protoio.NewDelimitedWriter(c).WriteMsg(niProto)
			if err != nil {
				t.Error(err)
			}
		}(c)
		go func(c net.Conn) {
			var pbni mxp2p.MultiNetworkNodeInfo

			protoReader := protoio.NewDelimitedReader(c, p2p.MaxNodeInfoSize())
			_, err := protoReader.ReadMsg(&pbni)
			if err != nil {
				t.Error(err)
			}

			_, err = mx.MultiNetworkNodeInfoFromProto(&pbni)
			if err != nil {
				t.Error(err)
			}
		}(c)
	}()

	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	ni, err := mx.MultiplexTransportHandshake(c, 20*time.Millisecond, &mx.MultiNetworkNodeInfo{})
	if err != nil {
		t.Fatal(err)
	}

	if have, want := ni, peerNodeInfo; !reflect.DeepEqual(have, want) {
		t.Errorf("have %v, want %v", have, want)
	}
}

func TestMultiplexReactorP2PMultiNetworkNodeInfoValidate(t *testing.T) {
	// empty fails
	ni := &mx.MultiNetworkNodeInfo{}
	require.Error(t, ni.Validate())

	maxNumChannels := p2p.MaxNumChannels()

	channels := make([]byte, maxNumChannels)
	for i := 0; i < maxNumChannels; i++ {
		channels[i] = byte(i)
	}
	dupChannels := make([]byte, 5)
	copy(dupChannels, channels[:5])
	dupChannels = append(dupChannels, testCh) //nolint:makezero // huge errors when we don't do it the "wrong" way

	nonASCII := "¢§µ"
	emptyTab := "\t"
	emptySpace := "  "

	testCases := []struct {
		testName         string
		malleateNodeInfo func(*mx.MultiNetworkNodeInfo)
		expectErr        bool
	}{
		{
			"Too Many Channels",
			func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(append(channels, byte(maxNumChannels))) }, //nolint: makezero
			true,
		},
		{"Duplicate Channel", func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(dupChannels) }, true},
		{"Good Channels", func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(ni.Channels[:5]) }, false},

		{"Invalid NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.SetListenAddr("not-an-address") }, true},
		{"Good NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.SetListenAddr("0.0.0.0:26656") }, false},

		{"Non-ASCII Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(nonASCII) }, true},
		{"Empty tab Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(emptyTab) }, true},
		{"Empty space Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(emptySpace) }, true},
		{"Empty Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion("") }, false},

		{"Non-ASCII Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(nonASCII) }, true},
		{"Empty tab Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(emptyTab) }, true},
		{"Empty space Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(emptySpace) }, true},
		{"Empty Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker("") }, true},
		{"Good Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker("hey its me") }, false},

		{"Non-ASCII TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, nonASCII) }, true},
		{"Empty tab TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, emptyTab) }, true},
		{"Empty space TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, emptySpace) }, true},
		{"Empty TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, "") }, false},
		{"Off TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, "off") }, false},

		{"Non-ASCII RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, nonASCII) }, true},
		{"Empty tab RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, emptyTab) }, true},
		{"Empty space RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, emptySpace) }, true},
		{"Empty RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, "") }, false},
		{"Good RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, "0.0.0.0:26657") }, false},
	}

	nodeKey := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	// test case passes
	ni = testNodeInfo(nodeKey.ID(), name).(*mx.MultiNetworkNodeInfo)
	ni.SetChannels(channels)
	require.NoError(t, ni.Validate())

	for i, tc := range testCases {
		ni := testNodeInfo(nodeKey.ID(), name).(*mx.MultiNetworkNodeInfo)
		ni.SetChannels(channels)
		tc.malleateNodeInfo(ni)
		err := ni.Validate()
		if tc.expectErr {
			require.Error(t, err, fmt.Sprintf(tc.testName+" should error at %d", i))
		} else {
			require.NoError(t, err, tc.testName)
		}
	}
}

func TestMultiplexReactorP2PMultiNetworkNodeInfoCompatible(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodeKey1 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeKey2 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	var newTestChannel byte = 0x2

	// test NodeInfo is compatible
	ni1 := testNodeInfo(nodeKey1.ID(), name).(*mx.MultiNetworkNodeInfo)
	ni2 := testNodeInfo(nodeKey2.ID(), name).(*mx.MultiNetworkNodeInfo)
	require.NoError(t, ni1.CompatibleWith(ni2))

	// add another channel; still compatible
	ni2.Channels = append(ni2.Channels, newTestChannel)
	assert.True(t, ni2.HasChannel(newTestChannel))
	require.NoError(t, ni1.CompatibleWith(ni2))

	// wrong NodeInfo type is not compatible
	_, netAddr := p2p.CreateRoutableAddr()
	ni3 := p2p.NewMockNodeInfo(netAddr)
	require.Error(t, ni1.CompatibleWith(ni3))

	testCases := []struct {
		testName         string
		malleateNodeInfo func(*mx.MultiNetworkNodeInfo)
	}{
		{"Wrong block version", func(ni *mx.MultiNetworkNodeInfo) { ni.ProtocolVersions[0].Block++ }},
		{"No common channels", func(ni *mx.MultiNetworkNodeInfo) { ni.Channels = []byte{newTestChannel} }},
	}

	for i, tc := range testCases {
		ni := testNodeInfo(nodeKey2.ID(), name).(*mx.MultiNetworkNodeInfo)
		tc.malleateNodeInfo(ni)
		require.Error(t, ni1.CompatibleWith(ni), fmt.Sprintf("should error at %d", i))
	}
}

// TODO(midas): TestMultiplexReactorP2PCreateOrLoadCometBFTEventSwitch
// TODO(midas): TestMultiplexReactorP2PMakeMultiNetworkNodeInfo
// TODO(midas): TestMultiplexReactorP2PAddConnectionChannels
// TODO(midas): TestMultiplexReactorP2PRemoveConnectionChannels

// -----------------------------------------------------------------------------
// TestMultiplexReactorRuntime

func TestMultiplexRuntimeMakeNetworkFilesystem(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = config.MultiplexConfig{
		Strategy: mx.NetworkReplicationStrategy(),
		// empty networks list
	}

	// Create a test reactor
	testReactor := makeTestReactor(t, nodeCfg)
	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	perUserFolder := "/" + testAddress + "/"

	// Act
	actualConfDir,
		actualDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	assert.NoError(t, err, "should create network filesystem")
	assert.NotEmpty(t, actualConfDir)
	assert.NotEmpty(t, actualDataDir)
	assert.Equal(t, true, cmtos.FileExists(actualConfDir))
	assert.Equal(t, true, cmtos.FileExists(actualDataDir))
	assert.Contains(t, actualConfDir, perUserFolder)
	assert.Contains(t, actualDataDir, perUserFolder)
}

func TestMultiplexRuntimeMakeNetworkDatabases(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir,
		_,
		testReactor := ResetTestMultiplexRuntime(t, 3)
	defer os.RemoveAll(rootDir)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	// Act
	err := testReactor.MakeNetworkDatabases(testExtChainID, []string{
		"state",
		"blockstore",
		"txindex",
		"evidence",
	}, true)
	assert.NoError(t, err, "should create network databases")

	testServicesProvider := testReactor.GetServicesProvider()
	testStateService := testServicesProvider(mx.ServiceKeyDatabaseState, testExtChainID.String())
	assert.NotNil(t, testStateService)
	testBlockService := testServicesProvider(mx.ServiceKeyDatabaseBlock, testExtChainID.String())
	assert.NotNil(t, testBlockService)
	testIndexService := testServicesProvider(mx.ServiceKeyDatabaseIndex, testExtChainID.String())
	assert.NotNil(t, testIndexService)
	testEvidenceService := testServicesProvider(mx.ServiceKeyDatabaseEvidence, testExtChainID.String())
	assert.NotNil(t, testEvidenceService)

	testStateDB := testStateService.(*mx.DBService).DB()

	// Test db read/write operations
	setErr := testStateDB.SetSync([]byte("testKey"), []byte("testValue"))
	assert.NoError(t, setErr, "should set test key in newly created database")

	actualValue, getErr := testStateDB.Get([]byte("testKey"))
	assert.NoError(t, getErr, "should get test key in newly created database")
	assert.Equal(t, []byte("testValue"), actualValue)

	testStateService.Stop()
	testBlockService.Stop()
	testIndexService.Stop()
	testEvidenceService.Stop()
}

func TestMultiplexRuntimeMakeNetworkValidator(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir,
		_,
		testReactor := ResetTestMultiplexRuntime(t, 5)
	defer os.RemoveAll(rootDir)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(t, err)

	// Act
	actualPrivValidator, err := testReactor.MakeNetworkValidator(
		testExtChainID,
		testConfDir,
		testDataDir,
	)
	assert.NoError(t, err)

	// Re-act should LOAD, not GEN!
	actualPrivValidatorReload, err := testReactor.MakeNetworkValidator(
		testExtChainID,
		testConfDir,
		testDataDir,
	)
	assert.NoError(t, err, "should create network validator")

	actualPubKey1, err := actualPrivValidator.GetPubKey()
	assert.NoError(t, err)

	actualPubKey2, err := actualPrivValidatorReload.GetPubKey()
	assert.NoError(t, err)

	assert.Equal(t, actualPubKey1, actualPubKey2)
	assert.Equal(t, actualPubKey1.Bytes(), actualPubKey2.Bytes())
}

func TestMultiplexRuntimeMakeNetworkGenesis(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir,
		_,
		testReactor := ResetTestMultiplexRuntime(t, 3)
	defer os.RemoveAll(rootDir)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(t, err)

	testPrivValidator, err := testReactor.MakeNetworkValidator(
		testExtChainID,
		testConfDir,
		testDataDir,
	)
	require.NoError(t, err)

	testPrivValPubKey, err := testPrivValidator.GetPubKey()
	require.NoError(t, err)

	testOtherValPubKey := ed25519.GenPrivKey().PubKey()
	testOtherValPubKeyHex := fmt.Sprintf("%X", testOtherValPubKey.Bytes())
	testOtherValidators := []string{testOtherValPubKeyHex}

	numNetworksBefore := testReactor.Size()

	// Act
	actualIcsGenDocSet, err := testReactor.MakeNetworkGenesis(
		testExtChainID,
		testConfDir,
		testPrivValidator,
		testOtherValidators,
	)
	assert.NoError(t, err, "should create a genesis doc")

	// Should have injected a new genesis doc
	assert.NotEmpty(t, actualIcsGenDocSet.GenesisDocs)
	assert.Len(t, actualIcsGenDocSet.GenesisDocs, numNetworksBefore+1)

	// Make sure we have the correct genesis doc
	actualGenesisDoc,
		actualFoundFlag,
		err := actualIcsGenDocSet.GenesisDocs.SearchGenesisDocByChainID(testChainID)
	assert.NoError(t, err, "could not find genesis doc by ChainID")
	assert.Equal(t, true, actualFoundFlag)
	assert.NotEmpty(t, actualGenesisDoc.Validators)

	expectedNumValidators := len(testOtherValidators) + 1 // +testPrivValidator
	assert.Len(t, actualGenesisDoc.Validators, expectedNumValidators)

	// Make sure we included the newly created priv validator
	actualValidator := actualGenesisDoc.Validators[0]
	assert.Equal(t, actualValidator.PubKey, testPrivValPubKey)
	assert.Equal(t, actualValidator.PubKey.Bytes(), testPrivValPubKey.Bytes())

	// .. and make sure we include otherValidators
	actualValidator2 := actualGenesisDoc.Validators[1]
	assert.Equal(t, actualValidator2.PubKey, testOtherValPubKey)
	assert.Equal(t, actualValidator2.PubKey.Bytes(), testOtherValPubKey.Bytes())
}

func TestMultiplexRuntimeMakeNetworkStateMachine(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir,
		testConfig,
		testReactor,
		testGenesisDocSet := ResetTestMultiplexRuntimeMock(t, 3)
	defer os.RemoveAll(rootDir)

	require.Equal(t, rootDir, testConfig.RootDir)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	err := testReactor.MakeNetworkDatabases(testExtChainID, []string{
		"state",
		"blockstore",
	}, true)
	require.NoError(t, err)

	actualGenesisDoc, _, err := testGenesisDocSet.SearchGenesisDocByChainID(
		testChainID,
	)
	require.NoError(t, err)

	testServicesProvider := testReactor.GetServicesProvider()
	testStateService := testServicesProvider(mx.ServiceKeyDatabaseState, testExtChainID.String())
	require.NotNil(t, testStateService)
	testBlockService := testServicesProvider(mx.ServiceKeyDatabaseBlock, testExtChainID.String())
	require.NotNil(t, testBlockService)

	testStateDB := testStateService.(*mx.DBService).DB()
	testBlockDB := testStateService.(*mx.DBService).DB()

	// Act
	actualStateMachine,
		actualStateStore,
		actualBlockStore,
		err := testReactor.MakeNetworkStateMachine(
		&actualGenesisDoc,
		testStateDB,
		testBlockDB,
	)
	assert.NoError(t, err, "should create network state machine")

	assert.NotNil(t, actualStateMachine)
	assert.NotNil(t, actualStateStore)
	assert.NotNil(t, actualBlockStore)

	assert.Equal(t, testChainID, actualStateMachine.ChainID)
	assert.Equal(t, int64(1), actualStateMachine.InitialHeight)
	assert.Equal(t, int64(0), actualStateMachine.LastBlockHeight) // LastBlockHeight=0 at genesis

	stopSmErr := testStateService.Stop()
	assert.NoError(t, stopSmErr)
	stopBsErr := testBlockService.Stop()
	assert.NoError(t, stopBsErr)
}

func TestMultiplexRuntimeMakeNetworkConfigOverwrite(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	rootDir,
		testConfig,
		testReactor,
		_ := ResetTestMultiplexRuntimeMock(t, numChains)
	defer os.RemoveAll(rootDir)

	require.Equal(t, rootDir, testConfig.RootDir)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	// Act
	actualCfgOverwrite, err := testReactor.MakeNetworkConfigOverwrite(testExtChainID)
	assert.NoError(t, err, "should create network config overwrite")
	assert.NotNil(t, actualCfgOverwrite)

	assert.Equal(t, testConfig.P2P.ListenAddress, actualCfgOverwrite.P2P.ListenAddress)
	assert.Equal(t, testConfig.RPC.ListenAddress, actualCfgOverwrite.RPC.ListenAddress)

	perUserFolder := "/" + testAddress + "/"
	assert.Contains(t, actualCfgOverwrite.Consensus.WalPath, perUserFolder)
}

func TestMultiplexRuntimeAllocateNetwork(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir,
		testConfig,
		testReactor,
		_ := ResetTestMultiplexRuntimeMock(t, numChains)
	require.Equal(t, rootDir, testConfig.RootDir)
	defer os.RemoveAll(rootDir)

	// Act
	allocErr := testReactor.AllocateNetwork(testChainID)
	assert.NoError(t, allocErr, "should allocate network resources")

	// Test that we injected a config path
	configsPaths := testReactor.GetConfigsPaths()
	require.Contains(t, configsPaths, testChainID)

	// Also test that instances were correctly registered
	servicesProvider := testReactor.GetServicesProvider()
	storageProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStorage)
	blockDBService := servicesProvider(mx.ServiceKeyDatabaseBlock, testChainID)
	stateDBService := servicesProvider(mx.ServiceKeyDatabaseState, testChainID)
	privValProvider := testReactor.GetInstanceProvider(mx.InstanceKeyPrivValidator)

	assert.NotNil(t, blockDBService, "blockstore DBService should not be nil")
	assert.NotNil(t, stateDBService, "statestore DBService should not be nil")

	perUserFolder := "/" + testAddress + "/"
	perChainFolder := "/" + testChainID
	chainDataFolder := storageProvider(testChainID)
	assert.NotEmpty(t, chainDataFolder)
	assert.Contains(t, chainDataFolder, perUserFolder)
	assert.Contains(t, chainDataFolder, perChainFolder)

	blockDB := blockDBService.(*mx.DBService).DB()
	stateDB := stateDBService.(*mx.DBService).DB()
	privVal := privValProvider(testChainID).(types.PrivValidator)

	// AllocateNetwork should not OPEN the db conn.
	assert.Nil(t, blockDB, "AllocateNetwork should not open blockstore database")
	assert.Nil(t, stateDB, "AllocateNetwork should not open statestore database")
	assert.NotNil(t, privVal)

	// Also test that we can query the priv validator
	actualPubKey, err := privVal.GetPubKey()
	assert.NoError(t, err)
	assert.NotNil(t, actualPubKey)
	assert.NotEmpty(t, actualPubKey.Bytes())

	// And after starting the DBService, the DB must be available.
	startBsErr := blockDBService.Start()
	assert.NoError(t, startBsErr)
	defer blockDBService.Stop()

	startSmErr := stateDBService.Start()
	assert.NoError(t, startSmErr)
	defer stateDBService.Stop()

	actualBlockDB := blockDBService.(*mx.DBService).DB()
	assert.NotNil(t, actualBlockDB)

	actualStateDB := stateDBService.(*mx.DBService).DB()
	assert.NotNil(t, actualStateDB)
}

func TestMultiplexRuntimeInjectGenesisDoc(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 4
	rootDir,
		testConfig,
		testReactor,
		_ := ResetTestMultiplexRuntimeMock(t, numChains)
	require.Equal(t, rootDir, testConfig.RootDir)
	defer os.RemoveAll(rootDir)

	configsPaths := testReactor.GetConfigsPaths()
	require.Contains(t, configsPaths, testChainID)

	testConfDir := configsPaths[testChainID]
	otherTestChainID := "mx-chain-" + testAddress + "-D1ED2B487F2E93CC"
	testGenesisDoc := types.GenesisDoc{
		GenesisTime:     cmttime.Now(),
		ChainID:         otherTestChainID,
		ConsensusParams: types.DefaultConsensusParams(),
		Validators:      []types.GenesisValidator{},
		InitialHeight:   int64(123),
	}

	// Act
	multiGenesisPath := testConfig.GenesisFile()
	singleGenesisPath := filepath.Join(testConfDir, "genesis.json")
	actualIcsGenDocSet, injectErr := testReactor.InjectGenesisDoc(
		otherTestChainID,
		testConfDir,
		testGenesisDoc,
	)
	assert.NoError(t, injectErr, "should inject network genesis doc")
	assert.Len(t, actualIcsGenDocSet.GenesisDocs, numChains+1) // Injected 1

	// Test that we created both genesis.json, the single one with only
	// one genesis doc and the multi with a genesis doc set.
	assert.Equal(t, true, cmtos.FileExists(singleGenesisPath))
	assert.Equal(t, true, cmtos.FileExists(multiGenesisPath))

	// Test that we can rebuild GenesisDoc and GenesisDocSet
	rebuiltGenesisDoc, err := types.GenesisDocFromFile(singleGenesisPath)
	assert.NoError(t, err, "should rebuilt GenesisDoc from file")
	assert.Equal(t, otherTestChainID, rebuiltGenesisDoc.ChainID)
	assert.Equal(t, testGenesisDoc.InitialHeight, rebuiltGenesisDoc.InitialHeight)

	rebuiltGenesisDocSet, err := mx.GenesisDocSetFromFile(multiGenesisPath)
	assert.NoError(t, err, "should rebuilt GenesisDocSet from file")
	assert.NotEmpty(t, rebuiltGenesisDocSet)
	assert.Len(t, rebuiltGenesisDocSet, numChains+1) // Injected 1
}

func TestMultiplexRuntimeInjectStateMachine(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 4
	rootDir,
		testConfig,
		testReactor := ResetTestMultiplexRuntimeWithInjection(t, numChains) // also injects testChainID!
	require.Equal(t, rootDir, testConfig.RootDir)
	defer os.RemoveAll(rootDir)

	testIcsGenDocSet := testReactor.GetChecksummedGenesisDocSet()

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(t, testExtChainID)

	testDbNames := []string{
		"state",
		"blockstore",
		"txindex",
		"evidence",
	}

	dbErr := testReactor.MakeNetworkDatabases(testExtChainID, testDbNames, true)
	require.NoError(t, dbErr, "should open network databases")

	// This test does not start the reactor, we must stop dbs manually.
	defer func() {
		servicesProvider := testReactor.GetServicesProvider()
		for _, dbName := range testDbNames {
			dbService := servicesProvider("database/"+dbName, testChainID)
			dbService.Stop()
		}
	}()

	// Act
	err := testReactor.InjectStateMachine(testChainID, testIcsGenDocSet)
	assert.NoError(t, err, "should inject network state machine")

	// Also test that instances were correctly registered
	stateProvider := testReactor.GetInstanceProvider(mx.InstanceKeyState)
	actualStateMachine := stateProvider(testChainID).(sm.State)
	assert.NotNil(t, actualStateMachine)

	// And test that the state machine is correctly setup
	actualGenesisDoc,
		actualFound,
		err := testIcsGenDocSet.GenesisDocs.SearchGenesisDocByChainID(testChainID)
	assert.NoError(t, err)
	assert.Equal(t, true, actualFound)

	assert.Equal(t, testChainID, actualStateMachine.ChainID)
	assert.Equal(t, actualGenesisDoc.InitialHeight, actualStateMachine.InitialHeight)
	assert.Equal(t, int64(0), actualStateMachine.LastBlockHeight) // LastBlockHeight=0 at genesis
}

// CAUTION: This tests the injection with a running reactor instance.
func TestMultiplexRuntimeInjectNewNetwork(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t, numChains, cmtlog.NewNopLogger(), true) // startServers=true

	defer shutdownFn(testReactor)

	// AllocateNetwork is NOT part of InjectNewNetwork anymore, due to it being
	// executed earlier, i.e. see MultiplexBackend.InitValidators.
	allocErr := testReactor.AllocateNetwork(testChainID)
	require.NoError(t, allocErr, "should allocate new network resources")

	// Act
	injectErr := testReactor.InjectNewNetwork(testChainID, []string{})
	assert.NoError(t, injectErr, "should inject new network")

	// Test that we injected the ChainID
	testChainIds := testReactor.GetNetworks()
	assert.Len(t, testChainIds, numChains+1) // Injected 1
	assert.Equal(t, true, testReactor.HasNetwork(testChainID))

	// Also test that we created a correct GenesisDoc
	testIcsGenDocSet := testReactor.GetChecksummedGenesisDocSet()
	actualGenesisDoc,
		actualFound,
		err := testIcsGenDocSet.GenesisDocs.SearchGenesisDocByChainID(testChainID)
	assert.NoError(t, err)
	assert.Equal(t, true, actualFound)
	assert.Equal(t, actualGenesisDoc.ChainID, testChainID)

	// ----------------------------------
	// Should have called AllocateNetwork

	confPaths := testReactor.GetConfigsPaths()
	dataPaths := testReactor.GetStoragePaths()
	assert.NotEmpty(t, confPaths)
	assert.NotEmpty(t, dataPaths)
	assert.Contains(t, confPaths, testChainID)
	assert.Contains(t, dataPaths, testChainID)

	// Test that we have the correct filesystem structure
	actualConfDir := confPaths[testChainID]
	actualDataDir := dataPaths[testChainID]
	assert.Equal(t, true, cmtos.FileExists(actualConfDir))
	assert.Equal(t, true, cmtos.FileExists(actualDataDir))

	perUserFolder := "/" + testAddress + "/"
	perChainFolder := "/" + testChainID
	assert.Contains(t, actualConfDir, perUserFolder)
	assert.Contains(t, actualConfDir, perChainFolder)
	assert.Contains(t, actualDataDir, perUserFolder)
	assert.Contains(t, actualDataDir, perChainFolder)

	// Also test that we have important database instances
	servicesProvider := testReactor.GetServicesProvider()
	stateDBService := servicesProvider(mx.ServiceKeyDatabaseState, testChainID)
	blockDBService := servicesProvider(mx.ServiceKeyDatabaseBlock, testChainID)

	assert.NotNil(t, stateDBService)
	stateDB := stateDBService.(*mx.DBService).DB()
	assert.NotNil(t, stateDB)

	assert.NotNil(t, blockDBService)
	blockDB := blockDBService.(*mx.DBService).DB()
	assert.NotNil(t, blockDB)

	// .. and a priv validator instance
	privValProvider := testReactor.GetInstanceProvider(mx.InstanceKeyPrivValidator)
	privValidatorUnsafe := privValProvider(testChainID)
	assert.NotNil(t, privValidatorUnsafe)
	privValidator := privValidatorUnsafe.(types.PrivValidator)
	assert.NotNil(t, privValidator)

	// ----------------------------------
	// Should have called InjectStateMachine

	stateMachineProvider := testReactor.GetInstanceProvider(mx.InstanceKeyState)
	stateStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStateStore)
	blockStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyBlockStore)

	stateMachineUnsafe := stateMachineProvider(testChainID)
	assert.NotNil(t, stateMachineUnsafe)
	stateMachine := stateMachineUnsafe.(sm.State)
	assert.NotNil(t, stateMachine)

	stateStoreUnsafe := stateStoreProvider(testChainID)
	assert.NotNil(t, stateStoreUnsafe)
	stateStore := stateStoreUnsafe.(sm.Store)
	assert.NotNil(t, stateStore)

	blockStoreUnsafe := blockStoreProvider(testChainID)
	assert.NotNil(t, blockStoreUnsafe)
	blockStore := blockStoreUnsafe.(*bs.BlockStore)
	assert.NotNil(t, blockStore)

	// And test that the state machine is correctly setup
	assert.Equal(t, testChainID, stateMachine.ChainID)
	assert.Equal(t, int64(0), stateMachine.LastBlockHeight) // LastBlockHeight=0 at genesis

	// ----------------------------------
	// Should have called RegisterNetwork

	// Also test that we updated MultiNetworkNodeInfo
	testMultiNetNodeInfo := testReactor.GetMultiNetworkNodeInfo()
	assert.NotNil(t, testMultiNetNodeInfo)

	actualChainNodeInfo, err := testMultiNetNodeInfo.GetNodeInfo(testChainID)
	assert.NoError(t, err)
	assert.NotNil(t, actualChainNodeInfo)
	assert.Equal(t, testChainID, actualChainNodeInfo.Network)
}

func TestMultiplexRuntimeInjectNewNetworkIncludesOtherValidators(t *testing.T) {
	// IMPORTANT: We use numChains=0 in this test so it is important
	// to test whether P2P and RPC servers will be shutdown.
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 0

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t, numChains, cmtlog.NewNopLogger(), true) // startServers=true

	defer shutdownFn(testReactor)

	numValidators := 10
	testOtherValidators := make([]string, 0, numValidators)
	for i := 0; i < numValidators; i++ {
		testOtherValPubKey := ed25519.GenPrivKey().PubKey()
		testOtherValPubKeyHex := fmt.Sprintf("%X", testOtherValPubKey.Bytes())

		testOtherValidators = append(testOtherValidators, testOtherValPubKeyHex)
	}

	testWithChainID := helpers.MakeChainID("test-chain-1")

	// AllocateNetwork is NOT part of InjectNewNetwork anymore, due to it being
	// executed earlier, i.e. see MultiplexBackend.InitValidators.
	allocErr := testReactor.AllocateNetwork(testWithChainID)
	require.NoError(t, allocErr, "should allocate new network resources")

	// Inject testWithChainID
	injectErr := testReactor.InjectNewNetwork(testWithChainID, testOtherValidators)
	require.NoError(t, injectErr,
		"should accept other validator public keys")

	// Test that we injected the ChainID
	testChainIds := testReactor.GetNetworks()
	assert.Len(t, testChainIds, numChains+1) // Injected 1
	assert.Equal(t, true, testReactor.HasNetwork(testWithChainID))

	// Also test that we created a correct GenesisDoc
	testIcsGenDocSet := testReactor.GetChecksummedGenesisDocSet()
	actualGenesisDoc,
		actualFound,
		err := testIcsGenDocSet.GenesisDocs.SearchGenesisDocByChainID(testWithChainID)
	assert.NoError(t, err)
	assert.Equal(t, true, actualFound)
	assert.Equal(t, actualGenesisDoc.ChainID, testWithChainID)

	// And test that validators are correctly added
	expectedNumValidators := numValidators + 1 // +self
	assert.Len(t, actualGenesisDoc.Validators, expectedNumValidators)
}

func TestMultiplexRuntimeInjectNewRuntime(t *testing.T) {
	// IMPORTANT: We use numChains=0 in this test so it is important
	// to test whether P2P and RPC servers will be shutdown.
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 0

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t, numChains, cmtlog.NewNopLogger(), true) // startServers=true

	defer shutdownFn(testReactor)

	injectChainID := helpers.MakeChainID("test-inject-1")

	// AllocateNetwork is NOT part of InjectNewNetwork anymore, due to it being
	// executed earlier, i.e. see MultiplexBackend.InitValidators.
	allocErr := testReactor.AllocateNetwork(injectChainID)
	require.NoError(t, allocErr, "should allocate new network resources")

	// Inject injectChainID
	injectErr := testReactor.InjectNewNetwork(injectChainID, []string{})
	require.NoError(t, injectErr)

	// For debug, change the logger cmtlog.TestingLogger()
	// i.e.: testReactor.SetLogger(cmtlog.TestingLogger())

	// Act
	runtimeErr := testReactor.InjectNewRuntime(t.Context(), injectChainID)
	assert.NoError(t, runtimeErr, "should spawn parallel process for node runtime")
}

// ----------------------------------------------------------------------------
// TestMultiplexNode

// CAUTION: do not remove this test because it makes sure that that multiplex
// implementation *does not interfere* with the legacy node implementation.
func TestMultiplexNodeLegacyNodeImplementation(t *testing.T) {
	defer goleak.VerifyNone(t)

	testChainID := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainID) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Make sure we have a privValidator
	privValidator, err := privval.LoadOrGenFilePV(
		globalCfg.PrivValidatorKeyFile(),
		globalCfg.PrivValidatorStateFile(),
		useDefaultKeyGenFunc(),
	)
	require.NoError(t, err)

	n, err := cmtnode.NewNode(
		t.Context(),
		globalCfg,
		privValidator,
		makeRandomNodeKey(),
		proxy.DefaultClientCreator(t.Context(), globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(globalCfg),
		config.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(globalCfg.Instrumentation),
		cmtlog.NewNopLogger(), // cmtlog.TestingLogger() more verbose
	)
	require.NoError(t, err)

	// Start and stop to test full run-up of node
	err = n.Start()
	assert.NoError(t, err)

	// TODO(midas): test that it uses the legacy test RPC listen address

	defer func() {
		err := n.Stop()
		assert.NoError(t, err)
	}()
}

func TestMultiplexNodeNewLegacyNodeMultiplex(t *testing.T) {
	defer goleak.VerifyNone(t)

	testChainID := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainID) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Create the [node.Node] instance, using [node.NewNode]
	// nil-Reactor instance is ignored
	testMultiplex, _, err := mx.NewLegacyNodeMultiplex(
		t.Context(),
		globalCfg,
		makeRandomNodeKey(),
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, 1, "should return a multiplex map with exactly one node")
	assert.Contains(t, testMultiplex, testChainID)
	assert.NotNil(t, testMultiplex[testChainID])

	// Type-assertion to verify that we have a correct instance
	legacyNode := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
	genesisDoc := legacyNode.GenesisDoc()

	// Verify that we are on the correct ChainID
	assert.Equal(t, testChainID, genesisDoc.ChainID)

	// Start and stop to close the db for later re-tests
	err = legacyNode.Start()
	require.NoError(t, err, "legacy node should start correctly")

	// TODO(midas): test that it uses the legacy test RPC listen address

	defer func() {
		err := legacyNode.Stop()
		require.NoError(t, err)
	}()
}

func TestMultiplexNodeNewNodesMultiplexFallback(t *testing.T) {
	defer goleak.VerifyNone(t)

	// We define the necessary infrastructure for a legacy node
	testChainID := "test-legacy-chain-id"
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Make sure we have /data and /config
	config.EnsureRoot(rootDir)

	// Make sure we have a *one-doc* genesis file (GenesisDoc)
	baseConfig := config.DefaultBaseConfig()
	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := fmt.Sprintf(testLegacyGenesisDocFmt, testChainID) // LEGACY!
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// Create a legacy Test configuration
	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)

	// Also generate a multiplex config *but* disable it using "disabled"
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 3, 30001)
	globalCfg.Strategy = mx.DisableReplicationStrategy()

	// The multiplex configuration will be ignored due to disabled flag.
	// Should create the [node.Node] instance, using [node.NewNode]
	testMultiplex, _, err := mx.NewNodesMultiplex(
		t.Context(),
		&client.DefaultAcceptor{},
		globalCfg,
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, 1, "should return a multiplex map with exactly one node")
	assert.Contains(t, testMultiplex, testChainID)
	assert.NotNil(t, testMultiplex[testChainID])

	// Type-assertion to verify that we have a correct instance
	legacyNode := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
	genesisDoc := legacyNode.GenesisDoc()

	// Verify that we are on the correct ChainID
	assert.Equal(t, testChainID, genesisDoc.ChainID)

	// Start and stop to close the db for later re-testing
	err = legacyNode.Start()
	require.NoError(t, err, "legacy node should start correctly")

	// TODO(midas): test that it uses the legacy test RPC listen address

	defer func() {
		err := legacyNode.Stop()
		require.NoError(t, err)
	}()
}

func TestMultiplexNodeNewNodesMultiplex(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, globalCfg := ResetTestMultiplexNode(t, numChains)
	defer os.RemoveAll(rootDir)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + t.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	_, testReactor, err := mx.NewNodesMultiplex(
		t.Context(),
		&client.DefaultAcceptor{},
		globalCfg,
		cmtlog.NewNopLogger(),
		node.NodeWithStartRPC(false),
		node.NodeWithStartP2P(false),
		node.NodeWithStartMonitor(false),
	)
	assert.NoError(t, err, "should create node instance")
	assert.Equal(t, numChains, testReactor.Size(), fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	configProvider := testReactor.GetInstanceProvider(mx.InstanceKeyConfig)
	assert.NotNil(t, configProvider)

	testChainIds := testReactor.GetNetworks()

	// CAUTION: This activates runtimes for pre-configured networks.
	mx.ReactorWithActiveRuntimes(t.Context(), testChainIds, map[string][]string{})(testReactor)

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testChainIds))

	// Test that we have all the required networks
	servicesProvider := testReactor.GetServicesProvider()
	for _, testChainID := range testChainIds {
		nodeRuntime := servicesProvider(mx.ServiceKeyNodeRuntime, testChainID)
		require.NotNil(t, nodeRuntime, "should create node.Node instance")

		// Type-assertion to verify that we have a correct instance
		nodeInstance := nodeRuntime.(*cmtnode.Node)
		genesisDoc := nodeInstance.GenesisDoc()
		cfgOverwrite := configProvider(testChainID).(*config.Config)

		stateSyncConf, err := testReactor.GetChainRegistry().GetStateSyncConfig(testChainID)
		assert.NoError(t, err, "should get state-sync configuration per network")
		assert.Equal(t, false, stateSyncConf.Enable, "state-sync should be disabled")

		// Verify that we are on the correct ChainID
		assert.Equal(t, testChainID, genesisDoc.ChainID)

		// Verify state-sync configuration
		assert.Equal(t, false, cfgOverwrite.StateSync.Enable, "state-sync should be disabled")

		// Verify that we can start the node correctly
		go func(cn *cmtnode.Node) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(t, err)
		}(nodeInstance)
	}

	// Wait for both nodes to have produced a block
	// t.Logf("Waiting for %d nodes to be up and running.", len(testChainIds))
	wg.Wait()

	// TODO(midas): test that it uses the multiplex RPC listen address

	// Shutdown routine
	defer func() {
		// Uses waitgroup to ensure complete shutdown
		wg := sync.WaitGroup{}
		wg.Add(len(testChainIds))

		for _, testChainID := range testChainIds {
			nodeRuntime := servicesProvider(mx.ServiceKeyNodeRuntime, testChainID)
			require.NotNil(t, nodeRuntime, "should create node.Node instance")

			// Type-assertion to verify that we have a correct instance
			nodeInstance := nodeRuntime.(*cmtnode.Node)

			// Stop the running node instance and continue
			go func(cn *cmtnode.Node) {
				defer wg.Done()

				if cn.IsRunning() {
					err := cn.Stop()
					require.NoError(t, err)
				}
			}(nodeInstance)
		}

		// Wait for all nodes to be shutdown
		//t.Logf("Waiting for %d nodes to be stopped.", len(testChainIds))
		wg.Wait()

		if testReactor.IsRunning() {
			err := testReactor.Stop()
			require.NoError(t, err)
		}
	}()
}

func TestMultiplexNodeNewNodesMultiplexSingleNetworkProduceBlocks(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 1

	// For debug, change the logger to cmtlog.TestingLogger()
	// Initialize and START the nodes multiplex,
	// i.e. calls method node.Node#Start.
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t,
		numChains,
		cmtlog.NewNopLogger(),
		true, // startServers
	)

	defer shutdownFn(testReactor)

	require.NotNil(t, testReactor)
	require.Len(t, testReactor.GetNetworks(), numChains)

	expectedBlocks := 3
	assertWaitForNodesMultiplexToProduceBlocks(t,
		testReactor,
		expectedBlocks,
		15*time.Second,
		"node_test",
		broadcastRawTx,
	)
}

func TestMultiplexNodeNewNodesMultiplexProduceBlocks(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t,
		numChains,
		cmtlog.NewNopLogger(),
		true, // startServers
	)

	defer shutdownFn(testReactor)

	require.NotNil(t, testReactor)
	require.Len(t, testReactor.GetNetworks(), numChains)

	expectedBlocks := 2
	assertWaitForNodesMultiplexToProduceBlocks(t,
		testReactor,
		expectedBlocks,
		30*time.Second, // 10 blocks in total, leaves 3s per block
		"node_test",
		broadcastRawTx,
	)
}
