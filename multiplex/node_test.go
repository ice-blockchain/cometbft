package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmttest "github.com/ice-blockchain/cometbft/internal/test"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	"github.com/ice-blockchain/cometbft/types"
)

// CAUTION: do not remove this test because it makes sure that that multiplex
// implementation *does not interfere* with the legacy node implementation.
func TestMultiplexNodeLegacyNodeImplementation(t *testing.T) {
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
		context.Background(),
		globalCfg,
		privValidator,
		makeRandomNodeKey(),
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(globalCfg),
		config.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(globalCfg.Instrumentation),
		cmtlog.NewNopLogger(), // cmtlog.TestingLogger() more verbose
	)
	require.NoError(t, err)

	// Start and stop to test full run-up of node
	err = n.Start()
	assert.NoError(t, err)

	defer func() {
		err := n.Stop()
		assert.NoError(t, err)
	}()
}

func TestMultiplexNodeNewLegacyNodeMultiplex(t *testing.T) {
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
	testMultiplex, testReactor, shutdownFn, err := mx.NewLegacyNodeMultiplex(
		context.Background(),
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

	defer func() {
		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()
}

func TestMultiplexNodeNewNodesMultiplexFallback(t *testing.T) {
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
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(t, 3)
	globalCfg.Strategy = mx.DisableReplicationStrategy()

	// The multiplex configuration will be ignored due to disabled flag.
	// Should create the [node.Node] instance, using [node.NewNode]
	testMultiplex, testReactor, shutdownFn, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		cmtlog.NewNopLogger(),
		&client.DefaultClient{},
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

	defer func() {
		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()
}

func TestMultiplexNodeNewNodesMultiplex(t *testing.T) {
	numChains := 5
	rootDir, globalCfg := ResetTestMultiplexNode(t, numChains)
	defer os.RemoveAll(rootDir)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + t.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, shutdownFn, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		cmtlog.NewNopLogger(),
		&client.DefaultClient{},
	)
	assert.NoError(t, err, "should create node instance")
	assert.NotNil(t, testMultiplex, "should return a multiplex map with a node")
	assert.Len(t, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	configProvider := testReactor.GetInstanceProvider(mx.InstanceKeyConfig)
	assert.NotNil(t, configProvider)

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	// Test that we have all the required networks
	for _, testChainID := range testReactor.GetNetworks() {
		assert.Contains(t, testMultiplex, testChainID)
		assert.NotNil(t, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
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
	// t.Logf("Waiting for %d nodes to be up and running.", len(testReactor.GetNetworks()))
	wg.Wait()

	// Shutdown routine
	defer func() {
		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()
}

func TestMultiplexNodeNewNodesMultiplexSingleNetworkProduceBlocks(t *testing.T) {
	numNetworks := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg,
		testMultiplex,
		testReactor,
		shutdownFn := assertStartNodesMultiplex(t, numNetworks, cmtlog.NewNopLogger()) // 1 NETWORK!

	// Shutdown routine
	defer func() {
		defer os.RemoveAll(globalCfg.RootDir)

		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()

	expectedBlocks := 3
	assertWaitForNodesMultiplexToProduceBlocks(t, testReactor, testMultiplex, expectedBlocks)
}

func TestMultiplexNodeNewNodesMultiplexProduceBlocks(t *testing.T) {
	numNetworks := 5

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg,
		testMultiplex,
		testReactor,
		shutdownFn := assertStartNodesMultiplex(t, numNetworks, cmtlog.NewNopLogger()) // 5 networks

	// Shutdown routine
	defer func() {
		defer os.RemoveAll(globalCfg.RootDir)

		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()

	expectedBlocks := 2
	assertWaitForNodesMultiplexToProduceBlocks(t, testReactor, testMultiplex, expectedBlocks)
}

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: this test method sets up a random multiplex with a valid GenesisDocSet.
// CAUTION: this method forcefully *disables state-sync* to permit starting new networks.
func ResetTestMultiplexNode(tb testing.TB, numChains int) (string, *config.Config) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains)

	// We always *disable* state-sync for network nodes
	for chainID := range globalCfg.SyncConfig {
		// Forcefully disable state-sync
		globalCfg.SyncConfig[chainID].Enable = false
	}

	// Make sure we have /data and /config
	_, err = mx.NewMultiplexFS(globalCfg)
	require.NoError(tb, err, "should create filesystem structure for multiplex")

	// Make sure we have a *multi-doc* genesis file (GenesisDocSet)
	genesisFilePath := filepath.Join(rootDir, globalCfg.Genesis)

	// IMPORTANT:
	// If there is a genesis file at the configured path, we will read it and expect it
	// to contain a genesis doc set ; otherwise create it with testOneScopedGenesisFmt.

	if !cmtos.FileExists(genesisFilePath) {
		testGenesis := `[`
		for userAddress, chainIds := range globalCfg.UserChains {
			for _, chainID := range chainIds {
				// Creates one genesis doc per pair of user address and chainId
				chainTestGenesis := fmt.Sprintf(testGenesisDocWithValidatorsFmt, chainID, testDefaultGenesisValidator)
				testGenesis += chainTestGenesis + ","

				// resets priv validators to default state/key (as present in genesis)
				// useDefaultPrivValidator=true
				ResetMultiplexPrivValidator(globalCfg.BaseConfig, userAddress, chainID, nil, true)
			}
		}

		// Removes last comma and closes json array
		testGenesis = testGenesis[:len(testGenesis)-1] + `]`
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	return rootDir, globalCfg
}

func ResetMultiplexPrivValidator(
	conf config.BaseConfig,
	userAddress string,
	chainID string,
	privValidator *privval.FilePV,
	useDefaultPrivValidator bool,
) *privval.FilePV {
	userConfDir := filepath.Join(conf.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(conf.RootDir, config.DefaultDataDir, userAddress)

	privValKeyDir := filepath.Join(userConfDir, chainID)
	privValStateDir := filepath.Join(userDataDir, chainID)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(conf.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(conf.PrivValidatorStateFile()))

	// fmt.Printf("Resetting priv validator key file: %s\n", privValKeyFile)
	// fmt.Printf("Resetting priv validator state file: %s\n", privValStateFile)

	if useDefaultPrivValidator {
		// CAUTION: careful this uses always the same priv validator,
		// if a change is made to it, please also update the genesis.validators.
		cmttest.ResetTestPrivValidatorFiles(privValKeyFile, privValStateFile)
		return privval.LoadFilePV(privValKeyFile, privValStateFile)
	}

	// Not using default priv validator, we will either generate a random
	// new priv validator key, or use the one provided with privValidator
	var err error
	var filePV *privval.FilePV
	if privValidator == nil {
		// IMPORTANT: This generates a random privValidator private key
		filePV, err = privval.GenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
		if err != nil {
			panic(fmt.Errorf("could not generate a priv validator: %w", err))
		}
	} else {
		filePV = privValidator
	}

	testPrivValidatorKey, _ := cmtjson.MarshalIndent(filePV.Key, "", "  ")
	cmtos.MustWriteFile(privValKeyFile, testPrivValidatorKey, 0o644)

	// We always reset priv validator state to 0-height
	cmtos.MustWriteFile(privValStateFile, []byte(testPrivValidatorState), 0o644)

	return filePV
}

// ----------------------------------------------------------------------------

func useDefaultKeyGenFunc() func() (crypto.PrivKey, error) {
	return func() (crypto.PrivKey, error) {
		return ed25519.GenPrivKey(), nil
	}
}

// assertStartNodesMultiplex configures a nodes multiplex *randomly* and starts
// individual nodes in a separate goroutine per network.
func assertStartNodesMultiplex(tb testing.TB, numChains int, customLogger cmtlog.Logger) (
	*config.Config,
	mx.MultiplexMap[*cmtnode.Node],
	*mx.Reactor,
	mx.NodesMultiplexShutdownFn,
) {
	tb.Helper()

	_, globalCfg := ResetTestMultiplexNode(tb, numChains)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + tb.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfg.ChainSeeds {
		globalCfg.ChainSeeds[chainID] = ""
	}

	if customLogger == nil {
		customLogger = cmtlog.NewNopLogger()
	}

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, shutdownFn, err := mx.NewNodesMultiplex(
		context.Background(),
		globalCfg,
		customLogger,
		&client.DefaultClient{},
	)
	require.NoError(tb, err, "should create node instance")
	require.NotNil(tb, testMultiplex, "should return a multiplex map with a node")
	require.Len(tb, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	// Test that we have all the required networks
	for _, testChainID := range testReactor.GetNetworks() {
		require.Contains(tb, testMultiplex, testChainID)
		require.NotNil(tb, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
		userAddress, err := testReactor.GetChainRegistry().GetAddress(testChainID)
		require.NoError(tb, err, "should find user address by ChainID")

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(tb, nodeInstance, globalCfg, userAddress, testChainID)

		// Verify that we can start the node correctly
		go func(cn *cmtnode.Node) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(tb, err)
		}(nodeInstance)
	}

	// Wait for all nodes to be up and running
	// t.Logf("Waiting for %d nodes to be up and running.", len(testReactor.GetNetworks()))
	wg.Wait()

	return globalCfg, testMultiplex, testReactor, shutdownFn
}

func assertWaitForNodesMultiplexToProduceBlocks(
	tb testing.TB,
	testReactor *mx.Reactor,
	testMultiplex mx.MultiplexMap[*cmtnode.Node],
	expectedBlocks int,
) {
	tb.Helper()

	if expectedBlocks == 0 {
		return // Nothing to do
	}

	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	actualNumBlocks := make(map[string]int, len(testReactor.GetNetworks()))
	for _, testChainID := range testReactor.GetNetworks() {
		// Test that we have the correct node instance
		assert.Contains(tb, testMultiplex, testChainID)
		assert.NotNil(tb, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)

		// Parallel goroutines with internal blocks loops
		go func(the_chain string, the_node *cmtnode.Node, maxBlocks int) {
			// Wait for the node to produce blocks
			blocksSub, err := the_node.EventBus().Subscribe(
				context.Background(),
				"node_test",
				types.EventQueryNewBlock,
			)
			assert.NoError(tb, err)

			numBlocks := 0

		NODE_BLOCKS_LOOP:
			for {
				select {
				case <-blocksSub.Out():
					numBlocks++
					if numBlocks == maxBlocks {
						actualNumBlocks[the_chain] = numBlocks
						wg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					wg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(15 * time.Second):
					wg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(testChainID, nodeInstance, expectedBlocks)
	}

	// Wait for all nodes to produce 3 blocks in parallel
	wg.Wait()

	// We assert that all networks produced at least 3 blocks, if an error
	// occurred on one of the networks, the map entry won't exist.
	for _, chainID := range testReactor.GetNetworks() {
		assert.Contains(tb, actualNumBlocks, chainID)
		assert.Equal(tb, expectedBlocks, actualNumBlocks[chainID])
	}
}

func usePrivValidatorFromFiles(
	tb testing.TB,
	n *cmtnode.Node,
	conf *config.Config,
	userAddress string,
	chainID string,
) {
	tb.Helper()

	userConfDir := filepath.Join(conf.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(conf.RootDir, config.DefaultDataDir, userAddress)

	privValKeyDir := filepath.Join(userConfDir, chainID)
	privValStateDir := filepath.Join(userDataDir, chainID)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(conf.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(conf.PrivValidatorStateFile()))

	// Reload the priv validator from files. This overwrites the PrivValidator
	// so that it uses the default privval or a generated privval.
	newPV, err := privval.LoadOrGenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
	require.NoError(tb, err)
	// t.Logf("Using priv validator from files: %s\n", newPV.GetAddress())

	n.SetPrivValidator(newPV)

	consensusReactor := n.Switch().Reactor("CONSENSUS").(*cs.Reactor)
	consensusReactor.SetPrivValidator(newPV)
}

// CAUTION: this is not a GenesisDocSet, but just a GenesisDoc and is used
// to test the multiplex fallback to a legacy node implementation.
var testLegacyGenesisDocFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
	  ` + testDefaultGenesisValidator + `
	],
	"app_hash": ""
}`

var (
	testGenesisValidatorPubKey  = "AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
	testDefaultGenesisValidator = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"` + testGenesisValidatorPubKey + `"
	},
	"power": "10",
	"name": ""
}`
)

// This produces a GenesisDocSet instance with exactly one chain.
var testGenesisDocWithValidatorsFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
		%s
	],
	"app_hash": ""
}`

var testPrivValidatorState = `{
  "height": "0",
  "round": 0,
  "step": 0
}`
