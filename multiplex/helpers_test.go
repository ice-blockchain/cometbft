package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmttest "github.com/ice-blockchain/cometbft/internal/test"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// -----------------------------------------------------------------------------
// TestMultiplexDB

func ResetMultiDBTestRoot(
	tb testing.TB,
	testName string,
	numDatabases int,
) (string, []dbm.DB) {
	tb.Helper()

	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Open numDatabases database adapters
	dbPtrs, err := createTempMemDB(tb, rootDir, numDatabases)
	require.NoError(tb, err, "should create MemDB instances in temp directory")

	return rootDir, dbPtrs
}

func ResetMultiplexDBTestRoot(
	tb testing.TB,
	testName string,
	numDatabases int,
) (string, mx.MultiplexDB) {
	tb.Helper()

	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Open numDatabases database adapters
	dbPtrs, err := createTempMemDB(tb, rootDir, numDatabases)
	require.NoError(tb, err, "should create MemDB instances in temp directory")

	// Create a database multiplex
	multiplex := mx.MultiplexDB{}
	for i, db := range dbPtrs {
		chainID := "test-chain-" + strconv.Itoa(i)
		multiplex[chainID] = &mx.ChainDB{
			ChainID: chainID,
			DB:      db,
		}
	}

	return rootDir, multiplex
}

// -----------------------------------------------------------------------------
// TestMultiplexFS

func ResetMultiplexFSTestRoot(t *testing.T, testName string) (string, *config.Config) {
	t.Helper()

	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	conf := config.TestConfig()
	conf.SetRoot(rootDir)
	return rootDir, conf
}

// -----------------------------------------------------------------------------
// TestMultiplexReactor

// CAUTION: This helper starts a nodes multiplex of numChains random chains.
// CAUTION: This helper injects a random chain in the pre-configured Reactor.
func ResetTestMultiplexReactorRuntimeWithInjection(
	tb testing.TB,
	numChains int,
	customLogger cmtlog.Logger,
) (helpers.ExtendedChainID, *mx.Reactor, func(*mx.Reactor)) {
	tb.Helper()

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(tb, numChains, customLogger, true) // startServers=true

	// Generate new random network ChainID
	newUserPubKey := ed25519.GenPrivKey().PubKey()
	newUserAddress := newUserPubKey.Address().String()
	fingerprint := helpers.MakeFingerprint("Posts") // This is the "scope"

	testInjectChainID := helpers.NewExtendedChainID(newUserAddress, fingerprint)
	require.NotNil(tb, testInjectChainID)

	injectChainID := testInjectChainID.String()

	// Inject injectChainID resources
	allocErr := testReactor.AllocateNetwork(injectChainID)
	require.NoError(tb, allocErr, "should allocate network resources")

	// Start the database connections for injectChainID.
	startDbErr := testReactor.MakeNetworkDatabases(testInjectChainID, []string{
		"blockstore",
		"state",
		"txindex",
		"evidence",
	}, true)
	require.NoError(tb, startDbErr, "should start databases")

	// Inject GenesisDoc to prepare state machine
	configsPaths := testReactor.GetConfigsPaths()
	require.Contains(tb, configsPaths, injectChainID)

	testConfDir := configsPaths[injectChainID]
	testValidator := ed25519.GenPrivKey()
	testGenesisDoc := types.GenesisDoc{
		GenesisTime:     cmttime.Now(),
		ChainID:         injectChainID,
		ConsensusParams: types.DefaultConsensusParams(),
		Validators: []types.GenesisValidator{
			types.GenesisValidator{
				Address: testValidator.PubKey().Address(),
				PubKey:  testValidator.PubKey(),
				Power:   10,
			},
		},
		InitialHeight: int64(123),
	}

	testIcsGenDocSet, injectErr := testReactor.InjectGenesisDoc(
		injectChainID,
		testConfDir,
		testGenesisDoc,
	)
	require.NoError(tb, injectErr, "should inject network genesis doc")

	// Prepare state machine for injected network
	stateErr := testReactor.InjectStateMachine(injectChainID, testIcsGenDocSet)
	require.NoError(tb, stateErr, "should inject network state machine")

	// Prepare config overwrite (ports, seeds, etc.)
	actualConfOverwrite, configErr := testReactor.MakeNetworkConfigOverwrite(testInjectChainID)
	require.NoError(tb, configErr, "should inject network config overwrite")

	// .. must also register in Reactor
	testReactor.RegisterInstance(mx.InstanceKeyConfig, injectChainID, actualConfOverwrite)

	registerErr := testReactor.RegisterNetwork(testInjectChainID.GetUserAddress(), injectChainID)
	require.NoError(tb, registerErr, "should register network in multiplex reactor")

	return testInjectChainID, testReactor, shutdownFn
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorConsensus

// CAUTION: the GenesisDocProvider is maleated to contain correct ChainIDs
// CAUTION: the MultiplexConfig is entirely random and *not synchronized* with genesis docs.
func ResetTestMultiplexConsensus(
	tb testing.TB,
	numChains int,
) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&nodeCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, nodeCfg, mockGenesisProvider)

	return rootDir, nodeCfg, reactor
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorState

// CAUTION: the GenesisDocProvider is maleated to contain correct ChainIDs
// CAUTION: the MultiplexConfig is entirely random and *not synchronized* with genesis docs.
func ResetTestMultiplexState(tb testing.TB, numChains int, dbServiceKey string) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&nodeCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, nodeCfg, mockGenesisProvider)

	// Prepares the database for each network
	testChainIds := reactor.GetNetworks()
	for index, chainID := range testChainIds {
		// We create one state database instance per chain
		dbName := "chaindb-state-" + strconv.Itoa(index)
		dbService := mx.NewDBService(reactor.Context(),
			dbName,
			rootDir,
			"memdb",
			cmtlog.NewNopLogger(),
		)

		startErr := dbService.Start()
		require.NoError(tb, startErr)

		reactor.RegisterService(dbServiceKey, chainID, dbService)
	}

	return rootDir, nodeCfg, reactor
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorP2P

// CAUTION: this test method sets up a full consensus multiplex with reactors.
func ResetTestMultiplexP2P(tb testing.TB, numChains int) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&globalCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, globalCfg, mockGenesisProvider)

	// Start the reactor
	err = reactor.Start()
	require.NoError(tb, err, "should start the multiplex reactor")

	testChainIds := reactor.GetNetworks()

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		tb.Context(),
		testChainIds,
		proxy.DefaultClientCreator(tb.Context(), globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(tb, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// CAUTION: This activates runtimes for pre-configured networks.
	mx.ReactorWithActiveRuntimes(tb.Context(), testChainIds, map[string][]string{})(reactor)

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range testChainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		require.NoError(tb, err, "should not error for consensus handshake")

		blockSync := false
		err := reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainID,
			blockSync,
			false, // waitSync
		)
		require.NoError(tb, err, "should not error creating consensus reactors")
	}

	return rootDir, globalCfg, reactor
}

// -----------------------------------------------------------------------------
// TestMultiplexReactorRuntime

// CAUTION: This helper uses a random multiplex config.
func ResetTestMultiplexRuntime(tb testing.TB, numChains int) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)

	// Create a test reactor
	testReactor := makeTestReactor(tb, nodeCfg)
	return rootDir, nodeCfg, testReactor
}

// CAUTION: This helper uses a random multiplex config.
// CAUTION: This helper injects a testChainID network.
func ResetTestMultiplexRuntimeMock(tb testing.TB, numChains int) (
	string,
	*config.Config,
	*mx.Reactor,
	mx.GenesisDocSet,
) {
	tb.Helper()

	rootDir,
		testConfig,
		testReactor := ResetTestMultiplexRuntime(tb, numChains)

	testExtChainID := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(tb, testExtChainID)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(tb, err)

	configsPaths := testReactor.GetConfigsPaths()
	configsPaths[testChainID] = testConfDir
	testReactor.SetConfigsPaths(configsPaths)

	testPrivValidator, err := testReactor.MakeNetworkValidator(
		testExtChainID,
		testConfDir,
		testDataDir,
	)
	require.NoError(tb, err)

	testIcsGenDocSet, err := testReactor.MakeNetworkGenesis(
		testExtChainID,
		testConfDir,
		testPrivValidator,
		[]string{},
	)
	assert.NoError(tb, err)

	return rootDir, testConfig, testReactor, testIcsGenDocSet.GenesisDocs
}

// CAUTION: This helper uses a random multiplex config.
// CAUTION: This helper injects a testChainID network.
func ResetTestMultiplexRuntimeWithInjection(tb testing.TB, numChains int) (
	string,
	*config.Config,
	*mx.Reactor,
) {
	tb.Helper()

	rootDir,
		testConfig,
		testReactor := ResetTestMultiplexRuntime(tb, numChains)

	// Inject testChainID
	err := testReactor.AllocateNetwork(testChainID)
	require.NoError(tb, err, "should allocate network resources")

	configsPaths := testReactor.GetConfigsPaths()
	require.Contains(tb, configsPaths, testChainID)

	testConfDir := configsPaths[testChainID]
	testValidator := ed25519.GenPrivKey()
	testGenesisDoc := types.GenesisDoc{
		GenesisTime:     cmttime.Now(),
		ChainID:         testChainID,
		ConsensusParams: types.DefaultConsensusParams(),
		Validators: []types.GenesisValidator{
			types.GenesisValidator{
				Address: testValidator.PubKey().Address(),
				PubKey:  testValidator.PubKey(),
				Power:   10,
			},
		},
		InitialHeight: int64(123),
	}

	_, injectErr := testReactor.InjectGenesisDoc(
		testChainID,
		testConfDir,
		testGenesisDoc,
	)
	require.NoError(tb, injectErr, "should inject network genesis doc")

	return rootDir, testConfig, testReactor
}

// ----------------------------------------------------------------------------
// TestMultiplexNode

func ResetTestMultiplexNodeWithConfigAndPorts(
	tb testing.TB,
	rootDir string,
	metricsSuffix string,
	mxConfig config.MultiplexConfig,
	discoveryPort uint16,
	createNewRootDir bool,
	mockGenesisAndPrivVal bool,
) (string, *config.Config) {
	tb.Helper()

	// Subsequent calls to MkdirTemp use different name.
	if createNewRootDir {
		tmpRootDir, err := os.MkdirTemp("", rootDir)
		require.NoError(tb, err)

		rootDir = tmpRootDir
	}

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = mxConfig
	nodeCfg.DiscoveryPort = discoveryPort
	nodeCfg.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", discoveryPort+1)
	nodeCfg.P2P.ExternalAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort+1)
	nodeCfg.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort-1)
	nodeCfg.Instrumentation.Namespace += metricsSuffix
	nodeCfg.Consensus.CreateEmptyBlocks = true // when using *Node

	// Make sure we have /data and /config
	testChainRegistry := makeChainRegistryFromConfig(tb, mxConfig)
	_, err := mx.NewMultiplexFS(nodeCfg, testChainRegistry.GetChains())
	require.NoError(tb, err, "should create filesystem structure for multiplex")

	if mockGenesisAndPrivVal {
		// Make sure we have a *multi-doc* genesis file (GenesisDocSet)
		genesisFilePath := filepath.Join(rootDir, nodeCfg.Genesis)

		// IMPORTANT:
		// If there is no genesis file at the configured path, we will create it
		// with testOneScopedGenesisFmt.

		if !cmtos.FileExists(genesisFilePath) {
			testGenesis := `[`
			for userAddress, chainIds := range nodeCfg.UserChains {
				for _, chainID := range chainIds {
					// Creates one genesis doc per pair of user address and chainId
					chainTestGenesis := fmt.Sprintf(testGenesisDocWithValidatorsFmt, chainID, testDefaultGenesisValidator)
					testGenesis += chainTestGenesis + ","

					// resets priv validators to default state/key (as present in genesis)
					// useDefaultPrivValidator=true
					ResetMultiplexPrivValidator(nodeCfg.BaseConfig, userAddress, chainID, nil, true)
				}
			}

			if 0 == len(nodeCfg.UserChains) {
				testGenesis = testGenesis + `]`
			} else {
				// Removes last comma and closes json array
				testGenesis = testGenesis[:len(testGenesis)-1] + `]`
			}

			cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
		}
	}

	return rootDir, nodeCfg
}

func ResetTestMultiplexNodeWithRootDirAndPorts(
	tb testing.TB,
	numChains int,
	rootDir string,
	discoveryPort uint16,
	mockGenesisAndPrivVal bool,
) (string, *config.Config) {
	tb.Helper()

	return ResetTestMultiplexNodeWithConfigAndPorts(
		tb,
		rootDir,
		"", // metricsSuffix
		makeRandomMultiplexConfig(tb, numChains, int(discoveryPort)),
		discoveryPort,
		true, // create new temp root dir
		mockGenesisAndPrivVal,
	)
}

// CAUTION: this test method sets up a random multiplex with a valid GenesisDocSet.
// CAUTION: this method forcefully *disables state-sync* to permit starting new networks.
func ResetTestMultiplexNode(tb testing.TB, numChains int) (string, *config.Config) {
	tb.Helper()

	return ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name(),
		30001, // discovery
		false, // tests using this helper have PrivVal/Genesis allocated dynamically.
	)
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
