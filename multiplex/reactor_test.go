package multiplex_test

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/node"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

func makeRandomNodeKey() *p2p.NodeKey {
	priv := ed25519.GenPrivKey()
	return &p2p.NodeKey{PrivKey: priv}
}

// mockGenesisDocSetProviderFunc mocks a GenesisDocSet provider helper.
func mockGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    randomGenesisDocSet(3),
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

// mockErrorGenesisDocSetProviderFunc mocks a provider helper that errors.
func mockErrorGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return nil, errors.New("testError")
	}
}

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

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: This helper starts a nodes multiplex of numChains random chains.
// CAUTION: This helper injects a random chain in the pre-configured Reactor.
func ResetTestMultiplexReactorRuntimeWithInjection(
	tb testing.TB,
	numChains int,
	customLogger cmtlog.Logger,
) (mx.ExtendedChainID, *mx.Reactor, func(*mx.Reactor)) {
	tb.Helper()

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(tb, numChains, customLogger, true) // startServers=true

	// Generate new random network ChainID
	newUserPubKey := ed25519.GenPrivKey().PubKey()
	newUserAddress := newUserPubKey.Address().String()
	fingerprint := makeFingerprint("Posts") // This is the "scope"

	testInjectChainID, err := mx.NewExtendedChainID(newUserAddress, fingerprint)
	require.NoError(tb, err)

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

// Do not use this in TestMultiplexReactorNewReactor.
func makeTestReactor(tb testing.TB, nodeCfg *config.Config) *mx.Reactor {
	tb.Helper()

	return makeTestReactorWithGenesisDocProvider(tb, nodeCfg, mockGenesisDocSetProviderFunc())
}

func makeTestReactorWithGenesisDocProvider(
	tb testing.TB,
	nodeCfg *config.Config,
	genDocProvider node.GenesisDocProvider,
) *mx.Reactor {
	tb.Helper()

	nodeKey := makeRandomNodeKey()

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(tb, err, "should create chain registry instance")

	return mx.NewReactor(
		tb.Context(),
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		genDocProvider,
	)
}

func setReactorNodesStopsServers(reactor *mx.Reactor) {
	testChainIds := reactor.GetNetworks()
	serviceProvider := reactor.GetServicesProvider()
	for _, chainID := range testChainIds {
		nodeForChain := serviceProvider(mx.ServiceKeyNodeRuntime, chainID)
		if nodeForChain == nil {
			continue
		}

		runningNode := nodeForChain.(*cmtnode.Node)

		node.NodeWithStartRPC(true)(runningNode)
		node.NodeWithStartP2P(true)(runningNode)
		node.NodeWithStartMonitor(true)(runningNode)
	}
}
