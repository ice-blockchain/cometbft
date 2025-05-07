package multiplex_test

import (
	"errors"
	"os"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/node"
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
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		mockGenesisDocSetProviderFunc(),
	)

	// NewReactor may not return nil
	assert.NotNil(t, reactor)

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

	// Create services per chain
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			// Test registering a valid service
			eventBus := types.NewEventBus()
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

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for _, chainIds := range nodeCfg.MultiplexConfig.UserChains {
		for _, chainID := range chainIds {
			wg.Add(1)
			go func(concurrentChainID string) {
				// Test registering a valid service in parallel goroutine
				eventBus := types.NewEventBus()
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

	testDBKey := []byte(`testChainId`)

	// Create instances per chain
	// Using ORDERED networks because of ports overwrite content test
	testChainIds := reactor.GetNetworks()
	for index, chainID := range testChainIds {
		// 1. We create a mutated config per chain
		perChainCfg := mx.NewConfigOverwrite(
			nodeCfg,
			reactor.GetChainRegistry(),
			chainID,
		)

		// 2. We create a database instance per chain
		dbName := "chaindb-" + strconv.Itoa(index)
		perChainDB, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)
		// .. and add some data to it
		err = perChainDB.SetSync(testDBKey, []byte(chainID))
		require.NoError(t, err)

		// Test registering a valid instance
		reactor.RegisterInstance(mx.InstanceKeyConfig, chainID, perChainCfg) // "config"
		reactor.RegisterInstance(mx.InstanceKeyDatabaseState, chainID, &mx.ChainDB{
			ChainID: chainID,
			DB:      perChainDB,
		}) // "database/state"
	}

	// And retrieve to assert
	configProvider := reactor.GetInstanceProvider(mx.InstanceKeyConfig)
	assert.NotNil(t, configProvider, "should return multiplex map of config instances")

	databaseProvider := reactor.GetInstanceProvider(mx.InstanceKeyDatabaseState)
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
	otherChainIds := otherReactor.GetNetworks()

	// Following tests the mutex for services and makes sure that the service
	// provider is thread-safe and retrieval of services is always possible.
	var wg sync.WaitGroup
	for index, chainID := range otherChainIds {
		wg.Add(1)
		go func(idx int, concurrentChainID string) {
			// 1. We create a mutated config per chain
			perChainCfg := mx.NewConfigOverwrite(
				nodeCfg,
				otherReactor.GetChainRegistry(),
				concurrentChainID,
			)

			// 2. We create a database instance per chain
			dbName := "chaindb-" + strconv.Itoa(idx)
			perChainDB, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
			require.NoError(t, err)
			// .. and add some data to it
			err = perChainDB.SetSync(testDBKey, []byte(concurrentChainID))
			require.NoError(t, err)

			// Test registering a valid instance in parallel goroutines
			otherReactor.RegisterInstance(mx.InstanceKeyConfig, concurrentChainID, perChainCfg) // "config"
			otherReactor.RegisterInstance(mx.InstanceKeyDatabaseState, concurrentChainID, &mx.ChainDB{
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

	otherDatabaseProvider := reactor.GetInstanceProvider(mx.InstanceKeyDatabaseState)
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
	defer goleak.VerifyNone(t)

	numNetworks := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	testExtChainID,
		testReactor,
		shutdownFn := ResetTestMultiplexReactorRuntimeWithInjection(t, numNetworks, cmtlog.NewNopLogger())

	// Shutdown routine
	defer shutdownFn()

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

	actualChainNodeInfo := testMultiNetNodeInfo.GetNodeInfo(testChainID)
	assert.NotNil(t, actualChainNodeInfo)
	assert.Equal(t, testChainID, actualChainNodeInfo.Network)
}

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: This helper starts a nodes multiplex of numChains random chains.
// CAUTION: This helper injects a random chain in the pre-configured Reactor.
func ResetTestMultiplexReactorRuntimeWithInjection(
	tb testing.TB,
	numChains int,
	customLogger cmtlog.Logger,
) (mx.ExtendedChainID, *mx.Reactor, func()) {
	tb.Helper()

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg, _,
		testReactor := assertStartNodesMultiplex(tb, numChains, customLogger, false) // startServers=false

	// Shutdown routine
	shutdownRoutine := func() {
		defer os.RemoveAll(globalCfg.RootDir)

		err := testReactor.Stop()
		require.NoError(tb, err)
	}

	// Generate new random network ChainID
	newUserPubKey := ed25519.GenPrivKey().PubKey()
	newUserAddress := newUserPubKey.Address().String()
	fingerprint := makeFingerprint("Posts") // This is the "scope"

	testExtChainID, err := mx.NewExtendedChainID(newUserAddress, fingerprint)
	require.NoError(tb, err)

	testChainID := testExtChainID.String()

	// Inject testChainID resources
	allocErr := testReactor.AllocateNetwork(testChainID)
	require.NoError(tb, allocErr, "should allocate network resources")

	// Inject GenesisDoc to prepare state machine
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

	testIcsGenDocSet, injectErr := testReactor.InjectGenesisDoc(
		testChainID,
		testConfDir,
		testGenesisDoc,
	)
	require.NoError(tb, injectErr, "should inject network genesis doc")

	// Prepare state machine for injected network
	stateErr := testReactor.InjectStateMachine(testChainID, testIcsGenDocSet)
	require.NoError(tb, stateErr, "should inject network state machine")

	// Prepare config overwrite (ports, seeds, etc.)
	actualConfOverwrite, configErr := testReactor.MakeNetworkConfigOverwrite(testExtChainID)
	require.NoError(tb, configErr, "should inject network config overwrite")

	// .. must also register in Reactor
	testReactor.RegisterInstance(mx.InstanceKeyConfig, testChainID, actualConfOverwrite)

	return testExtChainID, testReactor, shutdownRoutine
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
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		genDocProvider,
	)
}
