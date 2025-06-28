package multiplex_test

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/node"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
)

var stateKey = []byte("stateKey")

// multiplexGenesisDocProviderFunc mocks a GenesisDocSet provider helper
// by injecting ChainID values in genesis docs.
// CAUTION this should only be done for testing.
func mockMultiplexGenesisDocProviderFunc(
	conf *config.MultiplexConfig,
	numChains int,
) node.GenesisDocProvider {
	genesisDocSet := randomGenesisDocSet(numChains)

	// Requires synchrony between passed MultiplexConfig and numChains
	index := 0
	for _, chainIds := range conf.UserChains {
		for _, chainID := range chainIds {
			// CAUTION intentionally malleating GenesisDoc
			genesisDocSet[index].ChainID = chainID
			index++
		}
	}

	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    genesisDocSet,
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

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

// ----------------------------------------------------------------------------
// Helpers

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
