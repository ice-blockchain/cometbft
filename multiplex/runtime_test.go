package multiplex_test

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

const (
	testAddress = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	testFinHash = "1A63C0E60122F9BB"
	testChainID = "mx-chain-" + testAddress + "-" + testFinHash
)

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
	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

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

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	// Act
	actualDBs, err := testReactor.MakeNetworkDatabases(testExtChainID, []string{
		"state",
		"blockstore",
		"tx_index",
		"evidence",
	})
	assert.NoError(t, err, "should create network databases")
	assert.Len(t, actualDBs, 4)
	assert.Contains(t, actualDBs, "state")
	assert.Contains(t, actualDBs, "blockstore")
	assert.Contains(t, actualDBs, "tx_index")
	assert.Contains(t, actualDBs, "evidence")
	assert.NotNil(t, actualDBs["state"])

	// Test db read/write operations
	actualStateDB := actualDBs["state"]
	setErr := actualStateDB.SetSync([]byte("testKey"), []byte("testValue"))
	assert.NoError(t, setErr, "should set test key in newly created database")

	actualValue, getErr := actualStateDB.Get([]byte("testKey"))
	assert.NoError(t, getErr, "should get test key in newly created database")
	assert.Equal(t, []byte("testValue"), actualValue)
}

func TestMultiplexRuntimeMakeNetworkValidator(t *testing.T) {
	defer goleak.VerifyNone(t)

	rootDir,
		_,
		testReactor := ResetTestMultiplexRuntime(t, 5)
	defer os.RemoveAll(rootDir)

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(t, err)

	// Act
	actualPrivValidator, err := testReactor.MakeNetworkValidator(
		testConfDir,
		testDataDir,
	)
	assert.NoError(t, err)

	// Re-act should LOAD, not GEN!
	actualPrivValidatorReload, err := testReactor.MakeNetworkValidator(
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

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(t, err)

	testPrivValidator, err := testReactor.MakeNetworkValidator(
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

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	actualDBs, err := testReactor.MakeNetworkDatabases(testExtChainID, []string{
		"state",
		"blockstore",
	})
	require.NoError(t, err)

	actualGenesisDoc, _, err := testGenesisDocSet.SearchGenesisDocByChainID(
		testChainID,
	)
	require.NoError(t, err)

	// Act
	actualStateMachine,
		actualStateStore,
		actualBlockStore,
		err := testReactor.MakeNetworkStateMachine(
		&actualGenesisDoc,
		actualDBs["state"],
		actualDBs["blockstore"],
	)
	assert.NoError(t, err, "should create network state machine")

	assert.NotNil(t, actualStateMachine)
	assert.NotNil(t, actualStateStore)
	assert.NotNil(t, actualBlockStore)

	assert.Equal(t, testChainID, actualStateMachine.ChainID)
	assert.Equal(t, int64(1), actualStateMachine.InitialHeight)
	assert.Equal(t, int64(0), actualStateMachine.LastBlockHeight) // LastBlockHeight=0 at genesis
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

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

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
	storageProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStorage)
	blockDBProvider := testReactor.GetInstanceProvider(mx.InstanceKeyDatabaseBlock)
	stateDBProvider := testReactor.GetInstanceProvider(mx.InstanceKeyDatabaseState)
	privValProvider := testReactor.GetInstanceProvider(mx.InstanceKeyPrivValidator)

	perUserFolder := "/" + testAddress + "/"
	perChainFolder := "/" + testChainID
	chainDataFolder := storageProvider(testChainID)
	assert.NotEmpty(t, chainDataFolder)
	assert.Contains(t, chainDataFolder, perUserFolder)
	assert.Contains(t, chainDataFolder, perChainFolder)

	blockDB := blockDBProvider(testChainID).(dbm.DB)
	stateDB := stateDBProvider(testChainID).(dbm.DB)
	privVal := privValProvider(testChainID).(types.PrivValidator)

	assert.NotNil(t, blockDB)
	assert.NotNil(t, stateDB)
	assert.NotNil(t, privVal)

	// And test that we can query the priv validator
	actualPubKey, err := privVal.GetPubKey()
	assert.NoError(t, err)
	assert.NotNil(t, actualPubKey)
	assert.NotEmpty(t, actualPubKey.Bytes())
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
	defer goleak.VerifyNone(t)

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
}

// TODO(midas): move this test as AllocateNetwork was extracted.
func TestMultiplexRuntimeInjectNewNetworkCallsAllocateNetwork(t *testing.T) {
	defer goleak.VerifyNone(t)

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

	// - Should have called AllocateNetwork which encapsulates calls to:
	// MakeNetworkFilesystem, MakeNetworkDatabases and MakeNetworkValidator
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
	stateDBProvider := testReactor.GetInstanceProvider(mx.InstanceKeyDatabaseState)
	blockDBProvider := testReactor.GetInstanceProvider(mx.InstanceKeyDatabaseBlock)

	stateDBUnsafe := stateDBProvider(testChainID)
	assert.NotNil(t, stateDBUnsafe)
	stateDB := stateDBUnsafe.(dbm.DB)
	assert.NotNil(t, stateDB)

	blockDBUnsafe := blockDBProvider(testChainID)
	assert.NotNil(t, blockDBUnsafe)
	blockDB := blockDBUnsafe.(dbm.DB)
	assert.NotNil(t, blockDB)

	// .. and a priv validator instance
	privValProvider := testReactor.GetInstanceProvider(mx.InstanceKeyPrivValidator)
	privValidatorUnsafe := privValProvider(testChainID)
	assert.NotNil(t, privValidatorUnsafe)
	privValidator := privValidatorUnsafe.(types.PrivValidator)
	assert.NotNil(t, privValidator)

}

func TestMultiplexRuntimeInjectNewNetworkCallsInjectStateMachine(t *testing.T) {
	defer goleak.VerifyNone(t)

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

	// - Should have called InjectStateMachine which encapsulates the creation
	// of a state machine, a state store and a blocks store.
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
}

func TestMultiplexRuntimeInjectNewNetworkCallsRegisterNetwork(t *testing.T) {
	defer goleak.VerifyNone(t)

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

	// - Should have called RegisterNetwork
	testChainIds := testReactor.GetNetworks()
	assert.Len(t, testChainIds, numChains+1) // Injected 1
	assert.Equal(t, true, testReactor.HasNetwork(testChainID))

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
	defer goleak.VerifyNone(t)

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

	testWithChainID := makeChainID("test-chain-1")

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
	defer goleak.VerifyNone(t)

	numChains := 0

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t, numChains, cmtlog.NewNopLogger(), true) // startServers=true

	defer shutdownFn(testReactor)

	injectChainID := makeChainID("test-inject-1")

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

func TestMultiplexRuntimeInjectNewRuntimeWithOthers(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	_, testReactor, shutdownFn := assertStartNodesMultiplex(t, numChains, cmtlog.NewNopLogger(), true) // startServers=true

	defer shutdownFn(testReactor)

	injectChainID := makeChainID("test-inject-1")

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

	waitDuration := 2 * time.Second
	time.Sleep(waitDuration)
}

// ----------------------------------------------------------------------------
// Helpers

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

	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(tb, err)

	testConfDir,
		testDataDir,
		err := testReactor.MakeNetworkFilesystem(testExtChainID)
	require.NoError(tb, err)

	configsPaths := testReactor.GetConfigsPaths()
	configsPaths[testChainID] = testConfDir
	testReactor.SetConfigsPaths(configsPaths)

	testPrivValidator, err := testReactor.MakeNetworkValidator(
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
