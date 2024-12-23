package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	sm "github.com/ice-blockchain/cometbft/state"
)

func TestMultiplexBackendNewServer(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	rootDir,
		globalCfg := ResetTestMultiplexNode(t, 5) // 5 distinct networks
	require.NotNil(t, globalCfg)
	defer os.RemoveAll(rootDir)

	// Act
	server, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfg,
		cmtlog.NewNopLogger(),
	)
	assert.NoError(t, err, "should create server instance")
	assert.NotNil(t, server)
	assert.NotNil(t, server.GetAcceptor())

	defer func() {
		if server != nil {
			server.Close()
		}
	}()
}

func TestMultiplexBackendMustStart(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 2, cmtlog.NewNopLogger()) // 2 distinct networks
	require.NotNil(t, server)

	defer func() {
		defer os.RemoveAll(rootDir)
		if server != nil {
			err := server.Close()
			assert.NoError(t, err, "should shutdown gracefully")
		}
	}()

	// Act
	server.MustStart()
}

func TestMultiplexBackendGetLocalNetworkHeights(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 1, cmtlog.TestingLogger())
	require.NotNil(t, server)

	defer func() {
		defer os.RemoveAll(rootDir)
		if server != nil {
			err := server.Close()
			assert.NoError(t, err, "should shutdown gracefully")
		}
	}()

	// Start the node backend
	server.MustStart()
	testReactor := server.GetReactor()

	// Read some testables
	testChainID := testReactor.GetNetworks()[0]
	extdChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	testAddress := extdChainID.GetUserAddress()
	testScope := extdChainID.GetFingerprint()

	// Create some test transactions
	transactions := []client.Transaction{
		client.Transaction{Fingerprint: testScope, Data: []byte{1, 2, 3}},
		client.Transaction{Fingerprint: testScope, Data: []byte{4, 5, 6}},
	}

	// Act (1)
	expectedNumNetworks := 1
	actualRequiredNetworks,
		actualMustCreateNetworks := server.GetLocalNetworkHeights(
		testAddress,
		transactions...,
	)

	assert.NotEmpty(t, actualRequiredNetworks)
	assert.Len(t, actualRequiredNetworks, expectedNumNetworks)

	// CAUTION: we are using an existing state machine - not a new network
	assert.Empty(t, actualMustCreateNetworks)
	assert.Len(t, actualMustCreateNetworks, 0)

	// CAUTION: mutating state machine intentionally
	mutatedHeight := int64(1001)
	stateProvider := testReactor.GetInstanceProvider(mx.InstanceKeyState)
	stateMachine := stateProvider(testChainID).(sm.State)
	stateMachine.LastBlockHeight = mutatedHeight
	testReactor.RegisterInstance(mx.InstanceKeyState, testChainID, stateMachine)

	// Act (2)
	expectedNumNetworks = 1
	actualRequiredNetworks2, _ := server.GetLocalNetworkHeights(
		testAddress,
		transactions...,
	)

	assert.NotEmpty(t, actualRequiredNetworks2)
	assert.Len(t, actualRequiredNetworks2, expectedNumNetworks) // transactions use same fingerprint
	assert.Contains(t, actualRequiredNetworks2, testChainID)
	assert.Equal(t, mutatedHeight, actualRequiredNetworks2[testChainID])

	// Creating an unknown network transaction
	expectedNumNetworks = 2
	expectedNewNetworkHeight := int64(1)
	otherScope := makeFingerprint("another one")
	otherChainID := "mx-chain-" + testAddress + "-" + otherScope
	transactions = append(transactions, client.Transaction{
		Fingerprint: otherScope,
		Data:        []byte{7, 8, 9},
	})

	// Act (3)
	actualRequiredNetworks3,
		mustCreateNetworks3 := server.GetLocalNetworkHeights(
		testAddress,
		transactions...,
	)

	assert.NotEmpty(t, actualRequiredNetworks3)
	assert.Len(t, actualRequiredNetworks3, expectedNumNetworks)
	assert.Contains(t, actualRequiredNetworks3, testChainID)
	assert.Contains(t, actualRequiredNetworks3, otherChainID)
	assert.Equal(t, mutatedHeight, actualRequiredNetworks3[testChainID])
	assert.Equal(t, expectedNewNetworkHeight, actualRequiredNetworks3[otherChainID])

	// .. and we introduced an unknown network
	assert.NotEmpty(t, mustCreateNetworks3)
	assert.Len(t, mustCreateNetworks3, 1)
	assert.Equal(t, otherChainID, mustCreateNetworks3[0])
}

// ----------------------------------------------------------------------------
// Helpers

// CAUTION: This helper uses a random multiplex config.
func ResetTestMultiplexBackend(
	tb testing.TB,
	numChains int,
	customLogger cmtlog.Logger,
) (string, *mx.MultiplexBackend) {
	tb.Helper()

	// Uses config.TestConfig() and random MultiplexConfig
	rootDir,
		globalCfg := ResetTestMultiplexNode(tb, numChains)

	server, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfg,
		customLogger,
	)
	require.NoError(tb, err, "should create server instance")

	return rootDir, server
}
