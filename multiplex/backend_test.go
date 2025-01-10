package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
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
	testSwitch := server.EventSwitch()
	assert.NotNil(t, testSwitch)

	// Test that the broadcast port is opened
	testTransport := testSwitch.Transport()
	assert.NotNil(t, testTransport)
	assert.Equal(t, true, testTransport.IsListening()) // LISTEN
}

func TestMultiplexBackendClose(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 2, cmtlog.NewNopLogger()) // 2 distinct networks
	require.NotNil(t, server)

	defer os.RemoveAll(rootDir)

	// Prepare
	server.MustStart()
	testSwitch := server.EventSwitch()
	require.NotNil(t, testSwitch)

	// Act (1) - Test that we can shutdown gracefully
	err := server.Close()
	assert.NoError(t, err, "should shutdown gracefully")

	testReactor := testSwitch.Reactor("MULTIPLEX")
	assert.NotNil(t, testReactor)
	assert.Equal(t, false, testReactor.IsRunning())

	testTransport := testSwitch.Transport()
	assert.NotNil(t, testTransport)
	assert.Equal(t, false, testTransport.IsListening()) // NOLISTEN on close

	// Act (2) - Test that we can restart
	server.MustStart()
	testSwitch2 := server.EventSwitch()
	assert.NotNil(t, testSwitch2)

	testTransport2 := testSwitch2.Transport()
	assert.NotNil(t, testTransport2)
	assert.Equal(t, true, testTransport2.IsListening()) // LISTEN

	recloseErr := server.Close()
	assert.NoError(t, recloseErr, "should shutdown gracefully")
	assert.Equal(t, false, testTransport2.IsListening()) // NOLISTEN on close
}

func TestMultiplexBackendGetLocalNetworkHeights(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 1, cmtlog.NewNopLogger())
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

	// Act (1) - Test with *existing network*
	expectedNumNetworks := 1
	actualRequiredNetworks,
		actualMustCreateNetworks := server.GetLocalNetworkHeights(
		testAddress,
		transactions...,
	)

	assert.NotEmpty(t, actualRequiredNetworks)
	assert.Len(t, actualRequiredNetworks, expectedNumNetworks)
	assert.Empty(t, actualMustCreateNetworks)
	assert.Len(t, actualMustCreateNetworks, 0) // no new networks

	// CAUTION: mutating state machine intentionally
	mutatedHeight := int64(1001)
	stateProvider := testReactor.GetInstanceProvider(mx.InstanceKeyState)
	stateMachine := stateProvider(testChainID).(sm.State)
	stateMachine.LastBlockHeight = mutatedHeight
	testReactor.RegisterInstance(mx.InstanceKeyState, testChainID, stateMachine)

	// Act (2) - Test with *existing network* and mutated last height
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

	// Act (3) - Test with *existing network* and *unknown network*.
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

func TestMultiplexBackendDiscoverRelayNetworksWithUnreachableRelay(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 0, cmtlog.NewNopLogger())
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
	testSwitch := server.EventSwitch()

	randomPrivKey := ed25519.GenPrivKey()
	testRelayAddr := randomPrivKey.PubKey().Address().String() + "@0.0.0.0"

	// Act (1) - Test that unknown relays return error
	actualNetworks,
		actualListenAddrs,
		discoverErr := server.DiscoverRelayNetworks(
		testSwitch,
		testRelayAddr,
		0, // 0 means to read broadcast port from config
	)

	// Must error and return empty
	assert.Error(t, discoverErr)
	assert.Empty(t, actualNetworks)
	assert.Empty(t, actualListenAddrs)
}

func TestMultiplexBackendDiscoverRelayNetworksWithOnlySelfRelay(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 0, cmtlog.NewNopLogger())
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
	testSwitch := server.EventSwitch()

	// Act (2) - Test that functional relays return network info
	testRelayAddr := string(testReactor.GetNodeKey().ID()) + "@127.0.0.1"
	actualNetworks,
		actualListenAddrs,
		discoverErr := server.DiscoverRelayNetworks(
		testSwitch,
		testRelayAddr,
		0, // 0 means to read broadcast port from config
	)

	assert.NoError(t, discoverErr) // NO error!
	assert.Empty(t, actualNetworks)
	assert.Empty(t, actualListenAddrs)
}

func TestMultiplexBackendDiscoverRelayNetworksWithTwoRelays(t *testing.T) {
	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendTwoInParallel(
		t,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, 2)
	require.Len(t, servers, 2)

	defer func() {
		defer os.RemoveAll(rootDirs[0])
		defer os.RemoveAll(rootDirs[1])

		if servers[0] != nil {
			err := servers[0].Close()
			assert.NoError(t, err, "should shutdown first server gracefully")
		}

		if servers[1] != nil {
			err := servers[1].Close()
			assert.NoError(t, err, "should shutdown second server gracefully")
		}
	}()

	// Start the node backend
	servers[0].MustStart()
	servers[1].MustStart()

	// server 0 talks to server 1
	sourceSwitch := servers[0].EventSwitch()
	sourceReactor := servers[0].GetReactor()
	recipientSwitch := servers[1].EventSwitch()
	recipientReactor := servers[1].GetReactor()

	sourceSwitch.AddUnconditionalPeerIDs([]string{string(recipientReactor.GetNodeKey().ID())})
	recipientSwitch.AddUnconditionalPeerIDs([]string{string(sourceReactor.GetNodeKey().ID())})

	// Act - Relay 1 communicates with Relay 2
	testRelayAddr := string(recipientReactor.GetNodeKey().ID()) + "@127.0.0.1"
	actualNetworks,
		actualListenAddrs,
		discoverErr := servers[0].DiscoverRelayNetworks(
		sourceSwitch,
		testRelayAddr,
		50002, // second relay uses broadcast port 50002
	)

	assert.NoError(t, discoverErr) // NO error!
	assert.Empty(t, actualNetworks)
	assert.Empty(t, actualListenAddrs)
}

func TestMultiplexBackendFetchRelayAddresses(t *testing.T) {

}

func TestMultiplexBackendAddTransaction(t *testing.T) {

}

func TestMultiplexBackendRemoveTransaction(t *testing.T) {

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

// CAUTION: This helper uses a random multiplex config.
func ResetTestMultiplexBackendTwoInParallel(
	tb testing.TB,
	customLoggerRelay1 cmtlog.Logger,
	customLoggerRelay2 cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		0, // 0 networks
		tb.Name()+"-1",
		10001,
		20001,
		50001,
	)

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay2,
		globalCfgRelay2 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		0, // 0 networks
		tb.Name()+"-2",
		30001,
		40001,
		50002,
	)

	serverRelay1, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfgRelay1,
		customLoggerRelay1,
	)
	require.NoError(tb, err, "should create first server instance")

	serverRelay2, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfgRelay2,
		customLoggerRelay2,
	)
	require.NoError(tb, err, "should create second server instance")

	return []string{rootDirRelay1, rootDirRelay2}, []*mx.MultiplexBackend{
		serverRelay1,
		serverRelay2,
	}
}
