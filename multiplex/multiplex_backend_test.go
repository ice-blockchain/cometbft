package multiplex_test

import (
	"context"
	"encoding/hex"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	sm "github.com/ice-blockchain/cometbft/state"
)

func makeEmptyBackendOptions(numRelays int) [][]mx.MultiplexBackendOption {
	opts := make([][]mx.MultiplexBackendOption, numRelays)
	for i := 0; i < numRelays; i++ {
		opts[i] = []mx.MultiplexBackendOption{}
	}

	return opts
}

func closeAndRemoveAll(tb testing.TB, rootDir string, server *mx.MultiplexBackend) {
	tb.Helper()

	defer os.RemoveAll(rootDir)

	err := server.Close()
	assert.NoError(tb, err, "should shutdown server gracefully")
}

func TestMultiplexBackendNewServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	// TEST 1: Create a server with pre-configured networks.
	numChains := 5
	rootDir,
		globalCfg := ResetTestMultiplexNode(t, numChains)
	require.NotNil(t, globalCfg)

	backend, err := mx.NewServer(
		t.Context(),
		&client.DefaultAcceptor{},
		globalCfg,
		cmtlog.NewNopLogger(),
	)

	assert.NoError(t, err, "should create server instance")
	assert.NotNil(t, backend)
	assert.NotNil(t, backend.GetAcceptor())
	assert.NotNil(t, backend.GetReactor())

	actualNetworks := backend.GetReactor().GetNetworks()
	assert.Len(t, actualNetworks, numChains)
	closeAndRemoveAll(t, rootDir, backend)

	// TEST 2: Create a server without pre-configured networks.
	zeroChains := 0
	rootDir2,
		globalCfg2 := ResetTestMultiplexNode(t, zeroChains)
	require.NotNil(t, globalCfg2)

	// Act
	backend2, err2 := mx.NewServer(
		t.Context(),
		&client.DefaultAcceptor{},
		globalCfg2,
		cmtlog.NewNopLogger(),
	)

	assert.NoError(t, err2, "should create server instance")
	assert.NotNil(t, backend2)
	assert.NotNil(t, backend2.GetAcceptor())
	require.NotNil(t, backend2.GetReactor())

	actualNetworks2 := backend2.GetReactor().GetNetworks()
	assert.Len(t, actualNetworks2, zeroChains)
	closeAndRemoveAll(t, rootDir2, backend2)
}

func TestMultiplexBackendMustStart(t *testing.T) {
	defer goleak.VerifyNone(t)

	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 2
	rootDir,
		backend := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger())
	require.NotNil(t, backend)

	defer closeAndRemoveAll(t, rootDir, backend)

	// Act
	backend.MustStart(t.Context())

	testReactor := backend.GetReactor()
	require.NotNil(t, testReactor)

	testSwitch := testReactor.GetEventSwitchForDiscovery()
	require.NotNil(t, testSwitch)

	// Test that the discovery is listening
	testTransport := testSwitch.Transport()
	assert.NotNil(t, testTransport)
	assert.Equal(t, true, testTransport.IsListening()) // LISTEN
}

func TestMultiplexBackendMustStartEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 0
	rootDir,
		backend := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger())
	require.NotNil(t, backend)

	defer closeAndRemoveAll(t, rootDir, backend)

	// Act
	backend.MustStart(t.Context())

	testReactor := backend.GetReactor()
	require.NotNil(t, testReactor)

	testSwitch := testReactor.GetEventSwitchForDiscovery()
	require.NotNil(t, testSwitch)

	// Test that the discovery is listening
	testTransport := testSwitch.Transport()
	assert.NotNil(t, testTransport)
	assert.Equal(t, true, testTransport.IsListening()) // LISTEN
}

func TestMultiplexBackendGetLocalNetworkHeights(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 1
	rootDir,
		backend := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger())
	require.NotNil(t, backend)

	defer closeAndRemoveAll(t, rootDir, backend)

	// Start the node backend
	backend.MustStart(t.Context())

	testReactor := backend.GetReactor()
	require.NotNil(t, testReactor)

	testChainIds := testReactor.GetNetworks()
	require.Len(t, testChainIds, numChains)

	// Read some testables
	testChainID := testChainIds[0]
	extdChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	// Allocate + inject node runtime ("Start node listeners")
	allocErr := testReactor.AllocateNetwork(testChainID)
	require.NoError(t, allocErr, "should initialize network")
	createErr := testReactor.InjectNewNetwork(testChainID, []string{})
	require.NoError(t, createErr, "should inject network")
	injectErr := testReactor.InjectNewRuntime(context.Background(), testChainID)
	require.NoError(t, injectErr, "should inject runtime")

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
		actualMustCreateNetworks := backend.GetLocalNetworkHeights(
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
	actualRequiredNetworks2, _ := backend.GetLocalNetworkHeights(
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
		mustCreateNetworks3 := backend.GetLocalNetworkHeights(
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

func TestMultiplexBackendGetLocalNetworkHeightsEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 1
	rootDir,
		backend := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger())
	require.NotNil(t, backend)

	defer closeAndRemoveAll(t, rootDir, backend)

	// Start the node backend
	backend.MustStart(t.Context())

	testReactor := backend.GetReactor()
	require.NotNil(t, testReactor)

	testChainIds := testReactor.GetNetworks()
	require.Len(t, testChainIds, numChains)

	// Read some testables
	testChainID := makeChainID("random")
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
		actualMustCreateNetworks := backend.GetLocalNetworkHeights(
		testAddress,
		transactions...,
	)

	// Must report the new network (in a map[string]uint64)
	assert.NotEmpty(t, actualRequiredNetworks)
	assert.Len(t, actualRequiredNetworks, expectedNumNetworks)
	assert.Contains(t, actualRequiredNetworks, testChainID)

	// Must report the new network (in a []string)
	assert.NotEmpty(t, actualMustCreateNetworks)
	assert.Len(t, actualMustCreateNetworks, expectedNumNetworks) // new network
	assert.Equal(t, testChainID, actualMustCreateNetworks[0])
}

func TestMultiplexBackendCheckDialCompatibleRelayWithOnlySelfRelay(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		backend := ResetTestMultiplexBackend(t, 0, cmtlog.NewNopLogger())
	require.NotNil(t, backend)

	defer closeAndRemoveAll(t, rootDir, backend)

	// Start the node backend
	backend.MustStart(t.Context())

	testReactor := backend.GetReactor()
	require.NotNil(t, testReactor)

	// Act (2) - Test that functional relays return network info
	testRelayAddr, err := server.NewRelayAddress(
		string(testReactor.GetNodeKey().ID()) + "@127.0.0.1:30001",
	)
	require.NoError(t, err)

	sourceSwitch := testReactor.GetEventSwitchForDiscovery()
	discoverErr := backend.CheckDialCompatibleRelay(
		context.TODO(),
		sourceSwitch,
		testRelayAddr,
	)

	assert.NoError(t, discoverErr) // NO error!
}

func TestMultiplexBackendCheckDialCompatibleRelayWithTwoRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendTwoInParallel(
		t,
		0, // 0 networks
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, 2)
	require.Len(t, servers, 2)

	require.NotNil(t, servers[0])
	require.NotNil(t, servers[1])

	defer func() {
		go closeAndRemoveAll(t, rootDirs[0], servers[0])
		go closeAndRemoveAll(t, rootDirs[1], servers[1])
	}()

	// Start the node backend
	servers[0].MustStart(t.Context())
	servers[1].MustStart(t.Context())

	// server 0 talks to server 1
	recipientReactor := servers[1].GetReactor()
	require.NotNil(t, recipientReactor)
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	// Act - Relay 1 communicates with Relay 2
	testRelayAddr, err := server.NewRelayAddress(
		recipientNodeID + "@127.0.0.1:40001",
	)
	require.NoError(t, err)
	require.NotNil(t, testRelayAddr)

	sourceReactor := servers[0].GetReactor()
	require.NotNil(t, sourceReactor)

	sourceSwitch := sourceReactor.GetEventSwitchForDiscovery()
	require.NotNil(t, sourceSwitch)

	discoverErr := servers[0].CheckDialCompatibleRelay(
		context.TODO(),
		sourceSwitch,
		testRelayAddr,
	)

	assert.NoError(t, discoverErr)

	waitDuration := 2 * time.Second
	time.Sleep(waitDuration)
}

func TestMultiplexBackendCheckDialCompatibleRelaySevenCompatibleRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	numRelays := 7

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-4")
	loggerRelay5 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-5")
	loggerRelay6 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-6")
	loggerRelay7 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-7")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
		loggerRelay5,
		loggerRelay6,
		loggerRelay7,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Test where RELAY_1 talks to RELAY_X
	sourceReactor := servers[0].GetReactor()
	require.NotNil(t, sourceReactor)

	sourceSwitch := sourceReactor.GetEventSwitchForDiscovery()
	require.NotNil(t, sourceSwitch)

	for i := 1; i < len(servers); i++ {
		// recipientSwitch := servers[i].CreateOrLoadDiscoveryEventSwitch()
		recipientReactor := servers[i].GetReactor()
		require.NotNil(t, recipientReactor)

		recipientRelayID := string(recipientReactor.GetNodeKey().ID())

		// Act - Relay 1 communicates with Relay X
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.
		testRelayAddr, err := server.NewRelayAddress(
			recipientRelayID + "@127.0.0.1:" + testBroadcastPort,
		)
		require.NoError(t, err)

		discoverErr := servers[0].CheckDialCompatibleRelay(
			context.TODO(),
			sourceSwitch,
			testRelayAddr,
		)

		assert.NoError(t, discoverErr) // NO error!
	}
}

func TestMultiplexBackendGetRemoteRelayInfo(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Act - Relay 1 discovers ID of Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100))                                 // 50101 (second relay P2P)
	testRelayAddr, err := server.NewRelayAddress("tcp://127.0.0.1:" + testBroadcastPort) // NO ID!
	require.NoError(t, err)

	actualRelayID,
		actualError := servers[0].GetRemoteRelayInfo(context.TODO(), testRelayAddr)

	require.NoError(t, actualError)
	assert.Equal(t, servers[1].GetRelayID(), actualRelayID.DefaultNodeID)
}

func TestMultiplexBackendGetRemoteRelayInfoEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Act - Relay 1 discovers ID of Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100))                                 // 50101 (second relay P2P)
	testRelayAddr, err := server.NewRelayAddress("tcp://127.0.0.1:" + testBroadcastPort) // NO ID!
	require.NoError(t, err)

	actualRelayID,
		actualError := servers[0].GetRemoteRelayInfo(context.TODO(), testRelayAddr)

	require.NoError(t, actualError)
	assert.Equal(t, servers[1].GetRelayID(), actualRelayID.DefaultNodeID)
}

func TestMultiplexBackendGetRemoteRelayInfoWithFourRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	numRelays := 4

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-4")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Act - Relay 1 discovers ID of Relay X
	for i := 1; i < len(servers); i++ {
		testBroadcastPort := strconv.Itoa(50001 + (i * 100))                           // 50101, 50201, etc. (P2P discovery port)
		testRelayAddr, err := server.NewRelayAddress("127.0.0.1:" + testBroadcastPort) // NO ID!
		require.NoError(t, err)

		actualRelayID,
			actualError := servers[0].GetRemoteRelayInfo(context.TODO(), testRelayAddr)

		require.NoError(t, actualError)
		assert.Equal(t, servers[i].GetRelayID(), actualRelayID.DefaultNodeID)
	}
}

func TestMultiplexBackendGetRemoteRelayInfoWithFourRelaysEmpty(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 4

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-4")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Act - Relay 1 discovers ID of Relay X
	for i := 1; i < len(servers); i++ {
		testBroadcastPort := strconv.Itoa(50001 + (i * 100))                           // 50101, 50201, etc. (P2P discovery port)
		testRelayAddr, err := server.NewRelayAddress("127.0.0.1:" + testBroadcastPort) // NO ID!
		require.NoError(t, err)

		actualRelayID,
			actualError := servers[0].GetRemoteRelayInfo(context.TODO(), testRelayAddr)

		require.NoError(t, actualError)
		assert.Equal(t, servers[i].GetRelayID(), actualRelayID.DefaultNodeID)
	}
}

func TestMultiplexBackendGetRelaysByNetwork(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// server 0 talks to server 1
	recipientReactor := servers[1].GetReactor()
	require.NotNil(t, recipientReactor)
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	// Act - Relay 1 fetches addresses of Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100)) // 50101 (second relay)
	testRelayAddr, err := server.NewRelayAddress(recipientNodeID + "@127.0.0.1:" + testBroadcastPort)
	require.NoError(t, err)

	_, chainRelays, errorRelays := servers[0].GetRelaysByNetwork(context.TODO(), []*server.RelayAddress{
		testRelayAddr,
	})

	assert.Len(t, errorRelays, 0) // NO error!
	assert.Len(t, chainRelays, numChains)

	for testChainID, testChainRelays := range chainRelays {
		assert.NotEmpty(t, testChainID)
		assert.NotEmpty(t, testChainRelays)
	}
}

func TestMultiplexBackendGetRelaysByNetworkEmptyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// server 0 talks to server 1
	recipientReactor := servers[1].GetReactor()
	require.NotNil(t, recipientReactor)
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	// Act - Relay 1 fetches addresses of Relay 2
	testDiscoveryPort := 50001 + (1 * 100)
	testBroadcastPort := strconv.Itoa(testDiscoveryPort) // 50101 (second relay)
	testRelayAddr, err := server.NewRelayAddress(recipientNodeID + "@127.0.0.1:" + testBroadcastPort)
	require.NoError(t, err)

	_, chainRelays, errorRelays := servers[0].GetRelaysByNetwork(context.TODO(), []*server.RelayAddress{
		testRelayAddr,
	})

	assert.Len(t, errorRelays, 0) // NO error!
	assert.Len(t, chainRelays, numChains)

	for testChainID, testChainRelays := range chainRelays {
		assert.NotEmpty(t, testChainID)
		assert.NotEmpty(t, testChainRelays) // must return a healthy relay
	}

	expectedRelayID := testRelayAddr.ID()
	expectedListenAddr := testRelayAddr.StringWithoutScheme() // no tcp:// !
	expectedDiscoveryPort := uint16(testDiscoveryPort)

	sourceReactor := servers[0].GetReactor()
	require.NotNil(t, sourceReactor)

	actualRelayInfo := sourceReactor.GetRelayInfo(testRelayAddr.ID())
	require.NotNil(t, actualRelayInfo)
	assert.Equal(t, expectedRelayID, actualRelayInfo.DefaultNodeID,
		"should return correct DefaultNodeID in RelayInfo RPC")
	assert.Equal(t, expectedListenAddr, actualRelayInfo.ListenAddress,
		"should return correct ListenAddress in RelayInfo RPC")
	assert.Equal(t, expectedDiscoveryPort, actualRelayInfo.DiscoveryPort,
		"should return correct DiscoveryPort in RelayInfo RPC")
}

func TestMultiplexBackendGetRemoteValidatorsInfo(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	// Act - Relay 1 orchestrates validators of Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100))                                 // 50101 (second relay P2P)
	testRelayAddr, err := server.NewRelayAddress("tcp://127.0.0.1:" + testBroadcastPort) // NO ID!
	require.NoError(t, err)

	testWithChainID := makeChainID("test-chain-1")

	actualValidatorsResult,
		actualError := servers[0].GetRemoteValidatorsInfo(context.TODO(), testRelayAddr, []string{testWithChainID})

	require.NoError(t, actualError)
	assert.NotEmpty(t, actualValidatorsResult.ValidatorPubs)
	assert.Contains(t, actualValidatorsResult.ValidatorPubs, testWithChainID)
	assert.NotEmpty(t, actualValidatorsResult.ValidatorPubs[testWithChainID])

	// Test that we receive the same validator pubkeys "locally" for relay-2.
	actualValidatorPubs := servers[1].GetValidatorPubs()
	assert.NotEmpty(t, actualValidatorPubs)
	assert.Contains(t, actualValidatorPubs, testWithChainID)
	assert.NotEmpty(t, actualValidatorPubs[testWithChainID])
	assert.Equal(t, actualValidatorsResult.ValidatorPubs[testWithChainID], actualValidatorPubs[testWithChainID])
}

func TestMultiplexBackendGetValidatorsByNetwork(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 4

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-4")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	testRelayAddresses := []*server.RelayAddress{}
	for i := 1; i < numRelays; i++ {
		// server 0 talks to server X
		recipientReactor := servers[i].GetReactor()
		require.NotNil(t, recipientReactor)
		recipientNodeID := string(recipientReactor.GetNodeKey().ID())

		// Relay 1 fetches validators of Relay X
		testDiscoveryPort := 50001 + (i * 100)
		testBroadcastPort := strconv.Itoa(testDiscoveryPort) // 50101, 50201, 50301
		testRelayAddr, err := server.NewRelayAddress(recipientNodeID + "@127.0.0.1:" + testBroadcastPort)
		require.NoError(t, err)

		testRelayAddresses = append(testRelayAddresses, testRelayAddr)
	}

	testWithChainID := makeChainID("test-chain-1")

	actualValidatorsByChain,
		actualValidatorsErr := servers[0].GetValidatorsByNetwork(
		context.TODO(),
		testRelayAddresses,
		[]string{testWithChainID},
	)

	assert.NoError(t, actualValidatorsErr)
	assert.NotEmpty(t, actualValidatorsByChain)
	assert.Contains(t, actualValidatorsByChain, testWithChainID)

	expectedNumValidators := numRelays - 1 // -self
	assert.Len(t, actualValidatorsByChain[testWithChainID], expectedNumValidators)

	for _, testValsPubKeys := range actualValidatorsByChain {
		assert.NotEmpty(t, testValsPubKeys)
		assert.Len(t, testValsPubKeys, expectedNumValidators)

		for _, testValPubKey := range testValsPubKeys {
			actualPubKeyBz, bzErr := hex.DecodeString(testValPubKey)
			assert.NoError(t, bzErr)
			assert.Len(t, actualPubKeyBz, ed25519.PubKeySize)
		}
	}
}

func TestMultiplexBackendAddTransactions(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 1 // mempool requires a ChainID
	rootDir,
		server := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger()) // 1 network
	require.NotNil(t, server)

	defer closeAndRemoveAll(t, rootDir, server)

	// Start the node backend
	server.MustStart(t.Context())

	testReactor := server.GetReactor()
	require.NotNil(t, testReactor)

	testChainIds := testReactor.GetNetworks()
	require.Len(t, testChainIds, numChains)

	testChainID := testChainIds[0]
	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	// Allocate + inject node runtime ("Start node listeners")
	allocErr := testReactor.AllocateNetwork(testChainID)
	require.NoError(t, allocErr, "should initialize network")
	createErr := testReactor.InjectNewNetwork(testChainID, []string{})
	require.NoError(t, createErr, "should inject network")
	injectErr := testReactor.InjectNewRuntime(context.Background(), testChainID)
	require.NoError(t, injectErr, "should inject runtime")

	// Act - Adds a transaction to running mempool
	actualErr := server.AddTransactions(
		testExtChainID.GetUserAddress(),
		client.Transaction{
			Data:        []byte{1, 2, 3},
			Fingerprint: testExtChainID.GetFingerprint(),
		},
	)
	assert.NoError(t, actualErr, "should add transaction to mempool")
}

func TestMultiplexBackendRemoveTransactions(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	numChains := 1
	rootDir,
		server := ResetTestMultiplexBackend(t, numChains, cmtlog.NewNopLogger()) // 1 network
	require.NotNil(t, server)

	defer closeAndRemoveAll(t, rootDir, server)

	// Start the node backend
	server.MustStart(t.Context())

	testReactor := server.GetReactor()
	require.NotNil(t, testReactor)

	testChainIds := testReactor.GetNetworks()
	require.Len(t, testChainIds, numChains)

	testChainID := testChainIds[0]
	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

	// Allocate + inject node runtime ("Start node listeners")
	allocErr := testReactor.AllocateNetwork(testChainID)
	require.NoError(t, allocErr, "should initialize network")
	createErr := testReactor.InjectNewNetwork(testChainID, []string{})
	require.NoError(t, createErr, "should inject network")
	injectErr := testReactor.InjectNewRuntime(context.Background(), testChainID)
	require.NoError(t, injectErr, "should inject runtime")

	err = server.AddTransactions(
		testExtChainID.GetUserAddress(),
		client.Transaction{
			Data:        []byte{1, 2, 3},
			Fingerprint: testExtChainID.GetFingerprint(),
		},
	)
	require.NoError(t, err)

	// Act - Removes a transaction from running mempool
	actualErr := server.RemoveTransactions(
		testExtChainID.GetUserAddress(),
		client.Transaction{
			Data:        []byte{1, 2, 3},
			Fingerprint: testExtChainID.GetFingerprint(),
		},
	)
	assert.NoError(t, actualErr, "should remove transaction from mempool")
}

// TODO(midas): add test for 0-network compatible relays

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
		globalCfg := ResetTestMultiplexNode(tb, numChains) // DiscoveryPort=30001

	server, err := mx.NewServer(
		tb.Context(),
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
	numChains int,
	customLoggerRelay1 cmtlog.Logger,
	customLoggerRelay2 cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	backendOptionsPerRelay := makeEmptyBackendOptions(2)

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-1",
		50001,
	)

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay2,
		globalCfgRelay2 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-2",
		40001,
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelay1.ChainSeeds {
		globalCfgRelay1.ChainSeeds[chainID] = ""
	}
	for chainID := range globalCfgRelay2.ChainSeeds {
		globalCfgRelay2.ChainSeeds[chainID] = ""
	}

	serverRelay1, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfgRelay1,
		customLoggerRelay1,
		backendOptionsPerRelay[0]...,
	)
	require.NoError(tb, err, "should create first server instance")

	serverRelay2, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfgRelay2,
		customLoggerRelay2,
		backendOptionsPerRelay[1]...,
	)
	require.NoError(tb, err, "should create second server instance")

	return []string{rootDirRelay1, rootDirRelay2}, []*mx.MultiplexBackend{
		serverRelay1,
		serverRelay2,
	}
}

func ResetTestMultiplexBackendCompatibleRelaysWithOptions(
	tb testing.TB,
	numChains int,
	numRelays int,
	backendOptionsPerRelay [][]mx.MultiplexBackendOption,
	customLoggers ...cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	rootDirs := make([]string, numRelays)
	backends := make([]*mx.MultiplexBackend, numRelays)

	// The first relay is configured with a RANDOM multiplex config.
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-1", // rootDir
		50001,
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelay1.ChainSeeds {
		globalCfgRelay1.ChainSeeds[chainID] = ""
	}

	emptyBackendOpts := makeEmptyBackendOptions(numRelays)[0]
	relayBackendOpts := emptyBackendOpts
	if len(backendOptionsPerRelay) > 0 {
		relayBackendOpts = backendOptionsPerRelay[0]
	}
	serverRelay1, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfgRelay1,
		customLoggers[0],
		relayBackendOpts...,
	)
	require.NoError(tb, err, "should create first server instance")

	rootDirs[0] = rootDirRelay1
	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		// Uses config.TestConfig() and copy MultiplexConfig
		rootDirRelayX,
			globalCfgRelayX := ResetTestMultiplexNodeWithConfigAndPorts(
			tb,
			tb.Name()+"-"+strconv.Itoa(r+1), // rootDir
			"_"+strconv.Itoa(r+1),           // metricsSuffix
			globalCfgRelay1.MultiplexConfig,
			uint16(50001+(r*100)), // 50101, 50201, 50301, 50401
			true,                  // create new temp root dir
		)

		// Seeds must be valid (or empty), otherwise dialing will fail
		for chainID := range globalCfgRelayX.ChainSeeds {
			globalCfgRelayX.ChainSeeds[chainID] = ""
		}

		relayBackendOpts := emptyBackendOpts
		if len(backendOptionsPerRelay) > r {
			relayBackendOpts = backendOptionsPerRelay[r]
		}
		serverRelayX, err := mx.NewServer(
			tb.Context(),
			&client.DefaultAcceptor{},
			globalCfgRelayX,
			customLoggers[r],
			relayBackendOpts...,
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		rootDirs[r] = rootDirRelayX
		backends[r] = serverRelayX
	}

	return rootDirs, backends
}

// CAUTION: This helper uses a random multiplex config on multiple relays.
func ResetTestMultiplexBackendCompatibleRelays(
	tb testing.TB,
	numChains int,
	numRelays int,
	customLoggers ...cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	require.Len(tb, customLoggers, numRelays,
		"count of loggers passed should be equal numRelays")

	rootDirs := make([]string, numRelays)
	backends := make([]*mx.MultiplexBackend, numRelays)

	backendOptionsPerRelay := makeEmptyBackendOptions(numRelays)

	// The first relay is configured with a RANDOM multiplex config.
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-1", // rootDir
		50001,
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelay1.ChainSeeds {
		globalCfgRelay1.ChainSeeds[chainID] = ""
	}

	relayBackendOpts := backendOptionsPerRelay[0]
	serverRelay1, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfgRelay1,
		customLoggers[0],
		relayBackendOpts...,
	)
	require.NoError(tb, err, "should create first server instance")

	rootDirs[0] = rootDirRelay1
	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		// Uses config.TestConfig() and copy MultiplexConfig
		rootDirRelayX,
			globalCfgRelayX := ResetTestMultiplexNodeWithConfigAndPorts(
			tb,
			tb.Name()+"-"+strconv.Itoa(r+1), // rootDir
			"_"+strconv.Itoa(r+1),           // metricsSuffix
			globalCfgRelay1.MultiplexConfig,
			uint16(50001+(r*100)), // 50101, 50201, 50301, 50401
			true,                  // create new temp root dir
		)

		// Seeds must be valid (or empty), otherwise dialing will fail
		for chainID := range globalCfgRelayX.ChainSeeds {
			globalCfgRelayX.ChainSeeds[chainID] = ""
		}

		relayBackendOpts := backendOptionsPerRelay[r]
		serverRelayX, err := mx.NewServer(
			tb.Context(),
			&client.DefaultAcceptor{},
			globalCfgRelayX,
			customLoggers[r],
			relayBackendOpts...,
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		rootDirs[r] = rootDirRelayX
		backends[r] = serverRelayX
	}

	return rootDirs, backends
}
