package multiplex_test

import (
	"context"
	"os"
	"strconv"
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
	testRelayAddr := randomPrivKey.PubKey().Address().String() + "@0.0.0.0:1234"

	// Act (1) - Test that unknown relays return error
	actualNetworks,
		actualListenAddrs,
		discoverErr := server.DiscoverRelayNetworks(
		testSwitch,
		testRelayAddr,
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
	testRelayAddr := string(testReactor.GetNodeKey().ID()) + "@127.0.0.1:50001"
	actualNetworks,
		actualListenAddrs,
		discoverErr := server.DiscoverRelayNetworks(
		testSwitch,
		testRelayAddr,
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
		0, // 0 networks
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
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	sourceSwitch.AddUnconditionalPeerIDs([]string{string(recipientReactor.GetNodeKey().ID())})
	recipientSwitch.AddUnconditionalPeerIDs([]string{string(sourceReactor.GetNodeKey().ID())})

	// Act - Relay 1 communicates with Relay 2
	testRelayAddr := recipientNodeID + "@127.0.0.1:50002"
	actualNetworks,
		actualListenAddrs,
		discoverErr := servers[0].DiscoverRelayNetworks(
		sourceSwitch,
		testRelayAddr,
	)

	assert.NoError(t, discoverErr) // NO error!
	assert.Empty(t, actualNetworks)
	assert.Empty(t, actualListenAddrs)
}

func TestMultiplexBackendDiscoverRelayNetworksIncompatibleRelays(t *testing.T) {
	numChains := 3

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendTwoInParallel(
		t,
		numChains,
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
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	sourceSwitch.AddUnconditionalPeerIDs([]string{string(recipientReactor.GetNodeKey().ID())})
	recipientSwitch.AddUnconditionalPeerIDs([]string{string(sourceReactor.GetNodeKey().ID())})

	// Act - Relay 1 communicates with Relay 2
	testRelayAddr := recipientNodeID + "@127.0.0.1:50002"
	actualNetworks,
		actualListenAddrs,
		discoverErr := servers[0].DiscoverRelayNetworks(
		sourceSwitch,
		testRelayAddr,
	)

	assert.Error(t, discoverErr, "incompatible: peer does not have at least one replicated chain in common")
	assert.Empty(t, actualNetworks)
	assert.Empty(t, actualListenAddrs)
}

func TestMultiplexBackendDiscoverRelayNetworksCompatibleRelays(t *testing.T) {
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
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

	// Start the node backend
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// server 0 talks to server 1
	sourceSwitch := servers[0].EventSwitch()
	sourceReactor := servers[0].GetReactor()
	recipientSwitch := servers[1].EventSwitch()
	recipientReactor := servers[1].GetReactor()
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	sourceSwitch.AddUnconditionalPeerIDs([]string{string(recipientReactor.GetNodeKey().ID())})
	recipientSwitch.AddUnconditionalPeerIDs([]string{string(sourceReactor.GetNodeKey().ID())})

	// Act - Relay 1 communicates with Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100)) // 50101 (second relay)
	testRelayAddr := recipientNodeID + "@127.0.0.1:" + testBroadcastPort
	actualNetworks,
		actualListenAddrs,
		discoverErr := servers[0].DiscoverRelayNetworks(
		sourceSwitch,
		testRelayAddr,
	)

	assert.NoError(t, discoverErr) // NO error!
	assert.NotEmpty(t, actualNetworks)
	assert.NotEmpty(t, actualListenAddrs)
	assert.Len(t, actualNetworks, numChains)
}

func TestMultiplexBackendDiscoverRelayNetworksSevenCompatibleRelays(t *testing.T) {
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
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// Test where RELAY_1 talks to RELAY_X
	sourceSwitch := servers[0].EventSwitch()
	sourceReactor := servers[0].GetReactor()
	sourceRelayID := string(sourceReactor.GetNodeKey().ID())
	for i := 1; i < len(servers); i++ {
		recipientSwitch := servers[i].EventSwitch()
		recipientReactor := servers[i].GetReactor()
		recipientRelayID := string(recipientReactor.GetNodeKey().ID())

		sourceSwitch.AddUnconditionalPeerIDs([]string{recipientRelayID})
		recipientSwitch.AddUnconditionalPeerIDs([]string{sourceRelayID})

		// Act - Relay 1 communicates with Relay X
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.
		testRelayAddr := recipientRelayID + "@127.0.0.1:" + testBroadcastPort
		actualNetworks,
			actualListenAddrs,
			discoverErr := servers[0].DiscoverRelayNetworks(
			sourceSwitch,
			testRelayAddr,
		)

		assert.NoError(t, discoverErr) // NO error!
		assert.NotEmpty(t, actualNetworks)
		assert.NotEmpty(t, actualListenAddrs)
		assert.Len(t, actualNetworks, numChains)
	}
}

func TestMultiplexBackendFetchRelayAddresses(t *testing.T) {
	numChains := 3
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() //cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() //cmtlog.TestingLogger().With("process", "relay-2")

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
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// server 0 talks to server 1
	sourceSwitch := servers[0].EventSwitch()
	sourceReactor := servers[0].GetReactor()
	recipientSwitch := servers[1].EventSwitch()
	recipientReactor := servers[1].GetReactor()
	recipientNodeID := string(recipientReactor.GetNodeKey().ID())

	sourceSwitch.AddUnconditionalPeerIDs([]string{string(recipientReactor.GetNodeKey().ID())})
	recipientSwitch.AddUnconditionalPeerIDs([]string{string(sourceReactor.GetNodeKey().ID())})

	// Act - Relay 1 fetches addresses of Relay 2
	testBroadcastPort := strconv.Itoa(50001 + (1 * 100)) // 50101 (second relay)
	testRelayAddr := recipientNodeID + "@127.0.0.1:" + testBroadcastPort
	chainRelays, errorRelays := servers[0].FetchRelayAddresses([]string{
		testRelayAddr,
	})

	assert.Len(t, errorRelays, 0) // NO error!
	assert.Len(t, chainRelays, numChains)

	for testChainID, testChainRelays := range chainRelays {
		assert.NotEmpty(t, testChainID)
		assert.NotEmpty(t, testChainRelays)
	}
}

func TestMultiplexBackendAddTransactions(t *testing.T) {
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 1, cmtlog.NewNopLogger()) // 1 network
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

	testChainID := server.GetReactor().GetNetworks()[0]
	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

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
	// Uses config.TestConfig() and random MultiplexConfig
	// For debug, change the logger to cmtlog.TestingLogger()
	rootDir,
		server := ResetTestMultiplexBackend(t, 1, cmtlog.NewNopLogger()) // 1 network
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

	testChainID := server.GetReactor().GetNetworks()[0]
	testExtChainID, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err)

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

func TestMultiplexBackendRoutinesNodeReplRequest(t *testing.T) {
	numChains := 3
	numRelays := 3

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() //cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() //cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() //cmtlog.TestingLogger().With("process", "relay-3")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	testRelayAddrs := []string{}
	for i := 1; i < len(servers); i++ {
		testReactor := servers[i].GetReactor()
		testNodeID := string(testReactor.GetNodeKey().ID())
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.

		testRelayAddr := testNodeID + "@127.0.0.1:" + testBroadcastPort
		testRelayAddrs = append(testRelayAddrs, testRelayAddr)
	}

	chainRelays,
		errorRelays := servers[0].FetchRelayAddresses(testRelayAddrs)
	require.Len(t, errorRelays, 0) // NO error!
	require.Len(t, chainRelays, numChains)

	useChainID := servers[0].GetReactor().GetNetworks()[0]
	require.Contains(t, chainRelays, useChainID)

	// Act - Relay 1 asks Relay 2 AND Relay 3 to replicate chain x
	nodeReplRequestFn := servers[0].DefaultNodeReplRequestRoutine()
	nodeReplRequestFn(context.TODO(),
		chainRelays[useChainID],
		useChainID,
		&mx.StatusNotifier{},
	)

	// Test that ChainReplicationRequest was sent to relay 2
	actualRequestsSent := servers[0].GetReplRequestPeers(useChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, len(chainRelays[useChainID]))
}

func TestMultiplexBackendRoutinesNetworksCreator(t *testing.T) {

}

func TestMultiplexBackendRoutinesRelaysBroadcast(t *testing.T) {

}

func TestMultiplexBackendRoutinesCancelBroadcast(t *testing.T) {

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
	numChains int,
	customLoggerRelay1 cmtlog.Logger,
	customLoggerRelay2 cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-1",
		10001,
		20001,
		50001,
	)

	// Uses config.TestConfig() and empty MultiplexConfig
	rootDirRelay2,
		globalCfgRelay2 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
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

// CAUTION: This helper uses a random multiplex config on multiple relays.
func ResetTestMultiplexBackendCompatibleRelays(
	tb testing.TB,
	numChains int,
	numRelays int,
	customLoggers ...cmtlog.Logger,
) ([]string, []*mx.MultiplexBackend) {
	tb.Helper()

	require.Len(tb, customLoggers, numRelays)

	rootDirs := make([]string, numRelays)
	backends := make([]*mx.MultiplexBackend, numRelays)

	// The first relay is configured with a RANDOM multiplex config.
	rootDirRelay1,
		globalCfgRelay1 := ResetTestMultiplexNodeWithRootDirAndPorts(
		tb,
		numChains,
		tb.Name()+"-1", // rootDir
		10001,
		20001,
		50001,
	)

	serverRelay1, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfgRelay1,
		customLoggers[0],
	)
	require.NoError(tb, err, "should create first server instance")

	rootDirs[0] = rootDirRelay1
	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		// Uses config.TestConfig() and empty MultiplexConfig
		rootDirRelayX,
			globalCfgRelayX := ResetTestMultiplexNodeWithConfigAndPorts(
			tb,
			tb.Name()+"-"+strconv.Itoa(r+1), // rootDir
			"_"+strconv.Itoa(r+1),           // metricsSuffix
			globalCfgRelay1.MultiplexConfig,
			uint16(10001+(r*100)), // 10101, 10201, 10301, 10401
			uint16(20001+(r*100)), // 20101, 20201, 20301, 20401
			uint16(50001+(r*100)), // 50101, 50201, 50301, 50401
		)

		serverRelayX, err := mx.NewServer(
			&client.DefaultAcceptor{},
			globalCfgRelayX,
			customLoggers[r],
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		rootDirs[r] = rootDirRelayX
		backends[r] = serverRelayX
	}

	return rootDirs, backends
}
