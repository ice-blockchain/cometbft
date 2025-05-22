package multiplex_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

func TestMultiplexRoutinesNodeReplRequest(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 3
	numRelays := 3

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")

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
		time.Sleep(10 * time.Second)
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// To debug the service execution (excluding startup) change this logger
	backendLogger := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "backend-1")
	servers[0].SetLogger(backendLogger)

	testRelayAddrs := []*server.RelayAddress{}
	for i := 1; i < len(servers); i++ {
		testReactor := servers[i].GetReactor()
		testNodeID := string(testReactor.GetNodeKey().ID())
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.

		testRelayAddr, err := server.NewRelayAddress(testNodeID + "@127.0.0.1:" + testBroadcastPort)
		require.NoError(t, err)

		testRelayAddrs = append(testRelayAddrs, testRelayAddr)
	}

	// ReplRequest preparations (must dial)
	_, testChainRelays,
		errorRelays := servers[0].GetRelaysByNetwork(context.TODO(), testRelayAddrs)
	require.Len(t, errorRelays, 0) // NO error!
	require.Len(t, testChainRelays, numChains)

	testChainIds := servers[0].GetReactor().GetNetworks()
	useChainID := testChainIds[0]
	require.Contains(t, testChainRelays, useChainID)

	testRemoteRelayAddrs := make([]*server.RelayAddress, 0, len(servers)-1)
	for i := 1; i < len(servers); i++ {
		// Initializes relay 1 discovery switch
		servers[i].CreateOrLoadDiscoveryEventSwitch()

		recipientReactor := servers[i].GetReactor()
		recipientRelayID := string(recipientReactor.GetNodeKey().ID())

		// Relay 1 communicates with Relay X
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.
		testRelayAddr, err := server.NewRelayAddress(
			recipientRelayID + "@127.0.0.1:" + testBroadcastPort,
		)
		require.NoError(t, err)

		discoverErr := servers[0].CheckDialCompatibleRelay(
			context.TODO(),
			testRelayAddr,
		)
		require.NoError(t, discoverErr) // NO error!

		testRemoteRelayAddrs = append(testRemoteRelayAddrs, testRelayAddr)
	}

	servers[0].UpdateAvailableNetworks(testChainIds)

	testCatchupRelays := map[string][]*server.RelayAddress{}
	testCatchupRelays[useChainID] = make([]*server.RelayAddress, 0, len(testRemoteRelayAddrs))
	testCatchupRelays[useChainID] = append(testCatchupRelays[useChainID], testRemoteRelayAddrs...)

	// Act - Relay 1 asks Relay 2 AND Relay 3 to replicate chain x
	nodeReplRequestFn := servers[0].DefaultNodeReplRequestRoutine()
	nodeReplRequestFn(context.TODO(),
		testCatchupRelays[useChainID],
		useChainID,
		make(chan<- client.BroadcastStatus),
		servers[0].GetLogger().With("tx_batch", "test-no-txes"),
	)

	// Test that ChainReplicationRequest was sent to relay 2
	actualRequestsSent := servers[0].GetReplRequestPeers(useChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, len(testChainRelays[useChainID]))
}

func TestMultiplexBackendRoutinesNetworksCreator(t *testing.T) {

}

func TestMultiplexBackendRoutinesRelaysBroadcast(t *testing.T) {

}

func TestMultiplexBackendRoutinesCancelBroadcast(t *testing.T) {

}
