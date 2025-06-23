package multiplex_test

import (
	"context"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/p2p"
)

func TestMultiplexRoutinesNodeReplRequestEmptyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
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
		for i := 0; i < len(servers); i++ {
			go closeAndRemoveAll(t, rootDirs[i], servers[i])
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart(t.Context())
	}

	testRelayAddrs := []*server.RelayAddress{}
	for i := 1; i < len(servers); i++ {
		testReactor := servers[i].GetReactor()
		testNodeID := string(testReactor.GetNodeKey().ID())
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.

		testRelayAddr, err := server.NewRelayAddress(testNodeID + "@127.0.0.1:" + testBroadcastPort)
		require.NoError(t, err)

		testRelayAddrs = append(testRelayAddrs, testRelayAddr)
	}

	// ------------
	// ReplRequest preparations:
	// (1) must GET RelayInfo
	// (2) must INJECT new network
	// (3) must DIAL discovery peers

	// (1) fetch the relay information
	_, testChainRelays, errorRelays := servers[0].GetRelaysByNetwork(context.TODO(), testRelayAddrs)
	require.Len(t, errorRelays, 0)    // NO error!
	require.Empty(t, testChainRelays) // both relays MUST replicate for this test.

	// testChainIds := servers[0].GetReactor().GetNetworks()
	// useChainID := testChainIds[0]
	// require.Contains(t, testChainRelays, useChainID)
	useChainID := makeChainID("test-chain-1")
	testReactorRelayOne := servers[0].GetReactor()
	testReactorRelayTwo := servers[1].GetReactor()
	testReactorRelayThree := servers[2].GetReactor()

	// AllocateNetwork is NOT part of InjectNewNetwork anymore, due to it being
	// executed earlier, i.e. see MultiplexBackend.InitValidators.
	allocErr1 := testReactorRelayOne.AllocateNetwork(useChainID)
	require.NoError(t, allocErr1, "should allocate new network resources for relay-1")
	allocErr2 := testReactorRelayTwo.AllocateNetwork(useChainID)
	require.NoError(t, allocErr2, "should allocate new network resources for relay-2")
	allocErr3 := testReactorRelayThree.AllocateNetwork(useChainID)
	require.NoError(t, allocErr3, "should allocate new network resources for relay-3")

	// (2) inject new networks GenesisDoc
	injectErr := testReactorRelayOne.InjectNewNetwork(useChainID, []string{})
	require.NoError(t, injectErr)
	runtimeErr := testReactorRelayOne.InjectNewRuntime(context.Background(), useChainID)
	require.NoError(t, runtimeErr)

	waitGroup := sync.WaitGroup{}
	waitGroup.Add(len(servers) - 1) // -self

	// (3) dial the discovery peers
	sourceDiscoverySwitch := testReactorRelayOne.GetEventSwitchForDiscovery()
	testRemoteRelayAddrs := make([]*server.RelayAddress, 0, len(servers)-1)
	for i := 1; i < len(servers); i++ {
		recipientReactor := servers[i].GetReactor()
		recipientRelayID := string(recipientReactor.GetNodeKey().ID())

		// Relay 1 communicates with Relay X
		testBroadcastPort := strconv.Itoa(50001 + (i * 100)) // 50101, 50201, etc.
		testRelayAddr, err := server.NewRelayAddress(
			recipientRelayID + "@127.0.0.1:" + testBroadcastPort,
		)
		require.NoError(t, err)

		go func(sw *p2p.Switch, relayToDial *server.RelayAddress) {
			defer waitGroup.Done()
			discoverErr := servers[0].CheckDialCompatibleRelay(
				context.TODO(),
				sw,
				relayToDial,
			)
			require.NoError(t, discoverErr) // NO error!
		}(sourceDiscoverySwitch, testRelayAddr)

		testRemoteRelayAddrs = append(testRemoteRelayAddrs, testRelayAddr)
	}

	waitGroup.Wait()

	testCatchupRelays := map[string][]*server.RelayAddress{}
	testCatchupRelays[useChainID] = make([]*server.RelayAddress, 0, len(testRemoteRelayAddrs))
	testCatchupRelays[useChainID] = append(testCatchupRelays[useChainID], testRemoteRelayAddrs...)

	// ------------
	// ReplRequest TEST (Act)
	// - Relay 1 asks Relay 2 AND Relay 3 to replicate chain x

	nodeReplRequestFn := servers[0].DefaultNodeReplRequestRoutine()
	go func() {
		nodeReplRequestFn(context.TODO(),
			testCatchupRelays[useChainID],
			useChainID,
			make(chan<- client.BroadcastStatus, 1),
			loggerRelay1.With("tx_batch", "test-no-txes"),
		)
	}()

	// The recipients send the relay ID in a ChainReplicationResponse.
	// Blocks the broadcast thread until all networks have been acknowledged by relays.
	_,
		expectedNumResponses,
		actualNumResponses,
		replErr := servers[0].WaitForRelaysAckChainReplications(context.TODO(),
		testCatchupRelays,
		client.Transaction{},
	)
	assert.NoError(t, replErr)
	assert.GreaterOrEqual(t, actualNumResponses, expectedNumResponses) // actual >= expected

	// Wait also to receive ChainReplicationComplete, only then we should shutdown.
	// actualNumCompleted, compErr := servers[0].WaitForRelaysReplicationCompleted(context.TODO(),
	// 	[]string{useChainID},
	// 	client.Transaction{},
	// )
	// assert.NoError(t, compErr)
	// assert.GreaterOrEqual(t, actualNumCompleted, expectedNumResponses)

	// Test that ChainReplicationRequest was sent to relay-2 and relay-3
	actualRequestsSent := servers[0].GetReplRequestPeers(useChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, len(testCatchupRelays[useChainID]))

	assert.Contains(t, actualRequestsSent, string(testCatchupRelays[useChainID][0].ID()))
	assert.Contains(t, actualRequestsSent, string(testCatchupRelays[useChainID][1].ID()))
}

func TestMultiplexRoutinesNetworksCreator(t *testing.T) {

}

func TestMultiplexRoutinesRelaysBroadcast(t *testing.T) {

}

func TestMultiplexRoutinesCancelBroadcast(t *testing.T) {

}
