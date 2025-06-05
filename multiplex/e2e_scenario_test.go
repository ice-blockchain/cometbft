package multiplex_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/state/txindex"
)

// ----------------------------------------------------------------------------
// MultiplexClient Broadcast Test (Using client.BroadcastTx)

var randomizer = rand.New(rand.NewSource(time.Now().Unix()))

// NOTE: this removes the Relay ID from relays addresses.
func useRelaysWithoutIds(tb testing.TB, relays []string) []string {
	tb.Helper()

	relaysWithoutIds := []string{}
	for _, relayAddrStr := range relays {
		ra, err := server.NewRelayAddress(relayAddrStr)
		require.NoError(tb, err, "expected valid relay address, got: "+relayAddrStr)

		relaysWithoutIds = append(relaysWithoutIds, ra.StringWithoutId())
	}
	return relaysWithoutIds
}

func makeClientTransactions(
	tb testing.TB,
	chainInfo mx.ExtendedChainID,
	numTransactions int,
) []client.Transaction {
	tb.Helper()

	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		randomData := randomizer.Intn(999999999)
		testTransactions = append(testTransactions, client.Transaction{
			Data:        []byte{byte(i), byte(i + 1), byte(i + 2), byte(randomData)},
			Fingerprint: chainInfo.GetFingerprint(),
		})
	}

	return testTransactions
}

// Note that relays without IDs are *not* added to relay maps.
func mockRelayMapsForChainID(
	tb testing.TB,
	relay *mx.MultiplexBackend,
	relaysForTestCase []string,
	useChainID string,
	useCatchup bool,
) (
	chainRelays map[string][]*server.RelayAddress,
	catchupRelays map[string][]*server.RelayAddress,
) {
	chainRelays = make(map[string][]*server.RelayAddress, 1)
	catchupRelays = make(map[string][]*server.RelayAddress, 1)
	for _, relayAddrStr := range relaysForTestCase {
		ra, err := server.NewRelayAddress(relayAddrStr)
		require.NoError(tb, err, "expected valid relay address, got: "+relayAddrStr)

		// Do not include SELF in AckTransaction/Replication process, nor unhealthy relays
		if len(string(ra.ID())) == 0 || string(ra.ID()) == string(relay.GetRelayID()) {
			continue
		}

		if useCatchup {
			if _, ok := catchupRelays[useChainID]; !ok {
				catchupRelays[useChainID] = make([]*server.RelayAddress, 0, len(relaysForTestCase))
			}

			catchupRelays[useChainID] = append(catchupRelays[useChainID], ra)
		} else {
			if _, ok := chainRelays[useChainID]; !ok {
				chainRelays[useChainID] = make([]*server.RelayAddress, 0, len(relaysForTestCase))
			}

			chainRelays[useChainID] = append(chainRelays[useChainID], ra)
		}
	}

	return
}

// clientAckReplication mocks the waiting process for chain replication
// responses from remote relays.
// Note that in this mock implementation, a relay is considered healthy iff
// the relay address contains an ID.
func clientAckReplication(
	tb testing.TB,
	ctx context.Context,
	relay *mx.MultiplexBackend,
	catchupRelays map[string][]*server.RelayAddress,
) (relaysPerChain map[string][]string, numExpected int, numReceived int, err error) {
	tb.Helper()

	multiplexClient := mx.NewClient(
		mx.WithBackend(relay),
	)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// THREAD 1: Waiting for ChainReplicationResponse messages.
	go func() {
		defer wg.Done()

		relaysPerChain,
			numExpected,
			numReceived,
			err = multiplexClient.GetBackend().WaitForRelaysAckChainReplications(ctx,
			catchupRelays,
		)
	}()

	// THREAD 2: Sending mock ChainReplicationResponse messages.
	go func() {
		for chainID, catchupRelaysForChain := range catchupRelays {
			for _, relayAddr := range catchupRelaysForChain {
				if len(string(relayAddr.ID())) == 0 {
					continue // don't respond from unhealthy relays!
				}

				mockChainReplicationResponse := mockChainReplicationResponse(tb, relayAddr, chainID)

				remoteReplResCh := relay.GetReactor().ChannelForAckReplication(chainID)
				remoteReplResCh <- mockChainReplicationResponse
			}
		}
	}()

	// Coupled to finishing the execution of THREAD 1
	wg.Wait()

	return
}

// clientReplicationCompleted mocks the waiting process for chain replication
// completions from remote relays (ChainReplicationComplete).
// Note that in this mock implementation, a relay is considered healthy iff
// the relay address contains an ID.
func clientReplicationCompleted(
	tb testing.TB,
	ctx context.Context,
	relay *mx.MultiplexBackend,
	syncingChainIds []string,
	testTransactions []client.Transaction,
	completingRelays map[string][]*server.RelayAddress,
) (numCompleted int, err error) {
	tb.Helper()

	multiplexClient := mx.NewClient(
		mx.WithBackend(relay),
	)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// THREAD 1: Waiting for ChainReplicationComplete messages.
	go func() {
		defer wg.Done()

		numCompleted,
			err = multiplexClient.GetBackend().WaitForRelaysReplicationCompleted(ctx,
			syncingChainIds,
			testTransactions...,
		)
	}()

	// THREAD 2: Sending mock ChainReplicationComplete messages.
	go func() {
		for chainID, catchupRelaysForChain := range completingRelays {
			for _, relayAddr := range catchupRelaysForChain {
				if len(string(relayAddr.ID())) == 0 {
					continue // don't respond from unhealthy relays!
				}

				mockChainReplicationComplete := mockChainReplicationComplete(tb, relayAddr, chainID)

				remoteReplFinCh := relay.GetReactor().ChannelForRuntimeUpdates(chainID)
				remoteReplFinCh <- mockChainReplicationComplete
			}
		}
	}()

	// Coupled to finishing the execution of THREAD 1
	wg.Wait()

	return
}

// clientAckTransaction mocks the waiting process for transaction acks
// from remote relays.
// Note that in this mock implementation, a relay is considered healthy iff
// the relay address contains an ID.
func clientAckTransaction(
	tb testing.TB,
	ctx context.Context,
	relay *mx.MultiplexBackend,
	ackingRelays map[string][]*server.RelayAddress,
	catchupRelays map[string][]*server.RelayAddress,
	mustAckRelays map[string][]*server.RelayAddress,
	testTransactions []client.Transaction,
) (expectedRelaysPerTx, relaysPerTx map[string][]string, numExpected int, numReceived int, err error) {
	tb.Helper()

	multiplexClient := mx.NewClient(
		mx.WithBackend(relay),
	)

	wg := sync.WaitGroup{}
	wg.Add(1)

	// THREAD 1: Waiting for AckTransaction messages.
	go func() {
		defer wg.Done()

		expectedRelaysPerTx, relaysPerTx,
			numExpected,
			numReceived,
			err = multiplexClient.GetBackend().WaitForRelaysAckTransactionBatch(ctx,
			mustAckRelays,
			catchupRelays,
			testTransactions...,
		)
	}()

	// THREAD 2: Sending mock AckTransactionBroadcast messages.
	go func() {
		for chainID, relaysForChain := range ackingRelays {
			for _, relayAddr := range relaysForChain {
				if len(string(relayAddr.ID())) == 0 {
					continue // don't Ack from unhealthy relays!
				}

				for _, testTx := range testTransactions {
					// Mocks relayAddr ack for transaction
					txHashes := [][]byte{}
					txHashes = append(txHashes, testTx.Hash())

					mockAckTxBroadcast := mockAckTransactionBroadcast(tb, relayAddr, testTx, chainID)

					testTxHash := fmt.Sprintf("%X", mockAckTxBroadcast.TxHashes[0])
					acceptChForTxHash := relay.GetReactor().ChannelForAckTransaction(testTxHash)

					acceptChForTxHash <- mockAckTxBroadcast
				}
			}
		}
	}()

	// Coupled to finishing the execution of THREAD 1
	wg.Wait()

	return
}

// Mocks relayAddr ack for transaction
func mockAckTransactionBroadcast(
	tb testing.TB,
	relayAddr *server.RelayAddress,
	transaction client.Transaction,
	chainID string,
) *mxp2p.AckTransactionBroadcast {
	tb.Helper()

	txHashes := [][]byte{}
	txHashes = append(txHashes, transaction.Hash())

	return &mxp2p.AckTransactionBroadcast{
		TxHashes: txHashes,
		NodeId:   string(relayAddr.ID()),
		ChainID:  chainID,
	}
}

// Mocks relayAddr response for replication
func mockChainReplicationResponse(
	tb testing.TB,
	relayAddr *server.RelayAddress,
	chainID string,
) *mxp2p.ChainReplicationResponse {
	tb.Helper()

	return &mxp2p.ChainReplicationResponse{
		NodeId:  string(relayAddr.ID()),
		ChainID: chainID,
	}
}

// Mocks relayAddr response for replication
func mockChainReplicationComplete(
	tb testing.TB,
	relayAddr *server.RelayAddress,
	chainID string,
) *mxp2p.ChainReplicationComplete {
	tb.Helper()

	return &mxp2p.ChainReplicationComplete{
		NodeId:  string(relayAddr.ID()),
		ChainID: chainID,
	}
}

// Uses MultiplexClient to broadcast transactions.
func clientBroadcastTx(
	tb testing.TB,
	ctx context.Context,
	relay *mx.MultiplexBackend,
	relays []string,
	testChainID string,
	numTransactions int,
	notifyCh chan client.BroadcastStatus,
) {
	tb.Helper()

	chainInfo, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(tb, err, "should create correctly formatted ChainID")

	testTransactions := makeClientTransactions(tb, chainInfo, numTransactions)
	multiplexClient := mx.NewClient(
		mx.WithBackend(relay),
	)

	defer func() {
		// recover from panic caused by timing out, notifyCh already closed
		// note that any other panic *must* stop tests and print the error.
		if r := recover(); r != nil {
			require.Contains(tb, r, "send on closed channel")
		}
	}()

	multiplexClient.BroadcastTx(ctx,
		chainInfo.GetUserAddress(),
		relays,
		notifyCh,
		testTransactions...,
	)
}

// Consumes messages on notifyCh and/or context cancellation.
func waitForClientBroadcastStatus(
	tb testing.TB,
	ctx context.Context,
	testChainID string,
	notifyCh chan client.BroadcastStatus,
) client.BroadcastStatus {
	tb.Helper()

	wg := sync.WaitGroup{}
	wg.Add(1)

	resultStatusMsg := client.BroadcastStatus{}

	// Expects a BroadcastStatus update, or timeout after 30s.
	go func(status *client.BroadcastStatus) {
		defer wg.Done()

		for {
			select {
			case *status = <-notifyCh:
				return

			case <-ctx.Done():
				(*status).Error = fmt.Errorf(
					"Timed out waiting for broadcast status for %s", testChainID)
				return // cancels context
			}
		}
	}(&resultStatusMsg)

	// Waits for a status update or timeout
	wg.Wait()
	return resultStatusMsg
}

func TestScenarioClientBroadcastErrors(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 2

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	// TEST 1 - Errors
	//
	// Add 5 unavailable relays to the list and make sure errors match.
	// numRelays=7;numHealthy=2;numErrors=5;withSelf=true

	relaysForErrCase := healthyRelaysWithoutIds[:]
	numHealthy := len(relaysForErrCase)
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	numTransactions := 1
	testChainID := makeChainID("test-chain-1")
	notifyCh := make(chan client.BroadcastStatus)

	// Separate goroutine for client broadcast process
	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")
	close(notifyCh)

	numExpected := (numRelaysForErrCase / 2) + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)

	// TODO(midas): add other error cases as forwarded with Client.BroadcastTx.
}

// With a list of healthy relays, we test the ability to intercept replication
// channel message: ChainReplicationResponse from each of the relays.
func TestScenarioClientBroadcastWaitForReplicationResponses(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// TEST 1 - Success
	//
	// Use the 7 healthy relays including self and make sure we intercepted
	// ChainReplicationResponse messages (mocked in clientAckReplication).
	// numRelays=7;numHealthy=7;numErrors=0;withSelf=true

	relaysForTestCase := healthyRelays[:] // with IDs!
	numHealthy := len(relaysForTestCase)
	testChainID1 := makeChainID("test-chain-1")
	testRelayOne := servers[0]

	// Fill chainRelays such that all HEALTHY relays are expected to respond.
	_, testCatchupRelays := mockRelayMapsForChainID(t,
		testRelayOne,
		relaysForTestCase,
		testChainID1,
		true, // useCatchup
	)

	// Block main thread to test ChainReplicationResponse process
	actualRelaysPerChain,
		actualExpectedResponses,
		actualNumReceivedResponses,
		actualReplicationErr := clientAckReplication(t,
		broadcastCtx,
		testRelayOne,
		testCatchupRelays,
	)

	expectedNumAwaited := numHealthy - 1  // -self
	expectedNumReceived := numHealthy - 1 // -self

	assert.Equal(t, expectedNumAwaited, actualExpectedResponses)
	assert.Equal(t, expectedNumReceived, actualNumReceivedResponses)
	assert.NoError(t, actualReplicationErr, "should complete ChainReplication process")
	assert.NotEmpty(t, actualRelaysPerChain)
}

// With a list of healthy relays, we test the ability to intercept broadcast
// channel message: AckTransactionBroadcast from each of the relays.
// In a second iteration, we set 2 relays to be unhealthy and make sure that
// that the Ack process times out gracefully but still intercepts other Acks.
func TestScenarioClientBroadcastWaitForAckTransactions(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// TEST 1 - Success
	//
	// Use the 7 healthy relays including self and make sure we intercepted
	// AckTransactionBroadcast messages (mocked in clientAckTransaction).
	// numRelays=7;numHealthy=7;numErrors=0;withSelf=true

	relaysForTestCase := healthyRelays[:] // with IDs!
	numHealthy := len(relaysForTestCase)
	testChainID1 := makeChainID("test-chain-1")
	testRelayOne := servers[0]

	// Fill chainRelays such that all HEALTHY relays are expected to respond.
	testChainRelays,
		testCatchupRelays := mockRelayMapsForChainID(t, testRelayOne, relaysForTestCase, testChainID1, false) // false=useCatchup

	testChainInfo1, err := mx.NewExtendedChainIDFromLegacy(testChainID1)
	require.NoError(t, err, "should create correctly formatted ChainID")
	testTransactions1 := makeClientTransactions(t, testChainInfo1, 1)

	// Block main thread to test AckTransaction process
	_, actualRelaysPerTx,
		actualExpectedAcks,
		actualNumReceived,
		actualAcceptErr := clientAckTransaction(t,
		broadcastCtx,
		testRelayOne,
		testChainRelays,
		testCatchupRelays,
		testChainRelays, // mustAckRelays => ALL
		testTransactions1,
	)

	expectedNumAwaitedAcks := numHealthy - 1 // -self
	expectedNumReceived := numHealthy - 1    // -self

	assert.Equal(t, expectedNumAwaitedAcks, actualExpectedAcks)
	assert.Equal(t, expectedNumReceived, actualNumReceived)
	assert.NoError(t, actualAcceptErr, "should complete AckTransaction process")
	assert.NotEmpty(t, actualRelaysPerTx)

	// TEST 2 - Error
	//
	// Add 2 unhealthy relays to relays including self, and make sure we timeout
	// correctly for the 2 unhealthy relays
	// numRelays=7;numHealthy=5;numErrors=2;withSelf=true

	relaysForErrCase := healthyRelays[:len(healthyRelays)-2] // with IDs!
	numHealthy = len(relaysForErrCase)                       // 5
	testChainID2 := makeChainID("test-chain-2")
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// testChainRelays contains 2 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	testChainRelays,
		testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForErrCase, testChainID2, false) // false=useCatchup

	// Remove 2 healthy relays to force timeout, as we expect them to Ack
	// but they will not be sending a AckTransactionBroadcast message.
	testAckingRelays := make(map[string][]*server.RelayAddress, 1)
	testAckingRelays[testChainID2] = testChainRelays[testChainID2][:]

	// Now add back the 2 unhealthy relays so that they are expected to Ack.
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		// random node key
		privKey := ed25519.GenPrivKey()
		nodeKey := &p2p.NodeKey{
			PrivKey: privKey,
		}

		fakeRelayAddr, _ := server.NewRelayAddress(string(nodeKey.ID()) + "@1.2.3.4:" + strconv.Itoa(1000+i))
		testChainRelays[testChainID2] = append(testChainRelays[testChainID2], fakeRelayAddr)
	}

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	testChainInfo2, err := mx.NewExtendedChainIDFromLegacy(testChainID2)
	require.NoError(t, err, "should create correctly formatted ChainID")
	testTransactions2 := makeClientTransactions(t, testChainInfo2, 1)

	// Block main thread to test AckTransaction process
	_, _, actualExpectedAcks, _, actualAcceptErr = clientAckTransaction(t,
		secondBroadcastCtx,
		testRelayOne,
		testAckingRelays, // unhealthy removed
		testCatchupRelays,
		testChainRelays, // mustAckRelays => 2 are unhealthy
		testTransactions2,
	)

	// This error case must count unhealthy relays in "expected to Ack".
	expectedNumAwaitedAcks = len(testChainRelays[testChainID2]) // -self
	assert.Equal(t, expectedNumAwaitedAcks, actualExpectedAcks)

	// We won't receive all Acks, but should receive from all healthy relays.
	expectedNumReceived = numHealthy - 1 // -self-unhealthy
	testTxHash := fmt.Sprintf("%X", testTransactions2[0].Hash())
	actualAcksReceived := testRelayOne.GetAckResponsePeers(testTxHash)

	assert.Len(t, actualAcksReceived, expectedNumReceived)
	assert.Error(t, actualAcceptErr, "should timeout gracefully")
	assert.Contains(t, actualAcceptErr.Error(), "process timed out")
}

// With a list of healthy relays, we test the ability to intercept replication
// channel message: ChainReplicationComplete from each of the relays.
func TestScenarioClientBroadcastWaitForReplicationCompleted(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// TEST 1 - Success
	//
	// Use the 7 healthy relays including self and make sure we intercepted
	// ChainReplicationComplete messages (mocked in clientReplicationCompleted).
	// numRelays=7;numHealthy=7;numErrors=0;withSelf=true

	relaysForTestCase := healthyRelays[:] // with IDs!
	numHealthy := len(relaysForTestCase)
	testChainID1 := makeChainID("test-chain-1")
	testRelayOne := servers[0]

	// Fill chainRelays such that all HEALTHY relays are expected to respond.
	_, testCatchupRelays := mockRelayMapsForChainID(t,
		testRelayOne,
		relaysForTestCase,
		testChainID1,
		true, // useCatchup
	)

	testSyncingChainIds := []string{}
	for testSyncingChain, _ := range testCatchupRelays {
		testSyncingChainIds = append(testSyncingChainIds, testSyncingChain)
	}

	testChainInfo1, err := mx.NewExtendedChainIDFromLegacy(testChainID1)
	require.NoError(t, err, "should create correctly formatted ChainID")
	testTransactions1 := makeClientTransactions(t, testChainInfo1, 1)

	// First we feed some ChainReplicationResponse to fill ackResponsesRcvd.
	_, _, _, replErr := clientAckReplication(t,
		broadcastCtx,
		testRelayOne,
		testCatchupRelays,
	)
	require.NoError(t, replErr, "should pre-fill ReplResponsePeers")

	// Block main thread to test ChainReplicationComplete process
	actualNumCompleted,
		actualCompletionError := clientReplicationCompleted(t,
		broadcastCtx,
		testRelayOne,
		testSyncingChainIds,
		testTransactions1,
		testCatchupRelays,
	)

	expectedNumCompleted := numHealthy - 1 // -self

	assert.Equal(t, expectedNumCompleted, actualNumCompleted)
	assert.NoError(t, actualCompletionError, "should finalize ChainReplicationComplete process")

	// TEST 2 - Error
	//
	// Add 2 unhealthy relays to relays including self, and make sure we timeout
	// correctly for the 2 unhealthy relays
	// numRelays=7;numHealthy=5;numErrors=2;withSelf=true

	relaysForErrCase := healthyRelays[:len(healthyRelays)-2] // with IDs!
	numHealthy = len(relaysForErrCase)                       // 5
	testChainID2 := makeChainID("test-chain-2")
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// relaysForErrCase contains 2 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	_, testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForErrCase, testChainID2, true) // trure=useCatchup

	testSyncingChainIds2 := []string{}
	for testSyncingChain, _ := range testCatchupRelays {
		testSyncingChainIds2 = append(testSyncingChainIds2, testSyncingChain)
	}

	testCompletingRelays := make(map[string][]*server.RelayAddress, 1)
	testCompletingRelays[testChainID2] = testCatchupRelays[testChainID2][:]

	// Now add back the 2 unhealthy relays so that they are expected to Complete.
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		// random node key
		privKey := ed25519.GenPrivKey()
		nodeKey := &p2p.NodeKey{
			PrivKey: privKey,
		}

		fakeRelayAddr, _ := server.NewRelayAddress(string(nodeKey.ID()) + "@1.2.3.4:" + strconv.Itoa(1000+i))
		testCatchupRelays[testChainID2] = append(testCatchupRelays[testChainID2], fakeRelayAddr)
	}

	// testCatchupRelays DOES contain the unhealthy relays
	// testCompletingRelays does NOT contain the unhealthy relays

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	testChainInfo2, err := mx.NewExtendedChainIDFromLegacy(testChainID2)
	require.NoError(t, err, "should create correctly formatted ChainID")
	testTransactions2 := makeClientTransactions(t, testChainInfo2, 1)

	// First we feed some ChainReplicationResponse to fill ackResponsesRcvd.
	// Note that in this test unhealthy peers also send ChainReplicationResponse.
	_, _, _, replErr = clientAckReplication(t,
		secondBroadcastCtx,
		testRelayOne,
		testCatchupRelays,
	)
	require.NoError(t, replErr, "should pre-fill ReplResponsePeers")

	// Block main thread to test ChainReplicationComplete process
	_, errCaseCompletionError := clientReplicationCompleted(t,
		secondBroadcastCtx,
		testRelayOne,
		testSyncingChainIds2,
		testTransactions2,
		testCompletingRelays, // 2 unhealthy are NOT sending ChainReplicationComplete.
	)

	errCaseExpectedNumCompleted := numHealthy - 1
	errCaseActualNumCompleted := testRelayOne.GetReplCompletePeers(testChainID2)

	assert.Equal(t, errCaseExpectedNumCompleted, len(errCaseActualNumCompleted))
	assert.Error(t, errCaseCompletionError, "should timeout gracefully")
	assert.Contains(t, errCaseCompletionError.Error(), "process timed out")
}

// With a list of healthy relays, the transactions will be added locally
// and then shared with other relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message
// before we proceed to accepting the transaction.
func TestScenarioClientBroadcastHealthyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		5*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
	notifyCh := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := len(relays) - 1
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// With a list of empty relays, a ChainReplicationRequest must be sent,
// and a ChainReplicationResponse is expected before sharing transactions
// using a message on mempool channel, to which the relays respond with a
// AckTransactionBroadcast message before we proceed to accepting the transaction.
func TestScenarioClientBroadcastEmptyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testWithChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testWithChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testWithChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh)

	// Test that ChainReplicationRequest were sent
	expectedRequestCnt := len(relays) - 1
	actualRequestsSent := servers[0].GetReplRequestPeers(testWithChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that we received ChainReplicationResponse messages
	expectedResponseCnt := len(relays) - 1 // -1 for self
	actualResponsesRcvd := servers[0].GetReplResponsePeers(testWithChainID)
	assert.NotEmpty(t, actualResponsesRcvd)
	assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
}

// We further test the healthy relays counter process which implies successful
// calls to GetRemoteRelayInfo, and the exclusion of "self" from relays list
// if necessary.
func TestScenarioClientBroadcastCountsHealthyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numHealthy := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays contains self for this test
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// TEST 1 - Errors
	//
	// Add 4 unavailable relays to the list and make sure errors match.
	// numRelays=7;numHealthy=3;numErrors=4;withSelf=true
	relaysForErrCase := healthyRelays[:]
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg1 := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg1)
	assert.NotNil(t, resultStatusMsg1.Error)
	assert.Error(t, resultStatusMsg1.Error)
	assert.Contains(t, resultStatusMsg1.Error.Error(), "not enough healthy relays")
	close(notifyCh1)

	numExpected := (numRelaysForErrCase / 2) + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg1.Error.Error(), expectedMessage)

	// TEST 2 - Errors
	//
	// Add 6 unavailable relays to the list and make sure errors match.
	// numRelays=9;numHealthy=3;numErrors=6;withSelf=true
	relaysForErrCase = []string{}
	relaysForErrCase = healthyRelays[:]
	numRelaysForErrCase = 9
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg2 := waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg2)
	assert.NotNil(t, resultStatusMsg2.Error)
	assert.Error(t, resultStatusMsg2.Error)
	assert.Contains(t, resultStatusMsg2.Error.Error(), "not enough healthy relays")
	close(notifyCh2)

	numExpected = (numRelaysForErrCase / 2) + 1
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg2.Error.Error(), expectedMessage)

	// TEST 3 - Errors
	//
	// Add 5 unavailable relay to the list and remove self from relays.
	// numRelays=8;numHealthy=3;numErrors=4;withSelf=false
	relaysForErrCase = []string{}
	relaysForErrCase = healthyRelays[1:] // removes self
	numHealthy = 2
	numRelaysForErrCase = 8
	for i := numHealthy; i < numRelaysForErrCase-1; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh3 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForErrCase, // does not contain self!
		testChainID,
		numTransactions,
		notifyCh3,
	)

	// Blocks the main thread until we consume from notifyCh3.
	resultStatusMsg3 := waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh3,
	)
	assert.NotNil(t, resultStatusMsg3)
	assert.NotNil(t, resultStatusMsg3.Error)
	assert.Error(t, resultStatusMsg3.Error)
	assert.Contains(t, resultStatusMsg3.Error.Error(), "not enough healthy relays")
	close(notifyCh3)

	numExpected = (numRelaysForErrCase / 2) + 1
	numHealthy = numHealthy + 1 // "self" is healthy also if not in relays.
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg3.Error.Error(), expectedMessage)

	// TEST 4 - Success
	//
	// Add 1 unavailable relay to the list with self and broadcast successfully.
	// numRelays=4;numHealthy=3;numErrors=1;withSelf=true
	relaysForTestCase := healthyRelays[:]
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase := 4
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	fourthTimeoutAfter := 20 * time.Second // Time for broadcast
	fourthBroadcastCtx, fourthCancelCtxFn := context.WithTimeout(context.TODO(), fourthTimeoutAfter)
	defer fourthCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh4 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		fourthBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh4,
	)

	// Blocks the main thread until we consume from notifyCh4.
	resultStatusMsg4 := waitForClientBroadcastStatus(t,
		fourthBroadcastCtx,
		testChainID,
		notifyCh4,
	)
	assert.NotNil(t, resultStatusMsg4)
	assert.NoError(t, resultStatusMsg4.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg4.TxHashes, numTransactions)
	close(notifyCh4)

	// TEST 5 - Success
	//
	// Add 1 unavailable relay to the list without self and broadcast successfully.
	// numRelays=4;numHealthy=3;numErrors=1;withSelf=false
	relaysForTestCase = healthyRelays[1:] // removes self
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase = numHealthy + 1
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	fifthTimeoutAfter := 20 * time.Second // Time for broadcast
	fifthBroadcastCtx, fifthCancelCtxFn := context.WithTimeout(context.TODO(), fifthTimeoutAfter)
	defer fifthCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh5 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		fifthBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh5,
	)

	// Blocks the main thread until we consume from notifyCh5.
	resultStatusMsg5 := waitForClientBroadcastStatus(t,
		fifthBroadcastCtx,
		testChainID,
		notifyCh5,
	)
	assert.NotNil(t, resultStatusMsg5)
	assert.NoError(t, resultStatusMsg5.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg5.TxHashes, numTransactions)
	close(notifyCh5)

	// TEST 6 - Errors
	//
	// Add 3 unavailable relays to the list without self.
	// numRelays=6;numHealthy=3;numErrors=3;withSelf=false
	relaysForErrCase = healthyRelays[1:] // removes self
	numHealthy = len(relaysForErrCase)
	numFailing := 3 // should not contain "self" as failing.
	numRelaysForErrCase = numHealthy + numFailing + 1
	for i := numHealthy; i < numRelaysForErrCase-1; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	sixthTimeoutAfter := 20 * time.Second // Time for broadcast
	sixthBroadcastCtx, sixthCancelCtxFn := context.WithTimeout(context.TODO(), sixthTimeoutAfter)
	defer sixthCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh6 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		sixthBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh6,
	)

	// Blocks the main thread until we consume from notifyCh6.
	resultStatusMsg6 := waitForClientBroadcastStatus(t,
		sixthBroadcastCtx,
		testChainID,
		notifyCh6,
	)
	assert.NotNil(t, resultStatusMsg6)
	assert.NotNil(t, resultStatusMsg6.Error)
	assert.Error(t, resultStatusMsg6.Error)
	assert.Contains(t, resultStatusMsg6.Error.Error(), "not enough healthy relays")
	close(notifyCh6)

	numExpected = (numRelaysForErrCase / 2) + 1
	numHealthy = numHealthy + 1 // "self" is healthy also if not in relays.
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg6.Error.Error(), expectedMessage)
}

// We further test the healthy relays counter process when relying on relay
// address that DO NOT contain a relay ID. Namely, calls to GetRemoteRelayInfo
// should be successful and fill the healthyRemoteRelays slice correctly.
func TestScenarioClientBroadcastCountsRemoteRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numHealthy := 2

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays contains self for this test
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	// TEST 1 - Success
	//
	// Add 1 unavailable relay to the list WITH self, and removed relay IDs,
	// and should broadcast successfully.
	// numRelays=3;numHealthy=2;numErrors=1;withSelf=true
	relaysForTestCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase := 3
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// TEST 2 - Success
	//
	// Add 1 unavailable relay to the list and remove self from relays,
	// and remove relay IDs, should broadcast successfully.
	// numRelays=3;numHealthy=2;numErrors=1;withSelf=false
	relaysForTestCase = healthyRelaysWithoutIds[1:]
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase = 3
	for i := numHealthy; i < numRelaysForTestCase-1; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh2)

	// TEST 3 - Errors
	//
	// Add 5 unavailable relays to the list WITH self, and removed relay IDs.
	// numRelays=7;numHealthy=2;numErrors=5;withSelf=true
	relaysForErrCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForErrCase)
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh3 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh3,
	)

	// Blocks the main thread until we consume from notifyCh3.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh3,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")
	close(notifyCh3)

	numExpected := (numRelaysForErrCase / 2) + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)

	// TEST 4 - Errors
	//
	// Add 5 unavailable relays to the list and remove self from relays,
	// and removed relay IDs.
	// numRelays=7;numHealthy=2;numErrors=5;withSelf=false
	relaysForErrCase = healthyRelaysWithoutIds[1:]
	numHealthy = len(relaysForErrCase)
	numRelaysForErrCase = 7
	for i := numHealthy; i < numRelaysForErrCase-1; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	fourthTimeoutAfter := 20 * time.Second // Time for broadcast
	fourthBroadcastCtx, fourthCancelCtxFn := context.WithTimeout(context.TODO(), fourthTimeoutAfter)
	defer fourthCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh4 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		fourthBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh4,
	)

	// Blocks the main thread until we consume from notifyCh4.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		fourthBroadcastCtx,
		testChainID,
		notifyCh4,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")
	close(notifyCh4)

	numExpected = (numRelaysForErrCase / 2) + 1
	numHealthy = numHealthy + 1 // "self" is healthy also if not in relays.
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)
}

// With a list of empty relays, a first block of the network will be created,
// which includes the broadcast transactions data (using client.BroadcastTx),
// and the state machine and blocks store are updated with transactions data.
func TestScenarioClientBroadcastEmptyRelaysProduceBlockWithTx(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh)

	waitDuration := 5 * time.Second
	t.Logf("Waiting %.0fsec to evaluate state machine...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	testReactor := servers[0].GetReactor()

	stateStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStateStore)
	assert.NotNil(t, stateStoreProvider, "should not error getting state store provider")
	chainStore := stateStoreProvider(testChainID).(sm.Store)
	assert.NotNil(t, chainStore, "state store per chain must not be nil")

	stateMachine, err := chainStore.Load()
	assert.NoError(t, err, "should not error loading state")
	assert.Equal(t, testChainID, stateMachine.ChainID)
	assert.GreaterOrEqual(t, stateMachine.LastBlockHeight, int64(1))

	indexerProvider := testReactor.GetServicesProvider()
	indexerService := indexerProvider(mx.ServiceKeyIndexers, testChainID).(*txindex.IndexerService)

	for _, txHash := range resultStatusMsg.TxHashes {
		foundTx, idxErr := indexerService.GetTxIndexer().Get(txHash)

		assert.NoError(t, idxErr)
		assert.NotNil(t, foundTx)
	}
}

// With a list of healthy relays, i.e. just enough, the transactions will be added
// locally and then shared with healthy relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message before we
// proceed to accepting the transaction.
func TestScenarioClientBroadcastEnoughHealthyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7
	numHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numHealthy)

	// NOTE: this removes the Relay ID from relays addresses.
	relays = useRelaysWithoutIds(t, relays)

	// Note: this test consists in having *just enough* healthy relays actively
	// accept a client.BroadcastTx call. If enough healthy relays respond to a
	// broadcast operation, the operation should get accepted.
	for i := numHealthy; i < numRelays; i++ {
		relays = append(relays, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// TEST 1 - Success
	//
	// Added 3 unavailable relay to the list with self,
	// and remove relay IDs, should broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// Test that ChainReplicationResponse messages were received.
	expectedResponseCnt := numHealthy - 1 // -1 for self
	actualResponsesRcvd := servers[0].GetReplResponsePeers(testChainID1)
	assert.NotEmpty(t, actualResponsesRcvd)
	assert.Len(t, actualResponsesRcvd, expectedResponseCnt)

	// TEST 2 - Success
	//
	// Added 3 unavailable relay to the list and remove self,
	// and remove relay IDs, should broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=false
	relays = relays[1:] // removes self

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID1,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh2)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := numHealthy - 1 // -1 for self
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// With a list of empty relays, i.e. just enough that are healthy,
// a ChainReplicationRequest must be sent, and a ChainReplicationResponse
// is expected before sharing transactions using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message
// before we proceed to accepting the transaction.
func TestScenarioClientBroadcastEnoughEmptyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7
	numHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numHealthy)

	// NOTE: this removes the Relay ID from relays addresses.
	relays = useRelaysWithoutIds(t, relays)

	// Note: this test consists in having *just enough* healthy relays actively
	// accept a client.BroadcastTx call. If enough healthy relays respond to a
	// broadcast operation, the operation should get accepted.
	for i := numHealthy; i < numRelays; i++ {
		relays = append(relays, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// TEST 1 - Success
	//
	// Added 3 unavailable relays to the list with self,
	// and remove relay IDs, should broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testWithChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testWithChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testWithChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// Test that we sent ChainReplicationRequest messages
	expectedRequestCnt := numHealthy - 1 // -1 for self
	actualRequestsSent := servers[0].GetReplRequestPeers(testWithChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that we received ChainReplicationResponse messages
	expectedResponseCnt := numHealthy - 1 // -1 for self
	actualResponsesRcvd := servers[0].GetReplResponsePeers(testWithChainID)
	assert.NotEmpty(t, actualResponsesRcvd)
	assert.Len(t, actualResponsesRcvd, expectedResponseCnt)

	// TEST 2 - Success
	//
	// This second pass does not send ChainReplicationRequest!
	// Added 3 unavailable relay to the list and remove self,
	// and remove relay IDs, should broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=false
	relays = relays[1:] // removes self

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testWithChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testWithChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh2)

	// Test that ChainReplicationRequest were NOT sent! (due to TEST 1)
	expectedRequestCnt = 0
	actualRequestsSent = servers[0].GetReplRequestPeers(testWithChainID)
	assert.Empty(t, actualRequestsSent, "should not need to send ChainReplicationRequest")
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := numHealthy - 1 // -1 for self
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// With a list of empty relays, and not enough healthy relays,
// we first sanity check a successful broadcast completion with
// less minimum healthy relays, and then we test a broadcast failure
// with five relays failing, i.e. too many, to fail the broadcast.
func TestScenarioClientBroadcastNotEnoughHealthyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numHealthy := 2

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays contains self for this test
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	// TEST 1 - Success
	//
	// Add 1 unavailable relay to the list WITH self, and removed relay IDs,
	// and should broadcast successfully.
	// Makes sure successful response is possible given less required relays.
	// numRelays=3;numHealthy=2;numErrors=1;withSelf=true
	relaysForTestCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase := 3
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// TEST 2 - Errors
	//
	// Add 5 unavailable relays to the list WITH self, and removed relay IDs.
	// numRelays=7;numHealthy=2;numErrors=5;withSelf=true
	relaysForErrCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForErrCase)
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")
	close(notifyCh2)

	numExpected := (numRelaysForErrCase / 2) + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)
}

// With a list of healthy relays, i.e. just enough, the transactions will be added
// locally and then shared with healthy relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message before we
// proceed to accepting the transaction.
func TestScenarioClientBroadcastWithAndWithoutSelfRelayAddress(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7
	minHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays contains self for this test
	healthyRelays, firstBroadcastCtx, firstCancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer firstCancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, firstBroadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	// TEST 1 - Success
	//
	// Pass a relay list WITH self, and removed relay IDs, and
	// should broadcast successfully.
	// numRelays=7;numHealthy=7;numErrors=0;withSelf=true
	relaysForTestCase := healthyRelaysWithoutIds[:]

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID1,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		firstBroadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// TEST 2 - Success
	//
	// Pass a relay list WITHOUT self, and removed relay IDs, and
	// should broadcast successfully using different ChainID.
	// numRelays=7;numHealthy=7;numErrors=0;withSelf=false
	relaysForTestCase = healthyRelaysWithoutIds[1:] // removes self
	secondTimeoutAfter := 20 * time.Second          // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	testChainID2 := makeChainID("test-chain-2")
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID2,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID2,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	close(notifyCh2)

	// TEST 3 - Success
	//
	// Add 3 failing relays to the list.
	// Pass a relay list WITHOUT self and just enough healthy relays,
	// and removed relay IDs, and should broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=false
	relaysForTestCase = healthyRelaysWithoutIds[1:] // removes self
	numFailing := numRelays - minHealthy
	numRelaysForTestCase := minHealthy + numFailing // 7
	for i := minHealthy - 1; i < numRelaysForTestCase-1; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	testChainID3 := makeChainID("test-chain-3")
	notifyCh3 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID3,
		numTransactions,
		notifyCh3,
	)

	// Blocks the main thread until we consume from notifyCh3.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID3,
		notifyCh3,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	close(notifyCh3)
}

// With a list of enough healthy relays, they should proceed to
// accepting the transaction even with some other relays failing.
func TestScenarioClientBroadcastAcceptableRelaysFailure(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7
	numHealthy := (numRelays / 2) + 1 // Keep enough healthy relays

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Note: relays contains self for this test
	healthyRelays, firstBroadcastCtx, firstCancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer firstCancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, firstBroadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	// TEST 1 - Success
	//
	// Add 3 failing relays to the list with self and broadcast successfully.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true
	relaysForTestCase := healthyRelaysWithoutIds[:]
	numFailing := numRelays - numHealthy
	numRelaysForTestCase := numHealthy + numFailing // 7
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		firstBroadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	t.Logf("Broadcast completed with %d failing relays for test-chain-1...", numFailing)
	close(notifyCh1)

	// TEST 2 - Success
	//
	// Add 3 failing relays to the list with self and broadcast successfully
	// using a different ChainID.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true
	relaysForTestCase = healthyRelaysWithoutIds[:]
	numFailing = numRelays - numHealthy
	numRelaysForTestCase = numHealthy + numFailing // 7
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	testChainID = makeChainID("test-chain-2")
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	t.Logf("Broadcast completed with %d failing relays for test-chain-2...", numFailing)
	close(notifyCh2)

	// TEST 3 - Success (6x)
	//
	// Iterate 3 times and broadcast with 1, 2 and 3 failing relays,
	// keeping always *enough* healthy relays in the list with self
	// and broadcast successfully using multiple ChainIDs.
	//
	// NOTE(midas): Even though we are using goroutines, this test does NOT
	// test concurrent broadcast scenarios, since we consume notifyCh between
	// the multiple broadcast operations.
	for numFailing := 1; numFailing <= 3; numFailing++ {
		// numRelays=numHealthy+numFailing;numHealthy=4;numErrors=numFailing;withSelf=true
		relaysForTestCase = healthyRelaysWithoutIds[:]
		numRelaysForTestCase = numHealthy + numFailing // 5, 6, 7
		for i := numHealthy; i < numRelaysForTestCase; i++ {
			relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
		}

		testChain100 := "test-chain-" + strconv.Itoa(100+numFailing)
		testChain200 := "test-chain-" + strconv.Itoa(200+numFailing)

		thirdTimeoutAfter := 30 * time.Second // Time for broadcast
		thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
		defer thirdCancelCtxFn()

		// Separate goroutine for client broadcast process
		numTransactions = 1
		testChainID = makeChainID(testChain100)
		notifyCh3 := make(chan client.BroadcastStatus)
		go clientBroadcastTx(t,
			thirdBroadcastCtx,
			servers[0],
			relaysForTestCase,
			testChainID,
			numTransactions,
			notifyCh3,
		)

		// Blocks the main thread until we consume from notifyCh3.
		resultStatusMsg100 := waitForClientBroadcastStatus(t,
			thirdBroadcastCtx,
			testChainID,
			notifyCh3,
		)

		assert.NotNil(t, resultStatusMsg100)
		assert.Len(t, resultStatusMsg100.TxHashes, numTransactions)
		require.NoError(t, resultStatusMsg100.Error,
			"should not contain error status with numFailing: "+strconv.Itoa(numFailing))
		t.Logf("Broadcast completed with %d failing relays for %s...", numFailing, testChain100)
		close(notifyCh3)

		fourthTimeoutAfter := 30 * time.Second // Time for broadcast
		fourthBroadcastCtx, fourthCancelCtxFn := context.WithTimeout(context.TODO(), fourthTimeoutAfter)
		defer fourthCancelCtxFn()

		// Separate goroutine for client broadcast process
		numTransactions = 1
		testChainID = makeChainID(testChain200)
		notifyCh4 := make(chan client.BroadcastStatus)
		go clientBroadcastTx(t,
			fourthBroadcastCtx,
			servers[0],
			relaysForTestCase,
			testChainID,
			numTransactions,
			notifyCh4,
		)

		// Blocks the main thread until we consume from notifyCh4.
		resultStatusMsg200 := waitForClientBroadcastStatus(t,
			fourthBroadcastCtx,
			testChainID,
			notifyCh4,
		)

		assert.NotNil(t, resultStatusMsg200)
		assert.Len(t, resultStatusMsg200.TxHashes, numTransactions)
		require.NoError(t, resultStatusMsg200.Error,
			"should not contain error status with numFailing: "+strconv.Itoa(numFailing))
		t.Logf("Broadcast completed with %d failing relays for %s...", numFailing, testChain200)
		close(notifyCh4)
	}
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays.
func TestScenarioClientBroadcastAfterBackendRestart(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	// Using 0 waitDuration because others have plenty of time due to restart.
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		0*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	reuseRootDir := servers[0].GetReactor().GetNodeConfig().RootDir

	// Stop the receiving backend, then start it again.
	err := servers[0].Close()
	require.NoError(t, err, "should shutdown server")

	waitDuration := 3 * time.Second
	t.Logf("Waiting %.0fsec to restart backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// CAUTION:
	// We mimic one of the relay shutting down completely, i.e. its process
	// is not managed, corrupted or stopped. Setting nil on the "old" instance
	// is only necessary during shutdown tests.

	servers[0] = nil // Only for test
	resetRelay, newShutdownFn := ResetTestSingleCompatibleRelay(t,
		reuseRootDir,
		servers[1],
		0, // indexRelay (resetting relay-1)
		cmtlog.NewNopLogger(),
	)
	defer newShutdownFn(resetRelay)

	resetRelay.MustStart()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays. This
// test executes a broadcast operation before shutting down the backend and
// one after having restarted the backend to ensure that continuation works.
// Finally, it also broadcasts one more transaction using a different ChainID.
func TestScenarioClientBroadcastBeforeAndAfterBackendRestart(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	// Using 2 seconds waitDuration because we shall broadcast BEFORE shutdown.
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	reuseRootDir := servers[0].GetReactor().GetNodeConfig().RootDir

	// STEP 1:
	// We execute a complete broadcast process.

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"first broadcast to new ChainID should contain transaction hashes")
	close(notifyCh1)

	waitDuration := 15 * time.Second
	t.Logf("Waiting %.0fsec to shutdown backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// STEP 2:
	//
	// CAUTION:
	// We mimic one of the relay shutting down completely, i.e. its process
	// is not managed, corrupted or stopped. Setting nil on the "old" instance
	// is only necessary during shutdown tests.

	// Stop the receiving backend, then start it again.
	err := servers[0].Close()
	require.NoError(t, err, "should shutdown server")

	waitDuration = 3 * time.Second
	t.Logf("Waiting %.0fsec to restart backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	servers[0] = nil // Only for test
	resetRelay, newShutdownFn := ResetTestSingleCompatibleRelay(t,
		reuseRootDir,
		servers[1],
		0, // indexRelay (resetting relay-1)
		cmtlog.NewNopLogger(),
	)
	defer newShutdownFn(resetRelay)

	resetRelay.MustStart()

	waitDuration = 5 * time.Second
	t.Logf("Waiting %.0fsec to use node services...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// STEP 3:
	//
	// The relay has been fully restarted and we can use the created
	// cancelable/expirable context to broadcast *more* transactions.

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"broadcast to existing ChainID after restart should contain transaction hashes")
	close(notifyCh2)

	// STEP 4:
	//
	// Also try to broadcast using a different ChainID.

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	testChainID = makeChainID("test-chain-2")
	notifyCh3 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh3,
	)

	// Blocks the main thread until we consume from notifyCh3.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh3,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"broadcast to new ChainID after restart should contain transaction hashes")
	close(notifyCh3)

	t.Log("Test case done running...")

	waitDuration = 15 * time.Second
	t.Logf("Waiting %.0fsec before shutting down...", waitDuration.Seconds())
	time.Sleep(waitDuration)
	t.Logf("Done waiting before shutting down...")
}

func TestScenarioClientBroadcastAfterRuntimeIdling(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 3

	// To enable debug logs, change this indexes array to contain the indexes
	// of the relays for which you want to activate full logging.
	idxRelaysWithLogs := []int{} // e.g. []int{0, 1} for relay-1 and relay-2
	servers, shutdownFn := ResetTestScenarioRelaysWithOptions(t, numChains, numRelays, idxRelaysWithLogs, [][]mx.MultiplexBackendOption{
		[]mx.MultiplexBackendOption{
			mx.WithRuntimeRegistryOptions(
				server.RuntimeRegistryCleanerInterval(1*time.Second),   // run cleaner every sec
				server.RuntimeRegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
			),
		}, // relay-1
		[]mx.MultiplexBackendOption{
			mx.WithRuntimeRegistryOptions(
				server.RuntimeRegistryCleanerInterval(1*time.Second),   // run cleaner every sec
				server.RuntimeRegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
			),
		}, // relay-2
		[]mx.MultiplexBackendOption{
			mx.WithRuntimeRegistryOptions(
				server.RuntimeRegistryCleanerInterval(1*time.Second),   // run cleaner every sec
				server.RuntimeRegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
			),
		}, // relay-3
	})
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	// STEP 1:
	// We execute a complete broadcast process.

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	waitDuration := 15 * time.Second
	t.Logf("Waiting %.0fsec for completion of replications and cleaner to execute...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// TEST 2:
	// We execute another complete broadcast process using the previous ChainID
	// which should NOT include the chain replications and thus the OnBroadcastComplete
	// call should execute right after broadcast is complete.

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions = 1
	notifyCh2 := make(chan client.BroadcastStatus)

	// Separate goroutine for client broadcast process
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID1, // existing ChainID (0 ChainReplicationRequest)
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID1,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh2)

	waitDuration = 5 * time.Second
	t.Logf("Waiting %.0fsec for completion broadcast operation...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}

// Tests completion of remote chain replications (using new network),
// and evaluates OnIdle calls which should automatically trigger when all
// chain replications have been announce as being completed.
func TestScenarioClientBroadcastRuntimeRegistryIntegration(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	// Set a testable OnIdle callback on the first relay (testing locally).
	var testOnIdleCalls atomic.Uint64
	testOnIdleCallback := func(chainID string) error {
		testOnIdleCalls.Add(1)
		return nil
	}

	// To enable debug logs, change this indexes array to contain the indexes
	// of the relays for which you want to activate full logging.
	idxRelaysWithLogs := []int{} // e.g. []int{0, 1} for relay-1 and relay-2
	servers, shutdownFn := ResetTestScenarioRelaysWithOptions(t, numChains, numRelays, idxRelaysWithLogs, [][]mx.MultiplexBackendOption{
		[]mx.MultiplexBackendOption{
			mx.WithRuntimeRegistryOptions(
				server.RuntimeRegistryCleanerInterval(1*time.Second),   // run cleaner every sec
				server.RuntimeRegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
				server.RuntimeRegistryOnIdle(testOnIdleCallback),
			),
		}, // relay-1
	})
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// servers[0].SetLogger(cmtlog.TestingLogger().With("process", "relay-1"))
	// servers[1].SetLogger(cmtlog.TestingLogger().With("process", "relay-2"))

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	// TEST 1:
	// We execute a complete broadcast process using a new ChainID which should
	// include the chain replications and thus the OnBroadcastComplete call
	// should wait for the replications to be finalized

	// Given some chains must be replicated by remote relays, we will have to
	// wait for the completion of these before we can safely shutdown (idle)
	// the active node runtime for this ChainID.
	numChainReplications := numRelays - 1 // -self

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh1,
	)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh1)

	// Test that the node runtime has been activated. Since we include 6 replications,
	// it will take more time for the backend to proceed to calling the OnComplete
	// method, because it shall wait for all replications to be complete.
	testRuntimeRegistry := servers[0].GetRuntimeRegistry()
	expectedNumRuntimes := uint64(1) // test-chain-1
	actualNumRuntimes := testRuntimeRegistry.NumRuntimes()
	assert.Equal(t, expectedNumRuntimes, actualNumRuntimes)

	waitDuration := 20 * time.Second
	t.Logf("Waiting %.0fsec for %d replications then OnIdle...", waitDuration.Seconds(), numChainReplications)
	time.Sleep(waitDuration)

	// Test that OnIdle was called (through OnBroadcastComplete)
	expectedNumOnIdleCalls := uint64(1) // test-chain-1
	assert.Equal(t, expectedNumOnIdleCalls, testOnIdleCalls.Load())

	/////// RESET TESTS STATE
	testOnIdleCalls.Store(uint64(0))

	// TEST 2:
	// We execute another complete broadcast process using the previous ChainID
	// which should NOT include the chain replications and thus the OnBroadcastComplete
	// call should execute right after broadcast is complete.

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions = 1
	notifyCh2 := make(chan client.BroadcastStatus)

	// Separate goroutine for client broadcast process
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID1, // existing ChainID (0 ChainReplicationRequest)
		numTransactions,
		notifyCh2,
	)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID1,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	close(notifyCh2)

	waitDuration = 10 * time.Second
	t.Logf("Waiting %.0fsec before evaluating OnIdle calls...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// Test that OnIdle was called (through OnBroadcastComplete)
	expectedNumOnIdleCalls = uint64(1) // test-chain-1
	assert.Equal(t, expectedNumOnIdleCalls, testOnIdleCalls.Load())
}

// ----------------------------------------------------------------------------
// CONCURRENT Broadcast Tests

// With a list of empty relays, a ChainReplicationRequest must be sent,
// and a response is expected before sharing transactions using a message
// on mempool channel, to which the relays respond with a Ack message
// before we proceed to accepting the transaction.
// This test focusses on sending concurrent transactions for unknown chains
// to make sure in a concurrent scenario, multiple new chains may be created.
func TestScenarioConcurrentNewChains(t *testing.T) {
	defer goleak.VerifyNone(t)

	timeoutGlobal := 30 * time.Second // Time for full test round
	testCaseCtx, globalCancelFn := context.WithTimeout(context.TODO(), timeoutGlobal)
	defer globalCancelFn()

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	servers[0].SetAcceptor(client.NewMockAcceptorImpl())
	servers[1].SetAcceptor(client.NewMockAcceptorImpl())
	servers[2].SetAcceptor(client.NewMockAcceptorImpl())

	// Note: healthyRelays includes self
	healthyRelays, _, _ := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)

	numConcurrent := 2
	testDoneCh := make(chan struct{}, numConcurrent)
	relaysForTestCase := healthyRelaysWithoutIds[:]

	// Executes first transaction broadcast
	firstTimeoutAfter := 20 * time.Second // Time for broadcast
	firstBroadcastCtx, firstCancelCtxFn := context.WithTimeout(context.TODO(), firstTimeoutAfter)
	defer firstCancelCtxFn()

	numTransactions1 := 1
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID1,
		numTransactions1,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine started with %d relays and test-chain-1: %s...", len(relaysForTestCase), testChainID1)

	// Executes second transaction broadcast
	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions2 := 1
	testChainID2 := makeChainID("test-chain-2")
	notifyCh2 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID2,
		numTransactions2,
		notifyCh2,
	)
	t.Logf("Broadcast goroutine started with %d relays and test-chain-2: %s...", len(relaysForTestCase), testChainID2)

	go func(ch chan struct{}) {
		defer close(notifyCh1)
		t.Logf("Waiting for broadcast status on test-chain-1: %s...", testChainID1)

		// Blocks this thread until we consume from notifyCh1.
		resultStatusMsg := waitForClientBroadcastStatus(t,
			firstBroadcastCtx,
			testChainID1,
			notifyCh1,
		)
		assert.NotNil(t, resultStatusMsg)
		assert.Len(t, resultStatusMsg.TxHashes, numTransactions1)
		assert.NoError(t, resultStatusMsg.Error, "first broadcast should not contain error status")

		ch <- struct{}{}
	}(testDoneCh)

	go func(ch chan struct{}) {
		defer close(notifyCh2)
		t.Logf("Waiting for broadcast status on test-chain-2: %s...", testChainID2)

		// Blocks this thread until we consume from notifyCh2.
		resultStatusMsg := waitForClientBroadcastStatus(t,
			secondBroadcastCtx,
			testChainID2,
			notifyCh2,
		)
		assert.NotNil(t, resultStatusMsg)
		assert.Len(t, resultStatusMsg.TxHashes, numTransactions2)
		assert.NoError(t, resultStatusMsg.Error, "second broadcast should not contain error status")

		ch <- struct{}{}
	}(testDoneCh)

	// t.Log("Waiting to consume from testDoneCh or timeout globally...")

	// Waits to consume from testDoneCh or timeout after 30 seconds
	var (
		wg           sync.WaitGroup
		cntDone      int
		errBroadcast error
	)
	wg.Add(1)
	go func(ch chan struct{}) {
		defer wg.Done()
		for {
			select {
			case <-ch:
				cntDone++
				if cntDone == numConcurrent {
					return
				}

			case <-testCaseCtx.Done():
				errBroadcast = errors.New("Timed out waiting for concurrent broadcasts to end")
				return
			}
		}
	}(testDoneCh)
	wg.Wait()

	t.Log("Test case done running...")

	waitDuration := 30 * time.Second
	t.Logf("Waiting %.0fsec before evaluating results...", waitDuration.Seconds())
	time.Sleep(waitDuration)
	t.Logf("Done waiting before evaluating results...")

	assert.Equal(t, numConcurrent, cntDone)
	assert.NoError(t, errBroadcast, "concurrent broadcasts should not error")

	// -------------------
	// Also test callbacks
	t.Logf("Now evaluating callbacks execution...")

	totalExpectedCalls := numTransactions1 + numTransactions2
	testAcceptorRelay1 := servers[0].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay2 := servers[1].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay3 := servers[2].GetAcceptor().(*client.MockAcceptorImpl)

	// Test that client callbacks were executed correctly, every relay should
	// have executed the CommitBroadcastTx callback when the block is finalized.

	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay1.TxCommitCalls.Load(),
		"should locally execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay2.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay3.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
}

func TestScenarioConcurrentNewChainsAndExistingChains(t *testing.T) {
	defer goleak.VerifyNone(t)

	timeoutGlobal := 60 * time.Second // Time for full test round
	testCaseCtx, globalCancelFn := context.WithTimeout(context.TODO(), timeoutGlobal)
	defer globalCancelFn()

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	servers[0].SetAcceptor(client.NewMockAcceptorImpl())
	servers[1].SetAcceptor(client.NewMockAcceptorImpl())
	servers[2].SetAcceptor(client.NewMockAcceptorImpl())

	// Note: healthyRelays includes self
	healthyRelays, _, _ := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)
	relaysForTestCase := healthyRelaysWithoutIds[:]

	// STEP 1
	// ----------------
	// Creates a ChainID that will be reused in concurrent scenario.
	// Note that we wait for the broadcast to be done before next step.

	// Executes first transaction broadcast
	firstTimeoutAfter := 20 * time.Second // Time for broadcast
	firstBroadcastCtx, firstCancelCtxFn := context.WithTimeout(context.TODO(), firstTimeoutAfter)
	defer firstCancelCtxFn()

	numTransactions1 := 1
	testChainID1 := makeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID1,
		numTransactions1,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine started with %d relays and test-chain-1: %s...", len(relaysForTestCase), testChainID1)

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg1 := waitForClientBroadcastStatus(t,
		firstBroadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg1)
	assert.Len(t, resultStatusMsg1.TxHashes, numTransactions1, "first broadcast should return transaction hashes")
	assert.NoError(t, resultStatusMsg1.Error, "first broadcast should not contain error status")
	close(notifyCh1)

	// STEP 2
	// ----------------
	// Creates another ChainID that will be reused in concurrent scenario.

	// Executes first transaction broadcast
	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions2 := 1
	testChainID2 := makeChainID("test-chain-2")
	notifyCh2 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID2,
		numTransactions2,
		notifyCh2,
	)
	t.Logf("Broadcast goroutine started with %d relays and test-chain-2: %s...", len(relaysForTestCase), testChainID2)

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg2 := waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID2,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg2)
	assert.Len(t, resultStatusMsg2.TxHashes, numTransactions1, "second broadcast should return transaction hashes")
	assert.NoError(t, resultStatusMsg2.Error, "second broadcast should not contain error status")
	close(notifyCh2)

	waitDuration := 30 * time.Second
	t.Logf("Waiting %.0fsec to finalize networks creation...", waitDuration.Seconds())
	time.Sleep(waitDuration)
	t.Logf("Done waiting to finalize networks creation...")

	// STEP 3
	// ----------------
	// Concurrently broadcast transactions using a mix of the existing
	// test-chain-1 and multiple new chains.

	numConcurrent := 5
	testDoneCh := make(chan struct{}, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		timeoutAfter := 20 * time.Second // Time for broadcast
		broadcastCtx, cancelCtxFn := context.WithTimeout(context.TODO(), timeoutAfter)
		defer cancelCtxFn()

		testChainName := "test-chain-" + strconv.Itoa(i)
		iTestChainID := makeChainID(testChainName)
		if i == 1 {
			iTestChainID = testChainID1 // EXISTING ChainID!
		} else if i == 2 {
			iTestChainID = testChainID2 // EXISTING ChainID!
		}

		numTransactionsX := 1
		notifyCh := make(chan client.BroadcastStatus)
		go clientBroadcastTx(t,
			broadcastCtx,
			servers[0],
			relaysForTestCase,
			iTestChainID,
			numTransactionsX,
			notifyCh,
		)
		t.Logf("Broadcast goroutine started with %d relays and %s: %s...",
			len(relaysForTestCase), testChainName, iTestChainID)

		// Wait in a separate goroutine as we want to test thread-safety.
		go func(ch chan struct{}) {
			defer close(notifyCh)
			// t.Logf("Waiting for broadcast status on %s: %s...",
			// 	testChainName, iTestChainID)

			// Blocks this thread until we consume from notifyCh1.
			resultStatusMsg := waitForClientBroadcastStatus(t,
				broadcastCtx,
				iTestChainID,
				notifyCh,
			)
			assert.NotNil(t, resultStatusMsg)
			assert.Len(t, resultStatusMsg.TxHashes, numTransactionsX,
				"broadcast at "+strconv.Itoa(i)+" should return transaction hashes")
			assert.NoError(t, resultStatusMsg.Error,
				"broadcast at "+strconv.Itoa(i)+" should not contain error status")

			ch <- struct{}{}
		}(testDoneCh)
	}

	// t.Log("Waiting to consume from testDoneCh or timeout globally...")

	// Waits to consume from testDoneCh or timeout after 30 seconds
	var (
		wg           sync.WaitGroup
		cntDone      int
		errBroadcast error
	)
	wg.Add(1)
	go func(ch chan struct{}) {
		defer wg.Done()
		for {
			select {
			case <-ch:
				cntDone++
				if cntDone == numConcurrent {
					return
				}

			case <-testCaseCtx.Done():
				errBroadcast = errors.New("Timed out waiting for concurrent broadcasts to end")
				return
			}
		}
	}(testDoneCh)
	wg.Wait()

	t.Log("Test case done running...")

	waitDuration = 30 * time.Second
	t.Logf("Waiting %.0fsec before evaluating results...", waitDuration.Seconds())
	time.Sleep(waitDuration)
	t.Logf("Done waiting before evaluating results...")

	assert.Equal(t, numConcurrent, cntDone)
	assert.NoError(t, errBroadcast, "concurrent broadcasts should not error")

	// -------------------
	// Also test callbacks
	t.Logf("Now evaluating callbacks execution...")

	totalExpectedCalls := numTransactions1 + numTransactions2 + numConcurrent
	testAcceptorRelay1 := servers[0].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay2 := servers[1].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay3 := servers[2].GetAcceptor().(*client.MockAcceptorImpl)

	// Test that client callbacks were executed correctly, every relay should
	// have executed the CommitBroadcastTx callback when the block is finalized.

	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay1.TxCommitCalls.Load(),
		"should locally execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay2.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay3.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
}

func TestScenarioConcurrentNewChains3(t *testing.T) {
	defer goleak.VerifyNone(t)

	timeoutGlobal := 120 * time.Second // Time for full test round
	testCaseCtx, globalCancelFn := context.WithTimeout(context.TODO(), timeoutGlobal)
	defer globalCancelFn()

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	servers[0].SetAcceptor(client.NewMockAcceptorImpl())
	servers[1].SetAcceptor(client.NewMockAcceptorImpl())
	servers[2].SetAcceptor(client.NewMockAcceptorImpl())

	// Note: healthyRelays includes self
	healthyRelays, _, _ := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)
	relaysForTestCase := healthyRelaysWithoutIds[:]

	// Concurrently broadcast transactions using new chains.
	numConcurrent := 3
	testDoneCh := make(chan struct{}, numConcurrent)

	for i := 0; i < numConcurrent; i++ {
		timeoutAfter := 30 * time.Second // Time for broadcast
		broadcastCtx, cancelCtxFn := context.WithTimeout(context.TODO(), timeoutAfter)
		defer cancelCtxFn()

		numTransactions := 1
		testChainName := "test-chain-" + strconv.Itoa(i)
		testChainID := makeChainID(testChainName)
		notifyCh := make(chan client.BroadcastStatus)

		// Note we broadcast using the main thread to make sure broadcasting
		// happens before waiting for an update on notifyCh, as it is not
		// possible to force the order of execution of goroutines.
		go clientBroadcastTx(t,
			broadcastCtx,
			servers[0],
			relaysForTestCase,
			testChainID,
			numTransactions,
			notifyCh,
		)

		t.Logf("Broadcast started with %d relays and %s: %s...",
			len(relaysForTestCase), testChainName, testChainID)

		// Wait in a separate goroutine.
		go func(ch chan struct{}) {
			defer close(notifyCh)

			t.Logf("Waiting for broadcast status on %s: %s...",
				testChainName, testChainID)

			// Blocks this thread until we consume from notifyCh1.
			resultStatusMsg := waitForClientBroadcastStatus(t,
				broadcastCtx,
				testChainID,
				notifyCh,
			)
			require.NotNil(t, resultStatusMsg)
			require.Len(t, resultStatusMsg.TxHashes, numTransactions,
				"broadcast at "+strconv.Itoa(i)+" should contain transaction hashes")
			require.NoError(t, resultStatusMsg.Error,
				"broadcast at "+strconv.Itoa(i)+" should not contain error status")

			ch <- struct{}{}
		}(testDoneCh)
	}

	t.Log("Waiting to consume from testDoneCh or timeout globally...")

	// Waits to consume from testDoneCh or timeout after 30 seconds
	var (
		wg           sync.WaitGroup
		cntDone      int
		errBroadcast error
	)
	wg.Add(1)
	go func(ch chan struct{}) {
		defer wg.Done()
		for {
			select {
			case <-ch:
				cntDone++
				if cntDone == numConcurrent {
					return
				}

			case <-testCaseCtx.Done():
				errBroadcast = errors.New("Timed out waiting for concurrent broadcasts to end")
				return
			}
		}
	}(testDoneCh)
	wg.Wait()

	t.Log("Test case done running...")

	waitDuration := 30 * time.Second
	t.Logf("Waiting %.0fsec before evaluating results...", waitDuration.Seconds())
	time.Sleep(waitDuration)
	t.Logf("Done waiting before evaluating results...")

	assert.Equal(t, numConcurrent, cntDone)
	assert.NoError(t, errBroadcast, "concurrent broadcasts should not error")

	// -------------------
	// Also test callbacks
	t.Logf("Now evaluating callbacks execution...")

	totalExpectedCalls := numConcurrent
	testAcceptorRelay1 := servers[0].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay2 := servers[1].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay3 := servers[2].GetAcceptor().(*client.MockAcceptorImpl)

	// Test that client callbacks were executed correctly, every relay should
	// have executed the CommitBroadcastTx callback when the block is finalized.

	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay1.TxCommitCalls.Load(),
		"should locally execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay2.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(totalExpectedCalls), testAcceptorRelay3.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
}

// ----------------------------------------------------------------------------
// Client Callbacks Tests

func TestScenarioCallbacksCallsCommitBroadcastTx(t *testing.T) {

	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 3

	// To enable debug logs, change this indexes array to contain the indexes
	// of the relays for which you want to activate full logging.
	idxRelaysWithLogs := []int{} // e.g. []int{0, 1} for relay-1 and relay-2
	servers, shutdownFn := ResetTestScenarioRelaysWithOptions(t, numChains, numRelays, idxRelaysWithLogs, [][]mx.MultiplexBackendOption{})
	defer shutdownFn(servers)

	servers[0].SetAcceptor(client.NewMockAcceptorImpl())
	servers[1].SetAcceptor(client.NewMockAcceptorImpl())
	servers[2].SetAcceptor(client.NewMockAcceptorImpl())

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// TEST 1
	// ----------------
	// Creates a new ChainID and expects relay-1 to call CommitBroadcastTx
	// and relay-2,relay-3 to call AcceptBroadcastTx callbacks.

	numTransactions1 := 1
	testWithChainID := makeChainID("test-chain-1")

	requireCompleteClientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testWithChainID,
		numTransactions1,
	)

	waitDuration := 15 * time.Second
	t.Logf("Waiting %.0fsec before evaluating CommitBroadcastTx...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	testAcceptorRelay1 := servers[0].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay2 := servers[1].GetAcceptor().(*client.MockAcceptorImpl)
	testAcceptorRelay3 := servers[2].GetAcceptor().(*client.MockAcceptorImpl)

	// Test that client callbacks were executed correctly, every relay should
	// have executed the CommitBroadcastTx callback when the block is finalized.

	assert.Equal(t, uint64(numTransactions1), testAcceptorRelay1.TxCommitCalls.Load(),
		"should locally execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(numTransactions1), testAcceptorRelay2.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(numTransactions1), testAcceptorRelay3.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")

	// RESET test state

	testAcceptorRelay1.TxCommitCalls.Store(uint64(0))
	testAcceptorRelay2.TxCommitCalls.Store(uint64(0))
	testAcceptorRelay3.TxCommitCalls.Store(uint64(0))

	// TEST 2
	// ----------------
	// Re-use the same ChainID and expects relay-1 to call CommitBroadcastTx
	// and relay-2,relay-3 to call CommitBroadcastTx callbacks as well.

	// Executes second transaction broadcast
	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions2 := 1

	requireCompleteClientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testWithChainID, // EXISTING ChainID
		numTransactions2,
	)

	waitDuration = 15 * time.Second
	t.Logf("Waiting %.0fsec before evaluating CommitBroadcastTx...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// Test that client callbacks were executed correctly, every relay should
	// have executed the CommitBroadcastTx callback when the block is finalized.

	assert.Equal(t, uint64(numTransactions2), testAcceptorRelay1.TxCommitCalls.Load(),
		"should locally execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(numTransactions2), testAcceptorRelay2.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
	assert.Equal(t, uint64(numTransactions2), testAcceptorRelay3.TxCommitCalls.Load(),
		"should remotely execute CommitBroadcastTx callback for each transaction")
}

// TODO(midas): TestScenarioClientBroadcastCallsCommitBroadcastTx
// TODO(midas): TestScenarioClientBroadcastCallsReplayBroadcastTxBatch
// TODO(midas): TestScenarioClientBroadcastCallsRollbackTx

// ----------------------------------------------------------------------------
// LEGACY Broadcast Test (Using CometBFT RPC Server)

func nullTxBroadcasterOverwrite(tb testing.TB, _ string, _ int) {
	tb.Helper()
}

// Uses RPC Server to broadcast transactions.
func txBroadcasterOverwrite(tb testing.TB, chainID string, numTxes int) {
	tb.Helper()

	broadcastRawTxes(
		tb,
		[]string{chainID},
		1,
		1,
		numTxes,
	)
}

// Uses RPC Server to broadcast transactions.
func broadcastRawTxes(
	tb testing.TB,
	networks []string,
	numChains,
	numRelays,
	numTransactions int,
) {
	tb.Helper()

	// We shall randomly pick a node index and values
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(numTransactions)

	// Every iteration should create a transaction with a random value
	// and broadcast it using the second relay's rpc.
	for i := 0; i < numTransactions; i++ {
		mtx.Lock()
		randomizeData := randomizer.Intn(999999999)

		randomRelay := 0
		if numRelays > 1 {
			randomRelay = randomizer.Intn(numRelays) + 1
		}

		randomChain := 0
		if numChains > 1 {
			randomChain = randomizer.Intn(numChains)
		}
		mtx.Unlock()

		randomVal := strconv.Itoa(randomizeData)
		txData := "test=value" + randomVal

		// Uses CometBFT RPC Port (DiscoveryPort + 2)
		rpcPort := strconv.Itoa(50001 + (randomRelay * 100) + 2) // random relay RPC
		chainID := networks[randomChain]

		nodeRPC := "http://127.0.0.1:" + rpcPort
		rpcPath := "/broadcast_tx_commit/" + chainID

		go func(host, path, tx string) {
			defer wg.Done()

			// For debug, uncomment the following line
			tb.Logf("Now broadcasting transaction: %s to %s", tx, host)
			http.Get(host + path + "?tx=\"" + tx + "\"")
		}(nodeRPC, rpcPath, txData)
	}

	// Waits for numTransactions to be broadcast
	wg.Wait()
}

func TestScenarioLegacyBroadcastSevenHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	testReactor := servers[0].GetReactor()
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
	nodesProvider := testReactor.GetServicesProvider()

	nodeInstance := nodesProvider(mx.ServiceKeyNodeRuntime, testChainID).(*node.Node)

	testMultiplex := mx.MultiplexMap[*node.Node]{}
	testMultiplex[testChainID] = mx.NewChainInstance(testChainID, nodeInstance)

	// First create genesis block
	assertWaitForNodesMultiplexToProduceBlocks(t,
		testReactor,
		testMultiplex,
		1, // numBlocks
		5*time.Second,
		"node_genblock_test",
		txBroadcasterOverwrite,
	)

	// Then wait for next 10 blocks asynchronously,
	// transactions will be broadcasted soon.
	wgBlocks := sync.WaitGroup{}
	wgBlocks.Add(1)
	go func() {
		defer wgBlocks.Done()

		actualNumBlocks, actualNumTxes := assertWaitForNodesMultiplexToProduceBlocks(t,
			testReactor,
			testMultiplex,
			-1,             // as many blocks as necessary
			20*time.Second, // 20 seconds runtime
			"node_blocks_test",
			nullTxBroadcasterOverwrite,
		)

		assert.Contains(t, actualNumTxes, testChainID)
		assert.Contains(t, actualNumBlocks, testChainID)

		storeProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStateStore)
		stateMachine, err := storeProvider(testChainID).(sm.Store).Load()
		assert.NoError(t, err)
		assert.Greater(t, stateMachine.LastBlockHeight, int64(1))
	}()

	// In parallel, broadcast transactions and wait for
	// all to be broadcasted.
	wgTxes := sync.WaitGroup{}
	wgTxes.Add(1)

	numTransactions := 300
	go func(numTxes int) {
		defer wgTxes.Done()

		// Then broadcast many transactions
		chainIds := servers[0].GetNetworks()
		broadcastRawTxes(
			t,
			chainIds,
			numChains,
			1,       // sends all to first relay!
			numTxes, // numTransactions
		)
	}(numTransactions)

	// First wait for transactions to be broadcast
	wgTxes.Wait()

	// And also wait for enough blocks to be produced
	wgBlocks.Wait()
}

// ----------------------------------------------------------------------------
// Helpers

func ResetTestScenarioRelaysWithOptions(
	tb testing.TB,
	numChains int,
	numRelays int,
	idxRelaysWithLogs []int,
	backendOptionsPerRelay [][]mx.MultiplexBackendOption,
) ([]*mx.MultiplexBackend, func([]*mx.MultiplexBackend)) {
	tb.Helper()
	withLogger := cmtlog.NewNopLogger()
	if len(idxRelaysWithLogs) > 0 {
		withLogger = cmtlog.TestingLogger()
	}
	return ResetTestScenarioRelays(tb,
		numChains,
		numRelays,
		withLogger,
		idxRelaysWithLogs,
		backendOptionsPerRelay,
	)
}

func ResetTestScenarioRelaysWithSomeLogs(
	tb testing.TB,
	numChains int,
	numRelays int,
	idxRelaysWithLogs []int,
) ([]*mx.MultiplexBackend, func([]*mx.MultiplexBackend)) {
	tb.Helper()
	if len(idxRelaysWithLogs) == numRelays {
		return ResetTestScenarioRelaysWithLogs(tb, numChains, numRelays)
	}

	backendOpts := makeEmptyBackendOptions(numRelays)
	return ResetTestScenarioRelays(tb, numChains, numRelays, cmtlog.TestingLogger(), idxRelaysWithLogs, backendOpts)
}

func ResetTestScenarioRelaysWithLogs(
	tb testing.TB,
	numChains int,
	numRelays int,
) ([]*mx.MultiplexBackend, func([]*mx.MultiplexBackend)) {
	tb.Helper()
	backendOpts := makeEmptyBackendOptions(numRelays)
	return ResetTestScenarioRelays(tb, numChains, numRelays, cmtlog.TestingLogger(), []int{}, backendOpts)
}

func ResetTestScenarioRelaysWithoutLogs(
	tb testing.TB,
	numChains int,
	numRelays int,
) ([]*mx.MultiplexBackend, func([]*mx.MultiplexBackend)) {
	tb.Helper()
	backendOpts := makeEmptyBackendOptions(numRelays)
	return ResetTestScenarioRelays(tb, numChains, numRelays, cmtlog.NewNopLogger(), []int{}, backendOpts)
}

// Initializes numChains on a number of relays. This helper returns a list of
// configured multiplex backend instances and a shutdown functor.
func ResetTestScenarioRelays(
	tb testing.TB,
	numChains int,
	numRelays int,
	withLogger cmtlog.Logger,
	idxRelaysWithLogs []int,
	backendOptionsPerRelay [][]mx.MultiplexBackendOption,
) ([]*mx.MultiplexBackend, func([]*mx.MultiplexBackend)) {
	tb.Helper()

	// For debug, change the loggers to cmtlog.TestingLogger()
	customLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		if len(idxRelaysWithLogs) == 0 {
			customLoggers[i] = withLogger.With("process", "relay-"+strconv.Itoa(i+1))
		} else if slices.Contains(idxRelaysWithLogs, i) {
			customLoggers[i] = withLogger.With("process", "relay-"+strconv.Itoa(i+1))
		} else {
			customLoggers[i] = cmtlog.NewNopLogger()
		}
	}

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelaysWithOptions(
		tb,
		numChains,
		numRelays,
		backendOptionsPerRelay,
		customLoggers...,
	)
	require.NotEmpty(tb, servers)
	require.Len(tb, rootDirs, numRelays)
	require.Len(tb, servers, numRelays)

	shutdownFn := func(backends []*mx.MultiplexBackend) {
		waitDuration := 10 * time.Second
		tb.Logf("Waiting %.0fsec for shutdown...", waitDuration.Seconds())
		time.Sleep(waitDuration)

		wg := sync.WaitGroup{}
		wg.Add(len(backends))
		for i := 0; i < len(backends); i++ {
			if backends[i] == nil {
				wg.Done()
				continue // Reset/stopped backends are shutdown manually.
			}

			go func() {
				defer wg.Done()
				closeAndRemoveAll(tb, rootDirs[i], backends[i])
			}()
		}
		wg.Wait()
		tb.Logf("Done shutting down all backends")
	}

	return servers, shutdownFn
}

// Starts the multiplex backend instances and creates a cancelable context
// for the broadcast operation(s). The relays MAY contain a peer ID.
func StartTestScenarioRelays(
	tb testing.TB,
	servers []*mx.MultiplexBackend,
	waitDuration time.Duration,
	timeoutDuration time.Duration,
) (relays []string, ctx context.Context, cancelCtxFn func()) {
	tb.Helper()
	require.NotEmpty(tb, servers)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	if waitDuration.Seconds() > float64(0) {
		// Give the backend some time before starting broadcast context
		tb.Logf("Waiting %.0fsec to use node services...", waitDuration.Seconds())
		time.Sleep(waitDuration)
	}

	// Prepare the relays addresses
	relays = make([]string, 0, len(servers))
	for i := 0; i < len(servers); i++ {
		relays = append(relays, servers[i].GetListenAddress())
	}

	// Cancelable context to permit stopping by timeout
	ctx, cancelFn := context.WithTimeout(context.TODO(), timeoutDuration)
	return relays, ctx, cancelFn
}

// Resets a shutdown multiplex backend using otherRelay's configuration.
// This helper returns a configured multiplex backend instance and a shutdown functor.
// This helper *does not* call `MustStart` on the multiplex backend.
func ResetTestSingleCompatibleRelay(
	tb testing.TB,
	rootDir string,
	otherRelay *mx.MultiplexBackend,
	indexRelay int,
	customLogger cmtlog.Logger,
) (*mx.MultiplexBackend, func(*mx.MultiplexBackend)) {
	tb.Helper()
	require.NotNil(tb, otherRelay)

	baseCfg := otherRelay.GetReactor().GetNodeConfig()

	// Uses config.TestConfig() and compatible MultiplexConfig
	rootDirRelayX,
		globalCfgRelayX := ResetTestMultiplexNodeWithConfigAndPorts(
		tb,
		rootDir,
		"_"+strconv.Itoa(indexRelay+1), // metricsSuffix
		baseCfg.MultiplexConfig,
		uint16(50001+(indexRelay*100)), // 50001, 50101, 50201, 50301, 50401
		false,                          // don't create new root dir
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelayX.ChainSeeds {
		globalCfgRelayX.ChainSeeds[chainID] = ""
	}

	serverRelayX, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfgRelayX,
		customLogger,
	)
	require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(indexRelay))

	shutdownFn := func(backend *mx.MultiplexBackend) {
		defer os.RemoveAll(rootDirRelayX)

		if backend != nil {
			err := backend.Close()
			assert.NoError(tb, err, "should shutdown reset server at index: "+strconv.Itoa(indexRelay))
		}
	}

	return serverRelayX, shutdownFn
}

func requireCompleteClientBroadcastTx(
	tb testing.TB,
	broadcastCtx context.Context,
	backend *mx.MultiplexBackend,
	relays []string,
	withChainID string,
	numTransactions int,
) {
	// Separate goroutine for client broadcast process
	notifyCh := make(chan client.BroadcastStatus)

	go clientBroadcastTx(tb,
		broadcastCtx,
		backend,
		relays,
		withChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(tb,
		broadcastCtx,
		withChainID,
		notifyCh,
	)
	require.NotNil(tb, resultStatusMsg)
	require.NoError(tb, resultStatusMsg.Error,
		fmt.Sprintf("should not contain error status for transactions on: %s", withChainID))
	require.Len(tb, resultStatusMsg.TxHashes, numTransactions,
		fmt.Sprintf("should contain all accepted transaction hashes on: %s", withChainID))
	close(notifyCh)
}
