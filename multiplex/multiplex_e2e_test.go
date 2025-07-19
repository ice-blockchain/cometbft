package multiplex_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/p2p"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

// ----------------------------------------------------------------------------
// MultiplexClient Broadcast Test (Using client.BroadcastTx)
//
// -run=TestScenarioMultiplex

var randomizer = rand.New(rand.NewSource(time.Now().Unix()))

func TestScenarioMultiplexErrors(t *testing.T) {
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
		0*time.Second,  // Time for backend
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
	testChainID := helpers.MakeChainID("test-chain-1")
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

	numExpected := numRelaysForErrCase*2/3 + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)

	// TODO(midas): add other error cases as forwarded with Client.BroadcastTx.
	// TODO(midas): e.g. relay failing to respond with AckTransactionBroadcast.
	// TODO(midas): e.g. relay failing to respond with ChainReplicationRequest.
	// TODO(midas): e.g. test RollbackTx callback given late consensus failure.
}

// With a list of healthy relays, we test the ability to intercept replication
// channel message: ChainReplicationResponse from each of the relays.
func TestScenarioMultiplexWaitForReplicationResponses(t *testing.T) {
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
		0*time.Second,  // Time for backend
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
	testChainID1 := helpers.MakeChainID("test-chain-1")
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

	// TODO(midas): add error test case, see TEST 2 in TestScenarioClientBroadcastWaitForAckTransactions.
}

// With a list of healthy relays, we test the ability to intercept broadcast
// channel message: AckTransactionBroadcast from each of the relays.
// In a second iteration, we set 2 relays to be unhealthy and make sure that
// that the Ack process times out gracefully but still intercepts other Acks.
func TestScenarioMultiplexWaitForAckTransactions(t *testing.T) {
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
		0*time.Second,  // Time for backend
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
	testChainID1 := helpers.MakeChainID("test-chain-1")
	testRelayOne := servers[0]

	// Fill chainRelays such that all HEALTHY relays are expected to respond.
	testChainRelays,
		testCatchupRelays := mockRelayMapsForChainID(t, testRelayOne, relaysForTestCase, testChainID1, false) // false=useCatchup

	testChainInfo1 := helpers.NewExtendedChainIDFromString(testChainID1)
	require.NotNil(t, testChainInfo1, "should create correctly formatted ChainID")
	testTransactions1 := makeClientTransactions(t, testChainInfo1, 1)

	// Block main thread to test AckTransaction process
	_, actualRelaysPerTx,
		actualExpectedAcks,
		actualNumReceived,
		actualAcceptErr := clientAckTransaction(t,
		broadcastCtx,
		testRelayOne,
		testChainInfo1.GetUserAddress(),
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

	// TEST 2 - Success
	//
	// Add 2 unhealthy relays to relays including self, and make sure the ACK
	// process goes through because we have 2/3 of ACKs (+self) for the tx.
	// numRelays=7;numHealthy=5;numErrors=2;withSelf=true

	relaysForTestCase = healthyRelays[:len(healthyRelays)-2] // with IDs!
	numHealthy = len(relaysForTestCase)                      // 5
	testChainID2 := helpers.MakeChainID("test-chain-2")
	numRelaysForTestCase := 7
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// testChainRelays contains 2 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	testChainRelays,
		testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForTestCase, testChainID2, false) // false=useCatchup

	// Remove 2 healthy relays to force timeout, as we expect them to Ack
	// but they will not be sending a AckTransactionBroadcast message.
	testAckingRelays := make(map[string][]*server.RelayAddress, 1)
	testAckingRelays[testChainID2] = testChainRelays[testChainID2][:]

	// Now add back the 2 unhealthy relays so that they are expected to Ack.
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		// random node key
		privKey := ed25519.GenPrivKey()
		nodeKey := &p2p.NodeKey{
			PrivKey: privKey,
		}

		fakeRelayAddr, _ := server.NewRelayAddress(string(nodeKey.ID()) + "@1.2.3.4:" + strconv.Itoa(1000+i))
		testChainRelays[testChainID2] = append(testChainRelays[testChainID2], fakeRelayAddr)
	}

	// We don't want to stall tests here, fast timeout for failing Acks.
	secondTimeoutAfter := 300 * time.Millisecond
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	testChainInfo2 := helpers.NewExtendedChainIDFromString(testChainID2)
	require.NotNil(t, testChainInfo2, "should create correctly formatted ChainID")
	testTransactions2 := makeClientTransactions(t, testChainInfo2, 1)

	// Block main thread to test AckTransaction process
	_, _, actualExpectedAcks, _, actualAcceptErr = clientAckTransaction(t,
		secondBroadcastCtx,
		testRelayOne,
		testChainInfo1.GetUserAddress(),
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

	// Missing only 2/6 acks should NOT error
	assert.Len(t, actualAcksReceived, expectedNumReceived)
	assert.NoError(t, actualAcceptErr, "should not error given 2/3 ACKs")

	// TEST 3 - Error
	//
	// Add 3 unhealthy relays to relays including self, and make sure the ACK
	// process errors because we have less than 2/3 of ACKs (+self) for the tx.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true

	relaysForErrCase := healthyRelays[:len(healthyRelays)-3] // with IDs!
	numHealthy = len(relaysForErrCase)                       // 4
	testChainID3 := helpers.MakeChainID("test-chain-3")
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// testChainRelays contains 2 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	testChainRelays,
		testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForErrCase, testChainID3, false) // false=useCatchup

	// Remove 3 healthy relays to force timeout, as we expect them to Ack
	// but they will not be sending a AckTransactionBroadcast message.
	testAckingRelays = make(map[string][]*server.RelayAddress, 1)
	testAckingRelays[testChainID3] = testChainRelays[testChainID3][:]

	// Now add back the 2 unhealthy relays so that they are expected to Ack.
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		// random node key
		privKey := ed25519.GenPrivKey()
		nodeKey := &p2p.NodeKey{
			PrivKey: privKey,
		}

		fakeRelayAddr, _ := server.NewRelayAddress(string(nodeKey.ID()) + "@1.2.3.4:" + strconv.Itoa(1000+i))
		testChainRelays[testChainID3] = append(testChainRelays[testChainID3], fakeRelayAddr)
	}

	// We don't want to stall tests here, fast timeout for failing Acks.
	thirdTimeoutAfter := 300 * time.Millisecond
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	testChainInfo3 := helpers.NewExtendedChainIDFromString(testChainID3)
	require.NotNil(t, testChainInfo3, "should create correctly formatted ChainID")
	testTransactions3 := makeClientTransactions(t, testChainInfo3, 1)

	// Block main thread to test AckTransaction process
	_, _, actualExpectedAcks, _, actualAcceptErr = clientAckTransaction(t,
		thirdBroadcastCtx,
		testRelayOne,
		testChainInfo1.GetUserAddress(),
		testAckingRelays, // unhealthy removed
		testCatchupRelays,
		testChainRelays, // mustAckRelays => 2 are unhealthy
		testTransactions3,
	)

	// This error case must count unhealthy relays in "expected to Ack".
	expectedNumAwaitedAcks = len(testChainRelays[testChainID3]) // -self
	assert.Equal(t, expectedNumAwaitedAcks, actualExpectedAcks)

	// We won't receive all Acks, but should receive from all healthy relays.
	// In this test, we do NOT receive enough ACKs to proceed.
	expectedNumReceived = numHealthy - 1 // -self-unhealthy
	testTxHash = fmt.Sprintf("%X", testTransactions3[0].Hash())
	actualAcksReceived = testRelayOne.GetAckResponsePeers(testTxHash)

	// Should error because we have too few ACKs, i.e. less than 2/3
	assert.Len(t, actualAcksReceived, expectedNumReceived)
	require.Error(t, actualAcceptErr, "should timeout gracefully given less than 2/3 ACKs")
	assert.Contains(t, actualAcceptErr.Error(), "process timed out waiting for ack messages")
}

// With a list of healthy relays, we test the ability to intercept replication
// channel message: ChainReplicationComplete from each of the relays.
func TestScenarioMultiplexWaitForReplicationCompleted(t *testing.T) {
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
		0*time.Second,  // Time for backend
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
	testChainID1 := helpers.MakeChainID("test-chain-1")
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

	testChainInfo1 := helpers.NewExtendedChainIDFromString(testChainID1)
	require.NotNil(t, testChainInfo1, "should create correctly formatted ChainID")
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

	// TEST 2 - Success
	//
	// Add 2 unhealthy relays to relays including self, and make sure the process
	// goes through because we have 2/3 of completions (+self) for the ChainID.
	// numRelays=7;numHealthy=5;numErrors=2;withSelf=true

	relaysForTestCase = healthyRelays[:len(healthyRelays)-2] // with IDs!
	numHealthy = len(relaysForTestCase)                      // 5
	testChainID2 := helpers.MakeChainID("test-chain-2")
	numRelaysForTestCase := 7
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// relaysForTestCase contains 2 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	_, testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForTestCase, testChainID2, true) // trure=useCatchup

	testSyncingChainIds2 := []string{}
	for testSyncingChain, _ := range testCatchupRelays {
		testSyncingChainIds2 = append(testSyncingChainIds2, testSyncingChain)
	}

	testCompletingRelays := make(map[string][]*server.RelayAddress, 1)
	testCompletingRelays[testChainID2] = testCatchupRelays[testChainID2][:]

	// Now add back the 2 unhealthy relays so that they are expected to Complete.
	for i := numHealthy; i < numRelaysForTestCase; i++ {
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

	// We don't want to stall tests here, fast timeout for failing peers.
	secondTimeoutAfter := 300 * time.Millisecond
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	testChainInfo2 := helpers.NewExtendedChainIDFromString(testChainID2)
	require.NotNil(t, testChainInfo2, "should create correctly formatted ChainID")
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
	_, actualCompletionError2 := clientReplicationCompleted(t,
		secondBroadcastCtx,
		testRelayOne,
		testSyncingChainIds2,
		testTransactions2,
		testCompletingRelays, // 2 unhealthy are NOT sending ChainReplicationComplete.
	)

	expectedNumCompleted = numHealthy - 1
	actualReplCompletePeers := testRelayOne.GetReplCompletePeers(testChainID2)
	actualNumCompleted = len(actualReplCompletePeers)

	// Missing only 2/6 runtime updates should NOT error
	assert.Equal(t, expectedNumCompleted, actualNumCompleted)
	assert.NoError(t, actualCompletionError2, "should not error given 2/3 runtime updates")

	// TEST 3 - Error
	//
	// Add 3 unhealthy relays to relays including self, and make sure the process
	// errors because we have less than 2/3 of completions (+self) for the ChainID.
	// numRelays=7;numHealthy=4;numErrors=3;withSelf=true

	relaysForErrCase := healthyRelays[:len(healthyRelays)-3] // with IDs!
	numHealthy = len(relaysForErrCase)                       // 4
	testChainID3 := helpers.MakeChainID("test-chain-3")
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// relaysForErrCase contains 3 unhealthy relays (which don't have ID),
	// but these will be *filtered* out due to not being healthy.
	_, testCatchupRelays = mockRelayMapsForChainID(t, testRelayOne, relaysForErrCase, testChainID3, true) // trure=useCatchup

	testSyncingChainIds3 := []string{}
	for testSyncingChain, _ := range testCatchupRelays {
		testSyncingChainIds3 = append(testSyncingChainIds3, testSyncingChain)
	}

	testCompletingRelays3 := make(map[string][]*server.RelayAddress, 1)
	testCompletingRelays3[testChainID3] = testCatchupRelays[testChainID3][:]

	// Now add back the 3 unhealthy relays so that they are expected to Complete.
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		// random node key
		privKey := ed25519.GenPrivKey()
		nodeKey := &p2p.NodeKey{
			PrivKey: privKey,
		}

		fakeRelayAddr, _ := server.NewRelayAddress(string(nodeKey.ID()) + "@1.2.3.4:" + strconv.Itoa(1000+i))
		testCatchupRelays[testChainID3] = append(testCatchupRelays[testChainID3], fakeRelayAddr)
	}

	// testCatchupRelays DOES contain the unhealthy relays
	// testCompletingRelays3 does NOT contain the unhealthy relays

	// We don't want to stall tests here, fast timeout for failing peers.
	thirdTimeoutAfter := 300 * time.Millisecond
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	testChainInfo3 := helpers.NewExtendedChainIDFromString(testChainID3)
	require.NotNil(t, testChainInfo3, "should create correctly formatted ChainID")
	testTransactions3 := makeClientTransactions(t, testChainInfo3, 1)

	// First we feed some ChainReplicationResponse to fill ackResponsesRcvd.
	// Note that in this test unhealthy peers also send ChainReplicationResponse.
	_, _, _, replErr = clientAckReplication(t,
		thirdBroadcastCtx,
		testRelayOne,
		testCatchupRelays,
	)
	require.NoError(t, replErr, "should pre-fill ReplResponsePeers")

	// Block main thread to test ChainReplicationComplete process
	_, errCaseCompletionError := clientReplicationCompleted(t,
		thirdBroadcastCtx,
		testRelayOne,
		testSyncingChainIds3,
		testTransactions3,
		testCompletingRelays3, // 3 unhealthy are NOT sending ChainReplicationComplete.
	)

	errCaseExpectedNumCompleted := numHealthy - 1
	errCaseActualNumCompleted := testRelayOne.GetReplCompletePeers(testChainID3)

	// Missing only 2/6 runtime updates should NOT error
	assert.Equal(t, errCaseExpectedNumCompleted, len(errCaseActualNumCompleted))
	assert.Error(t, errCaseCompletionError, "should timeout gracefully")
	assert.Contains(t, errCaseCompletionError.Error(), "process timed out waiting for runtime status")
}

// With a list of empty relays, and not enough healthy relays,
// we first sanity check a successful broadcast completion with
// less minimum healthy relays, and then we test a broadcast failure
// with five relays failing, i.e. too many, to fail the broadcast.
func TestScenarioMultiplexNotEnoughHealthyRelays(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 0
	numHealthy := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy) // only healthy here
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// only 3 healthy relays to evaluate callbacks
	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	servers[0].SetAcceptor(testAcceptorRelay1)
	servers[1].SetAcceptor(testAcceptorRelay2)
	servers[2].SetAcceptor(testAcceptorRelay3)

	// Note: relays contains self for this test
	healthyRelays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		0*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, healthyRelays)
	require.NotNil(t, broadcastCtx)

	// NOTE: this removes the Relay ID from relays addresses.
	healthyRelaysWithoutIds := useRelaysWithoutIds(t, healthyRelays)
	mx.WithReplicationTimeout(10 * time.Second)(servers[0])

	// TEST 1 - Success
	//
	// Add 1 unavailable relay to the list WITH self, and removed relay IDs,
	// and should broadcast successfully.
	// Makes sure successful response is possible given less required relays.
	// numRelays=4;numHealthy=3;numErrors=1;withSelf=true
	relaysForTestCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForTestCase)
	numRelaysForTestCase := 4
	for i := numHealthy; i < numRelaysForTestCase; i++ {
		relaysForTestCase = append(relaysForTestCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := helpers.MakeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine #1 started and should succeed...")

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error,
		"first broadcast should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"first broadcast should contain transaction hashes")
	close(notifyCh1)

	// TEST 2 - Errors
	//
	// Add 4 unavailable relays to the list WITH self, and removed relay IDs.
	// numRelays=7;numHealthy=3;numErrors=4;withSelf=true
	relaysForErrCase := healthyRelaysWithoutIds[:]
	numHealthy = len(relaysForErrCase)
	numRelaysForErrCase := 7
	for i := numHealthy; i < numRelaysForErrCase; i++ {
		relaysForErrCase = append(relaysForErrCase, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// We don't want to stall tests here given failing relays
	secondTimeoutAfter := 5 * time.Second
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
	t.Logf("Broadcast goroutine #2 started and should error...")

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Error(t, resultStatusMsg.Error,
		"second broadcast should contain error status")
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")
	close(notifyCh2)

	numExpected := numRelaysForErrCase*2/3 + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)

	// -------------------

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	totalExpectedCommits := uint64(1) // only TEST 1 should have committed a block
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays. This
// test executes a broadcast operation before shutting down the backend and
// one after having restarted the backend to ensure that continuation works.
// Finally, it also broadcasts one more transaction using a different ChainID.
func TestScenarioMultiplexBeforeAndAfterBackendRestart(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	servers[0].SetAcceptor(testAcceptorRelay1)
	servers[1].SetAcceptor(testAcceptorRelay2)
	servers[2].SetAcceptor(testAcceptorRelay3)

	// Note: relays includes self
	// Using 2 seconds waitDuration because we shall broadcast BEFORE shutdown.
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		0*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	mx.WithReplicationTimeout(10 * time.Second)(servers[0])

	// STEP 1:
	// We execute two complete broadcast processes.

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID1 := helpers.MakeChainID("test chain")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine #1 started and should succeed...")

	// Blocks the main thread until we consume from notifyCh1.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID1,
		notifyCh1,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error,
		"first broadcast should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"first broadcast to new ChainID should contain transaction hashes")
	close(notifyCh1)

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	totalExpectedCommits := uint64(1)
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	notifyCh2 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh2,
	)
	t.Logf("Broadcast goroutine #2 started and should succeed...")

	// Blocks the main thread until we consume from notifyCh2.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID1,
		notifyCh2,
	)
	assert.NotNil(t, resultStatusMsg)
	require.NoError(t, resultStatusMsg.Error,
		"broadcast to existing ChainID before restart should not contain error status")
	require.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"broadcast to existing ChainID before restart should contain transaction hashes")
	close(notifyCh2)

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	totalExpectedCommits++
	roundExpectedCommits = uint64(1)
	maxCommitWaitTime = time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	// STEP 2:
	//
	// CAUTION:
	// We mimic one of the relay shutting down completely, i.e. its process
	// is not managed, corrupted or stopped. Setting nil on the "old" instance
	// is only necessary during shutdown tests.

	// Stop the receiving backend, then start it again.
	stopErr := servers[0].Stop()
	require.NoError(t, stopErr, "should shutdown relay")

	resetErr := servers[0].Reset(t.Context())
	require.NoError(t, resetErr, "should reset relay")

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to restart backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	startErr := servers[0].Start()
	require.NoError(t, startErr, "should restart relay")

	// STEP 3:
	//
	// The relay has been fully restarted and we can use the created
	// cancelable/expirable context to broadcast *more* transactions.

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	notifyCh3 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh3,
	)
	t.Logf("Broadcast goroutine #3 started and should succeed...")

	// Blocks the main thread until we consume from notifyCh3.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID1,
		notifyCh3,
	)
	assert.NotNil(t, resultStatusMsg)
	require.NoError(t, resultStatusMsg.Error,
		"broadcast to existing ChainID after restart should not contain error status")
	require.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"broadcast to existing ChainID after restart should contain transaction hashes")
	close(notifyCh3)

	// STEP 4:
	//
	// Also try to broadcast using a different ChainID.

	fourthTimeoutAfter := 20 * time.Second // Time for broadcast
	fourthBroadcastCtx, fourthCancelCtxFn := context.WithTimeout(context.TODO(), fourthTimeoutAfter)
	defer fourthCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 1
	testChainID2 := helpers.MakeChainID("test-chain-2")
	notifyCh4 := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		fourthBroadcastCtx,
		servers[0],
		relays,
		testChainID2,
		numTransactions,
		notifyCh4,
	)
	t.Logf("Broadcast goroutine #4 started and should succeed...")

	// Blocks the main thread until we consume from notifyCh4.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		fourthBroadcastCtx,
		testChainID2,
		notifyCh4,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error,
		"broadcast to new ChainID after restart should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions,
		"broadcast to new ChainID after restart should contain transaction hashes")
	close(notifyCh4)

	// -------------------

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	totalExpectedCommits = uint64(4)
	roundExpectedCommits = uint64(2)
	maxCommitWaitTime = time.Duration(30 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)
}

// Test the runtime.Registry#OnIdle which should shutdown sleeping runtimes,
// and broadcast operation before idling and after idling must succeed. This
// test also validates that blocks are committed on all relays.
func TestScenarioMultiplexAfterRuntimeIdling(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	numChains := 0
	numRelays := 3

	// To enable debug logs, change this indexes array to contain the indexes
	// of the relays for which you want to activate full logging.
	idxRelaysWithLogs := []int{} // e.g. []int{0, 1} for relay-1 and relay-2
	servers, shutdownFn := ResetTestScenarioRelaysWithOptions(t, numChains, numRelays, idxRelaysWithLogs, [][]mx.MultiplexBackendOption{})
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	testRuntimeRegistryOpts := []runtime.RegistryOption{
		runtime.RegistryCleanerInterval(10 * time.Second), // run cleaner every 10s
		runtime.RegistryIdleDuration(5 * time.Second),     // idle after 5s inactivity
	}

	// force-overwrite RuntimeRegistry
	useCustomRuntimeRegistry(t, servers[0].GetReactor(), testRuntimeRegistryOpts...)
	useCustomRuntimeRegistry(t, servers[1].GetReactor(), testRuntimeRegistryOpts...)
	useCustomRuntimeRegistry(t, servers[2].GetReactor(), testRuntimeRegistryOpts...)

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	servers[0].SetAcceptor(testAcceptorRelay1)
	servers[1].SetAcceptor(testAcceptorRelay2)
	servers[2].SetAcceptor(testAcceptorRelay3)

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
	testChainID1 := helpers.MakeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID1,
		numTransactions,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine #1 started and should succeed...")

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

	// Test that client callbacks executed correctly, because we shall shutdown
	// and continue the network after relay-1 has been restarted.
	totalExpectedCommits := uint64(1)
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	waitDuration := 20 * time.Second
	t.Logf("Waiting %.0fsec for cleaner routine...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// Test that OnIdle was called (through OnBroadcastComplete)
	testRuntimeRegRelay1 := servers[0].GetRuntimeRegistry()
	testRuntimeRegRelay2 := servers[1].GetRuntimeRegistry()
	testRuntimeRegRelay3 := servers[2].GetRuntimeRegistry()
	require.NotNil(t, testRuntimeRegRelay1)
	require.NotNil(t, testRuntimeRegRelay2)
	require.NotNil(t, testRuntimeRegRelay3)

	expectZeroRemains := uint64(0)
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay1.NumRuntimes(), // no more actives
		"relay-1 should have completed all active runtimes")
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay1.NumSleeping(), // no more to idle
		"relay-1 should have cleaned all sleeping runtimes")
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay2.NumRuntimes(),
		"relay-2 should have completed all active runtimes")
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay2.NumSleeping(),
		"relay-2 should have cleaned all sleeping runtimes")
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay3.NumRuntimes(),
		"relay-3 should have completed all active runtimes")
	require.Equal(t, expectZeroRemains, testRuntimeRegRelay3.NumSleeping(),
		"relay-3 should have cleaned all sleeping runtimes")

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
	t.Logf("Broadcast goroutine #2 started and should succeed...")

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

	// -------------------

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	totalExpectedCommits = uint64(2)
	roundExpectedCommits = uint64(1)
	maxCommitWaitTime = time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)
}

// TODO(midas): TestScenarioClientBroadcastUsingNonValidatorRelay
// TODO(midas): TestScenarioClientBroadcastHappyPath

// ----------------------------------------------------------------------------
// CONCURRENT Broadcast Tests

func TestConcurrentNewChainsAndExistingChains(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	timeoutGlobal := 120 * time.Second // Time for full test round
	testCaseCtx, globalCancelFn := context.WithTimeout(context.TODO(), timeoutGlobal)
	defer globalCancelFn()

	numChains := 0
	numRelays := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	servers[0].SetAcceptor(testAcceptorRelay1)
	servers[1].SetAcceptor(testAcceptorRelay2)
	servers[2].SetAcceptor(testAcceptorRelay3)

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
	testChainID1 := helpers.MakeChainID("test-chain-1")
	notifyCh1 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID1,
		numTransactions1,
		notifyCh1,
	)
	t.Logf("Broadcast goroutine #1 started with %s...", testChainID1)

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

	// Test that client callbacks executed correctly for the networks creation

	totalExpectedCommits := uint64(1)
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	// STEP 2
	// ----------------
	// Creates another ChainID that will be reused in concurrent scenario.

	// Executes first transaction broadcast
	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	numTransactions2 := 1
	testChainID2 := helpers.MakeChainID("test-chain-2")
	notifyCh2 := make(chan client.BroadcastStatus)

	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID2,
		numTransactions2,
		notifyCh2,
	)
	t.Logf("Broadcast goroutine #2 started with %s...", testChainID2)

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

	totalExpectedCommits++
	roundExpectedCommits = uint64(1)
	maxCommitWaitTime = time.Duration(20 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

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
		iTestChainID := helpers.MakeChainID(testChainName)
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
		t.Logf("Broadcast goroutine #%d started with %s...", i+3, iTestChainID)

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

	assert.Equal(t, numConcurrent, cntDone)
	assert.NoError(t, errBroadcast, "concurrent broadcasts should not error")

	// -------------------
	// Also test callbacks
	t.Logf("Now evaluating callbacks execution...")

	totalExpectedCommits += uint64(numConcurrent)
	roundExpectedCommits = uint64(numConcurrent)
	maxCommitWaitTime = time.Duration(120 * time.Second)
	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)
}

// TODO(midas): TestScenarioConcurrentFirstTransactionSameRelay
// TODO(midas): TestScenarioConcurrentFirstTransactionDiffRelays

// ----------------------------------------------------------------------------
// Helpers

// For each backend, we return a list of "other validators" by ChainID.
//
// Calls the [MultiplexBackend#InitValidators] method to initialize PrivValidator.
func ResetTestBackendValidators(
	tb testing.TB,
	servers []*mx.MultiplexBackend,
	testChainIds []string,
) (
	otherValidators []map[string][]string,
) {
	tb.Helper()

	for i, server := range servers {
		_, err := server.InitValidators(testChainIds)
		require.NoError(tb, err, "should initialize validators for relay-"+strconv.Itoa(i+1))
	}

	otherValidators = make([]map[string][]string, len(servers))
	for i := 0; i < len(servers); i++ {
		otherValidators[i] = map[string][]string{}
		serverValidators := []map[string]string{}
		for j, serverX := range servers {
			if j != i {
				serverValidators = append(serverValidators, serverX.GetValidatorPubs())
			}
		}

		for _, chainID := range testChainIds {
			for _, otherValPubKeys := range serverValidators {
				otherValidators[i][chainID] = append(otherValidators[i][chainID], otherValPubKeys[chainID])
			}
		}
	}

	return // otherValidators
}

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
		waitDuration := 2 * time.Second
		tb.Logf("Waiting %.0fsec for shutdown...", waitDuration.Seconds())
		time.Sleep(waitDuration)

		// We will wait until all backends are stopped.
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

		// Wait for all concurrent closing to be completed.
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
		servers[i].Start()
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
		false,                          // mockGenesisAndPrivVal
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelayX.ChainSeeds {
		globalCfgRelayX.ChainSeeds[chainID] = ""
	}

	serverRelayX, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfgRelayX,
		customLogger,
	)
	require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(indexRelay))

	shutdownFn := func(backend *mx.MultiplexBackend) {
		defer os.RemoveAll(rootDirRelayX)

		if backend != nil {
			err := backend.Stop()
			assert.NoError(tb, err, "should shutdown reset server at index: "+strconv.Itoa(indexRelay))
		}
	}

	return serverRelayX, shutdownFn
}

// ----------------------------------------------------------------------------

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
	chainInfo helpers.ExtendedChainID,
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
	userAddress string,
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
			userAddress,
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

	chainInfo := helpers.NewExtendedChainIDFromString(testChainID)
	require.NotNil(tb, chainInfo, "should create correctly formatted ChainID")

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

func useCustomRuntimeRegistry(tb testing.TB, testReactor *mx.Reactor, regOpts ...runtime.RegistryOption) {
	tb.Helper()

	stopErr := testReactor.GetRuntimeRegistry().Stop()
	require.NoError(tb, stopErr, "should stop default runtime registry")

	customRuntimeRegistry := runtime.NewRegistry(testReactor.Context(),
		testReactor.GetLogger().With("module", "idle-manager"),
		regOpts...,
	)

	testReactor.SetRuntimeRegistry(customRuntimeRegistry)

	startErr := customRuntimeRegistry.Start()
	require.NoError(tb, startErr, "should start custom runtime registry")
}

func requireAcceptorCommitCalls(
	tb testing.TB,
	maxWaitTime time.Duration,
	numTotalCommits uint64,
	numRoundCommits uint64,
	fromAcceptors ...*client.MockAcceptorImpl,
) {
	tb.Helper()

	hasExpectedCommits := false
	startWaitTz := time.Now()

	tb.Logf("Waiting for %d blocks commit (max %.0fsec)...", numRoundCommits, maxWaitTime.Seconds())

BLOCK_COMMITS_LOOP:
	for tb.Context().Err() == nil {
		hasAcceptorCommits := true
		for _, testAcceptor := range fromAcceptors {
			numAcceptorCommits := testAcceptor.TxCommitCalls.Load()
			hasExpectedCommits = hasAcceptorCommits && numAcceptorCommits == numTotalCommits
			if !hasExpectedCommits {
				break
			}
		}

		hasReachedTimeout := time.Since(startWaitTz) > maxWaitTime

		switch {
		case hasExpectedCommits == true:
			break BLOCK_COMMITS_LOOP

		case hasReachedTimeout == true:
			break BLOCK_COMMITS_LOOP

		default:
			time.Sleep(2 * time.Second)
			continue BLOCK_COMMITS_LOOP
		}
	}

	if hasExpectedCommits {
		tb.Logf("Intercepted %d blocks commit after %.0fs", numRoundCommits, time.Since(startWaitTz).Seconds())
	} else {
		tb.Fatalf("Failed to commit %d blocks", numRoundCommits)
	}
}
