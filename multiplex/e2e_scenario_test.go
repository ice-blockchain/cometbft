package multiplex_test

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/node"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/store"
)

// ----------------------------------------------------------------------------
// MultiplexClient Broadcast Test (Using client.BroadcastTx)

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

// Uses MultiplexClient to broadcast transactions.
func clientBroadcastTx(
	tb testing.TB,
	ctx context.Context,
	server *mx.MultiplexBackend,
	relays []string,
	testChainID string,
	numTransactions int,
	notifyCh chan client.BroadcastStatus,
) {
	tb.Helper()

	chainInfo, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(tb, err, "should create correctly formatted ChainID")

	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		randomData := randomizer.Intn(999999999)
		testTransactions = append(testTransactions, client.Transaction{
			Data:        []byte{byte(i), byte(i + 1), byte(i + 2), byte(randomData)},
			Fingerprint: chainInfo.GetFingerprint(),
		})
	}

	multiplexClient := mx.NewClient(
		mx.WithBackend(server),
	)

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
				resultStatusMsg.Error = fmt.Errorf(
					"Timed out waiting for broadcast status for %s", testChainID)
				return // cancels context
			}
		}
	}(&resultStatusMsg)

	// Waits for a status update or timeout
	wg.Wait()
	return resultStatusMsg
}

// With a list of healthy relays, the transactions will be added locally
// and then shared with other relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message
// before we proceed to accepting the transaction.
func TestScenarioClientBroadcastHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	defer close(notifyCh)

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
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	defer close(notifyCh)

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

	// Test that ChainReplicationRequest were sent
	expectedRequestCnt := len(relays) - 1
	actualRequestsSent := servers[0].GetReplRequestPeers(testChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := len(relays) - 1
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// We further test the healthy relays counter process which implies successful
// calls to GetRemoteRelayInfo, and the exclusion of "self" from relays list
// if necessary.
func TestScenarioClientBroadcastCountsHealthyRelays(t *testing.T) {
	numChains := 0
	numHealthy := 3

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg1 := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg1)
	assert.NotNil(t, resultStatusMsg1.Error)
	assert.Error(t, resultStatusMsg1.Error)
	assert.Contains(t, resultStatusMsg1.Error.Error(), "not enough healthy relays")

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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg2 := waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg2)
	assert.NotNil(t, resultStatusMsg2.Error)
	assert.Error(t, resultStatusMsg2.Error)
	assert.Contains(t, resultStatusMsg2.Error.Error(), "not enough healthy relays")

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
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForErrCase, // does not contain self!
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg3 := waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg3)
	assert.NotNil(t, resultStatusMsg3.Error)
	assert.Error(t, resultStatusMsg3.Error)
	assert.Contains(t, resultStatusMsg3.Error.Error(), "not enough healthy relays")

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
	go clientBroadcastTx(t,
		fourthBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg4 := waitForClientBroadcastStatus(t,
		fourthBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg4)
	assert.NoError(t, resultStatusMsg4.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg4.TxHashes, numTransactions)

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
	go clientBroadcastTx(t,
		fifthBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg5 := waitForClientBroadcastStatus(t,
		fifthBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg5)
	assert.NoError(t, resultStatusMsg5.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg5.TxHashes, numTransactions)

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
	go clientBroadcastTx(t,
		sixthBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg6 := waitForClientBroadcastStatus(t,
		sixthBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg6)
	assert.NotNil(t, resultStatusMsg6.Error)
	assert.Error(t, resultStatusMsg6.Error)
	assert.Contains(t, resultStatusMsg6.Error.Error(), "not enough healthy relays")

	numExpected = (numRelaysForErrCase / 2) + 1
	numHealthy = numHealthy + 1 // "self" is healthy also if not in relays.
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg6.Error.Error(), expectedMessage)
}

// We further test the healthy relays counter process when relying on relay
// address that DO NOT contain a relay ID. Namely, calls to GetRemoteRelayInfo
// should be successful and fill the healthyRemoteRelays slice correctly.
func TestScenarioClientBroadcastCountsRemoteRelays(t *testing.T) {
	numChains := 0
	numHealthy := 2

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForTestCase,
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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

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
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")

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
	go clientBroadcastTx(t,
		fourthBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		fourthBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")

	numExpected = (numRelaysForErrCase / 2) + 1
	numHealthy = numHealthy + 1 // "self" is healthy also if not in relays.
	expectedMessage = fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)
}

// With a list of empty relays, a first block of the network will be created,
// which includes the broadcast transactions data (using client.BroadcastTx),
// and the state machine and blocks store are updated with transactions data.
func TestScenarioClientBroadcastEmptyRelaysProduceBlockWithTx(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	defer close(notifyCh)

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
	assert.Equal(t, stateMachine.LastBlockHeight, int64(1))

	blockStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyBlockStore)
	assert.NotNil(t, blockStoreProvider, "should not error getting block store provider")
	blockStore := blockStoreProvider(testChainID).(*store.BlockStore)
	assert.NotNil(t, blockStore, "block store per chain must not be nil")

	actualBlock, actualMeta := blockStore.LoadBlock(stateMachine.LastBlockHeight)
	assert.NotNil(t, actualBlock, "should return correct block")
	assert.NotNil(t, actualMeta, "should return correct block meta")
	assert.NotEmpty(t, actualBlock.Data, "should return non-empty block data")
	assert.NotEmpty(t, actualBlock.Data.Txs, "should return non-empty block transactions")
	assert.Len(t, actualBlock.Data.Txs, numTransactions)
}

// With a list of healthy relays, i.e. just enough, the transactions will be added
// locally and then shared with healthy relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message before we
// proceed to accepting the transaction.
func TestScenarioClientBroadcastEnoughHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7
	numHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

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

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := numHealthy - 1 // -1 for self
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}

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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

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
	numChains := 0
	numRelays := 7
	numHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

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

	// Test that ChainReplicationRequest were sent
	expectedRequestCnt := numHealthy - 1 // -1 for self
	actualRequestsSent := servers[0].GetReplRequestPeers(testChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := numHealthy - 1 // -1 for self
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}

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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	// Test that ChainReplicationRequest were NOT sent! (due to TEST 1)
	expectedRequestCnt = 0
	actualRequestsSent = servers[0].GetReplRequestPeers(testChainID)
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
	numChains := 0
	numHealthy := 2

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relaysForTestCase,
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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForErrCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NotNil(t, resultStatusMsg.Error)
	assert.Error(t, resultStatusMsg.Error)
	assert.Contains(t, resultStatusMsg.Error.Error(), "not enough healthy relays")

	numExpected := (numRelaysForErrCase / 2) + 1
	expectedMessage := fmt.Sprintf("expected %d, got %d", numExpected, numHealthy)
	assert.Contains(t, resultStatusMsg.Error.Error(), expectedMessage)
}

// With a list of healthy relays, i.e. just enough, the transactions will be added
// locally and then shared with healthy relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message before we
// proceed to accepting the transaction.
func TestScenarioClientBroadcastWithAndWithoutSelfRelayAddress(t *testing.T) {
	numChains := 0
	numRelays := 7
	minHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	testChainID := makeChainID("test-chain-1")
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		firstBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

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
	testChainID = makeChainID("test-chain-2")
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")

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
	testChainID = makeChainID("test-chain-2")
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
}

// With a list of enough healthy relays, they should proceed to
// accepting the transaction even with some other relays failing.
func TestScenarioClientBroadcastAcceptableRelaysFailure(t *testing.T) {
	numChains := 0
	numRelays := 7
	numHealthy := (numRelays / 2) + 1 // Keep enough healthy relays

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numHealthy)
	defer shutdownFn()

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
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		firstBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		firstBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	t.Logf("Broadcast completed with %d failing relays for test-chain-1...", numFailing)

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
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		servers[0],
		relaysForTestCase,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
	require.NoError(t, resultStatusMsg.Error, "should not contain error status")
	t.Logf("Broadcast completed with %d failing relays for test-chain-2...", numFailing)

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

		thirdTimeoutAfter := 20 * time.Second // Time for broadcast
		thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
		defer thirdCancelCtxFn()

		// Separate goroutine for client broadcast process
		numTransactions = 1
		testChainID = makeChainID(testChain100)
		go clientBroadcastTx(t,
			thirdBroadcastCtx,
			servers[0],
			relaysForTestCase,
			testChainID,
			numTransactions,
			notifyCh,
		)

		// Blocks the main thread until we consume from notifyCh.
		resultStatusMsg100 := waitForClientBroadcastStatus(t,
			thirdBroadcastCtx,
			testChainID,
			notifyCh,
		)

		assert.NotNil(t, resultStatusMsg100)
		assert.Len(t, resultStatusMsg100.TxHashes, numTransactions)
		require.NoError(t, resultStatusMsg100.Error,
			"should not contain error status with numFailing: "+strconv.Itoa(numFailing))
		t.Logf("Broadcast completed with %d failing relays for %s...", numFailing, testChain100)

		fourthTimeoutAfter := 20 * time.Second // Time for broadcast
		fourthBroadcastCtx, fourthCancelCtxFn := context.WithTimeout(context.TODO(), fourthTimeoutAfter)
		defer fourthCancelCtxFn()

		// Separate goroutine for client broadcast process
		numTransactions = 1
		testChainID = makeChainID(testChain200)
		go clientBroadcastTx(t,
			fourthBroadcastCtx,
			servers[0],
			relaysForTestCase,
			testChainID,
			numTransactions,
			notifyCh,
		)

		// Blocks the main thread until we consume from notifyCh.
		resultStatusMsg200 := waitForClientBroadcastStatus(t,
			fourthBroadcastCtx,
			testChainID,
			notifyCh,
		)

		assert.NotNil(t, resultStatusMsg200)
		assert.Len(t, resultStatusMsg200.TxHashes, numTransactions)
		require.NoError(t, resultStatusMsg200.Error,
			"should not contain error status with numFailing: "+strconv.Itoa(numFailing))
		t.Logf("Broadcast completed with %d failing relays for %s...", numFailing, testChain200)
	}
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays.
func TestScenarioClientBroadcastAfterBackendRestart(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	defer newShutdownFn()

	resetRelay.MustStart()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

	go clientBroadcastTx(t,
		broadcastCtx,
		resetRelay,
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
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays. This
// test executes a broadcast operation before shutting down the backend and
// one after having restarted the backend to ensure that continuation works.
// Finally, it also broadcasts one more transaction using a different ChainID.
func TestScenarioClientBroadcastBeforeAndAfterBackendRestart(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithoutLogs(t, numChains, numRelays)
	defer shutdownFn()

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
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

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

	waitDuration := 5 * time.Second
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
	defer newShutdownFn()

	resetRelay.MustStart()

	// STEP 3:
	//
	// The relay has been fully restarted and we can use the created
	// cancelable/expirable context to broadcast *more* transactions.

	secondTimeoutAfter := 20 * time.Second // Time for broadcast
	secondBroadcastCtx, secondCancelCtxFn := context.WithTimeout(context.TODO(), secondTimeoutAfter)
	defer secondCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	go clientBroadcastTx(t,
		secondBroadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		secondBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	// STEP 4:
	//
	// Also try to broadcast using a different ChainID.

	thirdTimeoutAfter := 20 * time.Second // Time for broadcast
	thirdBroadcastCtx, thirdCancelCtxFn := context.WithTimeout(context.TODO(), thirdTimeoutAfter)
	defer thirdCancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions = 2
	testChainID = makeChainID("test-chain-2")
	go clientBroadcastTx(t,
		thirdBroadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		thirdBroadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

func TestScenarioClientBroadcastConcurrentNewChains(t *testing.T) {

}

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
	defer shutdownFn()

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

func ResetTestScenarioRelaysWithLogs(
	tb testing.TB,
	numChains int,
	numRelays int,
) ([]*mx.MultiplexBackend, func()) {
	tb.Helper()
	return ResetTestScenarioRelays(tb, numChains, numRelays, cmtlog.TestingLogger())
}

func ResetTestScenarioRelaysWithoutLogs(
	tb testing.TB,
	numChains int,
	numRelays int,
) ([]*mx.MultiplexBackend, func()) {
	tb.Helper()
	return ResetTestScenarioRelays(tb, numChains, numRelays, cmtlog.NewNopLogger())
}

// Initializes numChains on a number of relays. This helper returns a list of
// configured multiplex backend instances and a shutdown functor.
func ResetTestScenarioRelays(
	tb testing.TB,
	numChains int,
	numRelays int,
	withLogger cmtlog.Logger,
) ([]*mx.MultiplexBackend, func()) {
	tb.Helper()

	// For debug, change the loggers to cmtlog.TestingLogger()
	customLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		customLoggers[i] = withLogger.With("process", "relay-"+strconv.Itoa(i+1))
	}

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		tb,
		numChains,
		numRelays,
		customLoggers...,
	)
	require.NotEmpty(tb, servers)
	require.Len(tb, rootDirs, numRelays)
	require.Len(tb, servers, numRelays)

	shutdownFn := func() {
		for i := 0; i < len(servers); i++ {
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(tb, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
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

func ResetTestSingleCompatibleRelay(
	tb testing.TB,
	rootDir string,
	otherRelay *mx.MultiplexBackend,
	indexRelay int,
	customLogger cmtlog.Logger,
) (*mx.MultiplexBackend, func()) {
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

	shutdownFn := func() {
		defer os.RemoveAll(rootDirRelayX)

		if serverRelayX != nil {
			err := serverRelayX.Close()
			assert.NoError(tb, err, "should shutdown reset server at index: "+strconv.Itoa(indexRelay))
		}
	}

	return serverRelayX, shutdownFn
}
