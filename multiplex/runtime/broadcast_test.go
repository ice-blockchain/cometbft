package runtime_test

import (
	"context"
	"encoding/hex"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/e2e"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeBroadcastPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	testPool := runtime.NewBroadcastManager(t.Context(), resourceMgr, cmtlog.NewNopLogger())

	// Test simple pool start/stop
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start first broadcast pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	stopErr := testPool.Stop()
	require.NoError(t, stopErr, "should stop first broadcast pool")
	assert.Equal(t, true, testPool.IsStopped())

	resetErr := testPool.Reset(context.TODO()) // new context!
	require.NoError(t, resetErr, "should reset first broadcast pool")
	assert.Equal(t, false, testPool.IsRunning())

	restartErr := testPool.Start()
	require.NoError(t, restartErr, "should restart first broadcast pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	actualPartners := testPool.Partners("test-hash-1")
	actualResponses := testPool.Responses("test-hash-1")
	assert.Len(t, actualPartners, 0)
	assert.Len(t, actualResponses, 0)

	restopErr := testPool.Stop()
	require.NoError(t, restopErr, "should stop first broadcast pool")
}

func TestMultiplexRuntimeBroadcastPoolInit(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	testPool := runtime.NewBroadcastManager(t.Context(), resourceMgr, cmtlog.NewNopLogger())
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start broadcast pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())

	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, 100)

	// Test that initializing a broadcast is successful.
	initErr := testPool.Init("test-chain-0", "tx-hash-0", testRelayAddrs)
	assert.NoError(t, initErr, "should not error when initializing broadcast")

	actualRelays := testPool.Relays("tx-hash-0")
	shouldBeEmpty := testPool.Relays("tx-hash-1")
	assert.Len(t, actualRelays, len(testRelayAddrs))
	assert.Len(t, shouldBeEmpty, 0)

	// Test that re-Init() for same txHash appends relays.
	testRelayAddrs2 := e2e.MakeRelayAddresses(t, "127.0.0.1", 40000, 100)
	initErr = testPool.Init("test-chain-0", "tx-hash-0", testRelayAddrs2)
	assert.NoError(t, initErr, "should not error when re-initializing broadcast")

	actualRelays = testPool.Relays("tx-hash-0")
	shouldBeEmpty = testPool.Relays("tx-hash-1")
	assert.Len(t, actualRelays, len(testRelayAddrs)+len(testRelayAddrs2)) // 100+100
	assert.Len(t, shouldBeEmpty, 0)                                       // still empty

	restopErr := testPool.Stop()
	require.NoError(t, restopErr, "should stop broadcast pool")

	// Test that reset empties the relays list.
	resetErr := testPool.Reset(context.TODO()) // new context!
	require.NoError(t, resetErr, "should reset broadcast pool")
	assert.Equal(t, false, testPool.IsRunning())

	shouldBeEmptyNow := testPool.Relays("tx-hash-0")
	assert.Len(t, shouldBeEmptyNow, 0)
}

func TestMultiplexRuntimeBroadcastPoolProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	testPool := runtime.NewBroadcastManager(t.Context(), resourceMgr, cmtlog.NewNopLogger())
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start broadcast pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())
	defer testPool.Stop()

	ackTransactionIncoming := makeAckTransactionBroadcast("tx-hash-0", "test-node-0", "test-chain-0") // messages_test.go

	// Test valid AckTransactionBroadcast message for relay.
	processErr := testPool.Process(cmtp2p.ID("test-node-0"), ackTransactionIncoming)
	assert.NoError(t, processErr, "should accept valid incoming message")

	hexTxHash0 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-0")))
	hexTxHash1 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-1")))
	hexTxHash2 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-2")))
	hexTxHash3 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-3")))
	hexTxHash4 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-4")))

	actualPartners := testPool.Partners(hexTxHash0)
	actualRequests := testPool.Responses(hexTxHash0)
	assert.Len(t, actualPartners, 1)
	assert.Len(t, actualRequests, 1)

	ackTransactionIncoming2 := makeAckTransactionBroadcast("tx-hash-0", "test-node-1", "test-chain-0") // messages_test.go

	// Test valid AckTransactionBroadcast message from relay.
	processErr = testPool.Process(cmtp2p.ID("test-node-1"), ackTransactionIncoming2)
	assert.NoError(t, processErr, "should accept valid incoming message")

	actualPartners = testPool.Partners(hexTxHash0)
	actualResponses := testPool.Responses(hexTxHash0)
	assert.Len(t, actualPartners, 2)  // test-node-0, -1
	assert.Len(t, actualResponses, 2) // test-node-0, -1

	// IMPORTANT: configures the broadcast process for x relays.
	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, 100)
	init1Err := testPool.Init("test-chain-1", "tx-hash-1", testRelayAddrs[:50])
	init2Err := testPool.Init("test-chain-2", "tx-hash-2", testRelayAddrs[50:])
	require.NoError(t, init1Err, "should not error when initializing first broadcast")
	require.NoError(t, init2Err, "should not error when initializing second broadcast")

	// Test concurrent Process calls, must succeed.
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			withChainID := "test-chain-1"
			withTxHash := "tx-hash-1"
			withNodeId := "test-node-" + strconv.Itoa(c)
			if c%2 == 0 {
				withChainID = "test-chain-2"
				withTxHash = "tx-hash-2"
			}

			testAckEnvelope := makeAckTransactionBroadcast(withTxHash, withNodeId, withChainID)
			testPool.Process(cmtp2p.ID(withNodeId), testAckEnvelope)
		}(i + 1)
	}
	waitAll.Wait()

	// Test that we did not modify the state for unrelated txHash.
	newActualPartners := testPool.Partners(hexTxHash0)
	newActualResponses := testPool.Responses(hexTxHash0)
	assert.Len(t, newActualPartners, len(actualPartners))   // did not change
	assert.Len(t, newActualResponses, len(actualResponses)) // did not change

	// Test that we have the correct data set for broadcasts.
	actualResponsesTxHash1 := testPool.Responses(hexTxHash1)
	actualResponsesTxHash2 := testPool.Responses(hexTxHash2)
	assert.Len(t, actualResponsesTxHash1, 50)
	assert.Len(t, actualResponsesTxHash2, 50)

	// Test that receiving same message from same peer doesn't affect acceptance eval.
	testAckEnvelopeOk1 := makeAckTransactionBroadcast("tx-hash-3", "test-node-1001", "test-chain-3") // OK
	testAckEnvelopeErr := makeAckTransactionBroadcast("tx-hash-3", "test-node-1001", "test-chain-3") // NOK
	testAckEnvelopeOk2 := makeAckTransactionBroadcast("tx-hash-4", "test-node-1001", "test-chain-4") // OK
	testAckEnvelopeOk3 := makeAckTransactionBroadcast("tx-hash-4", "test-node-1002", "test-chain-4") // OK

	err1 := testPool.Process("test-node-1001", testAckEnvelopeOk1)
	assert.NoError(t, err1)
	actualResponsesTxHash3 := testPool.Responses(hexTxHash3)
	assert.Len(t, actualResponsesTxHash3, 1)

	err2 := testPool.Process("test-node-1001", testAckEnvelopeErr)
	assert.NoError(t, err2)
	actualResponsesTxHash3 = testPool.Responses(hexTxHash3)
	assert.Len(t, actualResponsesTxHash3, 1) // still 1!

	err3 := testPool.Process("test-node-1001", testAckEnvelopeOk2)
	assert.NoError(t, err3)
	actualResponsesTxHash3 = testPool.Responses(hexTxHash3)
	actualResponsesTxHash4 := testPool.Responses(hexTxHash4)
	assert.Len(t, actualResponsesTxHash3, 1) // did not change.
	assert.Len(t, actualResponsesTxHash4, 1)
	err4 := testPool.Process("test-node-1002", testAckEnvelopeOk3)
	assert.NoError(t, err4)
	actualResponsesTxHash3 = testPool.Responses(hexTxHash3)
	actualResponsesTxHash4 = testPool.Responses(hexTxHash4)
	assert.Len(t, actualResponsesTxHash3, 1) // did not change.
	assert.Len(t, actualResponsesTxHash4, 2) // test-node-1001, -1002
}

func TestMultiplexRuntimeBroadcastPoolWaitAccepted(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	testPool := runtime.NewBroadcastManager(t.Context(), resourceMgr, cmtlog.NewNopLogger(),
		runtime.BroadcastPoolWithAckBroadcastTimeout(1*time.Second), // 1s timeout for ACKs
	)
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start broadcast pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())
	defer testPool.Stop()

	hexTxHash0 := strings.ToUpper(hex.EncodeToString([]byte("tx-hash-0")))

	// IMPORTANT: configures the broadcast process for 7 relays.
	numRelays := 7
	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, numRelays) // 7 relays
	err := testPool.Init("test-chain-1", hexTxHash0, testRelayAddrs[:])
	require.NoError(t, err)

	// Helper that sends ChainReplicationResponse messages concurrently.
	sendConcurrentAcks := func(withTxHash, withChainID string, numResponses int) {
		waitAll := sync.WaitGroup{}
		waitAll.Add(numResponses)
		for i := 0; i < numResponses; i++ {
			go func(c int) {
				defer waitAll.Done()

				withNodeId := "test-node-" + strconv.Itoa(c)
				testAckEnvelope := makeAckTransactionBroadcast(withTxHash, withNodeId, withChainID)
				testPool.Process(cmtp2p.ID(withNodeId), testAckEnvelope)
			}(i + 1)
		}
		waitAll.Wait()
	}

	waitDone := sync.WaitGroup{}
	waitDone.Add(2)

	didAcceptResponse := false

	// Test that WaitAccepted blocks its thread until responses received.
	go func(withTxHash string) {
		defer waitDone.Done()

		actualAccepted := testPool.WaitAccepted(withTxHash)
		require.Equal(t, true, actualAccepted, "should mark broadcast as accepted")

		didAcceptResponse = actualAccepted
	}(hexTxHash0)

	go func(withTxHash, withChainID string) {
		defer waitDone.Done()

		sendConcurrentAcks(withTxHash, withChainID, numRelays)
	}("tx-hash-0", "test-chain-1")

	// Waits for both the above goroutines to finalize.
	waitDone.Wait()

	// Test that the broadcast acceptance got processed successfully.
	assert.Equal(t, true, didAcceptResponse, "should accept broadcast given enough responses")
}

func TestMultiplexRuntimeBroadcastPoolWaitIndexed(t *testing.T) {

}
