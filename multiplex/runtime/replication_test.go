package runtime_test

import (
	"context"
	"strconv"
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

func TestMultiplexRuntimeReplicationPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool := runtime.NewReplicationManager(t.Context(), cmtlog.NewNopLogger())

	// Test simple pool start/stop
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start replication pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	stopErr := testPool.Stop()
	require.NoError(t, stopErr, "should stop replication pool")
	assert.Equal(t, true, testPool.IsStopped())

	resetErr := testPool.Reset(context.TODO()) // new context!
	require.NoError(t, resetErr, "should reset replication pool")
	assert.Equal(t, false, testPool.IsRunning())

	restartErr := testPool.Start()
	require.NoError(t, restartErr, "should restart replication pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	actualPartners := testPool.Partners("test-chain-0")
	actualRequests := testPool.Requests("test-chain-0")
	actualResponses := testPool.Responses("test-chain-0")
	assert.Len(t, actualPartners, 0)
	assert.Len(t, actualRequests, 0)
	assert.Len(t, actualResponses, 0)

	restopErr := testPool.Stop()
	require.NoError(t, restopErr, "should stop replication pool")
}

func TestMultiplexRuntimeReplicationPoolInit(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool := runtime.NewReplicationManager(t.Context(), cmtlog.NewNopLogger())
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start replication pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())

	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, 100)

	// Test that initializing a replication is successful.
	initErr := testPool.Init("test-chain-0", testRelayAddrs)
	assert.NoError(t, initErr, "should not error when initializing replication")

	actualRelays := testPool.Relays("test-chain-0")
	shouldBeEmpty := testPool.Relays("test-chain-1")
	assert.Len(t, actualRelays, len(testRelayAddrs))
	assert.Len(t, shouldBeEmpty, 0)

	// Test that re-Init() for same chainID appends relays.
	testRelayAddrs2 := e2e.MakeRelayAddresses(t, "127.0.0.1", 40000, 100)
	initErr = testPool.Init("test-chain-0", testRelayAddrs2)
	assert.NoError(t, initErr, "should not error when re-initializing replication")

	actualRelays = testPool.Relays("test-chain-0")
	shouldBeEmpty = testPool.Relays("test-chain-1")
	assert.Len(t, actualRelays, len(testRelayAddrs)+len(testRelayAddrs2)) // 100+100
	assert.Len(t, shouldBeEmpty, 0)                                       // still empty

	restopErr := testPool.Stop()
	require.NoError(t, restopErr, "should stop replication pool")

	// Test that reset empties the relays list.
	resetErr := testPool.Reset(context.TODO()) // new context!
	require.NoError(t, resetErr, "should reset replication pool")
	assert.Equal(t, false, testPool.IsRunning())

	shouldBeEmptyNow := testPool.Relays("test-chain-0")
	assert.Len(t, shouldBeEmptyNow, 0)
}

func TestMultiplexRuntimeReplicationPoolProcess(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool := runtime.NewReplicationManager(t.Context(), cmtlog.NewNopLogger())
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start replication pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())
	defer testPool.Stop()

	replRequestOutgoing := makeChainReplicationRequest("test-node-0", "test-chain-0") // messages_test.go
	replRequestOutgoing.Src = nil

	// Test valid ChainReplicationRequest message for relay.
	processErr := testPool.Process(cmtp2p.ID("test-node-0"), replRequestOutgoing)
	assert.NoError(t, processErr, "should accept valid outgoing message")

	actualPartners := testPool.Partners("test-chain-0")
	actualRequests := testPool.Requests("test-chain-0")
	assert.Len(t, actualPartners, 1)
	assert.Len(t, actualRequests, 1)

	replResponseIncoming := makeChainReplicationResponse("test-node-1", "test-chain-0") // messages_test.go

	// Test valid ChainReplicationResponse message from relay.
	processErr = testPool.Process(cmtp2p.ID("test-node-1"), replResponseIncoming)
	assert.NoError(t, processErr, "should accept valid incoming message")

	actualPartners = testPool.Partners("test-chain-0")
	actualResponses := testPool.Responses("test-chain-0")
	assert.Len(t, actualPartners, 2) // test-node-0, -1
	assert.Len(t, actualResponses, 1)

	// IMPORTANT: configures the replication process for x relays.
	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, 100)
	init1Err := testPool.Init("test-chain-1", testRelayAddrs[:50])
	init2Err := testPool.Init("test-chain-2", testRelayAddrs[50:])
	require.NoError(t, init1Err, "should not error when initializing first replication")
	require.NoError(t, init2Err, "should not error when initializing second replication")

	// Test concurrent Process calls, must succeed.
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			withChainID := "test-chain-1"
			withNodeId := "test-node-" + strconv.Itoa(c)
			if c%2 == 0 {
				withChainID = "test-chain-2"
			}

			testRespEnvelope := makeChainReplicationResponse(withNodeId, withChainID)
			testPool.Process(cmtp2p.ID(withNodeId), testRespEnvelope)
		}(i + 1)
	}
	waitAll.Wait()

	// Test that we did not modify the state for unrelated ChainID.
	newActualPartners := testPool.Partners("test-chain-0")
	newActualResponses := testPool.Responses("test-chain-0")
	assert.Len(t, newActualPartners, len(actualPartners))   // did not change
	assert.Len(t, newActualResponses, len(actualResponses)) // did not change

	// Test that we have the correct data set for replications.
	actualResponsesChain1 := testPool.Responses("test-chain-1")
	actualResponsesChain2 := testPool.Responses("test-chain-2")
	assert.Len(t, actualResponsesChain1, 50)
	assert.Len(t, actualResponsesChain2, 50)

	// Test that receiving same message from same peer doesn't affect acceptance eval.
	testRespEnvelopeOk1 := makeChainReplicationResponse("test-node-1001", "test-chain-3") // OK
	testRespEnvelopeErr := makeChainReplicationResponse("test-node-1001", "test-chain-3") // NOK
	testRespEnvelopeOk2 := makeChainReplicationResponse("test-node-1001", "test-chain-4") // OK
	testRespEnvelopeOk3 := makeChainReplicationResponse("test-node-1002", "test-chain-4") // OK

	err1 := testPool.Process("test-node-1001", testRespEnvelopeOk1)
	assert.NoError(t, err1)
	actualResponsesChain3 := testPool.Responses("test-chain-3")
	assert.Len(t, actualResponsesChain3, 1)

	err2 := testPool.Process("test-node-1001", testRespEnvelopeErr)
	assert.NoError(t, err2)
	actualResponsesChain3 = testPool.Responses("test-chain-3")
	assert.Len(t, actualResponsesChain3, 1) // still 1!

	err3 := testPool.Process("test-node-1001", testRespEnvelopeOk2)
	assert.NoError(t, err3)
	actualResponsesChain3 = testPool.Responses("test-chain-3")
	actualResponsesChain4 := testPool.Responses("test-chain-4")
	assert.Len(t, actualResponsesChain3, 1) // did not change.
	assert.Len(t, actualResponsesChain4, 1)
	err4 := testPool.Process("test-node-1001", testRespEnvelopeOk3)
	assert.NoError(t, err4)
	actualResponsesChain3 = testPool.Responses("test-chain-3")
	actualResponsesChain4 = testPool.Responses("test-chain-4")
	assert.Len(t, actualResponsesChain3, 1) // did not change.
	assert.Len(t, actualResponsesChain4, 2) // test-node-1001, -1002
}

func TestMultiplexRuntimeReplicationPoolWaitAccepted(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool := runtime.NewReplicationManager(t.Context(), cmtlog.NewNopLogger(),
		runtime.ReplicationPoolWithAcceptanceTimeout(1*time.Second),
		runtime.ReplicationPoolWithReplicationTimeout(1*time.Second),
	)
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start replication pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())
	defer testPool.Stop()

	// IMPORTANT: configures the replication process for 7 relays.
	numRelays := 7
	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, numRelays) // 7 relays
	err := testPool.Init("test-chain-1", testRelayAddrs[:])
	require.NoError(t, err)

	// Helper that sends ChainReplicationResponse messages concurrently.
	sendConcurrentResponses := func(withChainID string, numResponses int) {
		waitAll := sync.WaitGroup{}
		waitAll.Add(numResponses)
		for i := 0; i < numResponses; i++ {
			go func(c int) {
				defer waitAll.Done()

				withNodeId := "test-node-" + strconv.Itoa(c)
				testRespEnvelope := makeChainReplicationResponse(withNodeId, withChainID)
				testPool.Process(cmtp2p.ID(withNodeId), testRespEnvelope)
			}(i + 1)
		}
		waitAll.Wait()
	}

	waitDone := sync.WaitGroup{}
	waitDone.Add(2)

	didAcceptResponse := false

	// Test that WaitAccepted blocks its thread until responses received.
	go func(withChainID string) {
		defer waitDone.Done()

		actualAccepted := testPool.WaitAccepted(withChainID)
		require.Equal(t, true, actualAccepted, "should mark replication as accepted")

		didAcceptResponse = actualAccepted
	}("test-chain-1")

	go func(withChainID string) {
		defer waitDone.Done()

		sendConcurrentResponses(withChainID, numRelays)
	}("test-chain-1")

	// Waits for both the above goroutines to finalize.
	waitDone.Wait()

	// Test that the replication acceptance got processed successfully.
	assert.Equal(t, true, didAcceptResponse, "should accept replication given enough responses")
}

func TestMultiplexRuntimeReplicationPoolWaitAcceptedTimeouts(t *testing.T) {

}

func TestMultiplexRuntimeReplicationPoolWaitCompleted(t *testing.T) {
	defer goleak.VerifyNone(t)

	testPool := runtime.NewReplicationManager(t.Context(), cmtlog.NewNopLogger(),
		runtime.ReplicationPoolWithAcceptanceTimeout(1*time.Second),
		runtime.ReplicationPoolWithReplicationTimeout(1*time.Second),
	)
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start replication pool")
	require.Equal(t, true, testPool.IsStarted())
	require.Equal(t, true, testPool.IsRunning())
	defer testPool.Stop()

	// IMPORTANT: configures the replication process for 7 relays.
	numRelays := 7
	testRelayAddrs := e2e.MakeRelayAddresses(t, "127.0.0.1", 30000, numRelays) // 7 relays
	err := testPool.Init("test-chain-1", testRelayAddrs[:])
	require.NoError(t, err)

	// Helper that sends ChainReplicationComplete messages concurrently.
	sendConcurrentCompletions := func(withChainID string, numCompletions int) {
		waitAll := sync.WaitGroup{}
		waitAll.Add(numCompletions)
		for i := 0; i < numCompletions; i++ {
			go func(c int) {
				defer waitAll.Done()

				withNodeId := "test-node-" + strconv.Itoa(c)
				testRespEnvelope := makeChainReplicationComplete(withNodeId, withChainID)
				testPool.Process(cmtp2p.ID(withNodeId), testRespEnvelope)
			}(i + 1)
		}
		waitAll.Wait()
	}

	waitDone := sync.WaitGroup{}
	waitDone.Add(2)

	didAcceptResponse := false

	// Test that WaitCompleted blocks its thread until completions received.
	go func(withChainID string) {
		defer waitDone.Done()

		actualCompleted := testPool.WaitCompleted(withChainID)
		require.Equal(t, true, actualCompleted, "should mark replication as completed")

		didAcceptResponse = actualCompleted
	}("test-chain-1")

	go func(withChainID string) {
		defer waitDone.Done()

		sendConcurrentCompletions(withChainID, numRelays)
	}("test-chain-1")

	// Waits for both the above goroutines to finalize.
	waitDone.Wait()

	// Test that the replication completion got processed successfully.
	assert.Equal(t, true, didAcceptResponse, "should complete replication given enough completions")
}

func TestMultiplexRuntimeReplicationPoolWaitCompletedTimeouts(t *testing.T) {

}
