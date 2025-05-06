package server_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexServerReplayPoolNewReplayPool(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test default instance
	testPool1 := server.NewReplayPool(cmtlog.NewNopLogger())
	assert.NotNil(t, testPool1)
	assert.NotNil(t, testPool1.Buckets)
	assert.NotNil(t, testPool1.UserTxs)
	assert.NotNil(t, testPool1.Size())
	assert.Equal(t, uint64(0), testPool1.Size())

	// Test instance with options
	testPool2 := server.NewReplayPool(cmtlog.NewNopLogger(),
		server.ReplayPoolThreshold(10),
		server.ReplayPoolMaxConcurrent(1), // only for testing
		server.ReplayPoolAcceptor(client.NewMockAcceptorImpl()),
		server.ReplayPoolFlushInterval(30*time.Second),
	)
	assert.NotNil(t, testPool2)
	assert.Equal(t, 10, testPool2.Threshold())
	assert.Equal(t, uint64(1), testPool2.MaxConcurrent())
	assert.Equal(t, 30*time.Second, testPool2.FlushInterval())
	assert.NotNil(t, testPool2.Acceptor())
}

func TestMultiplexServerReplayPoolAddGet(t *testing.T) {
	defer goleak.VerifyNone(t)

	pool := server.NewReplayPool(cmtlog.NewNopLogger())

	numBuckets := 100
	numTxPerBucket := 10
	totalNumTxs := uint64(numBuckets * numTxPerBucket)

	testBuckets := []string{}
	testUserTxs := map[string][]client.Transaction{}
	for i := 1; i <= numBuckets; i++ {
		testBucket := makeAddress().String()
		testBuckets = append(testBuckets, testBucket)
		testUserTxs[testBucket] = makeClientTransactions(t, "test-fingerprint", numTxPerBucket)
	}

	for _, testBucket := range testBuckets {
		err := pool.Add(testBucket, testUserTxs[testBucket]...)
		assert.NoError(t, err, "should not error adding transaction batch")
	}

	expectedNumBuckets := numBuckets
	actualBuckets := pool.GetBuckets()
	actualUserTxs := pool.GetTransactions()
	actualPoolLen := pool.Size()

	assert.Len(t, actualBuckets, expectedNumBuckets)
	assert.Len(t, actualUserTxs, expectedNumBuckets)
	assert.Equal(t, totalNumTxs, actualPoolLen,
		"Size() should return total number of transactions across buckets")

	for _, testBucket := range testBuckets {
		expectedNumTxs := numTxPerBucket

		actualTxsByBucket := pool.Get(testBucket)
		assert.Len(t, actualTxsByBucket, expectedNumTxs)
	}

	testUnknownBucket := makeAddress().String()
	actualTxs := pool.Get(testUnknownBucket)
	assert.Empty(t, actualTxs)
}

func TestMultiplexServerReplayPoolFlush(t *testing.T) {
	defer goleak.VerifyNone(t)

	pool := server.NewReplayPool(cmtlog.NewNopLogger())

	numBuckets := 10
	numTxPerBucket := 100
	totalNumTxs := uint64(numBuckets * numTxPerBucket)

	testBuckets := []string{}
	for i := 1; i <= numBuckets; i++ {
		testBucket := makeAddress().String()
		testBuckets = append(testBuckets, testBucket)

		testBucketTxes := makeClientTransactions(t, "test-fingerprint", numTxPerBucket)
		err := pool.Add(testBucket, testBucketTxes...)
		require.NoError(t, err)
	}

	actualPoolLen := pool.Size()
	require.Equal(t, totalNumTxs, actualPoolLen,
		"Size() should return total number of transactions across buckets")

	for _, testBucket := range testBuckets {
		err := pool.Flush(testBucket)
		assert.NoError(t, err, "should not error flushing bucket")
	}

	flushedPoolLen := pool.Size()
	assert.Equal(t, uint64(0), flushedPoolLen,
		"Size() should return empty after flush")

	testErrBucket := makeAddress().String()
	err := pool.Flush(testErrBucket)
	assert.Error(t, err, "Flush() should error given unknown bucket")
	assert.Equal(t, server.ErrBucketNotFound, err)

	// Test consecutive calls to flush with same buckets
	for _, testBucket := range testBuckets {
		err := pool.Flush(testBucket)
		assert.Error(t, err, "Flush() should error given flushed bucket")
		assert.Equal(t, server.ErrBucketNotFound, err)
	}

	// And test flushing directly after adding
	for i := 1; i <= numBuckets; i++ {
		testBucket := makeAddress().String()
		testBucketTxes := makeClientTransactions(t, "test-fingerprint", numTxPerBucket)
		addErr := pool.Add(testBucket, testBucketTxes...)
		assert.NoError(t, addErr)

		flushErr := pool.Flush(testBucket)
		assert.NoError(t, flushErr)
	}

	flushedPoolLen = pool.Size()
	assert.Equal(t, uint64(0), flushedPoolLen,
		"Size() should return empty after flush")
}

func TestMultiplexServerReplayPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test simple pool start/stop
	pool := server.NewReplayPool(cmtlog.NewNopLogger())

	startErr := pool.Start()
	assert.NoError(t, startErr)

	stopErr := pool.Stop()
	assert.NoError(t, stopErr)

	// Test pool start/stop with low threshold and mock acceptor
	testTxAcceptor := client.NewMockAcceptorImpl()
	pool2 := server.NewReplayPool(cmtlog.NewNopLogger(),
		server.ReplayPoolThreshold(2), // process after 2 txes
		server.ReplayPoolAcceptor(testTxAcceptor),
	)

	numBuckets := 10
	numTxPerBucket := 100

	testBuckets := []string{}
	for i := 1; i <= numBuckets; i++ {
		testBucket := makeAddress().String()
		testBuckets = append(testBuckets, testBucket)

		testBucketTxes := makeClientTransactions(t, "test-fingerprint", numTxPerBucket)
		err := pool2.Add(testBucket, testBucketTxes...)
		assert.NoError(t, err)
	}

	startErr = pool2.Start()
	assert.NoError(t, startErr)

	// CAUTION: We wait until pool.size == 0 using the main thread here!
	stopErr = pool2.StopAfterProcessing()
	assert.NoError(t, stopErr)

	assert.Equal(t, uint64(numBuckets), testTxAcceptor.TxReplayCalls.Load()) // replayed every bucket

	// Test pool start/stop using only auto-process (by interval)
	testTxAcceptor2 := client.NewMockAcceptorImpl()
	pool3 := server.NewReplayPool(cmtlog.NewNopLogger(),
		server.ReplayPoolThreshold(-1), // Process only through auto-process
		server.ReplayPoolAcceptor(testTxAcceptor2),
		server.ReplayPoolFlushInterval(2*time.Second), // Process after 2sec
	)

	for _, testBucket := range testBuckets {
		testBucketTxes := makeClientTransactions(t, "test-fingerprint", numTxPerBucket)
		err := pool3.Add(testBucket, testBucketTxes...)
		assert.NoError(t, err)
	}

	startErr = pool3.Start()
	assert.NoError(t, startErr)

	// CAUTION: We wait until pool.size == 0 using the main thread here!
	stopErr = pool3.StopAfterProcessing()
	assert.NoError(t, stopErr)

	assert.Equal(t, uint64(numBuckets), testTxAcceptor2.TxReplayCalls.Load()) // replayed every bucket
}

func TestMultiplexServerReplayPoolReplayBroadcastLoop(t *testing.T) {
	defer goleak.VerifyNone(t)
}

func TestMultiplexServerReplayPoolReplayBucket(t *testing.T) {
	defer goleak.VerifyNone(t)
}

func TestMultiplexServerReplayPoolProcessBucketsRoutine(t *testing.T) {
	defer goleak.VerifyNone(t)
}

func TestMultiplexServerReplayPoolFlushBucketsRoutine(t *testing.T) {
	defer goleak.VerifyNone(t)
}

func TestMultiplexServerReplayPoolConcurrentCalls(t *testing.T) {
	defer goleak.VerifyNone(t)
}
