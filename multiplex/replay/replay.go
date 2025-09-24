package replay

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

const (
	// DefaultReplayPoolThreshold contains a number of transactions to store
	// in buckets (globally), before they get processed by the pool.
	DefaultReplayPoolThreshold = 50

	// DefaultReplayFlushInterval contains the period of time after which the
	// pool should automatically process remaining transaction buckets.
	DefaultReplayFlushInterval = 30 * time.Second

	// DefaultReplayPauseInterval contains a period of time used for reducing
	// stress in processing routines after completion.
	DefaultReplayPauseInterval = 200 * time.Millisecond

	// DefaultReplayRetryInterval contains a period of time used for reducing
	// stress on the client implementation when acceptor calls are failing.
	DefaultReplayRetryInterval = 500 * time.Millisecond

	// DefaultReplayMaxConcurrent contains the default maximum number of
	// concurrent threads that may run to process buckets replay.
	DefaultReplayMaxConcurrent = 10
)

// ErrBucketNotFound is returned if user address is not found in buckets.
var ErrBucketNotFound = errors.New("transaction bucket not found in replay pool")

// ReplayPool defines a pool for concurrent processing of transaction buckets,
// which must be replayed due to a blocksync process. It processes transactions
// when a threshold is reached or after flushInterval. This structure allows
// for batching requests to [Acceptor#ReplayBroadcastTxBatch].
//
// Internally, we map transaction batches by user addresses to fill buckets,
// i.e. processing for one user address happens atomically for all batches.
//
// TODO(midas): TBI on testing post-shutdown replay of transactions, due to using
// this replay pool from inside FinalizeBlock(), we use cometbft restore strategy.
type ReplayPool struct {
	service.BaseService
	mtx *sync.Mutex

	// The maximum number of concurrent processing threads for buckets.
	maxc uint64 // atomic
	// The number of buckets being processed (concurrently).
	numc uint64 // atomic
	// The number of buckets currently awaiting capacity.
	numw uint64 // atomic
	// The size of the replay pool, contains a number of transactions.
	size uint64 // atomic

	// Transaction buckets are stored by user address.
	UserTxs map[string][]client.Transaction
	Buckets []string

	// Options
	threshold     int
	flushInterval time.Duration
	pauseInterval time.Duration
	retryInterval time.Duration
	txAcceptor    client.Acceptor
	logger        cmtlog.Logger

	// Contains one buffered channel per transaction bucket (user address).
	processingCh map[string]chan bool

	// Contains one unbuffered channel per transaction bucket,
	// i.e. must be consumed, see [WaitForBroadcastLoop].
	didProcessCh map[string]chan bool

	// Unbuffered channel written on when goroutines are done processing.
	mayProcessCh chan bool

	// Unbuffered channel that may be written on to shutdown processing,
	// this channel is consumed alongside the [Quit] channel.
	goShutdownCh chan bool
}

type ReplayOption func(*ReplayPool)

// NewReplayPool creates an empty [ReplayPool].
func NewReplayPool(ctx context.Context, logger cmtlog.Logger, options ...ReplayOption) *ReplayPool {
	pool := &ReplayPool{
		mtx: new(sync.Mutex),

		// Options
		threshold:     DefaultReplayPoolThreshold,
		flushInterval: DefaultReplayFlushInterval,
		pauseInterval: DefaultReplayPauseInterval,
		retryInterval: DefaultReplayRetryInterval,
		logger:        logger,

		// Storage
		Buckets: []string{},
		UserTxs: map[string][]client.Transaction{},

		// Channels
		didProcessCh: map[string]chan bool{},
		processingCh: map[string]chan bool{},
		mayProcessCh: make(chan bool), // unbuffered
		goShutdownCh: make(chan bool), // unbuffered
	}

	atomic.StoreUint64(&pool.maxc, uint64(DefaultReplayMaxConcurrent))
	atomic.StoreUint64(&pool.numc, uint64(0))
	atomic.StoreUint64(&pool.numw, uint64(0))
	atomic.StoreUint64(&pool.size, uint64(0))

	// Use option helpers
	pool.SetOptions(options...)

	pool.BaseService = *service.NewBaseService(ctx, logger, "ReplayPool", pool)

	return pool
}

// ReplayPoolThreshold sets a custom buckets size threshold.
func ReplayPoolThreshold(t int) ReplayOption {
	return func(p *ReplayPool) {
		p.threshold = t
	}
}

// ReplayPoolFlushInterval sets a custom flush interval. This interval is used
// to configure the timer-processing goroutine.
func ReplayPoolFlushInterval(fi time.Duration) ReplayOption {
	return func(p *ReplayPool) {
		p.flushInterval = fi
	}
}

// ReplayPoolPauseInterval sets a custom pause interval. This interval is used
// to introduce some waiting time after processing.
func ReplayPoolPauseInterval(pi time.Duration) ReplayOption {
	return func(p *ReplayPool) {
		p.pauseInterval = pi
	}
}

// ReplayPoolRetryInterval sets a custom retry interval. This interval is used
// wait before re-trying calls to the client [Acceptor#ReplayBroadcastTxBatch].
func ReplayPoolRetryInterval(ri time.Duration) ReplayOption {
	return func(p *ReplayPool) {
		p.pauseInterval = ri
	}
}

// ReplayPoolAcceptor injects a custom acceptor implementation.
func ReplayPoolAcceptor(acceptor client.Acceptor) ReplayOption {
	return func(p *ReplayPool) {
		p.txAcceptor = acceptor
	}
}

// ReplayPoolMaxConcurrent sets a custom buckets size threshold.
func ReplayPoolMaxConcurrent(m uint64) ReplayOption {
	return func(p *ReplayPool) {
		atomic.StoreUint64(&p.maxc, m)
	}
}

// ReplayPoolLogger injects a custom logger instance.
func ReplayPoolLogger(logger cmtlog.Logger) ReplayOption {
	return func(p *ReplayPool) {
		p.logger = logger
	}
}

// ----------------------------------------------------------------------------
// ReplayPool implements [service.Service]

// OnStart implements [service.Service] by spawning replay routines.
//
// Setting a negative threshold disables threshold-processing.
func (pool *ReplayPool) OnStart(ctx context.Context) error {
	if pool.txAcceptor == nil {
		// Do nothing.
		return nil
	}

	pool.logger.Debug("Starting replay pool routines",
		"size", pool.Size(),
		"maxc", pool.MaxConcurrent(),
		"threshold", pool.Threshold(),
		"timer", pool.FlushInterval(),
	)

	if pool.Threshold() >= 0 {
		// Starts the buckets processing routine (by threshold)
		go pool.thresholdProcessorRoutine()
	}

	// Starts a flushInterval processing routine
	go pool.timerProcessorRoutine()

	return nil
}

// OnStop implements [service.Service] by closing all open channels and
// freeing memory resources allocated per transaction bucket.
func (pool *ReplayPool) OnStop() {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// First, make sure all goroutines are stopped.
	close(pool.goShutdownCh)
	// Close pool capacity waiting channels.
	close(pool.mayProcessCh)

	// Then free allocated memory for channels.
	for userAddress, _ := range pool.processingCh {
		close(pool.processingCh[userAddress])
		delete(pool.processingCh, userAddress)
	}

	for userAddress, _ := range pool.didProcessCh {
		close(pool.didProcessCh[userAddress])
		delete(pool.didProcessCh, userAddress)
	}
}

// StopAfterProcessing stops the service when the pool's size reaches 0.
//
// TODO(midas): Rather than waiting for an interval, we should be waiting
// for updates on processingCh/mayProcessCh and/or introduce a channel
// for messages around flushing the pool or capacity updates.
func (pool *ReplayPool) StopAfterProcessing() error {
	// Loops until the pool has processed all buckets.
	for pool.Context().Err() == nil {
		// NOTE(midas): non-blocking select on shutdown channel makes
		// sure every time before verifying size, we know to shutdown.
		select {
		case <-pool.Quit():
			return nil

		case <-pool.goShutdownCh:
			return nil
		default: // Proceed to size check
		}

		var (
			currentSize = atomic.LoadUint64(&pool.size)
		)

		switch {
		case currentSize == 0:
			return pool.BaseService.Stop()
		default:
		}

		// Give the pool some time for/after processing, or shutdown.
		if ok := pool.waitForInterval(100 * time.Millisecond); !ok {
			return nil
		}
	}

	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers to configure a ReplayPool instance.
func (pool *ReplayPool) SetOptions(options ...ReplayOption) {
	for _, option := range options {
		option(pool)
	}
}

// Logger returns the logger instance.
func (pool *ReplayPool) Logger() cmtlog.Logger {
	return pool.logger
}

// Threshold returns the threshold of the replay pool.
func (pool *ReplayPool) Threshold() int {
	return pool.threshold
}

// Size returns the size of the replay pool, in total number of transactions.
func (pool *ReplayPool) Size() uint64 {
	return atomic.LoadUint64(&pool.size)
}

// MaxConcurrent returns the maximum number of concurrent threads
// that may be processing batch replays at the same time.
func (pool *ReplayPool) MaxConcurrent() uint64 {
	return atomic.LoadUint64(&pool.maxc)
}

// NumConcurrent returns the number of concurrent threads
// currently processing batch replays.
func (pool *ReplayPool) NumConcurrent() uint64 {
	return atomic.LoadUint64(&pool.numc)
}

// NumWaiting returns the number of concurrent threads
// currently waiting to process batch replays.
func (pool *ReplayPool) NumWaiting() uint64 {
	return atomic.LoadUint64(&pool.numw)
}

// FlushInterval returns the interval for automatically flushing buckets.
func (pool *ReplayPool) FlushInterval() time.Duration {
	return pool.flushInterval
}

// Acceptor returns the transaction acceptor implementation.
func (pool *ReplayPool) Acceptor() client.Acceptor {
	return pool.txAcceptor
}

// NumBuckets returns the number of available buckets.
func (pool *ReplayPool) NumBuckets() int {
	buckets := pool.GetBuckets()
	return len(buckets)
}

// GetBuckets returns the list of buckets (user addresses).
func (pool *ReplayPool) GetBuckets() []string {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	return pool.Buckets
}

// GetTransactions returns a map where keys are user addresses and values
// are slices of transactions.
func (pool *ReplayPool) GetTransactions() map[string][]client.Transaction {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	return pool.UserTxs
}

// ----------------------------------------------------------------------------

// Add adds a transaction batch to the replay pool. Transaction batches are
// mapped to a user address to fill a transaction bucket.
func (pool *ReplayPool) Add(userAddress string, batch ...client.Transaction) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if _, ok := pool.UserTxs[userAddress]; !ok {
		pool.UserTxs[userAddress] = make([]client.Transaction, 0, len(batch))

		// append is safe even with flush executed concurrently
		// because we always remove the last item.
		pool.Buckets = append(pool.Buckets, userAddress)
	}

	// Add to transaction bucket
	pool.UserTxs[userAddress] = append(pool.UserTxs[userAddress], batch...)
	atomic.AddUint64(&pool.size, uint64(len(batch)))

	// Allocate processing channels
	if _, ok := pool.processingCh[userAddress]; !ok {
		pool.processingCh[userAddress] = make(chan bool, 1) // buffered
	}

	if _, ok := pool.didProcessCh[userAddress]; !ok {
		pool.didProcessCh[userAddress] = make(chan bool) // unbuffered
	}

	pool.logger.Debug("Added transaction batch to bucket",
		"bucket", userAddress,
		"txes", len(batch),
		"size", atomic.LoadUint64(&pool.size),
	)

	return nil
}

// Get returns a transaction bucket by its' userAddress. Note that a transaction
// bucket is filled with one or many broadcast transaction batches.
func (pool *ReplayPool) Get(userAddress string) []client.Transaction {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if bucket, ok := pool.UserTxs[userAddress]; ok {
		return bucket
	}

	return []client.Transaction{}
}

// Flush empties a transaction bucket by userAddress. This method is called
// after processing a transaction bucket completely.
func (pool *ReplayPool) Flush(userAddress string) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	numTransactions := len(pool.UserTxs[userAddress])
	if err := pool.removeBucket(userAddress); err != nil {
		pool.logger.Error(
			"Error removing transaction bucket",
			"bucket", userAddress,
			"err", err.Error(),
		)
		return err
	}

	pool.logger.Debug("Removed bucket from replay pool",
		"bucket", userAddress,
		"txes", numTransactions,
		"size", atomic.LoadUint64(&pool.size),
	)

	return nil
}

// ----------------------------------------------------------------------------
// ReplayPool replay API implementation
//
// The mutex is locked temporarily by the methods listed below.

// ReplayBucket replays a transaction bucket and waits for completion.
// When the goroutines pool reaches its maximum capacity, we will be
// waiting for a message on mayProcessCh, then re-dispatch the call.
// See more about re-dispatching in [ThrottleBroadcastLoop].
func (pool *ReplayPool) ReplayBucket(userAddress string) (waiting bool) {
	maxConcurrent := atomic.LoadUint64(&pool.maxc)
	numConcurrent := atomic.LoadUint64(&pool.numc)

	// If we have enough capacity for creating a new goroutine, we issue
	// the ReplayBroadcastLoop call as soon as possible and flush the bucket
	// when the replay loop has completed.
	if numConcurrent < maxConcurrent {
		// Calls ReplayBroadcastTxBatch until it succeeds
		go pool.ReplayBroadcastLoop(userAddress)

		// Waits for ReplayBroadcastLoop with this bucket
		go pool.WaitForBroadcastLoop(userAddress)

		return false
	}

	// Tracks processes waiting concurrently.
	atomic.AddUint64(&pool.numw, uint64(1))

	// If we reached the number of maximum concurrent threads, we must wait
	// for some capacity to free up, then re-dispatch the ReplayBucket call.
	go pool.ThrottleBroadcastLoop(userAddress)

	return true // waiting
}

// ReplayBroadcastLoop calls [Acceptor#ReplayBroadcastTxBatch] until it succeeds.
// The processingCh channel is written on when processing begins. The mayProcessCh
// and didProcessCh channels are written on when processing has ended successfully
// (completed).
//
// When the callback produces an error, we shall retry executing the callback
// until it finally succeeds. See also: [RetryInterval].
func (pool *ReplayPool) ReplayBroadcastLoop(userAddress string) {
	pool.mtx.Lock()
	didProcessCh := pool.didProcessCh[userAddress]
	pool.mtx.Unlock()

	bucket := pool.Get(userAddress)
	if len(bucket) == 0 {
		didProcessCh <- true
		return
	}

	// Keeps track of open/running goroutines
	atomic.AddUint64(&pool.numc, uint64(1))

	pool.mtx.Lock()
	processingCh := pool.processingCh[userAddress]
	pool.mtx.Unlock()
	processingCh <- true

	defer func() {
		atomic.AddUint64(&pool.numc, ^uint64(0)) // -1
		if atomic.LoadUint64(&pool.numw) > uint64(0) {
			pool.mtx.Lock()
			mayProcessCh := pool.mayProcessCh
			pool.mtx.Unlock()
			mayProcessCh <- true // capacity frees up
		}
	}()

	pool.logger.Debug("Starting replay broadcast loop",
		"bucket", userAddress,
		"txes", len(bucket),
		"numc", pool.NumConcurrent(),
	)

	for pool.Context().Err() == nil {
		// NOTE(midas): non-blocking select on shutdown channel makes
		// sure every time before we try to replay, we know to shutdown.
		select {
		case <-pool.Quit():
			return

		case <-pool.goShutdownCh:
			// TODO(midas): Add tests to make sure that buckets which have not
			// been completely replayed yet, are *always* restarted after a shutdown.
			return
		default:
		}

		// Try the callback, and terminate on success.
		var err error
		if err = pool.txAcceptor.ReplayBroadcastTxBatch(
			context.Background(),
			bucket..., // one or many transaction batches
		); err == nil {
			pool.logger.Debug("Successfully processed bucket replay",
				"bucket", userAddress,
				"txes", len(bucket),
			)
			didProcessCh <- true
			return // Done processing bucket
		}

		pool.logger.Error("Error with replay callback",
			"bucket", userAddress,
			"retry", pool.retryInterval,
			"err", err,
		)

		// Give the client implementation some time before re-trying.
		if ok := pool.waitForInterval(pool.retryInterval); !ok {
			return
		}
	}
}

// WaitForBroadcastLoop selects messages from a didProcessCh channel
// for userAddress, i.e. it shall flush transactions when processing
// has ended successfully.
func (pool *ReplayPool) WaitForBroadcastLoop(userAddress string) {
	pool.mtx.Lock()
	didProcessCh := pool.didProcessCh[userAddress]
	pool.mtx.Unlock()

	for pool.Context().Err() == nil {
		select {
		case <-didProcessCh: // block until done
			// Removes the transaction bucket and closes channels.
			if err := pool.Flush(userAddress); err != nil {
				pool.logger.Error("Error flushing transaction bucket",
					"bucket", userAddress,
					"err", err.Error(),
				)
			}
			return

		case <-pool.Quit():
			return

		case <-pool.goShutdownCh:
			// Flush could not execute gracefully, not an error.
			return
		}
	}
}

// ThrottleBroadcastLoop waits for newly available capacity using mayProcessCh.
// When other processing goroutines are done, they shall send a message on
// mayProcessCh to indicate that some capacity is becoming available.
// This method re-dispatches a [ReplayBucket] call for userAddress.
func (pool *ReplayPool) ThrottleBroadcastLoop(userAddress string) {
	pool.logger.Debug("Waiting for capacity to process bucket",
		"bucket", userAddress,
		"maxc", pool.MaxConcurrent(),
		"numc", pool.NumConcurrent(),
		"numw", pool.NumWaiting(),
	)

	pool.mtx.Lock()
	mayProcessCh := pool.mayProcessCh
	pool.mtx.Unlock()

	for pool.Context().Err() == nil {
		select {
		case <-mayProcessCh: // block until available
			// This process is not waiting anymore.
			atomic.AddUint64(&pool.numw, ^uint64(0)) // -1

			// Start replaying this bucket now that capacity is available.
			if waiting := pool.ReplayBucket(userAddress); waiting {
				pool.logger.Debug("Bucket replay is waiting for capacity (again)",
					"bucket", userAddress,
					"maxc", pool.MaxConcurrent(),
					"numc", pool.NumConcurrent(),
					"numw", pool.NumWaiting(),
				)
			}
			return

		case <-pool.Quit():
			return

		case <-pool.goShutdownCh:
			// TODO(midas): Add tests to make sure that buckets in waiting state
			// are *always* restarted after a shutdown.

			return
		}
	}
}

// ----------------------------------------------------------------------------

// removeBucket removes a complete transaction bucket.
// CAUTION: The caller is responsible for locking the mutex.
func (pool *ReplayPool) removeBucket(userAddress string) error {
	if _, bucketExists := pool.UserTxs[userAddress]; !bucketExists {
		return ErrBucketNotFound
	}

	// Remove from atomic.Uint64 and from map
	atomic.AddUint64(&pool.size, ^uint64(0)*uint64(len(pool.UserTxs[userAddress])))
	delete(pool.UserTxs, userAddress)

	// Find bucket index, we always remove the last item.
	lastBucketIdx := len(pool.Buckets) - 1
	bucketIndex := slices.IndexFunc(pool.Buckets, func(b string) bool {
		return b == userAddress
	})

	// If it's not the last item, swap it.
	if bucketIndex != lastBucketIdx {
		lastBucket := pool.Buckets[lastBucketIdx]
		pool.Buckets[bucketIndex] = lastBucket
	}

	// Remove the last item from pool.Buckets.
	pool.Buckets = pool.Buckets[:lastBucketIdx]

	if _, ok := pool.processingCh[userAddress]; ok {
		close(pool.processingCh[userAddress])
		delete(pool.processingCh, userAddress)
	}

	if _, ok := pool.didProcessCh[userAddress]; ok {
		close(pool.didProcessCh[userAddress])
		delete(pool.didProcessCh, userAddress)
	}

	return nil
}

// ----------------------------------------------------------------------------
// Routines

// thresholdProcessorRoutine verifies the threshold and when it is reached,
// it will execute the replay loop using the maximum number of items it may
// process concurrently, i.e. it processes some buckets as soon as it should.
func (pool *ReplayPool) thresholdProcessorRoutine() {
	// Loops undefinitely and processes replays when threshold is reached.
	for pool.Context().Err() == nil {
		if !pool.IsRunning() {
			return
		}

		// NOTE(midas): non-blocking select on shutdown channel makes
		// sure every time before processing buckets, we know to shutdown.
		select {
		case <-pool.Quit():
			return
		case <-pool.goShutdownCh:
			return
		default: // Proceed to wait or handle
		}

		var (
			currentSize      = atomic.LoadUint64(&pool.size)
			thresholdReached = currentSize >= uint64(pool.threshold)
			bucketsAvailable = pool.GetBuckets()
		)

		switch {
		case thresholdReached: // If we have enough transactions, process some buckets.
			// We extract a max of items to process as many buckets as possible.
			numBuckets := min(
				pool.MaxConcurrent()-pool.NumConcurrent(), // capacity
				uint64(len(bucketsAvailable)),
			)
			bucketsToProcess := bucketsAvailable[:numBuckets]

			pool.logger.Debug("Pool threshold reached, now replaying buckets",
				"size", currentSize,
				"threshold", pool.Threshold(),
				"num_buckets", numBuckets,
			)

			for _, bucketAddress := range bucketsToProcess {
				// Start replaying this bucket if/when capacity is available.
				if waiting := pool.ReplayBucket(bucketAddress); waiting {
					pool.logger.Debug("Bucket replay is waiting for capacity",
						"bucket", bucketAddress,
						"maxc", pool.MaxConcurrent(),
						"numc", pool.NumConcurrent(),
						"numw", pool.NumWaiting(),
					)
				}

				// Give the pool some time for/after processing, or shutdown.
				if ok := pool.waitForInterval(pool.pauseInterval); !ok {
					return
				}
			}

		default:
		}

		// Give the pool some time for/after processing, or shutdown.
		if ok := pool.waitForInterval(pool.pauseInterval); !ok {
			return
		}
	}
}

// timerProcessorRoutine waits for flushInterval, then executes the replay loop
// using the maximum number of items it may process concurrently.
func (pool *ReplayPool) timerProcessorRoutine() {
	// Loops undefinitely and processes replays when timer ticks.
	for pool.Context().Err() == nil {
		flushInterval := pool.FlushInterval()

		select {
		case <-time.After(flushInterval): // Every flushInterval, we process some buckets.
			bucketsAvailable := pool.GetBuckets()

			// We extract a max of items to process as many buckets as possible.
			numBuckets := min(
				pool.MaxConcurrent()-pool.NumConcurrent(), // capacity
				uint64(len(bucketsAvailable)),
			)
			bucketsToProcess := bucketsAvailable[:numBuckets]

			for _, bucketAddress := range bucketsToProcess {
				// Start replaying this bucket if/when capacity is available.
				if waiting := pool.ReplayBucket(bucketAddress); waiting {
					pool.logger.Debug("Bucket replay is waiting for capacity",
						"bucket", bucketAddress,
						"maxc", pool.MaxConcurrent(),
						"numc", pool.NumConcurrent(),
						"numw", pool.NumWaiting(),
					)
				}

				// Give the pool some time for/after processing, or shutdown.
				if ok := pool.waitForInterval(pool.pauseInterval); !ok {
					return
				}
			}

		case <-pool.Quit():
			return
		case <-pool.goShutdownCh:
			return
		}

		// Give the pool some time for/after processing, or shutdown.
		if ok := pool.waitForInterval(pool.pauseInterval); !ok {
			return
		}
	}
}

// waitForInterval waits for i using time.After, or shutdown channels.
func (pool *ReplayPool) waitForInterval(i time.Duration) (waited bool) {
	for pool.Context().Err() == nil {
		select {
		case <-time.After(i):
			return true
		case <-pool.Quit():
			return false
		case <-pool.goShutdownCh:
			return false
		}
	}

	return false
}
