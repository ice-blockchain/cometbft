package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// -----------------------------------------------------------------------------

// To be called early in main/test
func enableLockProfiling(tb testing.TB) {
	runtime.SetMutexProfileFraction(1) // profile *every* mutex contention
	runtime.SetBlockProfileRate(1)     // profile channel/mutex blocking

	// When stuck, dump:
	pprof.Lookup("goroutine").WriteTo(os.Stderr, 2)
	pprof.Lookup("mutex").WriteTo(os.Stderr, 1)
	pprof.Lookup("block").WriteTo(os.Stderr, 1)
}

// -----------------------------------------------------------------------------

var randomizer = rand.New(rand.NewSource(time.Now().Unix()))

// useRelaysWithoutIds removes the relay IDs from helpers.RelayAddress instances.
// Note: this is not required but permits to consistently reproduce a live env.
func useRelaysWithoutIds(
	tb testing.TB,
	relays []string,
) []string {
	tb.Helper()

	relaysWithoutIds := []string{}
	for _, relayAddrStr := range relays {
		ra, err := helpers.NewRelayAddress(relayAddrStr)
		require.NoError(tb, err, "expected valid relay address, got: "+relayAddrStr)

		relaysWithoutIds = append(relaysWithoutIds, ra.StringWithoutId())
	}
	return relaysWithoutIds
}

// makeClientTransactions creates random numTransactions for chainInfo's fingerprint.
func makeClientTransactions(
	tb testing.TB,
	chainInfo helpers.ExtendedChainID,
	numTransactions int,
) []client.Transaction {
	tb.Helper()

	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		randomSize := randomizer.Intn(1024-300+1) + 300 // 300<=x<=1024
		randomData := make([]byte, randomSize+1)
		randomData[0] = byte(i)
		randomData[1] = byte(randomizer.Intn(255))
		randomData[2] = byte(randomizer.Intn(255))
		randomData[3] = byte(randomizer.Intn(255))

		testTransactions = append(testTransactions, client.Transaction{
			Data:        randomData,
			Fingerprint: chainInfo.GetFingerprint(),
		})
	}

	return testTransactions
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

		select {
		case *status = <-notifyCh:
			return

		case <-ctx.Done():
			(*status).Error = fmt.Errorf(
				"Timed out waiting for broadcast status for %s", testChainID)
			return // cancels context
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
	tb.Helper()

	// Separate goroutine for client broadcast process
	notifyCh := make(chan client.BroadcastStatus)
	defer close(notifyCh)

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
	assert.NoError(tb, resultStatusMsg.Error,
		fmt.Sprintf("should not contain error status for transactions on: %s", withChainID))
	assert.Len(tb, resultStatusMsg.TxHashes, numTransactions,
		fmt.Sprintf("should contain all accepted transaction hashes on: %s", withChainID))
}

func requireAcceptorCommitCalls(
	tb testing.TB,
	maxWaitTime time.Duration,
	numRoundCommits uint64,
	fromAcceptors ...*client.MockAcceptorImpl,
) (int, time.Duration) {
	tb.Helper()

	numActualCommits := 0
	startWaitTz := time.Now()

	commitStatusCh := make(chan bool)
	commitErrorCh := make(chan error, 1)

	tb.Logf("Waiting for %d blocks commit (max %.0fsec)...", numRoundCommits, maxWaitTime.Seconds())

	// Loops for maxWaitTime and loads the acceptor`s TxCommitCalls.
	// Note that ALL acceptors must report the *exact* numRoundCommits.
	go func(ch *chan bool, errCh *chan error) {
		hasExpectedCommits := false

		defer func() {
			*ch <- hasExpectedCommits
		}()

	BLOCK_COMMITS_LOOP:
		for tb.Context().Err() == nil {
			hasExpectedCommits = false
			for i, testAcceptor := range fromAcceptors {
				numAcceptorCommits := testAcceptor.TxCommitCalls.Load()
				numActualCommits = int(numAcceptorCommits)

				if numAcceptorCommits > numRoundCommits {
					hasExpectedCommits = false
					*errCh <- fmt.Errorf(
						"too many commit calls for acceptor-%d; expected %d, got %d...",
						i+1, numRoundCommits, numAcceptorCommits)
					return
				}

				hasExpectedCommits = numAcceptorCommits == numRoundCommits
				if !hasExpectedCommits {
					tb.Logf("missing commit calls for acceptor-%d; expected %d, got %d...",
						i+1, numRoundCommits, numAcceptorCommits)
					break
				} else {
					tb.Logf("Acceptor-%d reported correct %d block commits",
						i+1, numRoundCommits)
				}
			}

			hasReachedTimeout := time.Since(startWaitTz) > maxWaitTime

			switch {
			case hasExpectedCommits == true: // has commits from all acceptors
				return

			case hasReachedTimeout == true: // reached timeout
				*errCh <- fmt.Errorf(
					"timeout reached to intercept %d block commits", numRoundCommits)
				return

			default:
				time.Sleep(2 * time.Second)
				continue BLOCK_COMMITS_LOOP
			}
		}

		return
	}(&commitStatusCh, &commitErrorCh)

	tb.Logf("Starting commit status consumer for %d commits...", numRoundCommits)

	waitForStatus := sync.WaitGroup{}
	waitForStatus.Add(1)

	// Blocked until a status is transported on channel.
	go func(wg *sync.WaitGroup, ch *chan bool, errCh *chan error) {
		defer wg.Done()

		var status bool
		select {
		case status = <-*ch:
			elapsedSeconds := time.Since(startWaitTz).Seconds()
			if status {
				tb.Logf("Intercepted %d blocks commit after %.0fs", numRoundCommits, elapsedSeconds)
			} else {
				if err := <-*errCh; err != nil {
					fatalErr := fmt.Errorf("ERROR: failed to commit %d blocks: %w",
						numRoundCommits, err).Error()
					tb.Log(fatalErr)
				}
				tb.Fatalf("failed to commit %d blocks", numRoundCommits)
			}
		}
		close(*ch)
	}(&waitForStatus, &commitStatusCh, &commitErrorCh)

	// Block the main thread until an update has been processed.
	waitForStatus.Wait()

	// ... and use require to make sure about *exact* number of commits.
	require.Equal(tb, int(numRoundCommits), numActualCommits,
		fmt.Sprintf("expected %d block commits, got %d", numRoundCommits, numActualCommits))

	return numActualCommits, time.Since(startWaitTz)
}

// -----------------------------------------------------------------------------

// closeAndRemoveAll is a helper to shutdown a running [mx.MultiplexBackend] and
// remove all filesystem resources created under rootDir.
func closeAndRemoveAll(
	tb testing.TB,
	rootDir string,
	backend *mx.MultiplexBackend,
) {
	tb.Helper()

	defer os.RemoveAll(rootDir)

	if backend.IsRunning() {
		err := backend.Stop()
		assert.NoError(tb, err, "should shutdown backend gracefully")
		backend.Wait()
	}
}

// shutdownBackends stops all backends concurrently.
func shutdownBackends(
	tb testing.TB,
	backends ...*mx.MultiplexBackend,
) {
	tb.Helper()

	for i := 0; i < len(backends); i++ {
		backend := backends[i]
		rootDir := backend.Config().RootDir
		go closeAndRemoveAll(tb, rootDir, backend)
	}
}
