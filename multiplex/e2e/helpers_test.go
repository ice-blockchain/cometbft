package e2e

import (
	"context"
	"fmt"
	"math/rand"
	"os"
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
		randomData := randomizer.Intn(999999999)
		testTransactions = append(testTransactions, client.Transaction{
			Data:        []byte{byte(i), byte(i + 1), byte(i + 2), byte(randomData)},
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
	close(notifyCh)
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
