package e2e

import (
	"context"
	"fmt"
	"testing"
	"time"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// ----------------------------------------------------------------------------
// TestMultiplexClient

func TestMultiplexClientBroadcastTx(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	testWithAcceptors := []client.Acceptor{
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	}

	numRelays := 3
	backends := requireStartMultiplexRelays(t, numRelays, cmtlog.TestingLogger(), testWithAcceptors)
	defer shutdownBackends(t, backends...)

	relaysForTestCase := make([]string, 0, numRelays)
	for _, b := range backends {
		relaysForTestCase = append(relaysForTestCase, b.GetListenAddress())
	}
	relaysForTestCase = useRelaysWithoutIds(t, relaysForTestCase)

	firstTimeoutAfter := 30 * time.Second
	firstBroadcastCtx, firstCancelCtxFn := context.WithTimeout(context.TODO(), firstTimeoutAfter)
	defer firstCancelCtxFn()

	numTransactions := 1
	withChainID := helpers.MakeChainID("test-chain-1")

	// This method blocks the main thread a maximum of firstTimeoutAfter and
	// expects an update on notifyCh from client.BroadcastTx.
	requireCompleteClientBroadcastTx(
		t,
		firstBroadcastCtx,
		backends[0],
		relaysForTestCase,
		withChainID,
		numTransactions,
	)

	// -------------------

	// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)

	// This method blocks the main thread maxCommitWaitTime and loads TxCommitCalls
	// from all acceptors. Importantly the number of commits reported must be
	// identical to roundExpectedCommits for ALL acceptors.
	startCommitWait := time.Now()
	numActualCommits,
		elapsedCommitDuration := requireAcceptorCommitCalls(t,
		maxCommitWaitTime,
		roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)
	endCommitWait := time.Now()

	require.WithinDurationf(t, startCommitWait, endCommitWait, 6*time.Second,
		fmt.Sprintf("expected %d block commits in under %ds, took %.0fs",
			numActualCommits,
			6*time.Second,
			elapsedCommitDuration.Seconds(),
		))

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to shutdown...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}

func TestMultiplexClientBroadcastTxNetworkContinuation(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	testWithAcceptors := []client.Acceptor{
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	}

	numRelays := 3
	backends := requireStartMultiplexRelays(t, numRelays, cmtlog.TestingLogger(), testWithAcceptors)
	defer shutdownBackends(t, backends...)

	relaysForTestCase := make([]string, 0, numRelays)
	for _, b := range backends {
		relaysForTestCase = append(relaysForTestCase, b.GetListenAddress())
	}
	relaysForTestCase = useRelaysWithoutIds(t, relaysForTestCase)

	// ------------------------------------------------------------------------
	// Test that we can execute 3 rounds of client.BroadcastTx and that we get
	// the correct number of block commits, tracked using client.MockAcceptorImpl
	// to make sure about *local* and *remote* calls to `Acceptor#CommitBroadcastTx`.

	numTestBlocks := 3
	withChainID := helpers.MakeChainID("test-chain-1")
	for b := 1; b <= numTestBlocks; b++ {
		timeoutAfter := 20 * time.Second
		broadcastCtx, cancelCtxFn := context.WithTimeout(context.TODO(), timeoutAfter)

		// This method blocks the main thread a maximum of firstTimeoutAfter and
		// expects an update on notifyCh from client.BroadcastTx.
		numTransactions := 1
		requireCompleteClientBroadcastTx(
			t,
			broadcastCtx,
			backends[0],
			relaysForTestCase,
			withChainID,
			numTransactions,
		)

		// -------------------

		// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
		roundExpectedCommits := uint64(b) // 1, 2, 3...
		maxCommitWaitTime := 20 * time.Second

		// This method blocks the main thread maxCommitWaitTime and loads TxCommitCalls
		// from all acceptors. Importantly the number of commits reported must be
		// identical to roundExpectedCommits for ALL acceptors.
		startCommitWait := time.Now()
		_, elapsedCommitDuration := requireAcceptorCommitCalls(t,
			maxCommitWaitTime,
			roundExpectedCommits,
			testAcceptorRelay1,
			testAcceptorRelay2,
			testAcceptorRelay3,
		)
		endCommitWait := time.Now()

		// Fails here if any broadcast operation fails to be committed.
		require.WithinDurationf(t, startCommitWait, endCommitWait, maxCommitWaitTime,
			fmt.Sprintf("expected %d block commits in under %ds for %s on round %d, took %.0fs",
				roundExpectedCommits,
				maxCommitWaitTime,
				withChainID,
				b,
				elapsedCommitDuration.Seconds(),
			))

		cancelCtxFn()
	}

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to shutdown...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}

func TestMultiplexClientBroadcastTxRelayDowntime(t *testing.T) {
	defer func() {
		time.Sleep(2 * time.Second)
		goleak.VerifyNone(t)
	}()

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	testWithAcceptors := []client.Acceptor{
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	}

	numRelays := 3
	backends := requireStartMultiplexRelays(t, numRelays, cmtlog.TestingLogger(), testWithAcceptors)
	defer shutdownBackends(t, backends...)

	relaysForTestCase := make([]string, 0, numRelays)
	for _, b := range backends {
		relaysForTestCase = append(relaysForTestCase, b.GetListenAddress())
	}
	relaysForTestCase = useRelaysWithoutIds(t, relaysForTestCase)

	// ------------------------------------------------------------------------
	// Test that we can execute 3 rounds of client.BroadcastTx and that we get
	// the correct number of block commits, tracked using client.MockAcceptorImpl
	// to make sure about *local* and *remote* calls to `Acceptor#CommitBroadcastTx`.
	//
	// This test intentionally leaves out relay-3 during the second broadcast
	// operation, to test whether relay-3 will receive the missing transaction.

	withAcceptors := []*client.MockAcceptorImpl{
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	}

	backupAcceptors := withAcceptors[:]
	backupRelays := relaysForTestCase[:]

	numTestBlocks := 3
	withChainID := helpers.MakeChainID("test-chain-1")
	for b := 1; b <= numTestBlocks; b++ {
		timeoutAfter := 20 * time.Second
		broadcastCtx, cancelCtxFn := context.WithTimeout(context.TODO(), timeoutAfter)

		relaysForTestCase = backupRelays[:]
		withAcceptors = backupAcceptors[:]

		// Remove relay-3 for second broadcast operation.
		if b == 2 {
			relaysForTestCase = relaysForTestCase[:2]
			withAcceptors = withAcceptors[:2]
		}

		// This method blocks the main thread a maximum of firstTimeoutAfter and
		// expects an update on notifyCh from client.BroadcastTx.
		numTransactions := 1
		requireCompleteClientBroadcastTx(
			t,
			broadcastCtx,
			backends[0],
			relaysForTestCase,
			withChainID,
			numTransactions,
		)

		// -------------------

		// Test that client callbacks executed, i.e. Acceptor.CommitBroadcastTx.
		roundExpectedCommits := uint64(b) // 1, 2, 3...
		maxCommitWaitTime := 20 * time.Second

		// This method blocks the main thread maxCommitWaitTime and loads TxCommitCalls
		// from all acceptors. Importantly the number of commits reported must be
		// identical to roundExpectedCommits for ALL acceptors.
		startCommitWait := time.Now()
		_, elapsedCommitDuration := requireAcceptorCommitCalls(t,
			maxCommitWaitTime,
			roundExpectedCommits,
			withAcceptors...,
		)
		endCommitWait := time.Now()

		// Fails here if any broadcast operation fails to be committed.
		require.WithinDurationf(t, startCommitWait, endCommitWait, maxCommitWaitTime,
			fmt.Sprintf("expected %d block commits in under %ds for %s on round %d, took %.0fs",
				roundExpectedCommits,
				maxCommitWaitTime,
				withChainID,
				b,
				elapsedCommitDuration.Seconds(),
			))

		cancelCtxFn()
	}

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to shutdown...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}
