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
