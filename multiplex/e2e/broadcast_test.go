package e2e

import (
	"context"
	"testing"
	"time"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"go.uber.org/goleak"
)

// ----------------------------------------------------------------------------
// TestMultiplexClient

func TestMultiplexClientBroadcastTx(t *testing.T) {
	defer goleak.VerifyNone(t)

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
	requireAcceptorCommitCalls(t, maxCommitWaitTime, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to shutdown...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}
