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

// XXX
func TestMultiplexClientBroadcastTx(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 3
	backends := requireStartMultiplexRelays(t, numRelays, cmtlog.TestingLogger())
	defer shutdownBackends(t, backends...)

	testAcceptorRelay1 := client.NewMockAcceptorImpl()
	testAcceptorRelay2 := client.NewMockAcceptorImpl()
	testAcceptorRelay3 := client.NewMockAcceptorImpl()
	backends[0].SetAcceptor(testAcceptorRelay1)
	backends[1].SetAcceptor(testAcceptorRelay2)
	backends[2].SetAcceptor(testAcceptorRelay3)

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
	totalExpectedCommits := uint64(1)
	roundExpectedCommits := uint64(1)
	maxCommitWaitTime := time.Duration(20 * time.Second)

	requireAcceptorCommitCalls(t, maxCommitWaitTime, totalExpectedCommits, roundExpectedCommits,
		testAcceptorRelay1,
		testAcceptorRelay2,
		testAcceptorRelay3,
	)

	waitDuration := 2 * time.Second
	t.Logf("Waiting %.0fsec to shutdown...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}
