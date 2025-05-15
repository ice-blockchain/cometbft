package multiplex_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// Full transactions broadcast with runtime chain and 10 second wait time before pre-shutdown.
func TestScenarioValidatorsEmptyRelays(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelaysWithLogs(t, numChains, numRelays)
	defer shutdownFn(servers)

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	for _, server := range servers {
		relayLogger := cmtlog.TestingLogger().With("process", "relay-1")
		server.SetLogger(relayLogger)
	}

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
	t.Logf("Calling client.BroadcastTx with ChainID %s and %d transactions", testChainID, numTransactions)
	go clientBroadcastTx(t,
		broadcastCtx,
		servers[0],
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		testChainID,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	//XXX verify that other relays are validators for new network.

	waitDuration := 10 * time.Second
	t.Logf("Waiting %.0fsec before ending test case...", waitDuration.Seconds())
	time.Sleep(waitDuration)
}
