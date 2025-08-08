package e2e

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// ----------------------------------------------------------------------------
// TestMultiplexBackend

func TestMultiplexBackendNewServer(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 1
	backends := ResetTestMultiplexRelays(t, numRelays, cmtlog.NewNopLogger())
	require.NotEmpty(t, backends)
	defer shutdownBackends(t, backends...)

	assert.NotNil(t, backends[0])

	testBackend := backends[0]
	assert.NotNil(t, testBackend.Config())
	assert.NotNil(t, testBackend.Acceptor())
	assert.NotNil(t, testBackend.RuntimeManager())
	assert.NotNil(t, testBackend.IdleManager())
	assert.NotNil(t, testBackend.Discovery())
	assert.NotNil(t, testBackend.CometBFT())
	assert.NotNil(t, testBackend.SnapsApp())
	assert.NotNil(t, testBackend.ChainConns())
}

func TestMultiplexBackendOnStart(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 1
	backends := ResetTestMultiplexRelays(t, numRelays, cmtlog.NewNopLogger())
	require.NotEmpty(t, backends)
	defer shutdownBackends(t, backends...)

	testBackend := backends[0]
	require.NotNil(t, testBackend)

	startErr := testBackend.Start()
	assert.NoError(t, startErr)

	// Discovery P2P must be listening
	testDiscovery := testBackend.Discovery().Transport()
	assert.NotNil(t, testDiscovery)
	assert.Equal(t, true, testDiscovery.IsListening()) // LISTEN

	// CometBFT P2P must be listening
	testCometBFT := testBackend.CometBFT().Transport()
	assert.NotNil(t, testCometBFT)
	assert.Equal(t, true, testCometBFT.IsListening()) // LISTEN
}

func TestMultiplexBackendOnStartParallel(t *testing.T) {
	defer goleak.VerifyNone(t)

	numRelays := 3
	backends := ResetTestMultiplexRelays(t, numRelays, cmtlog.NewNopLogger())
	require.NotEmpty(t, backends)
	defer shutdownBackends(t, backends...)

	startWg := new(sync.WaitGroup)
	startWg.Add(numRelays)

	for _, b := range backends {
		go func(testBackend *mx.MultiplexBackend, wg *sync.WaitGroup) {
			defer wg.Done()
			require.NotNil(t, testBackend)

			startErr := testBackend.Start()
			assert.NoError(t, startErr)

			// Discovery P2P must be listening
			testDiscovery := testBackend.Discovery().Transport()
			assert.NotNil(t, testDiscovery)
			assert.Equal(t, true, testDiscovery.IsListening()) // LISTEN

			// CometBFT P2P must be listening
			testCometBFT := testBackend.CometBFT().Transport()
			assert.NotNil(t, testCometBFT)
			assert.Equal(t, true, testCometBFT.IsListening()) // LISTEN
		}(b, startWg)
	}
	startWg.Wait()
}

// ----------------------------------------------------------------------------
// TestMultiplexClient

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
}

// ----------------------------------------------------------------------------

// CAUTION: This helper uses an empty multiplex config on multiple relays.
func ResetTestMultiplexRelays(
	tb testing.TB,
	numRelays int,
	customLogger cmtlog.Logger,
) []*mx.MultiplexBackend {
	tb.Helper()

	backends := make([]*mx.MultiplexBackend, numRelays)

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	relayConf1 := MakeConfig(tb, tmpRootDir)

	serverRelay1, err := mx.NewServer(
		tb.Context(),
		&client.DefaultAcceptor{},
		relayConf1,
		customLogger.With("process", "relay-1"),
	)
	require.NoError(tb, err, "should create first server instance")

	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-"+strconv.Itoa(r+1))
		require.NoError(tb, err)

		discoveryPort := relayConf1.DiscoveryPort + uint16(r*100)

		relayConfX := MakeConfig(tb, tmpRootDir)
		relayConfX.DiscoveryPort = discoveryPort
		relayConfX.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", discoveryPort+1)
		relayConfX.P2P.ExternalAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort+1)
		relayConfX.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort-1)
		relayConfX.Instrumentation.Namespace += "_" + strconv.Itoa(r+1)

		serverRelayX, err := mx.NewServer(
			tb.Context(),
			&client.DefaultAcceptor{},
			relayConfX,
			customLogger.With("process", "relay-"+strconv.Itoa(r+1)),
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		backends[r] = serverRelayX
	}

	return backends
}

// ----------------------------------------------------------------------------
func requireStartMultiplexRelays(
	tb testing.TB,
	numRelays int,
	customLogger cmtlog.Logger,
) []*mx.MultiplexBackend {
	backends := ResetTestMultiplexRelays(tb, numRelays, customLogger)
	require.NotEmpty(tb, backends)

	startWg := new(sync.WaitGroup)
	startWg.Add(numRelays)

	for _, b := range backends {
		go func(testBackend *mx.MultiplexBackend, wg *sync.WaitGroup) {
			defer wg.Done()
			require.NotNil(tb, testBackend)

			startErr := testBackend.Start()
			assert.NoError(tb, startErr)
		}(b, startWg)
	}
	startWg.Wait()

	return backends
}
