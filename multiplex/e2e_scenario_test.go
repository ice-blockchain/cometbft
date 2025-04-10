package multiplex_test

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/node"
	sm "github.com/ice-blockchain/cometbft/state"
)

func nullTxBroadcasterOverwrite(tb testing.TB, _ string, _ int) {
	tb.Helper()
}

// Uses RPC Server to broadcast transactions.
func txBroadcasterOverwrite(tb testing.TB, chainID string, numTxes int) {
	tb.Helper()

	broadcastRawTxes(
		tb,
		[]string{chainID},
		1,
		1,
		numTxes,
	)
}

// Uses RPC Server to broadcast transactions.
func broadcastRawTxes(
	tb testing.TB,
	networks []string,
	numChains,
	numRelays,
	numTransactions int,
) {
	tb.Helper()

	// We shall randomly pick a node index and values
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}
	wg := sync.WaitGroup{}
	wg.Add(numTransactions)

	// Every iteration should create a transaction with a random value
	// and broadcast it using the second relay's rpc.
	for i := 0; i < numTransactions; i++ {
		mtx.Lock()
		randomizeData := randomizer.Intn(999999999)

		randomRelay := 0
		if numRelays > 1 {
			randomRelay = randomizer.Intn(numRelays) + 1
		}

		randomChain := 0
		if numChains > 1 {
			randomChain = randomizer.Intn(numChains)
		}
		mtx.Unlock()

		randomVal := strconv.Itoa(randomizeData)
		txData := "test=value" + randomVal

		// Uses CometBFT RPC Port (DiscoveryPort + 2)
		rpcPort := strconv.Itoa(50001 + (randomRelay * 100) + 2) // random relay RPC
		chainID := networks[randomChain]

		nodeRPC := "http://127.0.0.1:" + rpcPort
		rpcPath := "/broadcast_tx_commit/" + chainID

		go func(host, path, tx string) {
			defer wg.Done()

			// For debug, uncomment the following line
			tb.Logf("Now broadcasting transaction: %s to %s", tx, host)
			http.Get(host + path + "?tx=\"" + tx + "\"")
		}(nodeRPC, rpcPath, txData)
	}

	// Waits for numTransactions to be broadcast
	wg.Wait()
}

// Uses MultiplexClient to broadcast transactions.
func clientBroadcastTx(
	tb testing.TB,
	ctx context.Context,
	server *mx.MultiplexBackend,
	relays []string,
	testChainID string,
	numTransactions int,
	notifyCh chan client.BroadcastStatus,
) {
	tb.Helper()

	chainInfo, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(tb, err, "should create correctly formatted ChainID")

	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		testTransactions = append(testTransactions, client.Transaction{
			Data:        []byte{byte(i), byte(i + 1), byte(i + 2)},
			Fingerprint: chainInfo.GetFingerprint(),
		})
	}

	multiplexClient := mx.NewClient(
		mx.WithBackend(server),
		mx.WithNotifier(&client.StatusNotifier{}),
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
	notifyCh chan client.BroadcastStatus,
) client.BroadcastStatus {
	wg := sync.WaitGroup{}
	wg.Add(1)

	resultStatusMsg := client.BroadcastStatus{}

	// Expects a BroadcastStatus update, or timeout after 30s.
	go func(status *client.BroadcastStatus) {
		defer wg.Done()

		for {
			select {
			case *status = <-notifyCh:
				return

			case <-ctx.Done():
				tb.Error("Timed out waiting for broadcast status")
				return // cancels context
			}
		}
	}(&resultStatusMsg)

	// Waits for a status update or timeout
	wg.Wait()
	return resultStatusMsg
}

func TestScenarioServerBroadcastSevenHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	testReactor := servers[0].GetReactor()
	testChainID := servers[0].GetNetworks()[0]
	nodesProvider := testReactor.GetServicesProvider()

	nodeInstance := nodesProvider(mx.ServiceKeyNodeRuntime, testChainID).(*node.Node)

	testMultiplex := mx.MultiplexMap[*node.Node]{}
	testMultiplex[testChainID] = mx.NewChainInstance(testChainID, nodeInstance)

	// First create genesis block
	assertWaitForNodesMultiplexToProduceBlocks(t,
		testReactor,
		testMultiplex,
		1, // numBlocks
		5*time.Second,
		"node_genblock_test",
		txBroadcasterOverwrite,
	)

	// Then wait for next 10 blocks asynchronously,
	// transactions will be broadcasted soon.
	wgBlocks := sync.WaitGroup{}
	wgBlocks.Add(1)
	go func() {
		defer wgBlocks.Done()

		actualNumBlocks, actualNumTxes := assertWaitForNodesMultiplexToProduceBlocks(t,
			testReactor,
			testMultiplex,
			-1,             // as many blocks as necessary
			20*time.Second, // 20 seconds runtime
			"node_blocks_test",
			nullTxBroadcasterOverwrite,
		)

		assert.Contains(t, actualNumTxes, testChainID)
		assert.Contains(t, actualNumBlocks, testChainID)

		storeProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStateStore)
		stateMachine, err := storeProvider(testChainID).(sm.Store).Load()
		assert.NoError(t, err)
		assert.Greater(t, stateMachine.LastBlockHeight, int64(1))
	}()

	// In parallel, broadcast transactions and wait for
	// all to be broadcasted.
	wgTxes := sync.WaitGroup{}
	wgTxes.Add(1)

	numTransactions := 300
	go func(numTxes int) {
		defer wgTxes.Done()

		// Then broadcast many transactions
		broadcastRawTxes(
			t,
			servers[0].GetNetworks(),
			numChains,
			1,       // sends all to first relay!
			numTxes, // numTransactions
		)
	}(numTransactions)

	// First wait for transactions to be broadcast
	wgTxes.Wait()

	// And also wait for enough blocks to be produced
	wgBlocks.Wait()
}

func TestScenarioClientBroadcastSevenHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// Give the backend some time before broadcast
	// 5 seconds is long but bearable for a 7 relays setup.
	time.Sleep(5 * time.Second)

	// Prepare the data that we shall broadcast (skip "self")
	relaysAddresses := []string{}
	for i := 1; i < len(servers); i++ {
		relaysAddresses = append(relaysAddresses, servers[i].GetListenAddress())
	}

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Cancelable context to permit stopping by timeout
	ctx, cancelCtxFn := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := servers[0].GetNetworks()[0]
	notifyCh := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		ctx,
		servers[0],
		relaysAddresses,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		ctx,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)

	txHashes := []string{}
	for _, bzHash := range resultStatusMsg.TxHashes {
		txHashes = append(txHashes, fmt.Sprintf("%X", bzHash))
	}

	t.Logf("Status from BroadcastTx: <%d, %s, %v>",
		len(txHashes),
		txHashes,
		resultStatusMsg.Error)

	// assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

func TestScenarioClientReplRequestSevenHealthyRelays(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// Give the backend some time before broadcast
	time.Sleep(2 * time.Second)

	// Prepare the data that we shall broadcast (skip "self")
	relaysAddresses := []string{}
	for i := 1; i < len(servers); i++ {
		relaysAddresses = append(relaysAddresses, servers[i].GetListenAddress())
	}

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Cancelable context to permit stopping by timeout
	ctx, cancelCtxFn := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		ctx,
		servers[0],
		relaysAddresses,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		ctx,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)

	txHashes := []string{}
	for _, bzHash := range resultStatusMsg.TxHashes {
		txHashes = append(txHashes, fmt.Sprintf("%X", bzHash))
	}

	t.Logf("Status from BroadcastTx: <%d, %s, %v>",
		len(txHashes),
		txHashes,
		resultStatusMsg.Error)

	// assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

func TestScenarioClientBroadcastMinusOneHealthyRelays(t *testing.T) {

}

// ----------------------------------------------------------------------------
// Helpers

func ResetTestScenarioRelays(
	tb testing.TB,
	numChains int,
	numRelays int,
) ([]*mx.MultiplexBackend, func()) {
	tb.Helper()

	// For debug, change the loggers to cmtlog.TestingLogger()
	customLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		customLoggers[i] = cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-" + strconv.Itoa(i+1))
	}

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		tb,
		numChains,
		numRelays,
		customLoggers...,
	)
	require.NotEmpty(tb, servers)
	require.Len(tb, rootDirs, numRelays)
	require.Len(tb, servers, numRelays)

	shutdownFn := func() {
		for i := 0; i < len(servers); i++ {
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(tb, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}

	return servers, shutdownFn
}
