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
	"github.com/ice-blockchain/cometbft/store"
)

// ----------------------------------------------------------------------------
// MultiplexClient Broadcast Test (Using client.BroadcastTx)

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
	tb.Helper()

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

// With a list of healthy relays, the transactions will be added locally
// and then shared with other relays using a message on mempool channel,
// to which the relays respond with a AckTransactionBroadcast message
// before we proceed to accepting the transaction.
func TestScenarioClientBroadcastHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		5*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
	notifyCh := make(chan client.BroadcastStatus)
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
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := len(relays) - 1
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// With a list of empty relays, a ChainReplicationRequest must be sent,
// and a ChainReplicationResponse is expected before sharing transactions
// using a message on mempool channel, to which the relays respond with a
// AckTransactionBroadcast message before we proceed to accepting the transaction.
func TestScenarioClientBroadcastEmptyRelays(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

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
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	// Test that ChainReplicationRequest were sent
	expectedRequestCnt := len(relays) - 1
	actualRequestsSent := servers[0].GetReplRequestPeers(testChainID)
	assert.NotEmpty(t, actualRequestsSent)
	assert.Len(t, actualRequestsSent, expectedRequestCnt)

	// Test that AckTransactionBroadcast messages were received.
	for _, bzTxHash := range resultStatusMsg.TxHashes {
		testTxHash := fmt.Sprintf("%X", bzTxHash)

		expectedResponseCnt := len(relays) - 1
		actualResponsesRcvd := servers[0].GetAckResponsePeers(testTxHash)
		assert.NotEmpty(t, actualResponsesRcvd)
		assert.Len(t, actualResponsesRcvd, expectedResponseCnt)
	}
}

// With a list of empty relays, a first block of the network will be created,
// which includes the broadcast transactions data (using client.BroadcastTx),
// and the state machine and blocks store are updated with transactions data.
func TestScenarioClientBroadcastEmptyRelaysProduceBlockWithTx(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

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
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	waitDuration := 5 * time.Second
	t.Logf("Waiting %.0fsec to evaluate state machine...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	testReactor := servers[0].GetReactor()

	stateStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyStateStore)
	assert.NotNil(t, stateStoreProvider, "should not error getting state store provider")
	chainStore := stateStoreProvider(testChainID).(sm.Store)
	assert.NotNil(t, chainStore, "state store per chain must not be nil")

	stateMachine, err := chainStore.Load()
	assert.NoError(t, err, "should not error loading state")
	assert.Equal(t, testChainID, stateMachine.ChainID)
	assert.Equal(t, stateMachine.LastBlockHeight, int64(1))

	blockStoreProvider := testReactor.GetInstanceProvider(mx.InstanceKeyBlockStore)
	assert.NotNil(t, blockStoreProvider, "should not error getting block store provider")
	blockStore := blockStoreProvider(testChainID).(*store.BlockStore)
	assert.NotNil(t, blockStore, "block store per chain must not be nil")

	actualBlock, actualMeta := blockStore.LoadBlock(stateMachine.LastBlockHeight)
	assert.NotNil(t, actualBlock, "should return correct block")
	assert.NotNil(t, actualMeta, "should return correct block meta")
	assert.NotEmpty(t, actualBlock.Data, "should return non-empty block data")
	assert.NotEmpty(t, actualBlock.Data.Txs, "should return non-empty block transactions")
	assert.Len(t, actualBlock.Data.Txs, numTransactions)
}

// After a complete backend restart, due to a process failure or corruption,
// the transaction broadcast process must normally resume operations and the
// broadcast operation(s) must succeed without errors from the relays.
func TestScenarioClientBroadcastAfterBackendRestart(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Note: relays includes self
	// Using 0 waitDuration because others have plenty of time due to restart.
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		0*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	reuseRootDir := servers[0].GetReactor().GetNodeConfig().RootDir

	// Stop the receiving backend, then start it again.
	err := servers[0].Close()
	require.NoError(t, err, "should shutdown server")

	waitDuration := 3 * time.Second
	t.Logf("Waiting %.0fsec to restart backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// CAUTION:
	// We mimic one of the relay shutting down completely, i.e. its process
	// is not managed, corrupted or stopped. Setting nil on the "old" instance
	// is only necessary during shutdown tests.

	servers[0] = nil // Only for test
	resetRelay, newShutdownFn := ResetTestSingleCompatibleRelay(t,
		reuseRootDir,
		servers[1],
		0, // indexRelay (resetting relay-1)
		backendLogger,
	)
	defer newShutdownFn()

	resetRelay.MustStart()

	// Separate goroutine for client broadcast process
	numTransactions := 2
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
	go clientBroadcastTx(t,
		broadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh,
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg := waitForClientBroadcastStatus(t,
		broadcastCtx,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

func TestScenarioClientBroadcastBeforeAndAfterBackendRestart(t *testing.T) {
	numChains := 0
	numRelays := 7

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numRelays)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numRelays)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Note: relays includes self
	// Using 2 seconds waitDuration because we shall broadcast BEFORE shutdown.
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numRelays)

	reuseRootDir := servers[0].GetReactor().GetNodeConfig().RootDir

	// STEP 1:
	// We execute a complete broadcast process.

	// Separate goroutine for client broadcast process
	numTransactions := 1
	testChainID := makeChainID("test chain")
	notifyCh := make(chan client.BroadcastStatus)
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
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)

	waitDuration := 5 * time.Second
	t.Logf("Waiting %.0fsec to shutdown backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	// STEP 2:
	//
	// CAUTION:
	// We mimic one of the relay shutting down completely, i.e. its process
	// is not managed, corrupted or stopped. Setting nil on the "old" instance
	// is only necessary during shutdown tests.

	// Stop the receiving backend, then start it again.
	err := servers[0].Close()
	require.NoError(t, err, "should shutdown server")

	waitDuration = 3 * time.Second
	t.Logf("Waiting %.0fsec to restart backend...", waitDuration.Seconds())
	time.Sleep(waitDuration)

	servers[0] = nil // Only for test
	resetRelay, newShutdownFn := ResetTestSingleCompatibleRelay(t,
		reuseRootDir,
		servers[1],
		0, // indexRelay (resetting relay-1)
		backendLogger,
	)
	defer newShutdownFn()

	resetRelay.MustStart()

	// STEP 3:
	//
	// The relay has been fully restarted and we can use the created
	// cancelable/expirable context to broadcast *more* transactions.

	// Separate goroutine for client broadcast process
	numTransactions = 2
	go clientBroadcastTx(t,
		broadcastCtx,
		resetRelay,
		relays,
		testChainID,
		numTransactions,
		notifyCh, // XXX re-use?
	)

	// Blocks the main thread until we consume from notifyCh.
	resultStatusMsg = waitForClientBroadcastStatus(t,
		broadcastCtx,
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

func TestScenarioClientBroadcastEnoughHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7
	numHealthy := (numRelays / 2) + 1

	servers, shutdownFn := ResetTestScenarioRelays(t, numChains, numHealthy)
	defer shutdownFn()

	require.NotEmpty(t, servers)
	require.Len(t, servers, numHealthy)

	// Set a custom logger to log all backend messages
	// For debug, change this logger instance
	backendLogger := cmtlog.TestingLogger().With("process", "relay-1")
	servers[0].SetLogger(backendLogger)

	// Note: relays includes self
	relays, broadcastCtx, cancelCtxFn := StartTestScenarioRelays(t,
		servers,
		2*time.Second,  // Time for backend
		20*time.Second, // Time for broadcast
	)

	defer cancelCtxFn()

	require.NotEmpty(t, relays)
	require.NotNil(t, broadcastCtx)
	require.Len(t, relays, numHealthy)

	// Note: this test consists in having *just enough* healthy relays actively
	// accept a client.BroadcastTx call. If enough healthy relays respond to a
	// broadcast operation, the operation should get accepted.
	for i := numHealthy; i < numRelays; i++ {
		relays = append(relays, "1.2.3.4:"+strconv.Itoa(1000+i))
	}

	// Separate goroutine for client broadcast process
	numTransactions := 2
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
	notifyCh := make(chan client.BroadcastStatus)
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
		notifyCh,
	)
	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, numTransactions)
}

// ----------------------------------------------------------------------------
// LEGACY Broadcast Test (Using CometBFT RPC Server)

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

func TestScenarioLegacyBroadcastSevenHealthyRelays(t *testing.T) {
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
	chainIds := servers[0].GetNetworks()
	testChainID := chainIds[0]
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
		chainIds := servers[0].GetNetworks()
		broadcastRawTxes(
			t,
			chainIds,
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

// ----------------------------------------------------------------------------
// Helpers

// Initializes numChains on a number of relays. This helper returns a list of
// configured multiplex backend instances and a shutdown functor.
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

// Starts the multiplex backend instances and creates a cancelable context
// for the broadcast operation(s). The relays MAY contain a peer ID.
func StartTestScenarioRelays(
	tb testing.TB,
	servers []*mx.MultiplexBackend,
	waitDuration time.Duration,
	timeoutDuration time.Duration,
) (relays []string, ctx context.Context, cancelCtxFn func()) {
	tb.Helper()
	require.NotEmpty(tb, servers)

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	if waitDuration.Seconds() > float64(0) {
		// Give the backend some time before starting broadcast context
		tb.Logf("Waiting %.0fsec to use node services...", waitDuration.Seconds())
		time.Sleep(waitDuration)
	}

	// Prepare the relays addresses
	relays = make([]string, 0, len(servers))
	for i := 0; i < len(servers); i++ {
		relays = append(relays, servers[i].GetListenAddress())
	}

	// Cancelable context to permit stopping by timeout
	ctx, cancelFn := context.WithTimeout(context.TODO(), timeoutDuration)
	return relays, ctx, cancelFn
}

func ResetTestSingleCompatibleRelay(
	tb testing.TB,
	rootDir string,
	otherRelay *mx.MultiplexBackend,
	indexRelay int,
	customLogger cmtlog.Logger,
) (*mx.MultiplexBackend, func()) {
	tb.Helper()
	require.NotNil(tb, otherRelay)

	baseCfg := otherRelay.GetReactor().GetNodeConfig()

	// Uses config.TestConfig() and compatible MultiplexConfig
	rootDirRelayX,
		globalCfgRelayX := ResetTestMultiplexNodeWithConfigAndPorts(
		tb,
		rootDir,
		"_"+strconv.Itoa(indexRelay+1), // metricsSuffix
		baseCfg.MultiplexConfig,
		uint16(50001+(indexRelay*100)), // 50001, 50101, 50201, 50301, 50401
		false,                          // don't create new root dir
	)

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfgRelayX.ChainSeeds {
		globalCfgRelayX.ChainSeeds[chainID] = ""
	}

	serverRelayX, err := mx.NewServer(
		&client.DefaultAcceptor{},
		globalCfgRelayX,
		customLogger,
	)
	require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(indexRelay))

	shutdownFn := func() {
		defer os.RemoveAll(rootDirRelayX)

		if serverRelayX != nil {
			err := serverRelayX.Close()
			assert.NoError(tb, err, "should shutdown reset server at index: "+strconv.Itoa(indexRelay))
		}
	}

	return serverRelayX, shutdownFn
}
