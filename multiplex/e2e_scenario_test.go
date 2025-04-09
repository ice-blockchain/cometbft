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

func TestScenarioServerBroadcastSevenHealthyRelays(t *testing.T) {
	numChains := 1
	numRelays := 7

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.TestingLogger().With("process", "relay-4")
	loggerRelay5 := cmtlog.TestingLogger().With("process", "relay-5")
	loggerRelay6 := cmtlog.TestingLogger().With("process", "relay-6")
	loggerRelay7 := cmtlog.TestingLogger().With("process", "relay-7")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
		loggerRelay5,
		loggerRelay6,
		loggerRelay7,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

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

func TestScenarioClientBroadcastSevenHealthyRelaysClientBroadcast(t *testing.T) {
	numChains := 1
	numRelays := 7

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")
	loggerRelay3 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-3")
	loggerRelay4 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-4")
	loggerRelay5 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-5")
	loggerRelay6 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-6")
	loggerRelay7 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-7")

	// Uses config.TestConfig() and random MultiplexConfig
	rootDirs,
		servers := ResetTestMultiplexBackendCompatibleRelays(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
		loggerRelay3,
		loggerRelay4,
		loggerRelay5,
		loggerRelay6,
		loggerRelay7,
	)
	require.NotEmpty(t, servers)
	require.Len(t, rootDirs, numRelays)
	require.Len(t, servers, numRelays)

	defer func() {
		for i := 0; i < len(servers); i++ {
			defer os.RemoveAll(rootDirs[i])

			if servers[i] != nil {
				err := servers[i].Close()
				assert.NoError(t, err, "should shutdown server at index: "+strconv.Itoa(i))
			}
		}
	}()

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	// Give the backend some time before broadcast
	// 5 seconds is long but bearable for a 7 relays setup.
	time.Sleep(5 * time.Second)

	// Prepare the data that we shall broadcast
	relaysAddresses := []string{}
	for i := 1; i < len(servers); i++ {
		relaysAddresses = append(relaysAddresses, servers[i].GetListenAddress())
	}

	testChainID := servers[0].GetNetworks()[0]
	chainInfo, err := mx.NewExtendedChainIDFromLegacy(testChainID)
	require.NoError(t, err, "should create correctly formatted ChainID")

	testUserAddress := chainInfo.GetUserAddress()
	testFingerprint := chainInfo.GetFingerprint()
	testTransactions := []client.Transaction{
		client.Transaction{Data: []byte{1, 2, 3}, Fingerprint: testFingerprint},
		client.Transaction{Data: []byte{4, 5, 6}, Fingerprint: testFingerprint},
	}

	// Set a custom logger to log all backend messages
	servers[0].SetLogger(cmtlog.TestingLogger().With("process", "relay-1"))

	// Cancelable context to permit stopping by timeout
	ctx, cancelCtxFn := context.WithTimeout(context.TODO(), 20*time.Second)
	defer cancelCtxFn()

	// Separate goroutine for client broadcast process
	notifyCh := make(chan client.BroadcastStatus)
	go func() {
		t.Log("Initializing broadcast goroutine")

		multiplexClient := mx.NewClient(
			mx.WithBackend(servers[0]),
			mx.WithNotifier(&client.StatusNotifier{}),
		)

		t.Log("Sending call to BroadcastTx")

		multiplexClient.BroadcastTx(ctx,
			testUserAddress,
			relaysAddresses,
			notifyCh,
			testTransactions...,
		)

		t.Log("Finalizing broadcast goroutine")
	}()

	t.Log("Waiting for BroadcastStatus update from client")

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
				t.Error("Timed out waiting for broadcast status")
				return // cancels context
			}
		}
	}(&resultStatusMsg)

	// Waits for a status update or timeout
	wg.Wait()

	txHashes := []string{}
	for _, bzHash := range resultStatusMsg.TxHashes {
		txHashes = append(txHashes, fmt.Sprintf("%X", bzHash))
	}

	t.Logf("Status from BroadcastTx: <%d, %s, %v>",
		len(txHashes),
		txHashes,
		resultStatusMsg.Error)

	assert.NotNil(t, resultStatusMsg)
	assert.NoError(t, resultStatusMsg.Error, "should not contain error status")
	assert.Len(t, resultStatusMsg.TxHashes, len(testTransactions))
}
