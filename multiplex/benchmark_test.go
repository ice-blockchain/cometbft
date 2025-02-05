package multiplex_test

import (
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
)

func BenchmarkMultiplexNodeTriggerConsensus(t *testing.B) {
	numChains := 1
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	servers, shutdownRoutineFn := ResetTestMultiplexBenchmark(
		t,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(t, servers)

	// Shutdown routine
	defer shutdownRoutineFn()

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	// Uses CometBFT RPC Port (DiscoveryPort + 2)
	rpcPort := strconv.Itoa(50001 + (1 * 100) + 2) // 50103 (second relay RPC)
	chainID := servers[0].GetNetworks()[0]

	t.SetParallelism(100000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using the second relay's rpc.
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			nodeRPC := "http://127.0.0.1:" + rpcPort
			rpcPath := "/broadcast_tx_commit/" + chainID

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + rpcPath + "?tx=\"" + txData + "\"")
		}
	})
}

func ResetTestMultiplexBenchmark(
	tb testing.TB,
	numChains int,
	numRelays int,
	customLoggers ...cmtlog.Logger,
) ([]*mx.MultiplexBackend, func()) {
	tb.Helper()

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

	// Start the node backends
	for i := 0; i < len(servers); i++ {
		servers[i].MustStart()
	}

	return servers, shutdownFn
}
