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

func benchmarkRawTxThroughput(
	b *testing.B,
	networks []string,
	numChains,
	numRelays,
	numProcs int,
) {
	// We shall randomly pick a node index and values
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	b.SetParallelism(numProcs)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using the second relay's rpc.
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			randomRelay := randomizer.Intn(numRelays) + 1
			randomChain := randomizer.Intn(numChains)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			// Uses CometBFT RPC Port (DiscoveryPort + 2)
			rpcPort := strconv.Itoa(50001 + (randomRelay * 100) + 2) // random relay RPC
			chainID := networks[randomChain]

			nodeRPC := "http://127.0.0.1:" + rpcPort
			rpcPath := "/broadcast_tx_commit/" + chainID

			// For debug, uncomment the following line
			// b.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + rpcPath + "?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexRelaysTriggerConsensus(b *testing.B) {
	numChains := 1
	numRelays := 2

	// For debug, change the loggers to cmtlog.TestingLogger()
	loggerRelay1 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-1")
	loggerRelay2 := cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-2")

	servers, shutdownRoutineFn := ResetTestMultiplexBenchmark(
		b,
		numChains,
		numRelays,
		loggerRelay1,
		loggerRelay2,
	)
	require.NotEmpty(b, servers)

	// Shutdown routine
	defer shutdownRoutineFn()

	benchmarkRawTxThroughput(
		b,
		servers[0].GetNetworks(),
		numChains,
		numRelays,
		100000, // SetParallelism()
	)
}

func BenchmarkMultiplexRelaysWithThreeChainsAndThreeRelays(b *testing.B) {
	numChains := 3
	numRelays := 3

	relayLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		// For debug, change the loggers to cmtlog.TestingLogger()
		relayLoggers[i] = cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-"+strconv.Itoa(i+1))
	}

	servers, shutdownRoutineFn := ResetTestMultiplexBenchmark(
		b,
		numChains,
		numRelays,
		relayLoggers...,
	)
	require.NotEmpty(b, servers)

	// Shutdown routine
	defer shutdownRoutineFn()

	benchmarkRawTxThroughput(
		b,
		servers[0].GetNetworks(),
		numChains,
		numRelays,
		100000, // SetParallelism()
	)
}

func BenchmarkMultiplexRelaysWithTenChainsAndThreeRelays(b *testing.B) {
	numChains := 10
	numRelays := 3

	relayLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		// For debug, change the loggers to cmtlog.TestingLogger()
		relayLoggers[i] = cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-"+strconv.Itoa(i+1))
	}

	servers, shutdownRoutineFn := ResetTestMultiplexBenchmark(
		b,
		numChains,
		numRelays,
		relayLoggers...,
	)
	require.NotEmpty(b, servers)

	// Shutdown routine
	defer shutdownRoutineFn()

	benchmarkRawTxThroughput(
		b,
		servers[0].GetNetworks(),
		numChains,
		numRelays,
		100000, // SetParallelism()
	)
}

func BenchmarkMultiplexRelaysWithHundredChainsAndThreeRelays(b *testing.B) {
	numChains := 100
	numRelays := 3

	relayLoggers := make([]cmtlog.Logger, numRelays)
	for i := 0; i < numRelays; i++ {
		// For debug, change the loggers to cmtlog.TestingLogger()
		relayLoggers[i] = cmtlog.NewNopLogger() // cmtlog.TestingLogger().With("process", "relay-"+strconv.Itoa(i+1))
	}

	servers, shutdownRoutineFn := ResetTestMultiplexBenchmark(
		b,
		numChains,
		numRelays,
		relayLoggers...,
	)
	require.NotEmpty(b, servers)

	// Shutdown routine
	defer shutdownRoutineFn()

	benchmarkRawTxThroughput(
		b,
		servers[0].GetNetworks(),
		numChains,
		numRelays,
		1000000, // SetParallelism()
	)
}

// ----------------------------------------------------------------------------
// Helpers

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
