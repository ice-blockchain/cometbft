package multiplex

import (
	"encoding/hex"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	cmtrand "github.com/cometbft/cometbft/internal/rand"
)

func BenchmarkMultiplexNode1MsConsensusManyChains100(t *testing.B) {
	// initial setup for this benchmark:
	// - 100 replicated chains ("users")
	// - 7 relays per replicated chain ("per user")

	testName := "mx_test_node_1ms_consensus_many_chains_100"
	userScopes := createTestUserScopes(t, 100) // 100 users
	numValidators := 7                         // 7 relays / "validators"

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.NotEmpty(t, privValidators, "should create a valid privValidator set")

	// Starts 1 of the 7 validators
	config, r, _ := assertStartMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs,
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(100) // make equal to number of users!
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			time.Sleep(1 * time.Millisecond)
		}
	})
}

func BenchmarkMultiplexNode1MsConsensusManyChains200(t *testing.B) {
	// initial setup for this benchmark:
	// - 200 replicated chains ("users")
	// - 7 relays per replicated chain ("per user")

	testName := "mx_test_node_1ms_consensus_many_chains_200"
	userScopes := createTestUserScopes(t, 200) // 200 users
	numValidators := 7                         // 7 relays / "validators"

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.NotEmpty(t, privValidators, "should create a valid privValidator set")

	// Starts 1 of the 7 validators
	config, r, _ := assertStartMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs,
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(200) // make equal to number of users!
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			time.Sleep(1 * time.Millisecond)
		}
	})
}

func BenchmarkMultiplexNode1MsConsensusManyChains500(t *testing.B) {
	// initial setup for this benchmark:
	// - 500 replicated chains ("users")
	// - 7 relays per replicated chain ("per user")

	testName := "mx_test_node_1ms_consensus_many_chains_500"
	userScopes := createTestUserScopes(t, 500) // 500 users
	numValidators := 7                         // 7 relays / "validators"

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.NotEmpty(t, privValidators, "should create a valid privValidator set")

	// Starts 1 of the 7 validators
	config, r, _ := assertStartMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs,
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(500) // make equal to number of users!
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			time.Sleep(1 * time.Millisecond)
		}
	})
}

func BenchmarkMultiplexNode1MsConsensusManyChains1K(t *testing.B) {
	// initial setup for this benchmark:
	// - 1k replicated chains ("users")
	// - 7 relays per replicated chain ("per user")

	testName := "mx_test_node_1ms_consensus_many_chains_1k"
	userScopes := createTestUserScopes(t, 1000) // 1k users
	numValidators := 7                          // 7 relays / "validators"

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.NotEmpty(t, privValidators, "should create a valid privValidator set")

	// Starts 1 of the 7 validators
	config, r, _ := assertStartMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs,
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000) // make equal to number of users!
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			time.Sleep(1 * time.Millisecond)
		}
	})
}

func BenchmarkMultiplexNode1MsConsensusManyChains10K(t *testing.B) {
	// initial setup for this benchmark:
	// - 10k replicated chains ("users")
	// - 7 relays per replicated chain ("per user")

	testName := "mx_test_node_1ms_consensus_many_chains_10k"
	userScopes := createTestUserScopes(t, 10000) // 1k users
	numValidators := 7                           // 7 relays / "validators"

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.NotEmpty(t, privValidators, "should create a valid privValidator set")

	// Starts 1 of the 7 validators
	config, r, _ := assertStartMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs,
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(10000) // make equal to number of users!
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			time.Sleep(1 * time.Millisecond)
		}
	})
}

// ----------------------------------------------------------------------------

func createTestUserScopes(
	t testing.TB,
	numScopes int,
) map[string][]string {
	t.Helper()

	outScopes := make(map[string][]string, 0)
	for i := 0; i < numScopes; i++ {
		userAddress := cmtrand.Bytes(20)
		userAddressHex := strings.ToUpper(hex.EncodeToString(userAddress))
		randScopeLen := cmtrand.Intn(13) + 3 // virtual max 16-bytes, min 3-bytes
		userScope := cmtrand.Str(randScopeLen)

		if _, ok := outScopes[userAddressHex]; ok {
			outScopes[userAddressHex] = append(outScopes[userAddressHex], userScope)
		} else {
			outScopes[userAddressHex] = []string{userScope}
		}
	}

	return outScopes
}
