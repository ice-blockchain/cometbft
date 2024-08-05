package multiplex

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	"github.com/cometbft/cometbft/internal/test"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
)

func benchmarkMakeBlock(b *testing.B, dbs []dbm.DB, states []sm.State, numDatabases int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	b.SetParallelism(numDatabases) // 1 proc per state
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a block from state
		for pb.Next() {
			stateIdx := randomizer.Intn(numDatabases)
			state := states[stateIdx]

			// we always use the TEST PRIV VALIDATOR
			proposerAddress := state.Validators.Validators[0].Address
			stateVersion := state.Version.Consensus
			block := makeBlock(state, 2, new(types.Commit), proposerAddress)

			// test we set some fields
			assert.Equal(b, stateVersion, block.Version)
			assert.Equal(b, proposerAddress, block.ProposerAddress)
		}
	})
}

// ----------------------------------------------------------------------------
// cometbft State implementation benchmarks

func BenchmarkStateMakeBlock1K(b *testing.B) {
	numDatabases := 1000

	// Reset benchmark fs
	rootDir, dbPtrs, statePtrs := ResetStateTestRoot(b, "test-state-make-block-1k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkMakeBlock(b, dbPtrs, statePtrs, numDatabases)
}

func BenchmarkStateMakeBlock2K(b *testing.B) {
	numDatabases := 2000

	// Reset benchmark fs
	rootDir, dbPtrs, statePtrs := ResetStateTestRoot(b, "test-state-make-block-2k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkMakeBlock(b, dbPtrs, statePtrs, numDatabases)
}

func BenchmarkStateMakeBlock10K(b *testing.B) {
	numDatabases := 10000

	// Reset benchmark fs
	rootDir, dbPtrs, statePtrs := ResetStateTestRoot(b, "test-state-make-block-10k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkMakeBlock(b, dbPtrs, statePtrs, numDatabases)
}

// CAUTION: This benchmark currently breaks due to a time limitation (for benchtime=30s).
func BenchmarkStateMakeBlock100K(b *testing.B) {
	numDatabases := 100000

	// Reset benchmark fs
	rootDir, dbPtrs, statePtrs := ResetStateTestRoot(b, "test-state-make-block-100k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkMakeBlock(b, dbPtrs, statePtrs, numDatabases)
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetStateTestRoot(t testing.TB, testName string, numDatabases int) (string, []dbm.DB, []sm.State) {
	t.Helper()

	// Creates many *prefixed* database instances using global db
	rootDir, dbPtrs := ResetPrefixDBTestRoot(t, "state_", numDatabases)

	// Prepares slice of state machines
	statePtrs := make([]sm.State, numDatabases)

	// Creates many *prefixed* database instances using global db
	for i := 0; i < numDatabases; i++ {

		config := test.ResetTestRootWithChainID("state_", "state-chain-"+strconv.Itoa(i))

		stateDb := dbPtrs[i]
		stateStore := sm.NewStore(stateDb, sm.StoreOptions{
			DiscardABCIResponses: false,
		})

		state, err := stateStore.LoadFromDBOrGenesisFile(config.GenesisFile())
		require.NoError(t, err, "expected no error on LoadStateFromDBOrGenesisFile")

		statePtrs[i] = state
	}

	return rootDir, dbPtrs, statePtrs
}

// ----------------------------------------------------------------------------

func makeBlock(state sm.State, height int64, c *types.Commit, proposerAddress types.Address) *types.Block {
	return state.MakeBlock(
		height,
		[]types.Tx{},
		c,
		nil,
		proposerAddress,
	)
}
