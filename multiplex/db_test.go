package multiplex

import (
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
)

// ----------------------------------------------------------------------------
// Benchmarks

func benchmarkDBWrite(b *testing.B, dbs []dbm.DB, numDatabases int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	// Mocks a key-value pair
	key := []byte(`key`)
	data := []byte(`value`)

	b.SetParallelism(numDatabases) // 1 proc per DB
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should perform a write in DB
		for pb.Next() {
			// cmtrand is thread-safe
			dbIdx := randomizer.Intn(numDatabases)
			dbs[dbIdx].Set(key, data)
		}
	})
}

// ----------------------------------------------------------------------------
// cometbft-db MemDB implementation benchmarks

func BenchmarkMultiDBSetKey1K(b *testing.B) {
	numDatabases := 1000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(b, "test-multi-db-set-key-1k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkMultiDBSetKey2K(b *testing.B) {
	numDatabases := 2000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(b, "test-multi-db-set-key-2k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkMultiDBSetKey10K(b *testing.B) {
	numDatabases := 10000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(b, "test-multi-db-set-key-10k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

// CAUTION: This benchmark currently breaks due to a bottleneck in cometbft-db
func BenchmarkMultiDBSetKey100K(b *testing.B) {
	numDatabases := 100000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetMultiDBTestRoot(b, "test-multi-db-set-key-100k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetMultiDBTestRoot(
	t testing.TB,
	testName string,
	numDatabases int,
) (string, []dbm.DB) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Open numDatabases database adapters
	dbPtrs, err := createTempMemDB(t, rootDir, numDatabases)
	require.NoError(t, err, "should create MemDB instances in temp directory")

	return rootDir, dbPtrs
}

// ----------------------------------------------------------------------------

func createTempMemDB(
	t testing.TB,
	rootDir string,
	numDatabases int,
) ([]dbm.DB, error) {
	t.Helper()

	dbPtrs := make([]dbm.DB, numDatabases)
	dbType := dbm.BackendType("memdb")

	for i := 0; i < numDatabases; i++ {
		dbDir, err := os.MkdirTemp(rootDir, "cometbft-memdb-*")
		if err != nil {
			return dbPtrs, err
		}

		dbId := "db-" + strconv.Itoa(i)
		db, err := dbm.NewDB(dbId, dbType, dbDir)
		require.NoError(t, err, "should create new DB instance")

		dbPtrs[i] = db
		db.Close() // noop for memdb
	}

	return dbPtrs, nil
}
