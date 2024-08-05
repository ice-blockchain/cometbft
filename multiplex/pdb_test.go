package multiplex

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
)

// ----------------------------------------------------------------------------
// cometbft-db PrefixDB implementation benchmarks

func BenchmarkPrefixDBSetKey1K(b *testing.B) {
	numDatabases := 1000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetPrefixDBTestRoot(b, "test-prefix-db-set-key-1k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkPrefixDBSetKey2K(b *testing.B) {
	numDatabases := 2000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetPrefixDBTestRoot(b, "test-prefix-db-set-key-2k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkPrefixDBSetKey10K(b *testing.B) {
	numDatabases := 10000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetPrefixDBTestRoot(b, "test-prefix-db-set-key-10k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkPrefixDBSetKey100K(b *testing.B) {
	numDatabases := 100000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetPrefixDBTestRoot(b, "test-prefix-db-set-key-100k", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

func BenchmarkPrefixDBSetKey1M(b *testing.B) {
	numDatabases := 1000000

	// Reset benchmark fs
	rootDir, dbPtrs := ResetPrefixDBTestRoot(b, "test-prefix-db-set-key-1mio", numDatabases)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numDatabases
	benchmarkDBWrite(b, dbPtrs, numDatabases)
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetPrefixDBTestRoot(
	t testing.TB,
	testName string,
	numDatabases int,
) (string, []dbm.DB) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	dbDir, err := os.MkdirTemp(rootDir, "cometbft-prefixdb-*")
	require.NoError(t, err, "should create database directory in temp directory")

	// Open numDatabases database adapters
	dbPtrs, err := createTempPrefixDB(t, dbDir, numDatabases)
	require.NoError(t, err, "should create PrefixDB instances in temp directory")

	return rootDir, dbPtrs
}

// ----------------------------------------------------------------------------

func createTempPrefixDB(
	t testing.TB,
	dbDir string,
	numDatabases int,
) ([]dbm.DB, error) {
	t.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	// Prepares slice of database adapters
	dbPtrs := make([]dbm.DB, numDatabases)
	dbType := dbm.BackendType("memdb")

	// Creates one global database
	db, err := dbm.NewDB("global-db", dbType, dbDir)
	require.NoError(t, err, "should create new DB instance")

	// Creates many *prefixed* database instances using global db
	for i := 0; i < numDatabases; i++ {
		scopeHash := randomizer.Str(8) // random 8 bytes fingerprint
		pdb := dbm.NewPrefixDB(db, []byte(scopeHash))

		dbPtrs[i] = pdb
		pdb.Close()
	}

	return dbPtrs, nil
}
