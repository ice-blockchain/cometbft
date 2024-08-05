package multiplex

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/internal/autofile"
	cs "github.com/cometbft/cometbft/internal/consensus"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	cmttypes "github.com/cometbft/cometbft/types"
)

// ----------------------------------------------------------------------------
// Benchmarks

func benchmarkWALWrite(b *testing.B, wals []*cs.BaseWAL, data cs.WALMessage, numWalFiles int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	b.SetParallelism(numWalFiles) // 1 proc per WAL file
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should perform a write in WAL
		for pb.Next() {
			// cmtrand is thread-safe
			walIdx := randomizer.Intn(numWalFiles)
			wals[walIdx].Write(data)
		}
	})
}

func benchmarkMultiplexWALWrite(b *testing.B, wals MultiplexWAL, data cs.WALMessage, numWalFiles int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()
	scopeHashes := make([]string, len(wals))

	// slice of map keys
	i := 0
	for k := range wals {
		scopeHashes[i] = k
		i++
	}

	b.SetParallelism(numWalFiles) // 1 proc per WAL file
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should perform a write in WAL
		for pb.Next() {
			// cmtrand is thread-safe
			walIdx := randomizer.Intn(numWalFiles)

			wals[scopeHashes[walIdx]].Write(data)
		}
	})
}

// ----------------------------------------------------------------------------
// BaseWAL implementation benchmarks

func BenchmarkMultiwalFilesWriteRaw1K(b *testing.B) {
	numWalFiles := 1000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-1k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWALWrite(b, walPtrs, data, numWalFiles)
}

func BenchmarkMultiwalFilesWriteRaw2K(b *testing.B) {
	numWalFiles := 2000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-2k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWALWrite(b, walPtrs, data, numWalFiles)
}

// CAUTION: This benchmark currently breaks due to a bottleneck in autofile
func BenchmarkMultiwalFilesWriteRaw10K(b *testing.B) {
	numWalFiles := 10000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-10k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWALWrite(b, walPtrs, data, numWalFiles)
}

// ----------------------------------------------------------------------------
// MultiplexWAL implementation benchmarks

func BenchmarkMultiplexWALFilesWriteRaw1K(b *testing.B) {
	numWalFiles := 1000

	// Reset benchmark fs
	rootDir, multiplexWAL := ResetMultiplexWALTestRoot(b, "test-multiplex-wal-files-write-1k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkMultiplexWALWrite(b, *multiplexWAL, data, numWalFiles)
}

func BenchmarkMultiplexWALFilesWriteRaw2K(b *testing.B) {
	numWalFiles := 2000

	// Reset benchmark fs
	rootDir, multiplexWAL := ResetMultiplexWALTestRoot(b, "test-multiplex-wal-files-write-2k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkMultiplexWALWrite(b, *multiplexWAL, data, numWalFiles)
}

func BenchmarkMultiplexWALFilesWriteRaw10K(b *testing.B) {
	numWalFiles := 10000

	// Reset benchmark fs
	rootDir, multiplexWAL := ResetMultiplexWALTestRoot(b, "test-multiplex-wal-files-write-10k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkMultiplexWALWrite(b, *multiplexWAL, data, numWalFiles)
}

// CAUTION: This benchmark currently breaks due to a system limit "too many open files"
func BenchmarkMultiplexWALFilesWriteRaw100K(b *testing.B) {
	numWalFiles := 100000

	// Reset benchmark fs
	rootDir, multiplexWAL := ResetMultiplexWALTestRoot(b, "test-multiplex-wal-files-write-100k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data := getWALMessage(b)

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkMultiplexWALWrite(b, *multiplexWAL, data, numWalFiles)
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetMultiwalTestRoot(
	t testing.TB,
	testName string,
	numWalFiles int,
) (string, []string, []*cs.BaseWAL) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Create numWalFiles .wal files in temp and create cs.WAL instances
	walFiles, walPtrs, err := createTempWalFiles(t, rootDir, numWalFiles)
	require.NoError(t, err, "should create wal files in temp directory")

	return rootDir, walFiles, walPtrs
}

func ResetMultiplexWALTestRoot(
	t testing.TB,
	testName string,
	numWalFiles int,
) (string, *MultiplexWAL) {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	// Create numWalFiles .wal files in temp and create cs.WAL instances
	wals, err := createTempWalMultiplex(t, rootDir, numWalFiles)
	require.NoError(t, err, "should create wal files in temp directory")

	return rootDir, wals
}

// ----------------------------------------------------------------------------

// Note: This helper does not start the WAL service
func createTempWalFiles(
	t testing.TB,
	rootDir string,
	numWalFiles int,
) ([]string, []*cs.BaseWAL, error) {
	t.Helper()

	walPtrs := make([]*cs.BaseWAL, numWalFiles)
	walFiles := make([]string, numWalFiles)
	for i := 0; i < numWalFiles; i++ {
		walFile, err := os.CreateTemp(rootDir, "*.wal")
		if err != nil {
			return walFiles, walPtrs, err
		}

		wal, err := cs.NewWAL(walFile.Name(),
			autofile.GroupCheckDuration(60*time.Second), // XXX too long
			autofile.GroupHeadSizeLimit(100*1024*1024),  // 100M
		)
		require.NoError(t, err, "should create new WAL instance")

		walPtrs[i] = wal
		walFile.Close() // no need to keep open
	}

	return walFiles, walPtrs, nil
}

// createTempWalMultiplex attempts to create a WAL multiplex.
func createTempWalMultiplex(
	t testing.TB,
	rootDir string,
	numScopeHashes int,
) (*MultiplexWAL, error) {
	t.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	walFiles := make(map[string]string, numScopeHashes)
	for i := 0; i < numScopeHashes; i++ {
		scopeHash := randomizer.Str(32) // random 32 bytes user scope hash

		// create a unique, concurrency-safe wal directory under rootDir
		walDir := filepath.Join(rootDir, scopeHash[:16])
		err := os.MkdirAll(walDir, os.ModePerm)
		if err != nil {
			panic(err)
		}

		walFile, err := os.CreateTemp(walDir, "*.wal")
		if err != nil {
			return nil, err
		}

		walFiles[scopeHash] = walFile.Name()
		walFile.Close() // no need to keep open
	}

	multiplexWAL, err := NewScopedWAL(walFiles,
		autofile.GroupCheckDuration(60*time.Second), // XXX too long
		autofile.GroupHeadSizeLimit(100*1024*1024),  // 100M
	)
	require.NoError(t, err, "should create new WAL multiplex")

	return multiplexWAL, nil
}

func getWALMessage(t testing.TB) cs.WALMessage {
	t.Helper()

	return cmttypes.EventDataRoundState{
		Height: 1,
		Round:  1,
		Step:   "1",
	}
}
