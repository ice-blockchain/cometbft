package multiplex

import (
	"encoding/hex"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/crypto/merkle"
	"github.com/cometbft/cometbft/internal/autofile"
	cs "github.com/cometbft/cometbft/internal/consensus"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	cmttypes "github.com/cometbft/cometbft/types"
)

// ----------------------------------------------------------------------------
// Benchmarks

func benchmarkWalWrite(b *testing.B, wals []*cs.BaseWAL, data cs.WALMessage, numWalFiles int) {
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

func BenchmarkMultiwalFilesWriteRaw1K(b *testing.B) {
	numWalFiles := 1000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-1k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data, _ := hex.DecodeString("0102030405060708")

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWalWrite(b, walPtrs, cs.WALMessage(data), numWalFiles)
}

func BenchmarkMultiwalFilesWriteRaw2K(b *testing.B) {
	numWalFiles := 2000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-2k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data, _ := hex.DecodeString("0102030405060708")

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWalWrite(b, walPtrs, cs.WALMessage(data), numWalFiles)
}

// CAUTION: This benchmark currently breaks due to a too high amount of WAL files
func BenchmarkMultiwalFilesWriteRaw10K(b *testing.B) {
	numWalFiles := 10000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-10k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks an 8-byte WAL raw message
	data, _ := hex.DecodeString("0102030405060708")

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWalWrite(b, walPtrs, cs.WALMessage(data), numWalFiles)
}

func BenchmarkMultiwalFilesWriteBlock1K(b *testing.B) {
	numWalFiles := 1000

	// Reset benchmark fs
	rootDir, _, walPtrs := ResetMultiwalTestRoot(b, "test-multi-wal-files-write-2k", numWalFiles)
	defer os.RemoveAll(rootDir)

	// Mocks a block part message with 8 bytes 0-message
	data := &cs.BlockPartMessage{
		Height: 1,
		Round:  1,
		Part: &cmttypes.Part{
			Index: 1,
			Bytes: make([]byte, 1),
			Proof: merkle.Proof{
				Total:    1,
				Index:    1,
				LeafHash: make([]byte, 8), // 8-bytes 0 message
			},
		},
	}

	// Runs b.RunParallel() with GOMAXPROCS=numWalFiles
	benchmarkWalWrite(b, walPtrs, data, numWalFiles)
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
			autofile.GroupCheckDuration(60*time.Second),
			autofile.GroupHeadSizeLimit(100*1024*1024), // 100M
		)
		require.NoError(t, err, "should create new WAL instance")

		walPtrs[i] = wal
	}

	return walFiles, walPtrs, nil
}
