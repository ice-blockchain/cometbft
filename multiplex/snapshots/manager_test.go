package snapshots_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots/types"
)

var opts = config.NewSnapshotOptions(1, 1500, 2)

func TestManager_List(t *testing.T) {
	store := setupStore(t)
	stateSnashotter := &mockStateSnapshotter{}
	manager := snapshots.NewManager("test-chain", store, opts, stateSnashotter, cmtlog.NewNopLogger())

	mgrList, err := manager.List()
	require.NoError(t, err)
	storeList, err := store.List()
	require.NoError(t, err)

	require.NotEmpty(t, storeList)
	assert.Equal(t, storeList, mgrList)

	// list should not block or error on busy managers
	manager = setupBusyManager(t)
	list, err := manager.List()
	require.NoError(t, err)
	assert.Equal(t, []*types.Snapshot{}, list)

	require.NoError(t, manager.Close())
}

func TestManager_LoadChunk(t *testing.T) {
	store := setupStore(t)
	manager := snapshots.NewManager("test-chain", store, opts, &mockStateSnapshotter{}, cmtlog.NewNopLogger())

	// Existing chunk should return body
	chunk, err := manager.LoadChunk(2, 1, 1)
	require.NoError(t, err)
	assert.Equal(t, []byte{2, 1, 1}, chunk)

	// Missing chunk should return nil
	chunk, err = manager.LoadChunk(2, 1, 9)
	require.NoError(t, err)
	assert.Nil(t, chunk)

	// LoadChunk should not block or error on busy managers
	manager = setupBusyManager(t)
	chunk, err = manager.LoadChunk(2, 1, 0)
	require.NoError(t, err)
	assert.Nil(t, chunk)
}

func TestManager_Take(t *testing.T) {
	store := setupStore(t)
	bytesLatestHeight3 := [][]byte{
		{3, 2, 0}, {3, 2, 1}, {3, 2, 2},
	}

	stateSnapshotter := &mockStateSnapshotter{
		items: bytesLatestHeight3,
	}

	// see store_test.go setupStore() for height=3
	expectChunks := snapshotItems(bytesLatestHeight3)
	manager := snapshots.NewManager("test-chain", store, opts, stateSnapshotter, cmtlog.NewNopLogger())

	// nil manager should return error
	_, err := (*snapshots.Manager)(nil).Create(1)
	require.Error(t, err)

	// creating a snapshot at a lower height than the latest should error
	// see store_test.go setupStore() for latest Height=3
	_, err = manager.Create(2)
	require.Error(t, err)

	// creating a snapshot at a higher height should be fine, and should return it
	// see store_test.go setupStore() for latest Height=3
	snapshot, err := manager.Create(4)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 4,
		Format: stateSnapshotter.SnapshotFormat(),
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)

	storeSnapshot, chunks, err := store.Load(snapshot.Height, snapshot.Format)
	require.NoError(t, err)
	assert.Equal(t, snapshot, storeSnapshot)
	assert.Equal(t, expectChunks, readChunks(chunks))

	// creating a snapshot while a different snapshot is being created should error
	manager = setupBusyManager(t)
	_, err = manager.Create(9)
	require.Error(t, err)
}

func TestManager_Prune(t *testing.T) {
	store := setupStore(t)
	manager := snapshots.NewManager("test-chain", store, opts, &mockStateSnapshotter{}, cmtlog.NewNopLogger())

	pruned, err := manager.Prune(2)
	require.NoError(t, err)
	assert.EqualValues(t, 1, pruned)

	list, err := manager.List()
	require.NoError(t, err)
	assert.Len(t, list, 3)

	// Prune should error while a snapshot is being taken
	manager = setupBusyManager(t)
	_, err = manager.Prune(2)
	require.Error(t, err)
}

func TestManager_Restore(t *testing.T) {
	store := setupStore(t)
	target := &mockStateSnapshotter{}

	opts.Format = 2 // snapshot format 2 (see store_test.go)
	manager := snapshots.NewManager("test-chain", store, opts, target, cmtlog.NewNopLogger())

	expectItems := [][]byte{
		{1, 2, 3},
		{4, 5, 6},
		{7, 8, 9},
	}

	chunks := snapshotItems(expectItems)

	// Restore errors on invalid format
	err := manager.Restore(types.Snapshot{
		Height:   3,
		Format:   0,
		Hash:     []byte{1, 2, 3},
		Chunks:   uint32(len(chunks)),
		Metadata: types.Metadata{ChunkHashes: checksums(chunks)},
	})
	require.Error(t, err)
	require.Errorf(t, err, "invalid format: snapshot format %v", "0")

	// Restore errors on no chunks
	err = manager.Restore(types.Snapshot{Height: 3, Format: 2, Hash: []byte{1, 2, 3}})
	require.Error(t, err)

	// Restore errors on chunk and chunkhashes mismatch
	err = manager.Restore(types.Snapshot{
		Height:   3,
		Format:   2,
		Hash:     []byte{1, 2, 3},
		Chunks:   4,
		Metadata: types.Metadata{ChunkHashes: checksums(chunks)},
	})
	require.Error(t, err)

	// Starting a restore works
	err = manager.Restore(types.Snapshot{
		Height:   3,
		Format:   2,
		Hash:     []byte{1, 2, 3},
		Chunks:   1,
		Metadata: types.Metadata{ChunkHashes: checksums(chunks)},
	})
	require.NoError(t, err)

	// While the restore is in progress, any other operations fail
	_, err = manager.Create(4)
	require.Error(t, err)

	_, err = manager.Prune(1)
	require.Error(t, err)

	// Feeding an invalid chunk should error due to invalid checksum, but not abort restoration.
	_, err = manager.RestoreChunk([]byte{9, 9, 9})
	require.Error(t, err)
	require.ErrorIs(t, err, types.ErrChunkHashMismatch)

	// Feeding the chunks should work
	for i, chunk := range chunks {
		done, err := manager.RestoreChunk(chunk)
		require.NoError(t, err)
		if i == len(chunks)-1 {
			assert.True(t, done)
		} else {
			assert.False(t, done)
		}
	}

	assert.Equal(t, expectItems, target.items)

	// The snapshot is saved in local snapshot store
	snapshots, err := store.List()
	require.NoError(t, err)
	snapshot := snapshots[0]
	require.Equal(t, uint64(3), snapshot.Height)
	require.Equal(t, uint32(2), snapshot.Format) // snapshot format 2 (see store_test.go)

	// Starting a new restore should fail now, because the target already has contents.
	err = manager.Restore(types.Snapshot{
		Height:   3,
		Format:   2, // snapshot format 2 (see store_test.go)
		Hash:     []byte{1, 2, 3},
		Chunks:   3,
		Metadata: types.Metadata{ChunkHashes: checksums(chunks)},
	})
	require.Error(t, err)

	// But if we clear out the target we should be able to start a new restore. This time we'll
	// fail it with a checksum error. That error should stop the operation, so that we can do
	// a prune operation right after.
	target.items = nil
	err = manager.Restore(types.Snapshot{
		Height:   3,
		Format:   2, // snapshot format 2 (see store_test.go)
		Hash:     []byte{1, 2, 3},
		Chunks:   1,
		Metadata: types.Metadata{ChunkHashes: checksums(chunks)},
	})
	require.NoError(t, err)

	// Feeding the chunks should work
	for i, chunk := range chunks {
		done, err := manager.RestoreChunk(chunk)
		require.NoError(t, err)
		if i == len(chunks)-1 {
			assert.True(t, done)
		} else {
			assert.False(t, done)
		}
	}
}

func TestManager_TakeError(t *testing.T) {
	snapshotter := &mockErrorStateSnapshotter{}
	store, err := snapshots.NewStore(t.TempDir())
	require.NoError(t, err)
	manager := snapshots.NewManager("test-chain", store, opts, snapshotter, cmtlog.NewNopLogger())

	_, err = manager.Create(1)
	require.Error(t, err)
}

func TestSnapshot_Take_Restore(t *testing.T) {
	store := setupStore(t)
	bytesLatestHeight3 := [][]byte{
		{3, 2, 0}, {3, 2, 1}, {3, 2, 2},
	}

	stateSnapshotter := &mockStateSnapshotter{
		items: bytesLatestHeight3,
	}

	// see store_test.go setupStore() for height=3
	expectChunks := snapshotItems(bytesLatestHeight3)

	opts.Format = 2 // snapshot format 2 (see store_test.go)
	manager := snapshots.NewManager("test-chain", store, opts, stateSnapshotter, cmtlog.NewNopLogger())

	// creating a snapshot at a higher height should be fine, and should return it
	// see store_test.go setupStore() for latest Height=3
	snapshot, err := manager.Create(4)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 4,
		Format: 2,
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)

	storeSnapshot, chunks, err := store.Load(snapshot.Height, snapshot.Format)
	require.NoError(t, err)
	assert.Equal(t, snapshot, storeSnapshot)
	assert.Equal(t, expectChunks, readChunks(chunks))

	err = manager.Restore(*snapshot)
	require.NoError(t, err)

	// Feeding the chunks should work
	for i, chunk := range readChunks(chunks) {
		done, err := manager.RestoreChunk(chunk)
		require.NoError(t, err)
		if i == len(chunks)-1 {
			assert.True(t, done)
		} else {
			assert.False(t, done)
		}
	}

	// The snapshot is saved in local snapshot store
	snapshots, err := store.List()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshots[0].Height)
	require.Equal(t, opts.Format, snapshots[0].Format)

	// Starting a new restore should fail now, because the target already has contents.
	err = manager.Restore(*snapshot)
	require.Error(t, err)

	storeSnapshot, chunks, err = store.Load(snapshot.Height, snapshot.Format)
	require.NoError(t, err)
	assert.Equal(t, snapshot, storeSnapshot)
	assert.Equal(t, expectChunks, readChunks(chunks))

	// Feeding the chunks should work
	for i, chunk := range readChunks(chunks) {
		done, err := manager.RestoreChunk(chunk)
		require.NoError(t, err)
		if i == len(chunks)-1 {
			assert.True(t, done)
		} else {
			assert.False(t, done)
		}
	}

	assert.Equal(t, bytesLatestHeight3, stateSnapshotter.items)

	snapshots, err = store.List()
	require.NoError(t, err)
	require.Equal(t, uint64(4), snapshots[0].Height)
	require.Equal(t, opts.Format, snapshots[0].Format)
}

func TestSnapshot_Take_Prune(t *testing.T) {
	store := setupStore(t)
	bytesLatestHeight3 := [][]byte{
		{3, 2, 0}, {3, 2, 1}, {3, 2, 2},
	}

	stateSnapshotter := &mockStateSnapshotter{
		items: bytesLatestHeight3,
	}

	// see store_test.go setupStore() for height=3
	expectChunks := snapshotItems(bytesLatestHeight3)

	opts.Format = 2 // snapshot format 2 (see store_test.go)
	manager := snapshots.NewManager("test-chain", store, opts, stateSnapshotter, cmtlog.NewNopLogger())

	// creating a snapshot at a higher height should be fine, and should return it
	// see store_test.go setupStore() for latest Height=3
	snapshot, err := manager.Create(4)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 4,
		Format: 2,
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)

	pruned, err := manager.Prune(1)
	require.NoError(t, err)
	assert.EqualValues(t, 4, pruned)

	// creating a snapshot at a same height 4, should be error
	// since we prune all the previous snapshot except the latest at height 4
	_, err = manager.Create(4)
	require.Error(t, err)

	// prune all
	pruned, err = manager.Prune(0)
	require.NoError(t, err)
	assert.EqualValues(t, 1, pruned)

	// creating a snapshot at a same height 4, should be true since we prune all the previous snapshot
	snapshot, err = manager.Create(4)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 4,
		Format: 2,
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)

	storeSnapshot, chunks, err := store.Load(snapshot.Height, snapshot.Format)
	require.NoError(t, err)
	assert.Equal(t, snapshot, storeSnapshot)
	assert.Equal(t, expectChunks, readChunks(chunks))

	pruned, err = manager.Prune(2)
	require.NoError(t, err)
	assert.EqualValues(t, 0, pruned)

	list, err := manager.List()
	require.NoError(t, err)
	assert.Len(t, list, 1)

	// Prune should error while a snapshot is being taken
	manager = setupBusyManager(t)
	_, err = manager.Prune(2)
	require.Error(t, err)
}

func TestSnapshot_Pruning_Take_Snapshot_Parallel(t *testing.T) {
	store := setupStore(t)
	bytesLatestHeight3 := [][]byte{
		{3, 2, 0}, {3, 2, 1}, {3, 2, 2},
	}

	stateSnapshotter := &mockStateSnapshotter{
		items: bytesLatestHeight3,
	}

	// see store_test.go setupStore() for height=3
	expectChunks := snapshotItems(bytesLatestHeight3)

	opts.Format = 2 // snapshot format 2 (see store_test.go)
	manager := snapshots.NewManager("test-chain", store, opts, stateSnapshotter, cmtlog.NewNopLogger())

	var prunedCount uint64
	// try take snapshot and pruning parallel while prune operation begins first
	go func() {
		checkError := func() bool {
			_, err := manager.Create(4)
			return err != nil
		}

		require.Eventually(t, checkError, time.Millisecond*200, time.Millisecond)
	}()

	prunedCount, err := manager.Prune(1)
	require.NoError(t, err)
	assert.EqualValues(t, 3, prunedCount)

	// creating a snapshot at a same height 4, should be true since the prune has finished
	snapshot, err := manager.Create(4)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 4,
		Format: 2,
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)

	// try take snapshot and pruning parallel while snapshot operation begins first
	go func() {
		checkError := func() bool {
			_, err = manager.Prune(1)
			return err != nil
		}

		require.Eventually(t, checkError, time.Millisecond*200, time.Millisecond)
	}()

	snapshot, err = manager.Create(5)
	require.NoError(t, err)

	assert.Equal(t, &types.Snapshot{
		Height: 5,
		Format: 2,
		Chunks: 1,
		Hash:   []uint8{0x4b, 0xc5, 0xa4, 0x46, 0xc4, 0x53, 0x19, 0x30, 0x93, 0xc7, 0xd9, 0xd6, 0xcf, 0xb7, 0x42, 0x79, 0xbe, 0x5, 0x85, 0x6d, 0xa7, 0x4e, 0x92, 0x43, 0xe3, 0x8c, 0xed, 0x7e, 0xb4, 0xa7, 0x6d, 0x71},
		Metadata: types.Metadata{
			ChunkHashes: checksums(expectChunks),
		},
	}, snapshot)
}

func TestSnapshot_SnapshotIfApplicable(t *testing.T) {
	store := setupStore(t)

	bytesLatestHeight3 := [][]byte{
		{3, 2, 0}, {3, 2, 1}, {3, 2, 2},
	}

	stateSnapshotter := &mockStateSnapshotter{
		items: bytesLatestHeight3,
	}

	snapshotOpts := config.NewSnapshotOptions(1, 1, 1)

	opts.Format = 2 // snapshot format 2 (see store_test.go)
	manager := snapshots.NewManager("test-chain", store, snapshotOpts, stateSnapshotter, cmtlog.NewNopLogger())

	manager.SnapshotIfApplicable(4)

	checkLatestHeight := func() bool {
		latestSnapshot, _ := store.GetLatest()
		return latestSnapshot.Height == 4
	}

	require.Eventually(t, checkLatestHeight, time.Second*10, time.Second)

	pruned, err := manager.Prune(1)
	require.NoError(t, err)
	require.Equal(t, uint64(0), pruned)
}
