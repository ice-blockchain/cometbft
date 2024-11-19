package snapshots

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sync"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots/types"
)

// ----------------------------------------------------------------------------
// Manager
//
// Manager manages snapshot and restore operations for a replicated chain,
// making sure only a single long-running operation is in progress at any
// given time, and provides convenience methods mirroring the ABCI interface.
//
// Although the ABCI interface (and this manager) passes chunks as byte slices,
// the internal snapshot/restore APIs use IO streams (i.e. chan io.ReadCloser),
// for two reasons:
//
//  1. In the future, ABCI should support streaming. Consider e.g. InitChain during chain
//     upgrades, which currently passes the entire chain state as an in-memory byte slice.
//     https://github.com/tendermint/tendermint/issues/5184
//
//  2. io.ReadCloser streams automatically propagate IO errors, and can pass arbitrary
//     errors via io.Pipe.CloseWithError().
type Manager struct {
	// attaches the chain identifier
	ChainID string

	// store is the snapshot store where all completed snapshots are persisted.
	store *Store
	opts  config.SnapshotOptions

	// stateSnapshotter is the snapshotter for the commitment state.
	stateSnapshotter StateSnapshotter

	logger cmtlog.Logger

	mtx               sync.Mutex
	operation         operation
	chRestore         chan<- uint32
	chRestoreDone     <-chan restoreDone
	restoreSnapshot   *types.Snapshot
	restoreChunkIndex uint32
}

// operation represents a Manager operation. Only one operation can be in progress at a time.
type operation string

// restoreDone represents the result of a restore operation.
type restoreDone struct {
	complete bool  // if true, restore completed successfully (not prematurely)
	err      error // if non-nil, restore errored
}

const (
	opNone     operation = ""
	opSnapshot operation = "snapshot"
	opPrune    operation = "prune"
	opRestore  operation = "restore"

	chunkBufferSize                 = 4
	chunkIDBufferSize               = 1024
	defaultStorageChannelBufferSize = 1024

	snapshotMaxItemSize = int(64e6) // SDK has no key/value size limit, so we set an arbitrary limit
)

var ErrOptsZeroSnapshotInterval = errors.New("snapshot-interval must not be 0")

// NewManager creates a new manager.
func NewManager(
	chainID string,
	store *Store,
	opts config.SnapshotOptions,
	stateSnapshotter StateSnapshotter,
	logger cmtlog.Logger,
) *Manager {
	return &Manager{
		ChainID:          chainID,
		store:            store,
		opts:             opts,
		stateSnapshotter: stateSnapshotter,
		logger:           logger,
	}
}

// begin starts an operation, or errors if one is in progress. It manages the mutex itself.
func (m *Manager) begin(op operation) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.beginLocked(op)
}

// beginLocked begins an operation while already holding the mutex.
func (m *Manager) beginLocked(op operation) error {
	if op == opNone {
		return errors.New("can't begin a none operation")
	}
	if m.operation != opNone {
		return fmt.Errorf("a %v operation is in progress", m.operation)
	}
	m.operation = op
	return nil
}

// end ends the current operation.
func (m *Manager) end() {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	m.endLocked()
}

// endLocked ends the current operation while already holding the mutex.
func (m *Manager) endLocked() {
	m.operation = opNone
	if m.chRestore != nil {
		close(m.chRestore)
		m.chRestore = nil
	}
	m.chRestoreDone = nil
	m.restoreSnapshot = nil
	m.restoreChunkIndex = 0
}

// GetChainID returns the attached chain identifier.
func (m *Manager) GetChainID() string {
	return m.ChainID
}

// GetSnapshotter returns the state snapshotter.
func (m *Manager) GetSnapshotter() StateSnapshotter {
	return m.stateSnapshotter
}

// GetInterval returns snapshot interval represented in heights.
func (m *Manager) GetInterval() uint64 {
	return m.opts.Interval
}

// GetKeepRecent returns snapshot keep-recent represented in heights.
func (m *Manager) GetKeepRecent() uint32 {
	return m.opts.KeepRecent
}

// GetSnapshotBlockRetentionHeights returns the number of heights needed
// for block retention. Blocks since the oldest available snapshot must be
// available for state sync nodes to catch up (oldest because a node may be
// restoring an old snapshot while a new snapshot was taken).
func (m *Manager) GetSnapshotBlockRetentionHeights() int64 {
	return int64(m.opts.Interval * uint64(m.opts.KeepRecent))
}

// Create creates a snapshot and returns its metadata.
func (m *Manager) Create(height uint64) (*types.Snapshot, error) {
	if m == nil {
		return nil, errors.New("snapshot manager is nil")
	}

	err := m.begin(opSnapshot)
	if err != nil {
		return nil, err
	}
	defer m.end()

	latest, err := m.store.GetLatest()
	if err != nil {
		return nil, fmt.Errorf("failed to examine latest snapshot: %w", err)
	}
	if latest != nil && latest.Height >= height {
		return nil, fmt.Errorf("a more recent snapshot already exists at height %v", latest.Height)
	}

	// Spawn goroutine to generate snapshot chunks and pass their io.ReadClosers through a channel
	ch := make(chan io.ReadCloser)
	go m.createSnapshot(height, ch)

	return m.store.Save(height, m.opts.Format, ch)
}

// createSnapshot do the heavy work of snapshotting after the validations of request are done
// the produced chunks are written to the channel.
func (m *Manager) createSnapshot(height uint64, ch chan<- io.ReadCloser) {
	streamWriter := NewStreamWriter(ch)
	if streamWriter == nil {
		return
	}
	defer func() {
		if err := streamWriter.Close(); err != nil {
			streamWriter.CloseWithError(err)
		}
	}()

	if err := m.stateSnapshotter.Snapshot(height, streamWriter); err != nil {
		streamWriter.CloseWithError(err)
		return
	}
}

// List lists snapshots, mirroring ABCI ListSnapshots. It can be concurrent with other operations.
func (m *Manager) List() ([]*types.Snapshot, error) {
	return m.store.List()
}

// LoadChunk loads a chunk into a byte slice, mirroring ABCI LoadChunk. It can be called
// concurrently with other operations. If the chunk does not exist, nil is returned.
func (m *Manager) LoadChunk(height uint64, format, chunk uint32) ([]byte, error) {
	reader, err := m.store.LoadChunk(height, format, chunk)
	if err != nil {
		return nil, err
	}
	if reader == nil {
		return nil, nil
	}
	defer reader.Close()

	return io.ReadAll(reader)
}

// Prune prunes snapshots, if no other operations are in progress.
func (m *Manager) Prune(retain uint32) (uint64, error) {
	err := m.begin(opPrune)
	if err != nil {
		return 0, err
	}
	defer m.end()
	return m.store.Prune(retain)
}

// Restore begins an async snapshot restoration, mirroring ABCI OfferSnapshot. Chunks must be fed
// via RestoreChunk() until the restore is complete or a chunk fails.
func (m *Manager) Restore(snapshot types.Snapshot) error {
	if snapshot.Chunks == 0 {
		return errors.New("invalid metadata: no chunks")
	}
	if uint32(len(snapshot.Metadata.ChunkHashes)) != snapshot.Chunks {
		return fmt.Errorf("invalid metadata: snapshot has %v chunk hashes, but %v chunks",
			uint32(len(snapshot.Metadata.ChunkHashes)),
			snapshot.Chunks)
	}
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// check multistore supported format preemptive
	if snapshot.Format != m.opts.Format {
		return fmt.Errorf("invalid format: snapshot format %v", snapshot.Format)
	}
	if snapshot.Height == 0 {
		return errors.New("cannot restore snapshot at height 0")
	}

	// TODO(midas): TBI, does this constraint apply to cometbft or only cosmos-sdk?
	if snapshot.Height > uint64(math.MaxInt64) {
		return fmt.Errorf("snapshot height %v cannot exceed %v", snapshot.Height, int64(math.MaxInt64))
	}

	err := m.beginLocked(opRestore)
	if err != nil {
		return err
	}

	// Start an asynchronous snapshot restoration, passing chunks and completion status via channels.
	chChunkIDs := make(chan uint32, chunkIDBufferSize)
	chDone := make(chan restoreDone, 1)

	dir := m.store.pathSnapshot(snapshot.Height, snapshot.Format)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("failed to create snapshot directory %q: %w", dir, err)
	}

	chChunks := m.loadChunkStream(snapshot.Height, snapshot.Format, chChunkIDs)

	go func() {
		err := m.doRestoreSnapshot(snapshot, chChunks)
		chDone <- restoreDone{
			complete: err == nil,
			err:      err,
		}
		close(chDone)
	}()

	m.chRestore = chChunkIDs
	m.chRestoreDone = chDone
	m.restoreSnapshot = &snapshot
	m.restoreChunkIndex = 0
	return nil
}

func (m *Manager) loadChunkStream(height uint64, format uint32, chunkIDs <-chan uint32) <-chan io.ReadCloser {
	chunks := make(chan io.ReadCloser, chunkBufferSize)
	go func() {
		defer close(chunks)

		for chunkID := range chunkIDs {
			chunk, err := m.store.loadChunkFile(height, format, chunkID)
			if err != nil {
				m.logger.Error("load chunk file failed", "height", height, "format", format, "chunk", chunkID, "err", err)
				break
			}
			chunks <- chunk
		}
	}()

	return chunks
}

// doRestoreSnapshot do the heavy work of snapshot restoration after preliminary checks on request have passed.
func (m *Manager) doRestoreSnapshot(snapshot types.Snapshot, chChunks <-chan io.ReadCloser) error {
	dir := m.store.pathSnapshot(snapshot.Height, snapshot.Format)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return fmt.Errorf("failed to create snapshot directory %q: %w", dir, err)
	}

	var nextItem types.SnapshotItem
	streamReader, err := NewStreamReader(chChunks)
	if err != nil {
		return err
	}
	defer streamReader.Close()

	nextItem, err = m.stateSnapshotter.Restore(snapshot.Height, snapshot.Format, streamReader)
	if err != nil {
		return err
	}

	for {
		if nextItem.Item == nil {
			// end of stream
			break
		}
	}

	return nil
}

// RestoreChunk adds a chunk to an active snapshot restoration, mirroring ABCI ApplySnapshotChunk.
// Chunks must be given until the restore is complete, returning true, or a chunk errors.
func (m *Manager) RestoreChunk(chunk []byte) (bool, error) {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	if m.operation != opRestore {
		return false, errors.New("no restore operation in progress")
	}

	if int(m.restoreChunkIndex) >= len(m.restoreSnapshot.Metadata.ChunkHashes) {
		return false, errors.New("received unexpected chunk")
	}

	// Check if any errors have occurred yet.
	select {
	case done := <-m.chRestoreDone:
		m.endLocked()
		if done.err != nil {
			return false, done.err
		}
		return false, errors.New("restore ended unexpectedly")
	default:
	}

	// Verify the chunk hash.
	hash := sha256.Sum256(chunk)
	expected := m.restoreSnapshot.Metadata.ChunkHashes[m.restoreChunkIndex]
	if !bytes.Equal(hash[:], expected) {
		return false, types.ErrChunkHashMismatch
	}

	if err := m.store.saveChunkContent(chunk, m.restoreChunkIndex, m.restoreSnapshot); err != nil {
		return false, fmt.Errorf("save chunk content %d error: %w", m.restoreChunkIndex, err)
	}

	// Pass the chunk to the restore, and wait for completion if it was the final one.
	m.chRestore <- m.restoreChunkIndex
	m.restoreChunkIndex++

	if int(m.restoreChunkIndex) >= len(m.restoreSnapshot.Metadata.ChunkHashes) {
		close(m.chRestore)
		m.chRestore = nil

		// the chunks are all written into files, we can save the snapshot to the db,
		// even if the restoration may not completed yet.
		if err := m.store.saveSnapshot(m.restoreSnapshot); err != nil {
			return false, fmt.Errorf("save restoring snapshot error: %w", err)
		}

		done := <-m.chRestoreDone
		m.endLocked()
		if done.err != nil {
			return false, done.err
		}
		if !done.complete {
			return false, errors.New("restore ended prematurely")
		}

		return true, nil
	}
	return false, nil
}

// RestoreLocalSnapshot restores app state from a local snapshot.
func (m *Manager) RestoreLocalSnapshot(height uint64, format uint32) error {
	snapshot, ch, err := m.store.Load(height, format)
	if err != nil {
		return err
	}

	if snapshot == nil {
		return fmt.Errorf("snapshot doesn't exist, height: %d, format: %d", height, format)
	}

	m.mtx.Lock()
	defer m.mtx.Unlock()

	err = m.beginLocked(opRestore)
	if err != nil {
		return err
	}
	defer m.endLocked()

	return m.doRestoreSnapshot(*snapshot, ch)
}

// SnapshotIfApplicable takes a snapshot of the current state if we are on a snapshot height.
// It also prunes any old snapshots.
func (m *Manager) SnapshotIfApplicable(height int64) {
	if m == nil {
		return
	}
	if !m.shouldTakeSnapshot(height) {
		m.logger.Debug("snapshot is skipped", "height", height)
		return
	}
	// start the routine after need to create a snapshot
	go m.snapshot(height)
}

// shouldTakeSnapshot returns true is snapshot should be taken at height.
func (m *Manager) shouldTakeSnapshot(height int64) bool {
	return m.opts.Interval > 0 && uint64(height)%m.opts.Interval == 0
}

func (m *Manager) snapshot(height int64) {
	m.logger.Info("creating state snapshot", "height", height)

	if height <= 0 {
		m.logger.Error("snapshot height must be positive", "height", height)
		return
	}

	snapshot, err := m.Create(uint64(height))
	if err != nil {
		m.logger.Error("failed to create state snapshot", "height", height, "err", err)
		return
	}

	m.logger.Info("completed state snapshot", "height", height, "format", snapshot.Format)

	if m.opts.KeepRecent > 0 {
		m.logger.Debug("pruning state snapshots")

		pruned, err := m.Prune(m.opts.KeepRecent)
		if err != nil {
			m.logger.Error("Failed to prune state snapshots", "err", err)
			return
		}

		m.logger.Debug("pruned state snapshots", "pruned", pruned)
	}
}

// Close the snapshot database.
func (m *Manager) Close() error { return nil }
