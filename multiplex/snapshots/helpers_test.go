package snapshots_test

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"testing"
	"time"

	protoio "github.com/cosmos/gogoproto/io"
	"github.com/stretchr/testify/require"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots"
	snapshottypes "github.com/ice-blockchain/cometbft/multiplex/snapshots/types"
	sm "github.com/ice-blockchain/cometbft/state"
)

func checksums(slice [][]byte) [][]byte {
	hasher := sha256.New()
	checksums := make([][]byte, len(slice))
	for i, chunk := range slice {
		hasher.Write(chunk)
		checksums[i] = hasher.Sum(nil)
		hasher.Reset()
	}
	return checksums
}

func hash(chunks [][]byte) []byte {
	hasher := sha256.New()
	for _, chunk := range chunks {
		hasher.Write(chunk)
	}
	return hasher.Sum(nil)
}

func makeChunks(chunks [][]byte) <-chan io.ReadCloser {
	ch := make(chan io.ReadCloser, len(chunks))
	for _, chunk := range chunks {
		ch <- io.NopCloser(bytes.NewReader(chunk))
	}
	close(ch)
	return ch
}

func readChunks(chunks <-chan io.ReadCloser) [][]byte {
	bodies := [][]byte{}
	for chunk := range chunks {
		body, err := io.ReadAll(chunk)
		if err != nil {
			panic(err)
		}
		bodies = append(bodies, body)
	}
	return bodies
}

// snapshotItems serialize a array of bytes as SnapshotItem_Store, and return the chunks.
func snapshotItems(items [][]byte) [][]byte {
	// copy the same parameters from the code
	// see stream.go
	snapshotChunkSize := uint64(10e6)
	snapshotBufferSize := int(snapshotChunkSize)

	ch := make(chan io.ReadCloser)
	go func() {
		chunkWriter := snapshots.NewChunkWriter(ch, snapshotChunkSize)
		bufWriter := bufio.NewWriterSize(chunkWriter, snapshotBufferSize)
		zWriter, _ := zlib.NewWriterLevel(bufWriter, 7)
		protoWriter := protoio.NewDelimitedWriter(zWriter)
		for _, item := range items {
			_ = protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
				Item: &snapshottypes.SnapshotItem_Store{
					Store: &snapshottypes.SnapshotStoreItem{
						Payload: item,
					},
				},
			})
		}
		_ = protoWriter.Close()
		_ = bufWriter.Flush()
		_ = chunkWriter.Close()
	}()

	var chunks [][]byte //nolint:prealloc
	for chunkBody := range ch {
		chunk, err := io.ReadAll(chunkBody)
		if err != nil {
			panic(err)
		}
		chunks = append(chunks, chunk)
	}

	return chunks
}

type mockStateSnapshotter struct {
	items [][]byte
}

var _ snapshots.StateSnapshotter = (*mockStateSnapshotter)(nil)

func (m *mockStateSnapshotter) Snapshot(height uint64, protoWriter protoio.Writer) error {
	for _, item := range m.items {
		if err := protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
			Item: &snapshottypes.SnapshotItem_Store{
				Store: &snapshottypes.SnapshotStoreItem{
					Payload: item,
				},
			},
		}); err != nil {
			return err
		}
	}
	return nil
}

func (m *mockStateSnapshotter) Restore(
	height uint64, format uint32, protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	if format == 0 {
		return snapshottypes.SnapshotItem{}, snapshottypes.ErrUnknownFormat
	}
	if m.items != nil {
		return snapshottypes.SnapshotItem{}, errors.New("already has contents")
	}

	var item snapshottypes.SnapshotItem
	m.items = [][]byte{}
	keyCount := 0
	for {
		item.Reset()
		err := protoReader.ReadMsg(&item)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf("invalid protobuf message: %w", err)
		}
		payload := item.GetStore()
		if payload == nil {
			break
		}
		m.items = append(m.items, payload.Payload)
		keyCount++
	}

	return item, nil
}

func (m *mockStateSnapshotter) GetStateMachine() (sm.State, error) {
	return sm.State{ChainID: "test-chain"}, nil
}

func (m *mockStateSnapshotter) AppHash() []byte {
	return []byte("0x01")
}

func (m *mockStateSnapshotter) SnapshotFormat() uint32 {
	return snapshottypes.CurrentFormat
}

func (m *mockStateSnapshotter) SupportedFormats() []uint32 {
	return []uint32{snapshottypes.CurrentFormat}
}

type mockErrorStateSnapshotter struct{}

var _ snapshots.StateSnapshotter = (*mockErrorStateSnapshotter)(nil)

func (m *mockErrorStateSnapshotter) Snapshot(height uint64, protoWriter protoio.Writer) error {
	return errors.New("mock snapshot error")
}

func (m *mockErrorStateSnapshotter) Restore(
	height uint64, format uint32, protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	return snapshottypes.SnapshotItem{}, errors.New("mock restore error")
}

func (m *mockErrorStateSnapshotter) GetStateMachine() (sm.State, error) {
	return sm.State{ChainID: "test-chain"}, nil
}

func (m *mockErrorStateSnapshotter) AppHash() []byte {
	return []byte("0x01")
}

func (m *mockErrorStateSnapshotter) SnapshotFormat() uint32 {
	return snapshottypes.CurrentFormat
}

func (m *mockErrorStateSnapshotter) SupportedFormats() []uint32 {
	return []uint32{snapshottypes.CurrentFormat}
}

// setupBusyManager creates a manager with an empty store that is busy creating a snapshot at height 1.
// The snapshot will complete when the returned closer is called.
func setupBusyManager(t *testing.T) *snapshots.Manager {
	t.Helper()
	store, err := snapshots.NewStore(t.TempDir())
	require.NoError(t, err)
	hung := newHungStateSnapshotter()
	mgr := snapshots.NewManager("test-chain", store, opts, hung, cmtlog.NewNopLogger())

	// Channel to ensure the test doesn't finish until the goroutine is done.
	// Without this, there are intermittent test failures about
	// the t.TempDir() cleanup failing due to the directory not being empty.
	done := make(chan struct{})

	go func() {
		defer close(done)
		_, err := mgr.Create(1)
		require.NoError(t, err)
	}()
	time.Sleep(10 * time.Millisecond)

	t.Cleanup(func() {
		<-done
	})

	t.Cleanup(hung.Close)

	return mgr
}

// hungStateSnapshotter can be used to test operations in progress. Call close to end the snapshot.
type hungStateSnapshotter struct {
	ch chan struct{}
}

var _ snapshots.StateSnapshotter = (*hungStateSnapshotter)(nil)

func newHungStateSnapshotter() *hungStateSnapshotter {
	return &hungStateSnapshotter{
		ch: make(chan struct{}),
	}
}

func (m *hungStateSnapshotter) Close() {
	close(m.ch)
}

func (m *hungStateSnapshotter) Snapshot(height uint64, protoWriter protoio.Writer) error {
	<-m.ch
	return nil
}

func (m *hungStateSnapshotter) Restore(
	height uint64, format uint32, protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	panic("not implemented")
}

func (m *hungStateSnapshotter) GetStateMachine() (sm.State, error) {
	return sm.State{ChainID: "test-chain"}, nil
}

func (m *hungStateSnapshotter) AppHash() []byte {
	return []byte("0x01")
}
