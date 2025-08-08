// Package statesync may be internalized (made private) in future  releases.
// XXX Deprecated.
package statesync

import (
	"context"
	"errors"
	"sort"
	"time"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	ssproto "github.com/ice-blockchain/cometbft/api/cometbft/statesync/v1"
	"github.com/ice-blockchain/cometbft/config"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
)

const (
	// SnapshotChannel exchanges snapshot metadata.
	SnapshotChannel = byte(0x60)
	// ChunkChannel exchanges chunk contents.
	ChunkChannel = byte(0x61)
	// recentSnapshots is the number of recent snapshots to send and receive per peer.
	recentSnapshots = 10
)

// Reactor handles state sync, both restoring snapshots for the local node and serving snapshots
// for other nodes.
type Reactor struct {
	p2p.BaseReactor

	// ChainID contains the statesync'd chain identifier (multiplex)
	ChainID string

	cfg       config.StateSyncConfig
	conn      proxy.AppConnSnapshot
	connQuery proxy.AppConnQuery
	tempDir   string
	metrics   *Metrics

	// This will only be set when a state sync is in progress. It is used to feed received
	// snapshots and chunks into the sync.
	mtx    cmtsync.RWMutex
	syncer *syncer
}

// NewReactor creates a new state sync reactor.
func NewReactor(
	ctx context.Context,
	cfg config.StateSyncConfig,
	conn proxy.AppConnSnapshot,
	connQuery proxy.AppConnQuery,
	metrics *Metrics,
	reactorOptions ...func(*Reactor),
) *Reactor {
	r := &Reactor{
		cfg:       cfg,
		conn:      conn,
		connQuery: connQuery,
		metrics:   metrics,
	}

	// permits populating ChainID
	for _, option := range reactorOptions {
		option(r)
	}

	r.BaseReactor = *p2p.NewBaseReactor(ctx, "StateSync", r)

	return r
}

// CAUTION: This method is used to determine a static list of channels
// for the multiplex implementation. Do not use for state-sync.
func NewEmptyReactor(ctx context.Context) *Reactor {
	r := &Reactor{}
	r.BaseReactor = *p2p.NewBaseReactor(ctx, "StateSync", r)
	return r
}

// ReactorWithChainID sets the ChainID of a statesync reactor.
func ReactorWithChainID(chainID string) func(*Reactor) {
	return func(r *Reactor) {
		r.ChainID = chainID
	}
}

// GetChannels implements p2p.Reactor.
func (*Reactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID:                  SnapshotChannel,
			Priority:            5,
			SendQueueCapacity:   10,
			RecvMessageCapacity: snapshotMsgSize,
			MessageType:         &ssproto.Message{},
		},
		{
			ID:                  ChunkChannel,
			Priority:            3,
			SendQueueCapacity:   10,
			RecvMessageCapacity: chunkMsgSize,
			MessageType:         &ssproto.Message{},
		},
	}
}

// OnStart implements p2p.Reactor.
func (*Reactor) OnStart(ctx context.Context) error {
	return nil
}

// OnReset should not execute any business logic, but instead must be
// defined as it is called from [Service#Reset], which permits to later
// start back the service with stopped/started correctly reset.
func (r *Reactor) OnReset(ctx context.Context) error {
	r.Logger.Info("Statesync reactor service reset",
		"chain_id", r.ChainID,
	)
	return nil
}

// AddPeer implements p2p.Reactor.
func (r *Reactor) AddPeer(peer *p2p.PeerImpl) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if r.syncer != nil {
		r.syncer.AddPeer(peer)
	}
}

// RemovePeer implements p2p.Reactor.
func (r *Reactor) RemovePeer(peer *p2p.PeerImpl, _ any) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	if r.syncer != nil {
		r.syncer.RemovePeer(peer)
	}
}

// Receive implements p2p.Reactor.
func (r *Reactor) Receive(e p2p.Envelope) {
	if !r.IsRunning() {
		return
	}

	err := validateMsg(e.Message)
	if err != nil {
		r.Logger.Error("Invalid message", "peer", e.Src, "msg", e.Message, "err", err)
		r.Switch.StopPeerForError(e.Src, err)
		return
	}

	switch e.ChannelID {
	case SnapshotChannel:
		switch msg := e.Message.(type) {
		case *ssproto.SnapshotsRequest:
			snapshots, err := r.recentSnapshots(recentSnapshots)
			if err != nil {
				r.Logger.Error("Failed to fetch snapshots", "err", err)
				return
			}
			for _, snapshot := range snapshots {
				r.Logger.Debug("Advertising snapshot", "height", snapshot.Height,
					"format", snapshot.Format, "peer", e.Src.ID())
				e.Src.Send(r.ChainID, p2p.Envelope{
					ChainID:   r.ChainID,
					ChannelID: e.ChannelID,
					Message: &ssproto.SnapshotsResponse{
						Height:   snapshot.Height,
						Format:   snapshot.Format,
						Chunks:   snapshot.Chunks,
						Hash:     snapshot.Hash,
						Metadata: snapshot.Metadata,
					},
				})
			}

		case *ssproto.SnapshotsResponse:
			r.mtx.RLock()
			defer r.mtx.RUnlock()
			if r.syncer == nil {
				r.Logger.Debug("Received unexpected snapshot, no state sync in progress")
				return
			}
			r.Logger.Debug("Received snapshot", "height", msg.Height, "format", msg.Format, "peer", e.Src.ID())
			_, err := r.syncer.AddSnapshot(e.Src, &snapshot{
				Height:   msg.Height,
				Format:   msg.Format,
				Chunks:   msg.Chunks,
				Hash:     msg.Hash,
				Metadata: msg.Metadata,
			})
			// TODO: We may want to consider punishing the peer for certain errors
			if err != nil {
				r.Logger.Error("Failed to add snapshot", "height", msg.Height, "format", msg.Format,
					"peer", e.Src.ID(), "err", err)
				return
			}

		default:
			r.Logger.Error("Received unknown message %T", msg)
		}

	case ChunkChannel:
		switch msg := e.Message.(type) {
		case *ssproto.ChunkRequest:
			r.Logger.Debug("Received chunk request", "height", msg.Height, "format", msg.Format,
				"chunk", msg.Index, "peer", e.Src.ID())

			// Inject the ChainID for access in ABCI
			ctx := context.TODO()
			ctx = context.WithValue(ctx, client.KeyChainID, r.ChainID)

			resp, err := r.conn.LoadSnapshotChunk(ctx, &abci.LoadSnapshotChunkRequest{
				Height: msg.Height,
				Format: msg.Format,
				Chunk:  msg.Index,
			})
			if err != nil {
				r.Logger.Error("Failed to load chunk", "height", msg.Height, "format", msg.Format,
					"chunk", msg.Index, "err", err)
				return
			}
			r.Logger.Debug("Sending chunk", "height", msg.Height, "format", msg.Format,
				"chunk", msg.Index, "peer", e.Src.ID())
			e.Src.Send(r.ChainID, p2p.Envelope{
				ChainID:   r.ChainID,
				ChannelID: ChunkChannel,
				Message: &ssproto.ChunkResponse{
					Height:  msg.Height,
					Format:  msg.Format,
					Index:   msg.Index,
					Chunk:   resp.Chunk,
					Missing: resp.Chunk == nil,
				},
			})

		case *ssproto.ChunkResponse:
			r.mtx.RLock()
			defer r.mtx.RUnlock()
			if r.syncer == nil {
				r.Logger.Debug("Received unexpected chunk, no state sync in progress", "peer", e.Src.ID())
				return
			}
			r.Logger.Debug("Received chunk, adding to sync", "height", msg.Height, "format", msg.Format,
				"chunk", msg.Index, "peer", e.Src.ID())
			_, err := r.syncer.AddChunk(&chunk{
				Height: msg.Height,
				Format: msg.Format,
				Index:  msg.Index,
				Chunk:  msg.Chunk,
				Sender: e.Src.ID(),
			})
			if err != nil {
				r.Logger.Error("Failed to add chunk", "height", msg.Height, "format", msg.Format,
					"chunk", msg.Index, "err", err)
				return
			}

		default:
			r.Logger.Error("Received unknown message %T", msg)
		}

	default:
		r.Logger.Error("Received message on invalid channel %x", e.ChannelID)
	}
}

// recentSnapshots fetches the n most recent snapshots from the app.
func (r *Reactor) recentSnapshots(n uint32) ([]*snapshot, error) {
	// Inject the ChainID for access in ABCI
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, r.ChainID)

	resp, err := r.conn.ListSnapshots(ctx, &abci.ListSnapshotsRequest{})
	if err != nil {
		return nil, err
	}
	sort.Slice(resp.Snapshots, func(i, j int) bool {
		a := resp.Snapshots[i]
		b := resp.Snapshots[j]
		switch {
		case a.Height > b.Height:
			return true
		case a.Height == b.Height && a.Format > b.Format:
			return true
		default:
			return false
		}
	})
	snapshots := make([]*snapshot, 0, n)
	for i, s := range resp.Snapshots {
		if i >= recentSnapshots {
			break
		}
		snapshots = append(snapshots, &snapshot{
			Height:   s.Height,
			Format:   s.Format,
			Chunks:   s.Chunks,
			Hash:     s.Hash,
			Metadata: s.Metadata,
		})
	}
	return snapshots, nil
}

// Sync runs a state sync, returning the new state and last commit at the snapshot height.
// The caller must store the state and commit in the state database and block store.
func (r *Reactor) Sync(stateProvider StateProvider, discoveryTime time.Duration) (sm.State, *types.Commit, error) {
	r.mtx.Lock()
	if r.syncer != nil {
		r.mtx.Unlock()
		return sm.State{}, nil, errors.New("a state sync is already in progress")
	}
	r.metrics.Syncing.Set(1)
	r.syncer = newSyncer(r.cfg, r.Logger, r.conn, r.connQuery, stateProvider, r.tempDir, syncerWithChainID(r.ChainID))
	r.mtx.Unlock()

	hook := func() {
		r.Logger.Debug("Requesting snapshots from known peers")
		// Request snapshots from all currently connected peers

		r.Switch.Broadcast(r.ChainID, p2p.Envelope{
			ChainID:   r.ChainID,
			ChannelID: SnapshotChannel,
			Message:   &ssproto.SnapshotsRequest{},
		})
	}

	hook()

	state, commit, err := r.syncer.SyncAny(discoveryTime, hook)

	r.mtx.Lock()
	r.syncer = nil
	r.metrics.Syncing.Set(0)
	r.mtx.Unlock()
	return state, commit, err
}
