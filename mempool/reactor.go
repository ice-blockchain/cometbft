package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	protomem "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cfg "github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/types"
)

// Reactor handles mempool tx broadcasting amongst peers.
// It maintains a map from peer ID to counter, to prevent gossiping txs to the
// peers you received it from.
type Reactor struct {
	p2p.BaseReactor
	config  *cfg.MempoolConfig
	mempool *CListMempool

	waitSync   atomic.Bool
	waitSyncCh chan struct{} // for signaling when to start receiving and sending txs

	// Semaphores to keep track of how many connections to peers are active for broadcasting
	// transactions. Each semaphore has a capacity that puts an upper bound on the number of
	// connections for different groups of peers.
	activePersistentPeersSemaphore    *semaphore.Weighted
	activeNonPersistentPeersSemaphore *semaphore.Weighted

	// Inject custom transaction verification with an acceptor implementation.
	txAcceptor  client.Acceptor
	userAddress string
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(
	config *cfg.MempoolConfig,
	mempool *CListMempool,
	waitSync bool,
	options ...func(*Reactor),
) *Reactor {
	memR := &Reactor{
		config:   config,
		mempool:  mempool,
		waitSync: atomic.Bool{},
	}

	// Enable overwrite of some optional properties.
	for _, option := range options {
		option(memR)
	}

	memR.BaseReactor = *p2p.NewBaseReactor("Mempool", memR)
	if waitSync {
		memR.waitSync.Store(true)
		memR.waitSyncCh = make(chan struct{})
	}
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// WithAcceptor is an option helper to inject a custom acceptor implementation
// which accepts a user address and an acceptor.
func WithAcceptor(
	userAddress string,
	acceptor client.Acceptor,
) func(*Reactor) {
	return func(r *Reactor) {
		r.txAcceptor = acceptor
		r.userAddress = userAddress
	}
}

// GetMempoolPtr returns a pointer to the CListMempool object.
func (memR *Reactor) GetMempoolPtr() *CListMempool {
	return memR.mempool
}

// SetLogger sets the Logger on the reactor and the underlying mempool.
func (memR *Reactor) SetLogger(l log.Logger) {
	memR.Logger = l
	memR.mempool.SetLogger(l)
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart() error {
	if memR.WaitSync() {
		memR.Logger.Info("Starting reactor in sync mode: tx propagation will start once sync completes")
	}
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	return nil
}

// GetChannels implements Reactor by returning the list of channels for this
// reactor.
func (memR *Reactor) GetChannels() []*p2p.ChannelDescriptor {
	largestTx := make([]byte, memR.config.MaxTxBytes)
	batchMsg := protomem.Message{
		Sum: &protomem.Message_Txs{
			Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
		},
	}

	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: batchMsg.Size(),
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer p2p.Peer) {
	if memR.config.Broadcast {
		go func() {
			// Always forward transactions to unconditional peers.
			if !memR.Switch.IsPeerUnconditional(peer.ID()) {
				// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
				var peerSemaphore *semaphore.Weighted
				if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
					peerSemaphore = memR.activePersistentPeersSemaphore
				} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
					peerSemaphore = memR.activeNonPersistentPeersSemaphore
				}

				if peerSemaphore != nil {
					for peer.IsRunning() {
						// Block on the semaphore until a slot is available to start gossiping with this peer.
						// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
						ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
						// Block sending transactions to peer until one of the connections become
						// available in the semaphore.
						err := peerSemaphore.Acquire(ctxTimeout, 1)
						cancel()

						if err != nil {
							continue
						}

						// Release semaphore to allow other peer to start sending transactions.
						defer peerSemaphore.Release(1)
						break
					}
				}
			}

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)
			memR.broadcastTxRoutine(peer)
		}()
	}
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
	switch msg := e.Message.(type) {
	case *protomem.RollbackTxs:
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("Received empty RollbackTxs message from peer", "src", e.Src)
			return
		}

		// Rollback operations are processed iff we have an acceptor instance.
		if memR.txAcceptor == nil {
			return
		}

		// Format transaction batch for Acceptor call.
		batch := []client.Transaction{}
		for _, rawTx := range protoTxs {
			batch = append(batch, client.RawTxToTransaction(rawTx))
		}

		// Forward the transaction rollbacks to an Acceptor.
		err := memR.txAcceptor.RollbackTx(
			context.TODO(),
			memR.userAddress,
			batch...,
		)
		if err != nil {
			memR.Logger.Debug("Acceptor rejected batch rollback",
				"address", memR.userAddress,
			)
			return // Nothing to do
		}

		// Rollback transactions by removing them from the mempool.
		for _, rawTx := range protoTxs {
			memTx := types.Tx(rawTx) // mempool Tx from bytes
			if err := memR.mempool.RemoveTxByKey(memTx.Key()); err != nil {
				memR.Logger.Debug("Rollback transaction not in local mempool (not an error)",
					"tx", log.NewLazySprintf("%X", memTx.Hash()),
					"error", err.Error())
			}
		}
	case *protomem.Txs:
		if memR.WaitSync() {
			memR.Logger.Debug("Ignored message received while syncing", "msg", msg)
			return
		}

		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("Received empty Txs message from peer", "src", e.Src)
			return
		}

		// Forward the transaction to an Acceptor if any is available. This
		// will delegate transaction verification locally to an acceptor.
		if memR.txAcceptor != nil {
			batch := []client.Transaction{}
			for _, rawTx := range protoTxs {
				batch = append(batch, client.RawTxToTransaction(rawTx))
			}

			err := memR.txAcceptor.AcceptBroadcastTx(
				context.TODO(),
				memR.userAddress,
				batch...,
			)
			if err != nil {
				memR.Logger.Debug("Acceptor rejected batch broadcast",
					"address", memR.userAddress,
				)
				return // do not accept transaction
			}
		}

		for _, txBytes := range protoTxs {
			tx := types.Tx(txBytes)
			_, err := memR.mempool.CheckTx(tx, e.Src.ID())
			if err != nil {
				switch {
				case errors.Is(err, ErrTxInCache):
					memR.Logger.Debug("Tx already exists in cache", "tx", tx.Hash())
				case errors.As(err, &ErrMempoolIsFull{}):
					// using debug level to avoid flooding when traffic is high
					memR.Logger.Debug(err.Error())
				default:
					memR.Logger.Info("Could not check tx", "tx", tx.Hash(), "err", err)
				}
			}
		}

		// Uses the multiplex server.ReplicationChannel to send an acknowledgment
		// message, or receipt, to describe that the transaction has been checked.
		if err := memR.sendAckTransactionBroadcast(e.Src, protoTxs); err != nil {
			memR.Logger.Debug("Error with AckTransactionBroadcast",
				"err", err,
			)
			return
		}

	default:
		memR.Logger.Error("Unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

func (memR *Reactor) EnableInOutTxs() {
	memR.Logger.Info("Enabling inbound and outbound transactions")
	if !memR.waitSync.CompareAndSwap(true, false) {
		return
	}

	// Releases all the blocked broadcastTxRoutine instances.
	if memR.config.Broadcast {
		close(memR.waitSyncCh)
	}
}

func (memR *Reactor) WaitSync() bool {
	return memR.waitSync.Load()
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer p2p.Peer) {
	// If the node is catching up, don't start this routine immediately.
	if memR.WaitSync() {
		select {
		case <-memR.waitSyncCh:
			// EnableInOutTxs() has set WaitSync() to false.
		case <-memR.Quit():
			return
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		select {
		case <-peer.Quit():
			cancel()
		case <-memR.Quit():
			cancel()
		}
	}()

	iter := memR.mempool.NewIterator(ctx)
	for {
		// In case of both next.NextWaitChan() and peer.Quit() are variable at the same time
		if !memR.IsRunning() || !peer.IsRunning() {
			return
		}

		entry := <-iter.WaitNextCh()

		// If the entry we were looking at got garbage collected (removed), try again.
		if entry == nil {
			continue
		}

		// If we suspect that the peer is lagging behind, at least by more than
		// one block, we don't send the transaction immediately. This code
		// reduces the mempool size and the recheck-tx rate of the receiving
		// node. See [RFC 103] for an analysis on this optimization.
		//
		// [RFC 103]: https://github.com/CometBFT/cometbft/blob/main/docs/references/rfc/rfc-103-incoming-txs-when-catching-up.md
		for {
			// Make sure the peer's state is up to date. The peer may not have a
			// state yet. We set it in the consensus reactor, but when we add
			// peer in Switch, the order we call reactors#AddPeer is different
			// every time due to us using a map. Sometimes other reactors will
			// be initialized before the consensus reactor. We should wait a few
			// milliseconds and retry.
			peerState, ok := peer.Get(types.PeerStateKey).(PeerState)
			if ok && peerState.GetHeight()+1 >= entry.Height() {
				break
			}
			select {
			case <-time.After(PeerCatchupSleepIntervalMS * time.Millisecond):
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}

		// NOTE: Transaction batching was disabled due to
		// https://github.com/tendermint/tendermint/issues/5796

		// We are paying the cost of computing the transaction hash in
		// any case, even when logger level > debug. So it only once.
		// See: https://github.com/ice-blockchain/cometbft/issues/4167
		txHash := entry.Tx().Hash()

		// Do not send this transaction if we receive it from peer.
		if entry.IsSender(peer.ID()) {
			memR.Logger.Debug("Skipping transaction, peer is sender",
				"tx", log.NewLazySprintf("%X", txHash), "peer", peer.ID())
			continue
		}

		for {
			memR.Logger.Debug("Sending transaction to peer",
				"tx", log.NewLazySprintf("%X", txHash), "peer", peer.ID())

			success := peer.Send(p2p.Envelope{
				ChannelID: MempoolChannel,
				Message:   &protomem.Txs{Txs: [][]byte{entry.Tx()}},
			})
			if success {
				break
			}

			memR.Logger.Debug("Failed sending transaction to peer",
				"tx", log.NewLazySprintf("%X", txHash), "peer", peer.ID())

			select {
			case <-time.After(PeerCatchupSleepIntervalMS * time.Millisecond):
			case <-peer.Quit():
				return
			case <-memR.Quit():
				return
			}
		}
	}
}

// sendAckTransactionBroadcast sends a AckTransactionBroadcast message.
// This object may be used to determine that a relay acknowledges
// the receipt (and will process acceptance) of a transaction broadcast.
func (r *Reactor) sendAckTransactionBroadcast(
	peer p2p.Peer,
	protoTxs [][]byte,
) error {
	txHashes := [][]byte{}
	for _, rawTx := range protoTxs {
		memTx := types.Tx(rawTx)
		txHashes = append(txHashes, memTx.Hash())
	}

	peer.Send(p2p.Envelope{
		ChannelID: server.ReplicationChannel,
		Message: &mxp2p.Message{
			Sum: &mxp2p.Message_AckTransactionBroadcast{
				AckTransactionBroadcast: &mxp2p.AckTransactionBroadcast{
					TxHashes: txHashes,
				},
			},
		},
	})

	return nil
}
