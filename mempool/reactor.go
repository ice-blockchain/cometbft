package mempool

import (
	"context"
	"errors"
	"fmt"
	"sync"
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
	"github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/ice-blockchain/cometbft/types"
)

// RelayDialerFn can be used to implement a custom dialing process for peers.
type RelayDialerFn func(*p2p.Switch, *p2p.PeerImpl, string) (p2p.ID, error)

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
	nodeKey     *p2p.NodeKey
	txAcceptor  client.Acceptor
	userAddress string
	ChainID     string // Exported.
	dialerFn    RelayDialerFn

	// Stores messages received during WaitSync() which are processed
	// in [EnableInOutTxs] and then deleted.
	pendingMsgsMtx *sync.RWMutex
	pendingMsgs    map[string]p2p.Envelope

	// Precalculated max batch size.
	recvMessageCapacity int
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(
	config *cfg.MempoolConfig,
	mempool *CListMempool,
	waitSync bool,
	options ...func(*Reactor),
) *Reactor {
	memR := &Reactor{
		config:         config,
		mempool:        mempool,
		waitSync:       atomic.Bool{},
		pendingMsgsMtx: new(sync.RWMutex),
		pendingMsgs:    make(map[string]p2p.Envelope),
	}

	// Enable overwrite of some optional properties.
	for _, option := range options {
		option(memR)
	}

	// TODO(midas): refactor to WithOnUpdate() option helper.
	mempool.OnUpdate = func(txes []types.Tx) error {
		err := memR.clientAcceptTx(txes)
		return err
	}

	{
		largestTx := make([]byte, memR.config.MaxTxBytes)
		batchMsg := protomem.Message{
			Sum: &protomem.Message_Txs{
				Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
			},
		}
		memR.recvMessageCapacity = batchMsg.Size()
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

// WithChainID is an option helper to inject a custom ChainID.
func WithChainID(
	chainID string,
) func(*Reactor) {
	return func(r *Reactor) {
		r.ChainID = chainID
	}
}

// WithNodeKey is an option helper to inject a custom ChainID.
func WithNodeKey(
	nodeKey *p2p.NodeKey,
) func(*Reactor) {
	return func(r *Reactor) {
		r.nodeKey = nodeKey
	}
}

// WithDialerFn is an option helper to inject a custom [RelayDialerFn].
func WithDialerFn(
	dialerFn RelayDialerFn,
) func(*Reactor) {
	return func(r *Reactor) {
		r.dialerFn = dialerFn
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

// SetChainID sets a custom chainID.
func (memR *Reactor) SetChainID(chainID string) {
	memR.ChainID = chainID
}

// SetAcceptor sets a custom acceptor implementation.
func (memR *Reactor) SetAcceptor(acceptor client.Acceptor) {
	memR.txAcceptor = acceptor
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
	return []*p2p.ChannelDescriptor{
		{
			ID:                  MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: memR.recvMessageCapacity,
			MessageType:         &protomem.Message{},
		},
		{
			ID:          server.AckBroadcastChannel,
			Priority:    3,
			MessageType: &mxp2p.Receipt{},
		},
	}
}

// PeerStateKey returns the peer state key with a ChainID scope.
func (memR *Reactor) PeerStateKey() string {
	return types.PeerStateKey + "_" + memR.ChainID
}

// AddPeer implements Reactor.
// It starts a broadcast routine ensuring all txs are forwarded to the given peer.
func (memR *Reactor) AddPeer(peer *p2p.PeerImpl) {
	if memR.config.Broadcast {
		go func() {
			// BREAKING(midas):
			//
			// In a multiplex of chains, the active time of networks is reduced
			// to the lifetime of client broadcast operations, which makes the
			// following semaphore acquisition irrelevant.

			// Always forward transactions to unconditional peers.
			// if !memR.Switch.IsPeerUnconditional(peer.ID()) {
			// 	// Depending on the type of peer, we choose a semaphore to limit the gossiping peers.
			// 	var peerSemaphore *semaphore.Weighted
			// 	if peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers > 0 {
			// 		peerSemaphore = memR.activePersistentPeersSemaphore
			// 	} else if !peer.IsPersistent() && memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers > 0 {
			// 		peerSemaphore = memR.activeNonPersistentPeersSemaphore
			// 	}

			// 	if peerSemaphore != nil {
			// 		for peer.IsRunning() {
			// 			// Block on the semaphore until a slot is available to start gossiping with this peer.
			// 			// Do not block indefinitely, in case the peer is disconnected before gossiping starts.
			// 			ctxTimeout, cancel := context.WithTimeout(context.TODO(), 30*time.Second)
			// 			// Block sending transactions to peer until one of the connections become
			// 			// available in the semaphore.
			// 			err := peerSemaphore.Acquire(ctxTimeout, 1)
			// 			cancel()

			// 			if err != nil {
			// 				continue
			// 			}

			// 			// Release semaphore to allow other peer to start sending transactions.
			// 			defer peerSemaphore.Release(1)
			// 			break
			// 		}
			// 	}
			// }

			memR.mempool.metrics.ActiveOutboundConnections.Add(1)
			defer memR.mempool.metrics.ActiveOutboundConnections.Add(-1)

			// BREAKING(midas):
			//
			// In a multiplex of chains, the active time of networks is reduced
			// to the lifetime of client broadcast operations, which makes the
			// following broadcastTxRoutine call obsolete. The actual broadcast
			// of transactions is controlled by [multiplex.client#BroadcastTx],
			// which manually sends transactions to remote mempools after
			// consensus was reached about said broadcast operation.
			//
			// memR.broadcastTxRoutine(peer)
		}()
	}
}

// Receive implements Reactor.
// It adds any received transactions to the mempool.
func (memR *Reactor) Receive(e p2p.Envelope) {
	memR.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)

	if e.ChannelID == server.AckBroadcastChannel {
		mxReactor := memR.Switch.Reactor(conn.SharedChannelsNamespace, "MULTIPLEX")
		if mxReactor != nil && mxReactor.IsRunning() {
			mxReactor.Receive(e)
		}
		return
	}

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

		// Mark INBOUND peer active in CONSENSUS and BLOCKSYNC
		memR.Switch.InitPeerForScope(e.Src, memR.ChainID)
		memR.Switch.AddPeerForScope(e.Src, memR.ChainID)

		// Format transaction batch for Acceptor call.
		batch := []client.Transaction{}
		for _, rawTx := range protoTxs {
			batch = append(batch, client.RawTxToTransaction(rawTx))
		}

		// Forward the transaction rollbacks to an Acceptor.
		err := memR.txAcceptor.RollbackTx(
			context.TODO(),
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
		protoTxs := msg.GetTxs()
		if len(protoTxs) == 0 {
			memR.Logger.Error("Received empty Txs message from peer", "src", e.Src)
			return
		}

		// We must pre-dial the source peer to permit
		// sending AckTransactionBroadcast.
		if memR.dialerFn != nil {
			// dialerFn is an extension that permits to run [Reactor#GetRemoteDiscoveryAddress]
			// which returns a public discovery address which can be used to find CometBFT addr.
			_, err := memR.dialerFn(memR.Switch, e.Src, memR.ChainID)
			if err != nil {
				memR.Logger.Error(
					"failed to dial peer from transaction message",
					"chain_id", memR.ChainID,
					"peer_in", e.Src,
					"err", err,
				)
			}
		}

		// Mark INBOUND peer active in CONSENSUS and BLOCKSYNC
		memR.Switch.InitPeerForScope(e.Src, memR.ChainID)
		memR.Switch.AddPeerForScope(e.Src, memR.ChainID)

		if memR.WaitSync() {
			memR.Logger.Debug("Ignored message received while syncing", "msg", msg)

			// TODO(midas): fix bottleneck here, should not use only first tx,
			// but instead it should use a hash of the envelope or batch.
			memR.pendingMsgsMtx.Lock()
			memR.pendingMsgs[string(types.Tx(protoTxs[0]).Hash())] = e
			memR.pendingMsgsMtx.Unlock()

			return
		}

		// Get an updated PeerSet for this ChainID
		chainPeerSet := memR.Switch.Peers(memR.ChainID)
		peerForAckTx := e.Src
		if chainPeerSet.Has(e.Src.ID()) {
			peerForAckTx = chainPeerSet.GetInOrOut(e.Src.ID(), true) // prefer outbound
		}
		memR.processTxs(peerForAckTx, protoTxs) // also, ACK this transaction

	default:
		memR.Logger.Error("Unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// processTxs forwards transaction to the internal Acceptor to verify their
// acceptance, then calls [CheckTx] to validate the inclusion and finally
// it will send a [AckTransactionBroadcast] message.
func (memR *Reactor) processTxs(
	peer *p2p.PeerImpl,
	protoTxs [][]byte,
) {
	rawTx := []types.Tx{}
	txHashes := []string{}
	for _, txBytes := range protoTxs {
		tx := types.Tx(txBytes)
		rawTx = append(rawTx, tx)
		txHashes = append(txHashes, fmt.Sprintf("%X", tx.Hash()))
	}
	if aErr := memR.clientAcceptTx(rawTx); aErr != nil {
		return
	}
	for _, tx := range rawTx {
		if _, err := memR.mempool.CheckTx(tx, peer.ID()); err != nil {
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

	// Uses the multiplex server.AckBroadcastChannel to send an acknowledgment
	// message, or receipt, to describe that the transaction has been checked.
	// peerOutbound := memR.Switch.Peers(memR.ChainID).GetOutbound(publicPeerID)
	// if peerOutbound != nil {
	if err := memR.sendAckTransactionBroadcast(peer, protoTxs); err != nil {
		memR.Logger.Error("Failed to send AckTransactionBroadcast",
			"err", err,
			"chain", memR.ChainID,
			"toPeer", peer,
		)
		return
	}
	// }
}

// clientAcceptTx delegates the verification of transactions to an Acceptor
// if any is available. It returns an error if the Acceptor rejects the batch.
func (memR *Reactor) clientAcceptTx(protoTxs []types.Tx) error {
	if memR.txAcceptor == nil || len(protoTxs) == 0 {
		return nil // Nothing to do
	}

	batch := []client.Transaction{}
	for _, rawTx := range protoTxs {
		batch = append(batch, client.RawTxToTransaction(rawTx))
	}

	if err := memR.txAcceptor.AcceptBroadcastTx(
		context.TODO(),
		batch...,
	); err != nil {
		memR.Logger.Debug("Acceptor rejected batch broadcast",
			"address", memR.userAddress,
			"err", err,
		)
		return err // do not accept transactions
	}

	return nil
}

func (memR *Reactor) EnableInOutTxs() {
	// If switch is not available, don't move from wait-syncing.
	if memR.Switch == nil || !memR.Switch.IsRunning() {
		memR.Logger.Error("failed to enable in/out transactions - switch is not running")
		return
	}

	memR.Logger.Info("Enabling inbound and outbound transactions")
	if !memR.waitSync.CompareAndSwap(true, false) {
		return
	}

	// Releases all the blocked broadcastTxRoutine instances.
	if memR.config.Broadcast {
		close(memR.waitSyncCh)
	}

	// Get an updated PeerSet for this ChainID
	chainPeerSet := memR.Switch.Peers(memR.ChainID)

	// Delayed processing of transactions that we received during WaitSync.
	memR.pendingMsgsMtx.Lock()
	for k, e := range memR.pendingMsgs {
		peerForAckTx := e.Src
		if chainPeerSet.Has(e.Src.ID()) {
			peerForAckTx = chainPeerSet.GetInOrOut(e.Src.ID(), true) // prefer outbound
		}
		memR.processTxs(peerForAckTx, e.Message.(*protomem.Txs).GetTxs()) // also, ACK this transaction
		delete(memR.pendingMsgs, k)
	}
	memR.pendingMsgsMtx.Unlock()
}

func (memR *Reactor) WaitSync() bool {
	return memR.waitSync.Load()
}

// PeerState describes the state of a peer.
type PeerState interface {
	GetHeight() int64
}

// Send new mempool txs to peer.
func (memR *Reactor) broadcastTxRoutine(peer *p2p.PeerImpl) {
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
			peerState, ok := peer.Get(memR.PeerStateKey()).(PeerState)
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

			success := peer.Send(memR.ChainID, p2p.Envelope{
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
func (memR *Reactor) sendAckTransactionBroadcast(
	peer *p2p.PeerImpl,
	protoTxs [][]byte,
) error {
	myPeerID := memR.nodeKey.ID()
	txHashes := [][]byte{}
	txHashesHex := []string{}
	for _, rawTx := range protoTxs {
		memTx := types.Tx(rawTx)
		txHash := memTx.Hash()
		txHashes = append(txHashes, txHash)
		txHashesHex = append(txHashesHex, fmt.Sprintf("%X", txHash))
	}

	sendToPeer := func(fromID p2p.ID, toPeer *p2p.PeerImpl) error {
		if success := toPeer.Send(memR.ChainID, p2p.Envelope{
			ChannelID: server.AckBroadcastChannel,
			Message: &mxp2p.Receipt{
				Sum: &mxp2p.Receipt_AckTransactionBroadcast{
					AckTransactionBroadcast: &mxp2p.AckTransactionBroadcast{
						TxHashes: txHashes,
						NodeId:   string(fromID),
						ChainID:  memR.ChainID,
					},
				},
			},
		}); !success {
			return fmt.Errorf(
				"could not send message to peer, sender: %s, recipient: %s",
				string(fromID), string(toPeer.ID()))
		}
		return nil
	}

	if peer == nil {
		return fmt.Errorf(
			"failed sending AckTransactionBroadcast, got empty peer for ChainID %s and txHash %v",
			string(memR.ChainID), txHashesHex)
	}

	// TODO(midas): remove debug logs
	memR.Logger.Debug("Sending AckTransactionBroadcast to peer",
		"from_id", myPeerID,
		"peer_id", peer.ID(),
		"peer", peer,
		"chain_id", memR.ChainID,
		"tx_hashes", txHashesHex,
	)

	if err := sendToPeer(myPeerID, peer); err != nil {
		return err
	}

	return nil
}
