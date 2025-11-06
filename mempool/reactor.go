package mempool

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	protomem "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cfg "github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/cmap"
	"github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	mxtypes "github.com/ice-blockchain/cometbft/multiplex/types"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/types"
)

const (
	defaultMaxTxBytes = 1024 * 1024 // 1MiB
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
	nodeKey         *p2p.NodeKey
	txAcceptor      client.Acceptor
	userAddress     string
	ChainID         string // Exported.
	dialerFn        RelayDialerFn
	runtimeRegistry mxtypes.IdleManager

	// ensuredActiveChains contains boolean values by ChainID (string) keys.
	ensuredActiveChains *cmap.CMap
	// batchesPendingIndex contains boolean values by concatenated tx hash (hex string) keys.
	batchesPendingIndex *cmap.CMap

	// Stores messages received during WaitSync() which are processed
	// in [EnableInOutTxs] and then deleted.
	// pendingMsgs contains p2p.Envelope instance by tx hash (hex string) keys.
	pendingMsgs *cmap.CMap

	// Precalculated max batch size.
	recvMessageCapacity int
}

// NewReactor returns a new Reactor with the given config and mempool.
func NewReactor(
	ctx context.Context,
	config *cfg.MempoolConfig,
	mempool *CListMempool,
	waitSync bool,
	options ...func(*Reactor),
) *Reactor {
	memR := &Reactor{
		config:              config,
		mempool:             mempool,
		waitSync:            atomic.Bool{},
		pendingMsgs:         cmap.NewCMap(),
		batchesPendingIndex: cmap.NewCMap(),
		ensuredActiveChains: cmap.NewCMap(),
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

	memR.BaseReactor = *p2p.NewBaseReactor(ctx, "Mempool", memR)
	if waitSync {
		memR.waitSync.Store(true)
		memR.waitSyncCh = make(chan struct{})
	}
	memR.activePersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToPersistentPeers))
	memR.activeNonPersistentPeersSemaphore = semaphore.NewWeighted(int64(memR.config.ExperimentalMaxGossipConnectionsToNonPersistentPeers))

	return memR
}

// CAUTION: This method is used to determine a static list of channels
// for the multiplex implementation. Do not use for transactions.
func NewEmptyReactor(ctx context.Context) *Reactor {
	baseConf := cfg.DefaultMempoolConfig()
	baseConf.Broadcast = false

	memR := &Reactor{config: baseConf}
	memR.BaseReactor = *p2p.NewBaseReactor(ctx, "Mempool", memR)

	{
		largestTx := make([]byte, defaultMaxTxBytes)
		batchMsg := protomem.Message{
			Sum: &protomem.Message_Txs{
				Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
			},
		}
		memR.recvMessageCapacity = batchMsg.Size()
	}
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

// WithIdleManager is an option helper to inject a custom runtime registry.
func WithIdleManager(
	reg mxtypes.IdleManager,
) func(*Reactor) {
	return func(r *Reactor) {
		r.runtimeRegistry = reg
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

// SetRuntimeRegistry sets a cuustom runtime registry.
func (memR *Reactor) SetRuntimeRegistry(reg mxtypes.IdleManager) {
	memR.runtimeRegistry = reg
}

// OnStart implements p2p.BaseReactor.
func (memR *Reactor) OnStart(ctx context.Context) error {
	if memR.WaitSync() {
		memR.Logger.Info("Starting reactor in sync mode: tx propagation will start once sync completes")
	}
	if !memR.config.Broadcast {
		memR.Logger.Info("Tx broadcasting is disabled")
	}
	return nil
}

// OnReset should not execute any business logic, but instead must be
// defined as it is called from [Service#Reset], which permits to later
// start back the service with stopped/started correctly reset.
func (memR *Reactor) OnReset(ctx context.Context) error {

	memR.batchesPendingIndex = cmap.NewCMap()
	memR.ensuredActiveChains = cmap.NewCMap()

	memR.Logger.Info("Mempool reactor service reset",
		"chain_id", memR.ChainID,
	)
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
			ID:          mxtypes.AckBroadcastChannel,
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

	if e.ChannelID == mxtypes.AckBroadcastChannel {
		mxReactor := memR.Switch.GetMultiplexReactor()
		memR.Logger.Debug("Forwarding bytes",
			"mx", mxReactor,
			"running", mxReactor.IsRunning(),
			"msg", e.Message)
		if mxReactor != nil && mxReactor.IsRunning() {
			mxReactor.Receive(e)
		}
		return // Forwarded
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

		// We must locally activate this ChainID for consensus routines.
		if err := memR.ensureActiveRuntime(memR.ChainID, protoTxs); err != nil {
			memR.Logger.Error("failed to activate runtime for RollbackTxs",
				"chainId", memR.ChainID,
				"numTxes", len(protoTxs),
				"err", err)
			return // must not ignore!
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
			memR.Context(),
			batch...,
		)
		if err != nil {
			// TODO(midas): remove debug logs
			memR.Logger.Debug("Acceptor rejected batch rollback",
				"chainId", memR.ChainID,
				"numTxes", len(protoTxs),
				"address", memR.userAddress,
				"err", err,
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

		// We must dial back the source to send AckTransactionBroadcast.
		// Creates a OUTBOUND peer from the INBOUND (dialing back).
		if err := memR.ensureConnectionToPeer(e.Src); err != nil {
			memR.Logger.Error("failed to dial back broadcast partner",
				"chainId", memR.ChainID,
				"numTxes", len(protoTxs),
				"peer", e.Src,
				"err", err)
			// error MAY be ignored
		}

		// We must locally activate this ChainID for consensus routines.
		if err := memR.ensureActiveRuntime(memR.ChainID, protoTxs); err != nil {
			memR.Logger.Error("failed to activate runtime for Tx",
				"chainId", memR.ChainID,
				"numTxes", len(protoTxs),
				"err", err)
			return // must not ignore!
		}

		// Mark INBOUND peer active in CONSENSUS and BLOCKSYNC
		memR.Switch.InitPeerForScope(e.Src, memR.ChainID)
		memR.Switch.AddPeerForScope(e.Src, memR.ChainID)

		if memR.WaitSync() {
			// TODO(midas): fix bottleneck here, should not use only first tx,
			// but instead it should use a hash of the envelope or batch.
			txHash := string(types.Tx(protoTxs[0]).Hash())
			memR.pendingMsgs.Set(txHash, e)

			return
		}

		// Get an updated PeerSet for this ChainID
		chainPeerSet := memR.Switch.Peers(memR.ChainID)
		peerForAckTx := e.Src
		if chainPeerSet.Has(e.Src.ID()) {
			peerForAckTx = chainPeerSet.Get(e.Src.ID())
		}
		memR.processTxs(peerForAckTx, protoTxs) // also, ACK this transaction

	default:
		memR.Logger.Error("Unknown message type", "src", e.Src, "chId", e.ChannelID, "msg", e.Message)
		memR.Switch.StopPeerForError(e.Src, fmt.Errorf("mempool cannot handle message of type: %T", e.Message))
		return
	}

	// broadcasting happens from go routines per peer
}

// ensureConnectionToPeer dials back p to permit sending AckTransactionBroadcast
// messages. Creates a OUTBOUND peer.
// See also: [multiplex.Reactor#GetRelayDialerForCometBFT].
func (memR *Reactor) ensureConnectionToPeer(p *p2p.PeerImpl) error {
	if memR.dialerFn == nil {
		return fmt.Errorf("custom dialer function is not set for %s", memR.ChainID)
	}

	// dialerFn is an extension that permits to run a custom dialer.
	if _, err := memR.dialerFn(memR.Switch, p, memR.ChainID); err != nil {
		memR.Logger.Error("failed to dial peer with dialer extension",
			"chainId", memR.ChainID,
			"peerIn", p,
			"err", err,
		)

		return err
	}

	return nil
}

// ensureActiveRuntime activates chainID using the multiplex reactor and
// ensures that connection channels are opened for chainID.
func (memR *Reactor) ensureActiveRuntime(chainID string, protoTxs [][]byte) error {
	if memR.runtimeRegistry == nil {
		return fmt.Errorf("ERROR: idle manager is not set for %s", memR.ChainID)
	}

	txHashesStr := ""
	for _, rawTx := range protoTxs {
		txHashesStr += string(types.Tx(rawTx).Hash())
	}

	hasWaiterForTxIdx := memR.batchesPendingIndex.Has(txHashesStr)
	if hasWaiterForTxIdx {
		return nil // Nothing to do.
	}

	// CAUTION: This runtime for ChainID *must be long-living* because it
	// is used to execute cometbft consensus (blocks proposal). Thus we shall
	// wait for transactions to be **indexed** before the runtime is completed.
	//
	// Activate this runtime in idle manager.
	hasEnsuredChainID := memR.ensuredActiveChains.Has(chainID)
	if !hasEnsuredChainID {
		memR.runtimeRegistry.OnActivate(chainID)
		memR.ensuredActiveChains.Set(chainID, true)
	}

	// Waits for transactions to be indexed before completing the runtime.
	memR.batchesPendingIndex.Set(txHashesStr, true)
	return memR.startWaitIndexedRoutine(chainID, protoTxs)
}

// startWaitIndexedRoutine starts a goroutine which is blocked until all of
// the txes in protoTxs have been indexed locally or until shutdown.
func (memR *Reactor) startWaitIndexedRoutine(chainID string, protoTxs [][]byte) error {
	if memR.runtimeRegistry == nil {
		return fmt.Errorf("ERROR: idle manager is not set for %s", memR.ChainID)
	}

	cliTxes := make([]client.Transaction, 0, len(protoTxs))
	for _, rawTx := range protoTxs {
		cliTxes = append(cliTxes, client.RawTxToTransaction(types.Tx(rawTx)))
	}

	relevantChainIds := []string{chainID}
	txesByChainIds := map[string][]client.Transaction{}
	txesByChainIds[chainID] = cliTxes[:]

	// Creates a goroutine that completes runtimes when txes are indexed.
	go memR.runtimeRegistry.WaitForIndexedTransactions(
		relevantChainIds,
		txesByChainIds,
	)

	return nil
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
		txHashes = append(txHashes, string(tx.Hash()))
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

	// Uses the multiplex mxtypes.AckBroadcastChannel to send an acknowledgment
	// message, or receipt, to describe that the transaction has been checked.
	if err := memR.sendAckTransactionBroadcast(peer, protoTxs); err != nil {
		memR.Logger.Error("Failed to send AckTransactionBroadcast",
			"err", err,
			"chain", memR.ChainID,
			"toPeer", peer,
		)
		return
	}

	// If we received a ChainReplicationRequest for this ChainID, we should also
	// report to the sender relay that the replication is complete.

	if multiplexReactor := memR.Switch.GetMultiplexReactor(); multiplexReactor != nil {
		type inlineCompletionAnnouncer interface {
			ShouldAnnounceReplication(chainID string) bool
			UnsetAnnounceReplication(chainID string)
		}

		// Use type assertion to access multiplex reactor methods.
		if mxR, ok := multiplexReactor.(inlineCompletionAnnouncer); ok {
			if mxR.ShouldAnnounceReplication(memR.ChainID) {
				memR.sendChainReplicationComplete(memR.ChainID, protoTxs)
				mxR.UnsetAnnounceReplication(memR.ChainID)
			}
		}
	}
}

// clientAcceptTx delegates the verification of transactions to an Acceptor
// if any is available. It returns an error if the Acceptor rejects the batch.
func (memR *Reactor) clientAcceptTx(protoTxs []types.Tx) error {
	if memR.txAcceptor == nil || len(protoTxs) == 0 {
		return nil // Nothing to do
	}

	batch := []client.Transaction{}
	txHashes := []string{}
	for _, rawTx := range protoTxs {
		tx := client.RawTxToTransaction(rawTx)
		txHash := string(tx.Hash())
		batch = append(batch, tx)
		txHashes = append(txHashes, txHash)
	}

	// TODO(midas): remove debug logs
	memR.Logger.Debug("Forward transaction batch to acceptor: AcceptBroadcastTx",
		"chain_id", memR.ChainID,
		"tx_batch", txHashes,
	)

	if err := memR.txAcceptor.AcceptBroadcastTx(
		context.TODO(),
		batch...,
	); err != nil {
		memR.Logger.Error(
			"Acceptor callback AcceptBroadcastTx rejected transaction batch",
			"chain_id", memR.ChainID,
			"tx_batch", txHashes,
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
	for _, k := range memR.pendingMsgs.Keys() {
		e := memR.pendingMsgs.Get(k).(p2p.Envelope)
		peerForAckTx := e.Src
		if chainPeerSet.Has(e.Src.ID()) {
			peerForAckTx = chainPeerSet.Get(e.Src.ID())
		}
		memR.processTxs(peerForAckTx, e.Message.(*protomem.Txs).GetTxs()) // also, ACK this transaction
		memR.pendingMsgs.Delete(k)
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
	for memR.Context().Err() == nil {
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
		for memR.Context().Err() == nil {
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
			case <-memR.Context().Done():
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

		for memR.Context().Err() == nil {
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
			case <-memR.Context().Done():
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
	txHash := types.Tx(protoTxs[0]).Hash()
	txHashHex := strings.ToUpper(
		hex.EncodeToString(txHash),
	)

	sendToPeer := func(fromID p2p.ID, toPeer *p2p.PeerImpl) error {
		if success := toPeer.Send(memR.ChainID, p2p.Envelope{
			ChainID:   memR.ChainID,
			ChannelID: mxtypes.AckBroadcastChannel,
			Message: &mxp2p.Receipt{
				Sum: &mxp2p.Receipt_AckTransactionBroadcast{
					AckTransactionBroadcast: &mxp2p.AckTransactionBroadcast{
						TxHash:  txHash,
						NodeId:  string(fromID),
						ChainID: memR.ChainID,
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
			string(memR.ChainID), txHashHex)
	}

	// TODO(midas): remove debug logs
	memR.Logger.Debug("Sending AckTransactionBroadcast to peer",
		"from", myPeerID,
		"to", peer.ID(),
		"peer", peer,
		"chainId", memR.ChainID,
		"txHash", txHashHex,
	)

	if err := sendToPeer(myPeerID, peer); err != nil {
		return err
	}

	return nil
}

// sendChainReplicationComplete sends a ChainReplicationComplete message
// to all peers we are connected to for chainID.
func (memR *Reactor) sendChainReplicationComplete(
	chainID string,
	protoTxs [][]byte,
) {
	sendReplCompleteToPeer := func(fromID p2p.ID, toPeer *p2p.PeerImpl) error {
		if success := toPeer.Send(chainID, p2p.Envelope{
			ChannelID: mxtypes.RuntimeChannel,
			Message: &mxp2p.Message{
				Sum: &mxp2p.Message_ChainReplicationComplete{
					ChainReplicationComplete: &mxp2p.ChainReplicationComplete{
						NodeId:  string(fromID),
						ChainID: chainID,
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

	myPeerID := memR.nodeKey.ID()
	peerSet := memR.Switch.Peers(chainID)
	peersToSend := peerSet.Copy()

	// TODO(midas): remove debug logs
	memR.Logger.Debug("Sending ChainReplicationComplete to peers",
		"from_id", myPeerID,
		"num_peers", len(peersToSend),
		"peers", peersToSend,
		"chain_id", chainID,
	)

	var wg sync.WaitGroup
	wg.Add(len(peersToSend))

	for _, p := range peersToSend {
		// Broadcast this message concurrently to our peers. Note that
		// we will be waiting for the operations to complete to proceed.
		go func(peer *p2p.PeerImpl) {
			defer wg.Done()

			if !peer.IsOutbound() && peerSet.HasOutbound(peer.ID()) {
				return // prefer sending to outbound
			}

			if err := sendReplCompleteToPeer(myPeerID, peer); err != nil {
				memR.Logger.Error("Failed to send ChainReplicationComplete",
					"chain_id", chainID,
					"from", memR.nodeKey.ID(),
					"to", peer.ID(),
					"err", err,
				)
			}
		}(p)
	}
	wg.Wait()

	// TODO(midas): remove debug logs
	memR.Logger.Debug("Done sending ChainReplicationComplete to peers",
		"from_id", myPeerID,
		"num_peers", len(peersToSend),
		"peers", peersToSend,
		"chain_id", chainID,
	)

	if memR.runtimeRegistry != nil {
		// CAUTION: This runtime for ChainID *is not* the one that will be used
		// to execute cometbft consensus (blocks proposal). Thus we mark this
		// runtime as completed because another one gets activated for consensus.
		//
		// Completes the runtime activated in [multiplex.Reactor#Receive] upon
		// reception of a ChainReplicationRequest.
		memR.runtimeRegistry.OnComplete(chainID)
	}

	return
}
