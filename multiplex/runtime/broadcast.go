package runtime

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/internal/cmap"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmttypes "github.com/ice-blockchain/cometbft/types"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// BroadcastPool defines a transaction broadcast pool.
type BroadcastPool struct {
	service.BaseService
	mtx *sync.Mutex

	// A message pool is used to store incoming/outgoing messages
	// by type. This pool handles messages of types:
	// - mxp2p.AckTransactionBroadcast
	pool        *MessagePool
	resourceMgr *ResourceRegistry

	// chainIdsByTxHash contains ChainID values (string), mapped by hexadecimal
	// tx hash keys.
	chainIdsByTxHash *cmap.CMap
	// eventSubscribers contains subscriber names (string) by ChainID keys.
	eventSubscribers *cmap.CMap
	timeoutIndex     time.Duration
	timeoutAckTx     time.Duration

	// Contains buffered channels of size 1 as we expect exactly 1
	// update on those channels, per transaction hash.

	// acceptedChs contains a buffered channel of size 1 `chan struct{}`,
	// mapped by hexadecimal tx hash keys. The size of 1 is because we expect
	// exactly 1 update on these channels, i.e. 1 event per transaction hash.
	// Used to indicate the remote acknowledgment of a txHash.
	acceptedChs *cmap.CMap
	// doneAcceptedChs contains a boolean value by hexadecimal tx hash keys.
	// Note that a txHash key present in this map indicates that the above
	// acceptedChs channel for this txHash is *closed*.
	doneAcceptedChs *cmap.CMap

	// indexedChs contains a buffered channel of size 1 `chan struct{}`,
	// mapped by hexadecimal tx hash keys. The size of 1 is because we expect
	// exactly 1 update on these channels, i.e. 1 event per transaction hash.
	// Used to indicate the local indexing of a txHash.
	indexedChs *cmap.CMap
	// doneIndexedChs contains a boolean value by hexadecimal tx hash keys.
	// Note that a txHash key present in this map indicates that the above
	// indexedChs channel for this txHash is *closed*.
	doneIndexedChs *cmap.CMap

	// relays contains slices of `*helpers.RelayAddress` instances, mapped
	// by hexadecimal tx hash keys.
	// CAUTION: These slices of relays are used to evaluate the super-majority
	// of remote acknowledgments of tx hashes.
	relays *cmap.CMap
	// partners contains slices of `cmtp2p.ID` instances, mapped by hexadecimal
	// tx hash keys.
	partners *cmap.CMap

	// Unbuffered channel that may be written on to shutdown indexer routines.
	goShutdownCh chan bool

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.BroadcastManager = (*BroadcastPool)(nil)

type BroadcastPoolOption func(*BroadcastPool)

// NewBroadcastManager creates a new broadcast manager.
func NewBroadcastManager(
	ctx context.Context,
	resourceMgr *ResourceRegistry,
	logger cmtlog.Logger,
	options ...BroadcastPoolOption,
) types.BroadcastManager {
	mgr := &BroadcastPool{
		mtx:         new(sync.Mutex),
		pool:        NewMessageManager(logger),
		resourceMgr: resourceMgr,

		chainIdsByTxHash: cmap.NewCMap(),
		eventSubscribers: cmap.NewCMap(),
		timeoutIndex:     -1,
		timeoutAckTx:     -1,

		acceptedChs:     cmap.NewCMap(),
		doneAcceptedChs: cmap.NewCMap(),
		indexedChs:      cmap.NewCMap(),
		doneIndexedChs:  cmap.NewCMap(),

		goShutdownCh: make(chan bool), // unbuffered

		relays:   cmap.NewCMap(),
		partners: cmap.NewCMap(),

		// Options
		logger: logger,
	}

	// Use option helpers
	mgr.SetOptions(options...)

	// Overwrite timeouts with default if not set.
	if mgr.timeoutAckTx == -1 {
		mgr.timeoutAckTx = DefaultAckBroadcastTimeout
	}

	if mgr.timeoutIndex == -1 {
		mgr.timeoutIndex = DefaultTransactionTimeout
	}

	mgr.BaseService = *service.NewBaseService(ctx, logger, "BroadcastPool", mgr)
	return mgr
}

// BroadcastPoolWithLogger injects a custom logger instance.
func BroadcastPoolWithLogger(logger cmtlog.Logger) BroadcastPoolOption {
	return func(mgr *BroadcastPool) {
		mgr.logger = logger
	}
}

// BroadcastPoolWithAckBroadcastTimeout injects a custom index timeout.
func BroadcastPoolWithAckBroadcastTimeout(timeoutIndex time.Duration) BroadcastPoolOption {
	return func(mgr *BroadcastPool) {
		mgr.timeoutAckTx = timeoutIndex
	}
}

// BroadcastPoolWithTransactionTimeout injects a custom index timeout.
func BroadcastPoolWithTransactionTimeout(timeoutIndex time.Duration) BroadcastPoolOption {
	return func(mgr *BroadcastPool) {
		mgr.timeoutIndex = timeoutIndex
	}
}

// ----------------------------------------------------------------------------
// BroadcastPool implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (mgr *BroadcastPool) OnStart(ctx context.Context) (err error) {
	// TODO(midas): persistence of broadcast status in database
	return nil
}

// OnStop implements [service.Service] by closing the database.
func (mgr *BroadcastPool) OnStop() {
	// TODO(midas): persistence of broadcast status in database

	// First, make sure all goroutines are stopped.
	mgr.mtx.Lock()
	close(mgr.goShutdownCh)
	mgr.mtx.Unlock()

	// Then free allocated event subscribers.
	for _, chainID := range mgr.eventSubscribers.Keys() {
		subscriberName := mgr.eventSubscribers.Get(chainID).(string)
		if eventBus := mgr.EventBus(chainID); eventBus != nil {
			eventBus.UnsubscribeAll(mgr.Context(), subscriberName)
		}

		mgr.eventSubscribers.Delete(chainID)
	}
}

// OnReset implements [service.Service] by resetting the service.
func (mgr *BroadcastPool) OnReset(ctx context.Context) error {
	if err := mgr.pool.Reset(); err != nil {
		return fmt.Errorf("failed to reset broadcast pool: %w", err)
	}

	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	// Reset all internal maps
	mgr.chainIdsByTxHash = cmap.NewCMap()
	mgr.eventSubscribers = cmap.NewCMap()

	mgr.acceptedChs = cmap.NewCMap()
	mgr.doneAcceptedChs = cmap.NewCMap()
	mgr.indexedChs = cmap.NewCMap()
	mgr.doneIndexedChs = cmap.NewCMap()
	mgr.relays = cmap.NewCMap()
	mgr.partners = cmap.NewCMap()

	// Reset internal shutdown channel
	mgr.goShutdownCh = make(chan bool) // unbuffered

	mgr.logger.Debug("Reset broadcast pool")
	return nil
}

// ----------------------------------------------------------------------------
// BroadcastManager API implementation

// EventBus returns an event bus for chainID.
func (mgr *BroadcastPool) EventBus(chainID string) *cmttypes.EventBus {
	if !mgr.resourceMgr.Has(chainID, types.ServiceKeyEventBus) {
		return nil
	}

	return mgr.resourceMgr.Get(
		chainID,
		types.ServiceKeyEventBus,
	).(*cmttypes.EventBus)
}

// Init initializes a replication processor for chainID with relays.
func (mgr *BroadcastPool) Init(
	chainID string,
	txHash string,
	relays []*helpers.RelayAddress,
) error {
	// TODO(midas): remove debug logs
	mgr.logger.Debug("BroadcastPool#Init",
		"chainId", chainID,
		"txHash", txHash,
		"numRelays", len(relays),
	)

	var prev []*helpers.RelayAddress
	if mgr.relays.Has(txHash) {
		prev = mgr.relays.Get(txHash).([]*helpers.RelayAddress)
	}

	if len(relays) > 0 {
		for _, relay := range relays {
			if -1 == slices.IndexFunc(prev, func(ra *helpers.RelayAddress) bool {
				return ra.String() == relay.String()
			}) {
				prev = append(prev, relay)
			}
		}
	}

	mgr.relays.Set(txHash, prev)
	mgr.chainIdsByTxHash.Set(txHash, chainID)

	// The transaction indexer pushes events on an event bus instance per ChainID,
	// so we only need the indexer routine once per ChainID, and not for every tx.
	if !mgr.eventSubscribers.Has(chainID) {
		go mgr.indexerRoutine(chainID)
	}

	return nil
}

// Process processes a received message e to the replication pool.
func (mgr *BroadcastPool) Process(peerID cmtp2p.ID, e cmtp2p.Envelope) error {
	// TODO(midas): remove debug logs
	mgr.logger.Debug("BroadcastPool#Process",
		"chainId", e.ChainID,
		"peerID", string(peerID),
		"msg", e.Message,
	)

	msgTypes := []string{"AckTransactionBroadcast"}
	if !slices.Contains(msgTypes, mgr.pool.GetMsgType(e.Message)) {
		mgr.logger.Info("Skipping message in broadcast pool (wrong type)",
			"chainId", e.ChainID,
			"peerID", string(peerID),
			"msg", e.Message,
		)
		return nil
	}

	addToMessagePool := func(peerID cmtp2p.ID, e cmtp2p.Envelope) {
		if e.Src != nil {
			mgr.pool.AddIncoming(e)
		} else {
			mgr.pool.AddOutgoing(peerID, e)
		}
	}

	switch extMsg := e.Message.(type) {
	case *mxp2p.Receipt:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Receipt_AckTransactionBroadcast:
			ackTxBroadcast := extMsg.GetAckTransactionBroadcast()
			txHash := bytesToHex(ackTxBroadcast.TxHash)

			// If we already received this response from peer, don't count for acceptance.
			prevResponses := mgr.Responses(txHash)
			if -1 != slices.IndexFunc(prevResponses, func(msg *mxp2p.AckTransactionBroadcast) bool {
				sameNodeId := msg.NodeId == ackTxBroadcast.NodeId
				sameChainID := msg.ChainID == ackTxBroadcast.ChainID
				sameTxHash := bytesToHex(msg.TxHash) == txHash
				return sameNodeId && sameChainID && sameTxHash
			}) {
				mgr.logger.Info("Skipping already received ack transaction broadcast",
					"chainId", ackTxBroadcast.ChainID,
					"txHash", txHash,
					"peerID", string(peerID),
					"msg", e.Message,
				)
				return nil // Nothing to do with this message.
			}

			addToMessagePool(peerID, e)
			mgr.addPartner(txHash, e.Src.ID())

			if ackd := mgr.evaluateAcceptanceMajority(txHash); !ackd {
				return nil // More acks expected for this txHash.
			}

			if mgr.doneAcceptedChs.Has(txHash) {
				return nil // Already completed, nothing more to do.
			}

			mgr.mtx.Lock()
			defer mgr.mtx.Unlock()

			// Close the "Accepted" channel when we have 2/3+1 ACK messages (self included).
			if !mgr.doneAcceptedChs.Has(txHash) {
				if ch := mgr.Accepted(txHash); ch != nil {
					mgr.doneAcceptedChs.Set(txHash, true)
					close(ch)
				}
			}
			return nil // DONE!
		}
	}

	return nil
}

// Relays returns a list of relays that are *expected* to respond about txHash.
func (mgr *BroadcastPool) Relays(txHash string) []*helpers.RelayAddress {
	if mgr.relays.Has(txHash) {
		return mgr.relays.Get(txHash).([]*helpers.RelayAddress)
	}

	return []*helpers.RelayAddress{}
}

// Partners returns a list of relay ID from broadcast partners for txHash.
func (mgr *BroadcastPool) Partners(txHash string) []cmtp2p.ID {
	if mgr.partners.Has(txHash) {
		return mgr.partners.Get(txHash).([]cmtp2p.ID)
	}

	return []cmtp2p.ID{}
}

// Responses returns the incoming AckTransactionBroadcast by txHash.
func (mgr *BroadcastPool) Responses(txHash string) []*mxp2p.AckTransactionBroadcast {
	responses := []*mxp2p.AckTransactionBroadcast{}

	// Collect all chain replication responses.
	envelopes := mgr.pool.IncomingByType("AckTransactionBroadcast")
	if len(envelopes) == 0 {
		return responses
	}

	// Keep only relevant ones for this txHash.
	for _, envelope := range envelopes {
		switch extMsg := envelope.Message.(type) {
		case *mxp2p.Receipt:
			msg := extMsg.GetSum()
			switch msg.(type) {
			case *mxp2p.Receipt_AckTransactionBroadcast:
				ackTx := extMsg.GetAckTransactionBroadcast()
				ackTxHash := bytesToHex(ackTx.TxHash)
				if ackTxHash == txHash {
					responses = append(responses, ackTx)
				}
			}
		}
	}

	return responses
}

// Accepted returns a channel, which is closed when txHash has 2/3+1 ACK messages.
func (mgr *BroadcastPool) Accepted(txHash string) chan struct{} {
	if mgr.doneAcceptedChs.Has(txHash) {
		return nil
	}

	if mgr.acceptedChs.Has(txHash) {
		return mgr.acceptedChs.Get(txHash).(chan struct{})
	}

	acceptedCh := make(chan struct{}, 1) // buffered
	mgr.acceptedChs.Set(txHash, acceptedCh)
	return acceptedCh
}

// Indexed returns a channel, which is closed when txHash got indexed locally
func (mgr *BroadcastPool) Indexed(txHash string) chan struct{} {
	if mgr.doneIndexedChs.Has(txHash) {
		return nil
	}

	if mgr.indexedChs.Has(txHash) {
		return mgr.indexedChs.Get(txHash).(chan struct{})
	}

	indexedCh := make(chan struct{}, 1) // buffered
	mgr.indexedChs.Set(txHash, indexedCh)
	return indexedCh
}

// WaitAccepted blocks the thread until txHash has 2/3+1 ACK messages.
func (mgr *BroadcastPool) WaitAccepted(txHash string) bool {
	ch := mgr.Accepted(txHash)

	cancelTimer := time.NewTimer(mgr.timeoutAckTx)
	if mgr.timeoutAckTx == 0 {
		cancelTimer.Stop()
	} else {
		defer cancelTimer.Stop()
	}

	for mgr.Context().Err() == nil {
		select {
		case <-ch:
			return true
		case <-cancelTimer.C:
			return mgr.doneAcceptedChs.Has(txHash)
		case <-mgr.Context().Done():
			return mgr.doneAcceptedChs.Has(txHash)
		case <-mgr.Quit():
			return mgr.doneAcceptedChs.Has(txHash)
		}
	}

	return mgr.doneAcceptedChs.Has(txHash)
}

// WaitIndexed blocks the thread until txHash got indexed locally
func (mgr *BroadcastPool) WaitIndexed(txHash string) bool {
	txIndexedCh := mgr.Indexed(txHash)

	cancelTimer := time.NewTimer(mgr.timeoutIndex)
	if mgr.timeoutIndex == 0 {
		cancelTimer.Stop()
	} else {
		defer cancelTimer.Stop()
	}

	for mgr.Context().Err() == nil {
		select {
		case <-txIndexedCh:
			return true
		case <-cancelTimer.C:
			return mgr.doneIndexedChs.Has(txHash)
		case <-mgr.Context().Done():
			return mgr.doneIndexedChs.Has(txHash)
		case <-mgr.Quit():
			return mgr.doneIndexedChs.Has(txHash)
		}
	}

	return mgr.doneIndexedChs.Has(txHash)
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (mgr *BroadcastPool) SetOptions(options ...BroadcastPoolOption) {
	for _, option := range options {
		option(mgr)
	}
}

// Logger returns the logger instance.
func (mgr *BroadcastPool) Logger() cmtlog.Logger {
	return mgr.logger
}

// ----------------------------------------------------------------------------

// indexerRoutine subscribes to [cmttypes.EventDataTx] events for chainID and
// closes the internal indexedChs channel to stop waiting for indexed txes.
func (mgr *BroadcastPool) indexerRoutine(chainID string) {
	// TODO(midas): remove debug logs
	mgr.logger.Debug("BroadcastPool#indexerRoutine",
		"chainId", chainID,
		"timeout", mgr.timeoutIndex)

	cancelTimer := time.NewTimer(mgr.timeoutIndex)
	if mgr.timeoutIndex == 0 {
		cancelTimer.Stop()
	} else {
		defer cancelTimer.Stop()
	}

	subsName := mgr.getSubscriberName(chainID)
	eventBus := mgr.EventBus(chainID)

	if eventBus == nil {
		mgr.logger.Error("failed to start indexerRoutine; nil-EventBus", "chainId", chainID)
		return
	}

	txsSub, _ := eventBus.Subscribe(mgr.Context(), subsName, cmttypes.EventQueryTx)
	mgr.eventSubscribers.Set(chainID, subsName)

	defer func(subscriberName string) {
		eventBus.UnsubscribeAll(mgr.Context(), subscriberName)
		mgr.eventSubscribers.Delete(chainID)
	}(subsName)

	for mgr.Context().Err() == nil {
		select {
		case tx, ok := <-txsSub.Out():
			if !ok {
				return
			}

			// Interpret received transaction result
			txResult := tx.Data().(cmttypes.EventDataTx).TxResult
			rawTx := cmttypes.Tx(txResult.Tx)
			txHash := bytesToHex(rawTx.Hash())

			if mgr.doneIndexedChs.Has(txHash) {
				return // Already indexed, stop here.
			}

			mgr.mtx.Lock()
			defer mgr.mtx.Unlock()

			// Close the "Completion" channel when we have 2/3+1 responses (self included).
			if !mgr.doneIndexedChs.Has(txHash) {
				if ch := mgr.Indexed(txHash); ch != nil {
					mgr.doneIndexedChs.Set(txHash, true)
					close(ch)
				}
			}
			return // DONE
		case <-cancelTimer.C:
			return
		case <-mgr.goShutdownCh:
			return
		}
	}
}

// ----------------------------------------------------------------------------

// getSubscriberName returns a unique subscriber name per transaction hash.
func (mgr *BroadcastPool) getSubscriberName(chainID string) string {
	return strings.Join([]string{
		"BroadcastPool",
		chainID,
	}, "_")
}

// addPartner adds a peer ID as a partner for broadcast of txHash in the pool.
func (mgr *BroadcastPool) addPartner(txHash string, peerID cmtp2p.ID) {
	var prev []cmtp2p.ID
	if mgr.partners.Has(txHash) {
		prev = mgr.partners.Get(txHash).([]cmtp2p.ID)
	}

	if -1 == slices.IndexFunc(prev, func(p cmtp2p.ID) bool {
		return p == peerID
	}) {
		prev = append(prev, peerID)
	}

	mgr.partners.Set(txHash, prev)
}

// evaluateAcceptanceMajority counts the received AckTransactionBroadcast
// messages and evaluates whether we have a super majority (2/3+1).
//
// Note that we don't evaluate a super-majority here because there is always
// the one relay which sends the mempool.Tx messages ("self").
func (mgr *BroadcastPool) evaluateAcceptanceMajority(
	txHash string,
) bool {
	relays := mgr.Relays(txHash)
	if len(relays) == 0 {
		return true // Nothing to do
	}

	// Keep only relevant ones for this acceptance evaluation.
	responses := mgr.Responses(txHash)

	// The number of received ChainReplicationResponse messages
	numRequired := len(relays) * 2 / 3
	return len(responses) >= numRequired
}
