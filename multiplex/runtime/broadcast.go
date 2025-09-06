package runtime

import (
	"context"
	"slices"
	"strings"
	"sync"
	"time"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
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

	pool        *MessagePool
	resourceMgr *ResourceRegistry

	chainIdsByTxHash map[string]string
	eventSubscribers map[string]string
	timeoutIndex     time.Duration

	// Contains buffered channels of size 1 as we expect exactly 1
	// update on those channels, per transaction hash.
	acceptedChs map[string]chan struct{}
	indexedChs  map[string]chan struct{}

	relays   map[string][]*helpers.RelayAddress
	partners map[string][]cmtp2p.ID

	messages map[string][]*mxp2p.AckTransactionBroadcast

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
		pool:        NewMessageManager(ctx, logger),
		resourceMgr: resourceMgr,

		chainIdsByTxHash: map[string]string{},
		eventSubscribers: map[string]string{},

		acceptedChs:  map[string]chan struct{}{},
		indexedChs:   map[string]chan struct{}{},
		goShutdownCh: make(chan bool), // unbuffered

		relays:   map[string][]*helpers.RelayAddress{},
		partners: map[string][]cmtp2p.ID{},
		messages: map[string][]*mxp2p.AckTransactionBroadcast{},

		// Options
		logger: logger,
	}

	// Use option helpers
	mgr.SetOptions(options...)

	mgr.BaseService = *service.NewBaseService(ctx, nil, "BroadcastPool", mgr)
	return mgr
}

// BroadcastPoolWithLogger injects a custom logger instance.
func BroadcastPoolWithLogger(logger cmtlog.Logger) BroadcastPoolOption {
	return func(mgr *BroadcastPool) {
		mgr.logger = logger
	}
}

// BroadcastPoolWithTimeout injects a custom index timeout.
func BroadcastPoolWithTimeout(timeoutIndex time.Duration) BroadcastPoolOption {
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
	for chainID, subscriberName := range mgr.eventSubscribers {
		if eventBus := mgr.EventBus(chainID); eventBus != nil {
			eventBus.UnsubscribeAll(mgr.Context(), subscriberName)
		}

		delete(mgr.eventSubscribers, chainID)
	}
}

// OnReset implements [service.Service] by resetting the service.
func (mgr *BroadcastPool) OnReset(ctx context.Context) error {
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

	mgr.mtx.Lock()
	prev, has := mgr.relays[txHash]
	mgr.mtx.Unlock()
	if !has {
		prev = []*helpers.RelayAddress{}
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

	mgr.mtx.Lock()
	mgr.relays[txHash] = prev
	mgr.chainIdsByTxHash[txHash] = chainID
	_, hasEventSubscriber := mgr.eventSubscribers[chainID]
	mgr.mtx.Unlock()

	// The transaction indexer pushes events on an event bus instance per ChainID,
	// so we only need the indexer routine once per ChainID, and not for every tx.
	if !hasEventSubscriber {
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

	if e.Src != nil {
		// Adds incoming message to message pool.
		mgr.pool.AddIncoming(e)
	} else {
		mgr.pool.AddOutgoing(peerID, e)
	}

	switch extMsg := e.Message.(type) {
	case *mxp2p.Receipt:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Receipt_AckTransactionBroadcast:
			ackTxBroadcast := extMsg.GetAckTransactionBroadcast()

			// TODO(midas): remove debug logs
			mgr.logger.Debug("Processing AckTransactionBroadcast", "msg", ackTxBroadcast)

			txHash := bytesToHex(ackTxBroadcast.TxHash)

			mgr.mtx.Lock()
			mgr.addPartner(txHash, e.Src.ID())
			mgr.addMessage(ackTxBroadcast)

			isAccepted := mgr.evaluateAcceptanceMajority(txHash)
			mgr.mtx.Unlock()

			// Close the "Accepted" channel when we have 2/3+1 ACK messages.
			ch := mgr.Accepted(txHash)
			if isAccepted {
				if _, ok := <-ch; ok {
					close(ch) // DONE!
				}
			}
		}
	}

	return nil
}

// Partners returns a list of relay ID from broadcast partners for txHash.
func (mgr *BroadcastPool) Partners(txHash string) []cmtp2p.ID {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if p, ok := mgr.partners[txHash]; ok {
		return p
	}

	return []cmtp2p.ID{}
}

// Responses returns the stored ack transaction messages for txHash.
func (mgr *BroadcastPool) Responses(txHash string) []*mxp2p.AckTransactionBroadcast {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if m, ok := mgr.messages[txHash]; ok {
		return m
	}

	return []*mxp2p.AckTransactionBroadcast{}
}

// Accepted returns a channel, which is closed when txHash has 2/3+1 ACK messages.
func (mgr *BroadcastPool) Accepted(txHash string) chan struct{} {
	mgr.mtx.Lock()
	acceptedCh, hasChannel := mgr.acceptedChs[txHash]
	mgr.mtx.Unlock()

	if !hasChannel {
		acceptedCh = make(chan struct{}, 1) // buffered

		mgr.mtx.Lock()
		mgr.acceptedChs[txHash] = acceptedCh
		mgr.mtx.Unlock()
	}

	return acceptedCh
}

// Indexed returns a channel, which is closed when txHash got indexed locally
func (mgr *BroadcastPool) Indexed(txHash string) chan struct{} {
	mgr.mtx.Lock()
	indexedCh, hasChannel := mgr.indexedChs[txHash]
	mgr.mtx.Unlock()

	if !hasChannel {
		indexedCh = make(chan struct{}, 1) // buffered

		mgr.mtx.Lock()
		mgr.indexedChs[txHash] = indexedCh
		mgr.mtx.Unlock()
	}

	return indexedCh
}

// WaitAccepted blocks the thread until txHash has 2/3+1 ACK messages.
func (mgr *BroadcastPool) WaitAccepted(txHash string) bool {
	ch := mgr.Accepted(txHash)

	for mgr.Context().Err() == nil {
		select {
		case <-ch:
			return true
		case <-mgr.Context().Done():
			return false
		}
	}

	return false
}

// WaitIndexed blocks the thread until txHash got indexed locally
func (mgr *BroadcastPool) WaitIndexed(txHash string) bool {
	txIndexedCh := mgr.Indexed(txHash)

	for mgr.Context().Err() == nil {
		select {
		case <-txIndexedCh:
			return true
		case <-mgr.Context().Done():
			return false
		}
	}

	return false
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

	subsName := mgr.getSubscriberName(chainID)
	eventBus := mgr.EventBus(chainID)

	if eventBus == nil {
		mgr.logger.Error("failed to start indexerRoutine; nil-EventBus", "chainId", chainID)
		return
	}

	txsSub, _ := eventBus.Subscribe(mgr.Context(), subsName, cmttypes.EventQueryTx)

	defer func(subscriberName string) {
		eventBus.UnsubscribeAll(mgr.Context(), subscriberName)
		delete(mgr.eventSubscribers, chainID)
	}(subsName)

	defer cancelTimer.Stop()

	mgr.mtx.Lock()
	mgr.eventSubscribers[chainID] = subsName
	mgr.mtx.Unlock()

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

			txIndexedCh := mgr.Indexed(txHash)
			close(txIndexedCh)
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
	prev, has := mgr.partners[txHash]
	if !has {
		prev = []cmtp2p.ID{}
	}

	if -1 == slices.IndexFunc(prev, func(p cmtp2p.ID) bool {
		return p == peerID
	}) {
		prev = append(prev, peerID)
	}

	mgr.partners[txHash] = prev
}

// addMessage adds a AckTransactionBroadcast to the pool.
func (mgr *BroadcastPool) addMessage(msg *mxp2p.AckTransactionBroadcast) {
	txHash := bytesToHex(msg.TxHash)
	prev, has := mgr.messages[txHash]
	if !has {
		prev = []*mxp2p.AckTransactionBroadcast{}
	}

	prev = append(prev, msg)
	mgr.messages[txHash] = prev
}

// evaluateAcceptanceMajority counts the received AckTransactionBroadcast
// messages and evaluates whether we have a super majority (2/3+1).
func (mgr *BroadcastPool) evaluateAcceptanceMajority(
	txHash string,
) bool {
	if _, ok := mgr.relays[txHash]; !ok {
		return false
	} else if len(mgr.relays[txHash]) == 0 {
		return true
	}
	if _, ok := mgr.messages[txHash]; !ok {
		return false
	}

	// The number of received AckTransactionBroadcast messages
	messages := mgr.messages[txHash]

	numRequired := len(mgr.relays[txHash])*2/3 + 1
	return len(messages) >= numRequired
}
