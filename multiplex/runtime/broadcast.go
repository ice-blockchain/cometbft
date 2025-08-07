package runtime

import (
	"context"
	"slices"
	"sync"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// BroadcastPool defines a transaction broadcast pool.
type BroadcastPool struct {
	service.BaseService
	mtx *sync.Mutex

	pool            *MessagePool
	resourceManager *ResourceRegistry

	acceptedChs map[string]chan struct{}
	indexedChs  map[string]chan struct{}

	relays   map[string][]*helpers.RelayAddress
	partners map[string][]cmtp2p.ID

	messages map[string][]*mxp2p.AckTransactionBroadcast

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.BroadcastManager = (*BroadcastPool)(nil)

type BroadcastPoolOption func(*BroadcastPool)

// NewBroadcastManager creates a new broadcast manager.
func NewBroadcastManager(
	ctx context.Context,
	logger cmtlog.Logger,
	options ...BroadcastPoolOption,
) types.BroadcastManager {
	mgr := &BroadcastPool{
		mtx:  new(sync.Mutex),
		pool: NewMessageManager(ctx, logger),

		acceptedChs: map[string]chan struct{}{},
		indexedChs:  map[string]chan struct{}{},

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
}

// OnReset implements [service.Service] by resetting the service.
func (mgr *BroadcastPool) OnReset(ctx context.Context) error {
	return nil
}

// ----------------------------------------------------------------------------
// BroadcastManager API implementation

// Init initializes a replication processor for chainID with relays.
func (mgr *BroadcastPool) Init(
	txHash string,
	relays []*helpers.RelayAddress,
) error {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	prev, has := mgr.relays[txHash]
	if !has {
		prev = []*helpers.RelayAddress{}
	}

	prev = append(prev, relays...)
	mgr.relays[txHash] = prev
	return nil
}

// Process processes a received message e to the replication pool.
func (mgr *BroadcastPool) Process(e cmtp2p.Envelope) error {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	// Adds incoming message to message pool.
	mgr.pool.AddIncoming(e)

	switch extMsg := e.Message.(type) {
	case *mxp2p.Receipt:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Receipt_AckTransactionBroadcast:
			ackTxBroadcast := extMsg.GetAckTransactionBroadcast()

			mgr.logger.Debug("Processing AckTransactionBroadcast", "msg", ackTxBroadcast)

			txHash := bytesToHex(ackTxBroadcast.TxHash)
			mgr.addPartner(txHash, e.Src.ID())
			mgr.addMessage(ackTxBroadcast)

			// Close the "Accepted" channel when we have 2/3+1 ACK messages.
			if mgr.evaluateAcceptanceMajority(txHash) {
				ch := mgr.Accepted(txHash)
				close(ch) // DONE!
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
	ch := mgr.Indexed(txHash)

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
