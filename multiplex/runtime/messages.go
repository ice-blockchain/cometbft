package runtime

import (
	"context"
	"sync"

	"github.com/cosmos/gogoproto/proto"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// MessageStore contains a map to store messages by string keys.
type MessageStore struct {
	msgs map[string][]cmtp2p.Envelope
}

// NewMessageStore creates a new message store.
func NewMessageStore() *MessageStore {
	s := &MessageStore{
		msgs: map[string][]cmtp2p.Envelope{},
	}

	return s
}

// MessagePool defines a chain replication pool.
type MessagePool struct {
	service.BaseService
	mtx *sync.Mutex

	incoming *MessageStore
	outgoing *MessageStore

	incomingByType *MessageStore
	outgoingByType *MessageStore

	incomingByChainID *MessageStore
	outgoingByChainID *MessageStore

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.MessageManager = (*MessagePool)(nil)

type MessagePoolOption func(*MessagePool)

// NewMessageManager creates a new database service.
func NewMessageManager(
	ctx context.Context,
	logger cmtlog.Logger,
	options ...MessagePoolOption,
) *MessagePool {
	pool := &MessagePool{
		mtx: new(sync.Mutex),

		incoming: NewMessageStore(),
		outgoing: NewMessageStore(),

		incomingByType: NewMessageStore(),
		outgoingByType: NewMessageStore(),

		incomingByChainID: NewMessageStore(),
		outgoingByChainID: NewMessageStore(),

		// Options
		logger: logger,
	}

	// Use option helpers
	pool.SetOptions(options...)

	pool.BaseService = *service.NewBaseService(ctx, logger, "MessagePool", pool)
	return pool
}

// MessagePoolWithLogger injects a custom logger instance.
func MessagePoolWithLogger(logger cmtlog.Logger) MessagePoolOption {
	return func(pool *MessagePool) {
		pool.logger = logger
	}
}

// ----------------------------------------------------------------------------
// MessageManager API implementation

// AddIncoming adds a received message to the pool.
func (pool *MessagePool) AddIncoming(e cmtp2p.Envelope) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// Store message by source peer ID.
	pool.addMessage(string(e.Src.ID()), pool.incoming, e)

	// Also store by parsed message type.
	msgType := pool.getMsgType(e.Message)
	pool.addMessage(msgType, pool.incomingByType, e)

	// And store by ChainID.
	pool.addMessage(e.ChainID, pool.incomingByChainID, e)
	return nil
}

// AddOutgoing adds a sent message to the pool.
func (pool *MessagePool) AddOutgoing(dest cmtp2p.ID, e cmtp2p.Envelope) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// Store message by source peer ID.
	pool.addMessage(string(dest), pool.outgoing, e)

	// Also store by parsed message type.
	msgType := pool.getMsgType(e.Message)
	pool.addMessage(msgType, pool.outgoingByType, e)

	// And store by ChainID.
	pool.addMessage(e.ChainID, pool.outgoingByChainID, e)
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (pool *MessagePool) SetOptions(options ...MessagePoolOption) {
	for _, option := range options {
		option(pool)
	}
}

// Logger returns the logger instance.
func (pool *MessagePool) Logger() cmtlog.Logger {
	return pool.logger
}

// ----------------------------------------------------------------------------

// addMessage stores a message e under key in a [MessageStore].
func (pool *MessagePool) addMessage(
	key string, // peerID, msgType or chainID
	store *MessageStore,
	e cmtp2p.Envelope,
) []cmtp2p.Envelope {
	// Store message by key.
	byKey, ok := store.msgs[key]
	if !ok {
		byKey = []cmtp2p.Envelope{}
	}

	byKey = append(byKey, e)
	store.msgs[key] = byKey
	return store.msgs[key]
}

// getMsgType parses the type of message in msg.
func (pool *MessagePool) getMsgType(msg proto.Message) string {
	var msgType string
	switch extMsg := msg.(type) {
	case *mxp2p.Receipt:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Receipt_AckTransactionBroadcast:
			msgType = "AckTransactionBroadcast"
		}
	case *mxp2p.Message:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Message_ChainReplicationRequest:
			msgType = "ChainReplicationRequest"
		case *mxp2p.Message_ChainReplicationResponse:
			msgType = "ChainReplicationResponse"
		case *mxp2p.Message_ChainReplicationComplete:
			msgType = "ChainReplicationComplete"
		}
	}

	return msgType
}
