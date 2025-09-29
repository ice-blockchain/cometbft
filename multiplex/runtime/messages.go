package runtime

import (
	"sync"

	"github.com/cosmos/gogoproto/proto"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// MessageStore contains a map to store messages by string keys.
type MessageStore struct {
	msgs map[string][]*cmtp2p.Envelope
}

// NewMessageStore creates a new message store.
func NewMessageStore() *MessageStore {
	s := &MessageStore{
		msgs: map[string][]*cmtp2p.Envelope{},
	}

	return s
}

// Size returns the number of entries in the store.
func (s *MessageStore) Size() int {
	total := 0
	for _, a := range s.msgs {
		total += len(a)
	}
	return total
}

// Data returns the underlying map of the store.
func (s *MessageStore) Data() map[string][]*cmtp2p.Envelope {
	return s.msgs
}

// ----------------------------------------------------------------------------

// MessagePool defines a chain replication pool.
type MessagePool struct {
	mtx *sync.Mutex
	// TODO(midas): optional persistence using db

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
	// TODO(midas): accept db for optional persistence
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

// Reset clears all message stores from the pool.
func (pool *MessagePool) Reset() error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	pool.incoming = NewMessageStore()
	pool.outgoing = NewMessageStore()
	pool.incomingByType = NewMessageStore()
	pool.outgoingByType = NewMessageStore()
	pool.incomingByChainID = NewMessageStore()
	pool.outgoingByChainID = NewMessageStore()
	return nil
}

// GetMsgType parses the type of message in msg and returns a string.
func (pool *MessagePool) GetMsgType(msg proto.Message) string {
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

// IncomingByPeer returns all the incoming messages from peer p.
func (pool *MessagePool) IncomingByPeer(p cmtp2p.ID) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.incoming.msgs[string(p)]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// OutgoingByPeer returns all the outgoing messages sent to peer p.
func (pool *MessagePool) OutgoingByPeer(p cmtp2p.ID) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.outgoing.msgs[string(p)]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// IncomingByType returns all the incoming messages for type t.
func (pool *MessagePool) IncomingByType(t string) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.incomingByType.msgs[t]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// OutgoingByType returns all the outgoing messages for type t.
func (pool *MessagePool) OutgoingByType(t string) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.outgoingByType.msgs[t]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// IncomingByChainID returns all the incoming messages with ChainID c.
func (pool *MessagePool) IncomingByChainID(c string) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.incomingByChainID.msgs[c]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// OutgoingByChainID returns all the outgoing messages with ChainID c.
func (pool *MessagePool) OutgoingByChainID(c string) []*cmtp2p.Envelope {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if msgs, ok := pool.outgoingByChainID.msgs[c]; ok {
		return msgs
	}
	return []*cmtp2p.Envelope{}
}

// AddIncoming adds a received message to the pool.
func (pool *MessagePool) AddIncoming(e cmtp2p.Envelope) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// Store message by source peer ID.
	if e.Src != nil {
		pool.addMessage(string(e.Src.ID()), pool.incoming, &e)
	}

	// Also store by parsed message type.
	msgType := pool.GetMsgType(e.Message)
	pool.addMessage(msgType, pool.incomingByType, &e)

	// And store by ChainID.
	pool.addMessage(e.ChainID, pool.incomingByChainID, &e)
	return nil
}

// AddOutgoing adds a sent message to the pool.
func (pool *MessagePool) AddOutgoing(dest cmtp2p.ID, e cmtp2p.Envelope) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// Store message by destination peer ID.
	pool.addMessage(string(dest), pool.outgoing, &e)

	// Also store by parsed message type.
	msgType := pool.GetMsgType(e.Message)
	pool.addMessage(msgType, pool.outgoingByType, &e)

	// And store by ChainID.
	pool.addMessage(e.ChainID, pool.outgoingByChainID, &e)
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
	e *cmtp2p.Envelope,
) []*cmtp2p.Envelope {
	// Store message by key.
	byKey, ok := store.msgs[key]
	if !ok {
		byKey = []*cmtp2p.Envelope{}
	}

	byKey = append(byKey, e)
	store.msgs[key] = byKey
	return store.msgs[key]
}
