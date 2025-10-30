package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cosmos/gogoproto/proto"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/types"
)

// PeerConnector defines a peer connector as decribed with [cmtp2p.Connector].
type PeerConnector struct {
	service.BaseService
	mtx *sync.Mutex

	// Resources
	transport  *cmtp2p.MultiplexTransport
	dispatcher cmtp2p.Dispatcher

	// Services
	pool *ConnectionPool

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ cmtp2p.Connector = (*PeerConnector)(nil)
var _ cmtp2p.Messager = (*PeerConnector)(nil)

type ConnectorOption func(*PeerConnector)

// NewConnector creates a new database service.
func NewConnector(
	ctx context.Context,
	transport *cmtp2p.MultiplexTransport,
	dispatcher cmtp2p.Dispatcher,
	logger cmtlog.Logger,
	options ...ConnectorOption,
) *PeerConnector {
	conn := &PeerConnector{
		mtx:        new(sync.Mutex),
		transport:  transport,
		dispatcher: dispatcher,

		// Options
		logger: logger,
	}

	// Use option helpers
	conn.SetOptions(options...)

	conn.BaseService = *service.NewBaseService(ctx, logger, "PeerConnector", conn)
	return conn
}

// ConnectorWithLogger injects a custom logger instance.
func ConnectorWithLogger(logger cmtlog.Logger) ConnectorOption {
	return func(conn *PeerConnector) {
		conn.logger = logger
	}
}

// ConnectorWithPool injects a custom connection pool.
func ConnectorWithPool(pool *ConnectionPool) ConnectorOption {
	return func(conn *PeerConnector) {
		conn.pool = pool
	}
}

// ConnectorWithTransport injects a custom packet transporter.
func ConnectorWithTransport(transport *cmtp2p.MultiplexTransport) ConnectorOption {
	return func(conn *PeerConnector) {
		conn.transport = transport
	}
}

// ConnectorWithDispatcher injects a custom packet dispatcher.
func ConnectorWithDispatcher(dispatcher cmtp2p.Dispatcher) ConnectorOption {
	return func(conn *PeerConnector) {
		conn.dispatcher = dispatcher

		if conn.pool != nil {
			conn.pool.dispatcher = dispatcher
		}
	}
}

// ----------------------------------------------------------------------------
// PeerConnector implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (conn *PeerConnector) OnStart(ctx context.Context) (err error) {
	if conn.pool == nil {
		return errors.New("PeerConnector requires a connection pool")
	}

	// TODO(midas): remove debug logs
	conn.logger.Debug("Starting peer connector",
		"nodeId", conn.pool.NodeKey().ID(),
		"nodeInfo", conn.pool.NodeInfo(),
	)

	// MultiplexBackend#NewServer sets a multiplex reactor such that
	// InitChannels may be called here to initialize a channels store.
	conn.dispatcher.InitChannels()

	// Start accepting Peers.
	go conn.Listen()

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (conn *PeerConnector) OnStop() {
	// TODO(midas): remove debug logs
	conn.logger.Debug("Stopping peer connector",
		"nodeId", conn.pool.NodeKey().ID(),
		"nodeInfo", conn.pool.NodeInfo(),
	)

	// Note: we don't need to stop the Listen() routine here because
	// it stops automatically when the conn.Context() expires.
}

// OnReset implements [service.Service] by resetting the service.
func (conn *PeerConnector) OnReset(ctx context.Context) error {
	// TODO(midas): remove debug logs
	conn.logger.Debug("Peer connector reset",
		"nodeId", conn.pool.NodeKey().ID(),
		"nodeInfo", conn.pool.NodeInfo(),
	)

	return nil
}

// ----------------------------------------------------------------------------
// cmtp2p.Connector API implementation

// Pool returns the connection manager (registry).
func (conn *PeerConnector) Pool() cmtp2p.Pool {
	return conn.pool
}

// Transport returns the packet transporter.
func (conn *PeerConnector) Transport() *cmtp2p.MultiplexTransport {
	return conn.transport
}

// Dispatcher returns the injected packet dispatcher.
func (conn *PeerConnector) Dispatcher() cmtp2p.Dispatcher {
	return conn.dispatcher
}

// Dial dials addr or returns an error.
func (conn *PeerConnector) Dial(addr *cmtp2p.NetAddress) (*cmtp2p.PeerImpl, error) {
	loggerWithSelf := conn.logger.With("self", string(conn.transport.NodeInfo().ID()))

	if conn.pool.HasPeerID(addr.ID) {
		// TODO(midas): remove debug logs
		loggerWithSelf.Debug("Skipping dial - already dialed", "address", addr)

		p := conn.pool.peers.Get(addr.ID)
		return p, nil
	}

	// TODO(midas): remove debug logs
	loggerWithSelf.Debug("Dialing peer", "address", addr)

	// Dial the remote relay
	p, err := conn.transport.Dial(conn.Context(), *addr, cmtp2p.NewPeerConfig(
		conn.dispatcher,
		conn.pool.stopPeerForError,
		cmtp2p.PeerConfigOutbound(true),
	))
	if err != nil {
		conn.handleErrorGracefully(err)
		loggerWithSelf.Error("Outbound peer rejected",
			"addr", addr.DialString(),
			"peerId", addr.ID,
			"err", err,
		)
		return nil, err
	}

	// Inject this connector instance as the peer messager.
	cmtp2p.PeerMessager(conn)(p)
	// Inject a custom logger with "self" and "peer".
	cmtp2p.PeerLogger(loggerWithSelf.With("peer", p))(p)

	// AddPeer stores a MConnection instance and starts it.
	if err := conn.pool.AddPeer(p); err != nil {
		loggerWithSelf.Error("failed to add outbound peer to set",
			"addr", addr.DialString(),
			"peer", p,
			"err", err,
		)
		return p, err
	}

	// TODO(midas): remove debug logs
	loggerWithSelf.Debug("Added outbound peer", "peer", p)
	return p, nil
}

// Listen listens for peer connections.
func (conn *PeerConnector) Listen() error {
	peerLogger := conn.logger.With("self", string(conn.transport.NodeInfo().ID()))

	// TODO(midas): remove debug logs
	peerLogger.Debug("PeerConnector#Listen")

	for conn.Context().Err() == nil {
		// Early shutdown detection
		switch {
		case conn.transport.IsClosing():
			return nil
		default: // proceed to Accept
		}

		// Accept incoming remote relay connections.
		p, err := conn.transport.Accept(conn.Context(), cmtp2p.NewPeerConfig(
			conn.dispatcher,
			conn.pool.stopPeerForError,
			cmtp2p.PeerConfigOutbound(false),
		))
		// If Close() was called, exit silently
		if err != nil && conn.transport.IsClosing() {
			return err
		}
		// If another error happened, handle gracefully.
		if err != nil {
			if ok := conn.handleErrorGracefully(err); !ok {
				return err // not possible to continue (transport closed)
			}

			peerLogger.Error("Inbound peer rejected",
				"err", err,
			)
			continue
		}

		// Inject the messager
		cmtp2p.PeerMessager(conn)(p)

		// AddPeer stores a MConnection instance and starts it.
		if err := conn.pool.AddPeer(p); err != nil {
			peerLogger.Error("failed to add inbound peer to set",
				"peer", p,
				"err", err,
			)
			continue
		}

		// TODO(midas): remove debug logs
		peerLogger.Debug("Added inbound peer", "peer", p)
	}

	return nil
}

// Send sends a packet to peerID.
func (conn *PeerConnector) Send(dest cmtp2p.ID, e cmtp2p.Envelope) error {
	peerLogger := conn.logger.With("self", string(conn.transport.NodeInfo().ID()))

	if !conn.pool.HasConnection(dest) {
		return fmt.Errorf(
			"failed to send message; missing MConnection for peer %s", dest)
	}

	msgBytes, err := conn.wrapMsgBytes(e.Message)
	if err != nil {
		return fmt.Errorf("failed to send message for peer %s: %w", dest, err)
	}
	if len(msgBytes) == 0 {
		return fmt.Errorf("failed to send message for peer %s: empty msgBytes", dest)
	}

	// Make sure this peer appears in the peerset per ChainID.
	conn.pool.SetPeerForChainID(
		dest,
		e.ChainID,
	)

	// Retrieve the cmtconn.MConnection instance.
	mconn := conn.pool.Connection(dest)

	// TODO(midas): remove debug logs
	peerLogger.Debug("PeerConnector#Send",
		"peerId", dest,
		"chID", e.ChannelID,
		"msg", msgBytes,
		"mconn", mconn,
		"connected", mconn.IsRunning(),
	)

	if sent := mconn.Send(e.ChainID, e.ChannelID, msgBytes); !sent {
		return fmt.Errorf(
			"failed to send message; Timeout after 10s.")
	}
	return nil
}

// TrySend tries to send a packet to peerID.
func (conn *PeerConnector) TrySend(dest cmtp2p.ID, e cmtp2p.Envelope) error {
	peerLogger := conn.logger.With("self", string(conn.transport.NodeInfo().ID()))

	if !conn.pool.HasConnection(dest) {
		return fmt.Errorf(
			"failed to send message; missing MConnection for peer %s", dest)
	}

	msgBytes, err := conn.wrapMsgBytes(e.Message)
	if err != nil {
		return fmt.Errorf("failed to send message for peer %s: %w", dest, err)
	}
	if len(msgBytes) == 0 {
		return fmt.Errorf("failed to send message for peer %s: empty msgBytes", dest)
	}

	// Make sure this peer appears in the peerset per ChainID.
	conn.pool.SetPeerForChainID(
		dest,
		e.ChainID,
	)

	// Retrieve the cmtconn.MConnection instance.
	mconn := conn.pool.Connection(dest)

	// TODO(midas): remove debug logs
	peerLogger.Debug("PeerConnector#TrySend",
		"dest", dest,
		"chID", e.ChannelID,
		"msg", msgBytes,
		"mconn", mconn.IsRunning(),
	)

	if sent := mconn.TrySend(e.ChainID, e.ChannelID, msgBytes); !sent {
		return fmt.Errorf(
			"failed to send message; Send queue for %s is full.", e.ChainID)
	}
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (conn *PeerConnector) SetOptions(options ...ConnectorOption) {
	for _, option := range options {
		option(conn)
	}
}

// Logger returns the logger instance.
func (conn *PeerConnector) Logger() cmtlog.Logger {
	return conn.logger
}

// Lock locks the connector mutex.
func (conn *PeerConnector) Lock() {
	conn.logger.Debug("PeerConnector#Lock")
	conn.mtx.Lock()
}

// Unlock locks the connector mutex.
func (conn *PeerConnector) Unlock() {
	conn.logger.Debug("PeerConnector#Unlock")
	conn.mtx.Unlock()
}

// ----------------------------------------------------------------------------

// wrapMsgBytes wraps a [proto.Message] and marshals it, or errors.
func (conn *PeerConnector) wrapMsgBytes(msg proto.Message) ([]byte, error) {
	if w, ok := msg.(types.Wrapper); ok {
		msg = w.Wrap()
	}

	msgBytes, err := proto.Marshal(msg)
	if err != nil {
		conn.logger.Error("failed to marshal message to send", "error", err)
		return []byte{}, err
	}

	return msgBytes, nil
}

// handleErrorGracefully returns true if the connector should proceed with
// routines when the error is processed. This method returns false to signal
// that routines must be killed and/or transport must be closed.
func (conn *PeerConnector) handleErrorGracefully(err error) bool {
	switch err := err.(type) {
	case cmtp2p.ErrRejected:
		conn.logger.Error("Peer rejected", "err", err)

		return true
	case cmtp2p.ErrFilterTimeout:
		conn.logger.Error("Peer filter timed out", "err", err)

		return true
	case cmtp2p.ErrTransportClosed:
		conn.logger.Error("Transport is closed", "err", err)
	default:
		conn.logger.Error("Transport errored", "err", err)
	}

	return false
}
