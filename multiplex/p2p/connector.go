package p2p

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmtconn "github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/ice-blockchain/cometbft/types"
)

// peerConnector defines a peer connector as decribed with [cmtp2p.Connector].
type peerConnector struct {
	service.BaseService
	mtx *sync.Mutex

	// Resources
	transport  cmtp2p.Transport
	dispatcher cmtp2p.Dispatcher

	// Services
	pool   *ConnectionPool
	mconns map[cmtp2p.ID]*cmtconn.MConnection

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ cmtp2p.Connector = (*peerConnector)(nil)
var _ cmtp2p.Messager = (*peerConnector)(nil)

type ConnectorOption func(*peerConnector)

// NewConnector creates a new database service.
func NewConnector(
	ctx context.Context,
	transport cmtp2p.Transport,
	dispatcher cmtp2p.Dispatcher,
	logger cmtlog.Logger,
	options ...ConnectorOption,
) *peerConnector {
	conn := &peerConnector{
		mtx:        new(sync.Mutex),
		transport:  transport,
		dispatcher: dispatcher,

		// Services
		mconns: make(map[cmtp2p.ID]*cmtconn.MConnection, 0),

		// Options
		logger: logger,
	}

	// Use option helpers
	conn.SetOptions(options...)

	conn.BaseService = *service.NewBaseService(ctx, nil, "peerConnector", conn)
	return conn
}

// ConnectorWithLogger injects a custom logger instance.
func ConnectorWithLogger(logger cmtlog.Logger) ConnectorOption {
	return func(conn *peerConnector) {
		conn.logger = logger
	}
}

// ConnectorWithPool injects a custom connection pool.
func ConnectorWithPool(pool *ConnectionPool) ConnectorOption {
	return func(conn *peerConnector) {
		conn.pool = pool
	}
}

// ----------------------------------------------------------------------------
// peerConnector implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (conn *peerConnector) OnStart(ctx context.Context) (err error) {
	if conn.pool == nil {
		return errors.New("peerConnector requires a connection pool")
	}

	// Start accepting Peers.
	go conn.Listen()

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (conn *peerConnector) OnStop() {}

// OnReset implements [service.Service] by resetting the service.
func (conn *peerConnector) OnReset(ctx context.Context) error {
	return nil
}

// ----------------------------------------------------------------------------
// cmtp2p.Connector API implementation

// Dial dials addr or returns an error.
func (conn *peerConnector) Dial(addr *cmtp2p.NetAddress) (*cmtp2p.PeerImpl, error) {
	// TODO(midas): remove debug logs
	conn.logger.Debug("Dialing peer", "address", addr)

	conn.mtx.Lock()
	defer conn.mtx.Unlock()

	// Dial the remote relay
	p, err := conn.transport.Dial(conn.Context(), *addr, cmtp2p.NewPeerConfig(
		conn.dispatcher,
		conn.stopPeerForError,
		cmtp2p.PeerConfigOutbound(true),
	))
	if err != nil {
		conn.handleErrorGracefully(err)
		conn.logger.Error("Outbound peer rejected",
			"addr", addr.DialString(),
			"peerId", addr.ID,
			"err", err,
		)
		return nil, err
	}

	// Inject the messager
	cmtp2p.PeerMessager(conn)(p)

	if err := conn.pool.AddPeer(p); err != nil {
		conn.logger.Error("failed to add outbound peer to set",
			"addr", addr.DialString(),
			"peer", p,
			"err", err,
		)
		return p, err
	}

	return p, nil
}

// Listen listens for peer connections.
func (conn *peerConnector) Listen() error {
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
			conn.stopPeerForError,
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

			conn.logger.Error("Inbound peer rejected",
				"err", err,
			)
			continue
		}

		// Inject the messager
		cmtp2p.PeerMessager(conn)(p)

		if err := conn.pool.AddPeer(p); err != nil {
			conn.logger.Error("failed to add inbound peer to set",
				"peer", p,
				"err", err,
			)
			continue
		}
	}

	return nil
}

// Send sends a packet to peerID.
func (conn *peerConnector) Send(e cmtp2p.Envelope) error {
	if _, ok := conn.mconns[e.Src.ID()]; !ok {
		return fmt.Errorf(
			"missing MConnection for peer %s", e.Src.ID())
	}

	msgBytes, err := conn.wrapMsgBytes(e.Message)
	if err != nil {
		return err
	}

	// Make sure this peer appears in the peerset per ChainID.
	defer conn.pool.SetPeerForChainID(
		e.Src.ID(),
		e.ChainID,
	)

	mconn := conn.mconns[e.Src.ID()]
	if sent := mconn.Send(e.ChainID, e.ChannelID, msgBytes); !sent {
		return fmt.Errorf(
			"error while sending message to %s", e.Src.ID())
	}
	return nil
}

// TrySend tries to send a packet to peerID.
func (conn *peerConnector) TrySend(e cmtp2p.Envelope) error {
	if _, ok := conn.mconns[e.Src.ID()]; !ok {
		return fmt.Errorf(
			"missing MConnection for peer %s", e.Src.ID())
	}

	msgBytes, err := conn.wrapMsgBytes(e.Message)
	if err != nil {
		return err
	}

	// Make sure this peer appears in the peerset per ChainID.
	defer conn.pool.SetPeerForChainID(
		e.Src.ID(),
		e.ChainID,
	)

	mconn := conn.mconns[e.Src.ID()]
	if sent := mconn.TrySend(e.ChainID, e.ChannelID, msgBytes); !sent {
		return fmt.Errorf(
			"error while sending message to %s", e.Src.ID())
	}
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (conn *peerConnector) SetOptions(options ...ConnectorOption) {
	for _, option := range options {
		option(conn)
	}
}

// Logger returns the logger instance.
func (conn *peerConnector) Logger() cmtlog.Logger {
	return conn.logger
}

// ----------------------------------------------------------------------------

// startRoutines starts the send and receive routines for peer.
func (conn *peerConnector) startRoutines(peer *cmtp2p.PeerImpl) error {
	mconn := cmtconn.NewMConnection(conn.Context(),
		peer.Conn(),
		conn.dispatcher,
		func(chainID string, chID byte, msgBytes []byte) {
			conn.dispatcher.Dispatch(peer, tmp2p.PacketMsg{
				ChainID:   chainID,
				ChannelID: int32(chID),
				Data:      msgBytes,
			})
		},
		func(reason any) {
			conn.stopPeerForError(peer, reason)
		},
	)
	mconn.SetLogger(conn.logger)

	conn.mtx.Lock()
	conn.mconns[peer.ID()] = mconn
	conn.mtx.Unlock()

	if !mconn.IsRunning() {
		if err := mconn.Start(); err != nil {
			conn.logger.Error("failed to start routines",
				"peer", peer,
				"err", err,
			)
		}
	}

	return nil
}

// stopRoutines stops the send and receive routines for peer.
func (conn *peerConnector) stopRoutines(peer *cmtp2p.PeerImpl) error {
	conn.mtx.Lock()
	defer conn.mtx.Unlock()

	if err := conn.mconns[peer.ID()].Stop(); err != nil {
		conn.logger.Error("failed to stop routines",
			"peer", peer,
			"err", err,
		)
	}

	return nil
}

// ----------------------------------------------------------------------------

// isPersistent returns false because multiplex doesn't allow persistent peers.
func (conn *peerConnector) isPersistent(*cmtp2p.NetAddress) bool {
	return false
}

// stopPeerForError removes a peer from the peer set after an error happened.
func (conn *peerConnector) stopPeerForError(p *cmtp2p.PeerImpl, r any) {
	conn.logger.Error("Stopping peer for error", "peer", p, "reason", r)

	conn.pool.RemovePeer(p.ID())
	conn.stopRoutines(p)
}

// wrapMsgBytes wraps a [proto.Message] and marshals it, or errors.
func (conn *peerConnector) wrapMsgBytes(msg proto.Message) ([]byte, error) {
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
func (conn *peerConnector) handleErrorGracefully(err error) bool {
	switch err := err.(type) {
	case cmtp2p.ErrRejected:
		conn.logger.Error("Peer rejected",
			"err", err,
		)

		return true
	case cmtp2p.ErrFilterTimeout:
		conn.logger.Error("Peer filter timed out",
			"err", err,
		)

		return true
	case cmtp2p.ErrTransportClosed:
		conn.logger.Error("Stopped accept routine, as transport is closed",
			"err", err,
		)
	default:
		conn.logger.Error("Accept on transport errored - accept routine exited",
			"err", err,
		)
	}

	return false
}
