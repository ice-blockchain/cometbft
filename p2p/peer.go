package p2p

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/ice-blockchain/cometbft/internal/cmap"
	"github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtconn "github.com/ice-blockchain/cometbft/p2p/conn"
)

//go:generate ../scripts/mockery_generate.sh Peer

// Same as the default Prometheus scrape interval in order to not lose
// granularity.
const metricsTickerDuration = 1 * time.Second

// Peer is an interface representing a peer connected on a reactor.
type Peer interface {
	service.Service
	FlushStop()

	ID() ID               // peer's cryptographic ID
	RemoteIP() net.IP     // remote IP of the connection
	RemoteAddr() net.Addr // remote address of the connection

	IsOutbound() bool   // did we dial the peer
	IsPersistent() bool // do we redial this peer when we disconnect

	NodeInfo() NodeInfo      // peer's info
	SocketAddr() *NetAddress // actual address of the socket

	Send(chainID string, e Envelope) bool
	TrySend(chainID string, e Envelope) bool

	Set(key string, value any)
	Get(key string) any
	Has(key string) bool

	GetLogger() log.Logger
}

// ----------------------------------------------------------

// peerConn contains the raw connection and its config.
type peerConn struct {
	outbound   bool
	persistent bool
	conn       net.Conn // source connection

	socketAddr *NetAddress

	// cached RemoteIP()
	ip net.IP
}

func newPeerConn(
	outbound, persistent bool,
	conn net.Conn,
	socketAddr *NetAddress,
) peerConn {
	return peerConn{
		outbound:   outbound,
		persistent: persistent,
		conn:       conn,
		socketAddr: socketAddr,
	}
}

// ID only exists for SecretConnection.
// NOTE: Will panic if conn is not *SecretConnection.
func (pc peerConn) ID() ID {
	return PubKeyToID(pc.conn.(*cmtconn.SecretConnection).RemotePubKey())
}

// Return the IP from the connection RemoteAddr.
func (pc peerConn) RemoteIP() net.IP {
	if pc.ip != nil {
		return pc.ip
	}

	host, _, err := net.SplitHostPort(pc.conn.RemoteAddr().String())
	if err != nil {
		panic(err)
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		panic(err)
	}

	pc.ip = ips[0]

	return pc.ip
}

// peer implements Peer.
//
// Before using a peer, you will need to perform a handshake on connection.
type PeerImpl struct {
	service.BaseService

	// raw peerConn and the multiplex connection
	peerConn
	msgr Messager

	// peer's node info and the channel it knows about
	// channels = nodeInfo.Channels
	// cached to avoid copying nodeInfo in hasChannel
	nodeInfo NodeInfo
	channels []byte

	// User data
	Data *cmap.CMap

	metrics        *Metrics
	pendingMetrics *peerPendingMetricsCache
}

type PeerOption func(*PeerImpl)

func newPeer(
	ctx context.Context,
	pc peerConn,
	nodeInfo NodeInfo,
	options ...PeerOption,
) *PeerImpl {
	p := &PeerImpl{
		peerConn:       pc,
		nodeInfo:       nodeInfo,
		channels:       nodeInfo.GetChannels(),
		Data:           cmap.NewCMap(),
		metrics:        NopMetrics(),
		pendingMetrics: newPeerPendingMetricsCache(),
	}

	p.BaseService = *service.NewBaseService(ctx, nil, "Peer", p)
	for _, option := range options {
		option(p)
	}

	return p
}

// String representation.
func (p *PeerImpl) String() string {
	if p.outbound {
		return fmt.Sprintf("Peer{%v out}", p.ID())
	}

	// return fmt.Sprintf("Peer{%v %v in}", p.mconn, p.ID())
	return fmt.Sprintf("Peer{%v in}", p.ID())
}

// ---------------------------------------------------
// Implements service.Service

// SetLogger implements BaseService.
func (p *PeerImpl) SetLogger(l log.Logger) {
	p.Logger = l
	// p.mconn.SetLogger(l)
}

// GetLogger returns the Logger.
func (p *PeerImpl) GetLogger() log.Logger {
	return p.Logger
}

// OnStart implements BaseService.
func (p *PeerImpl) OnStart(ctx context.Context) error {
	if err := p.BaseService.OnStart(ctx); err != nil {
		p.Logger.Error("Error starting peer service", "err", err)
	}

	p.Logger.Debug("Peer started")
	return nil
}

// FlushStop mimics OnStop but additionally ensures that all successful
// .Send() calls will get flushed before closing the connection.
//
// NOTE: it is not safe to call this method more than once.
func (p *PeerImpl) FlushStop() {}

// OnStop implements BaseService.
func (p *PeerImpl) OnStop() {
	p.Logger.Debug("Peer stopped")
}

// OnReset implements service.Service.
func (p *PeerImpl) OnReset(ctx context.Context) error {
	p.Logger.Debug("Peer reset")
	return nil
}

// ---------------------------------------------------
// Implements Peer

// ID returns the peer's ID - the hex encoded hash of its pubkey.
func (p *PeerImpl) ID() ID {
	return p.nodeInfo.ID()
}

// IsOutbound returns true if the connection is outbound, false otherwise.
func (p *PeerImpl) IsOutbound() bool {
	return p.peerConn.outbound
}

// IsPersistent returns true if the peer is persistent, false otherwise.
func (p *PeerImpl) IsPersistent() bool {
	return p.peerConn.persistent
}

// NodeInfo returns a copy of the peer's NodeInfo.
func (p *PeerImpl) NodeInfo() NodeInfo {
	return p.nodeInfo
}

// SocketAddr returns the address of the socket.
// For outbound peers, it's the address dialed (after DNS resolution).
// For inbound peers, it's the address returned by the underlying connection
// (not what's reported in the peer's NodeInfo).
func (p *PeerImpl) SocketAddr() *NetAddress {
	return p.peerConn.socketAddr
}

// Send msg bytes to the channel identified by chID byte. Returns false if the
// send queue is full after timeout, specified by MConnection.
//
// thread safe.
func (p *PeerImpl) Send(chainID string, e Envelope) bool {
	if p.msgr == nil {
		p.Logger.Error("failed to send message; missing Messager",
			"peer", p,
			"msg", e,
		)
		return false
	}

	if err := p.msgr.Send(p.ID(), e); err != nil {
		p.Logger.Error("failed to send message",
			"peer", p,
			"msg", e,
			"err", err,
		)
		return false
	}

	return true
}

// TrySend msg bytes to the channel identified by chID byte. Immediately returns
// false if the send queue is full.
//
// thread safe.
func (p *PeerImpl) TrySend(chainID string, e Envelope) bool {
	if p.msgr == nil {
		p.Logger.Error("failed to try-send message; missing Messager",
			"peer", p,
			"msg", e,
		)
		return false
	}

	if err := p.msgr.TrySend(p.ID(), e); err != nil {
		p.Logger.Error("failed to try-send message",
			"peer", e.Src,
			"err", err,
		)
		return false
	}

	return true
}

// Has checks if data is present for the given key.
//
// thread safe.
func (p *PeerImpl) Has(key string) bool {
	return p.Data.Has(key)
}

// Get the data for a given key.
//
// thread safe.
func (p *PeerImpl) Get(key string) any {
	return p.Data.Get(key)
}

// Set sets the data for the given key.
//
// thread safe.
func (p *PeerImpl) Set(key string, data any) {
	p.Data.Set(key, data)
}

// hasChannel returns true if the peer reported
// knowing about the given chID.
func (p *PeerImpl) hasChannel(chID byte) bool {
	for _, ch := range p.channels {
		if ch == chID {
			return true
		}
	}
	return false
}

// ---------------------------------------------------
// methods only used for testing
// TODO: can we remove these?

// Conn returns the net.Conn instance.
func (pc *peerConn) Conn() net.Conn {
	return pc.conn
}

// CloseConn closes the underlying connection.
func (pc *peerConn) CloseConn() {
	pc.conn.Close()
}

// RemoteAddr returns peer's remote network address.
func (p *PeerImpl) RemoteAddr() net.Addr {
	return p.peerConn.conn.RemoteAddr()
}

// // CanSend returns true if the send queue is not full, false otherwise.
// func (p *PeerImpl) CanSend(chainID string, chID byte) bool {
// 	if !p.IsRunning() {
// 		return false
// 	}
// 	return p.mconn.CanSend(chainID, chID)
// }

// ---------------------------------------------------

func PeerMetrics(metrics *Metrics) PeerOption {
	return func(p *PeerImpl) {
		p.metrics = metrics
	}
}

func PeerMessager(msgr Messager) PeerOption {
	return func(p *PeerImpl) {
		p.msgr = msgr
	}
}

func (p *PeerImpl) metricsReporter() {
	metricsTicker := time.NewTicker(metricsTickerDuration)
	defer metricsTicker.Stop()

	for p.Context().Err() == nil {
		select {
		case <-metricsTicker.C:
			// status := p.mconn.Status()
			// var sendQueueSize float64
			// for _, chStatus := range status.Channels {
			// 	sendQueueSize += float64(chStatus.SendQueueSize)
			// }

			// p.metrics.PeerPendingSendBytes.With("peer_id", string(p.ID())).Set(sendQueueSize)
			// Report per peer, per message total bytes, since the last interval
			func() {
				p.pendingMetrics.mtx.Lock()
				defer p.pendingMetrics.mtx.Unlock()
				for _, entry := range p.pendingMetrics.perMessageCache {
					if entry.pendingSendBytes > 0 {
						p.metrics.MessageSendBytesTotal.
							With("message_type", entry.label).
							Add(float64(entry.pendingSendBytes))
						entry.pendingSendBytes = 0
					}
					if entry.pendingRecvBytes > 0 {
						p.metrics.MessageReceiveBytesTotal.
							With("message_type", entry.label).
							Add(float64(entry.pendingRecvBytes))
						entry.pendingRecvBytes = 0
					}
				}
			}()

		case <-p.Quit():
			return
		}
	}
}
