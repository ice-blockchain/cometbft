package p2p

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/internal/cmap"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
)

// ConnectionPool defines a connection pool.
type ConnectionPool struct {
	service.BaseService
	mtx *sync.Mutex

	// Services
	transport  cmtp2p.Transport
	connector  cmtp2p.Connector
	dispatcher cmtp2p.Dispatcher
	handshaker types.Handshaker

	// Resources
	nodeInfo  *MultiNetworkNodeInfo
	nodeKey   *cmtp2p.NodeKey
	peers     *cmtp2p.PeerSet
	dialing   *cmap.CMap
	connected *cmap.CMap

	peerIdsByChainIds *cmap.CMap
	initTimeByPeerKey *cmap.CMap
	peersForReactors  *cmap.CMap

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.ConnectionManager = (*ConnectionPool)(nil)
var _ cmtp2p.Pool = (*ConnectionPool)(nil)

type ConnectionPoolOption func(*ConnectionPool)

// NewConnectionManager creates a new connection manager around a transport.
func NewConnectionManager(
	ctx context.Context,
	nodeKey *cmtp2p.NodeKey,
	transport cmtp2p.Transport,
	resourceManager types.ResourceManager,
	logger cmtlog.Logger,
	options ...ConnectionPoolOption,
) *ConnectionPool {
	dispatcher := NewDispatcher(ctx, nodeInfo, resourceManager, logger)
	connector := NewConnector(ctx, transport, dispatcher, logger)

	pool := &ConnectionPool{
		mtx:      new(sync.Mutex),
		nodeKey:  nodeKey,
		nodeInfo: transport.NodeInfo(),

		handshaker: NewHandshaker(ctx, nodeInfo, logger),
		dispatcher: dispatcher,
		transport:  transport,
		connector:  connector,

		peers:             cmtp2p.NewPeerSet(),
		dialing:           cmap.NewCMap(),
		connected:         cmap.NewCMap(),
		peerIdsByChainIds: cmap.NewCMap(),
		initTimeByPeerKey: cmap.NewCMap(),
		peersForReactors:  cmap.NewCMap(),

		// Options
		logger: logger,
	}

	// Use option helpers
	pool.SetOptions(options...)
	pool.BaseService = *service.NewBaseService(ctx, nil, "ConnectionPool", pool)

	// Makes sure peer connector knows about this pool.
	connector.SetOptions(ConnectorWithPool(pool))
	return pool
}

// ConnectionPoolWithLogger injects a custom logger instance.
func ConnectionPoolWithLogger(logger cmtlog.Logger) ConnectionPoolOption {
	return func(pool *ConnectionPool) {
		pool.logger = logger
	}
}

// ----------------------------------------------------------------------------
// ConnectionPool implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (pool *ConnectionPool) OnStart(ctx context.Context) (err error) {
	pool.logger.Debug("Starting connection pool",
		"nodeId", pool.nodeKey.ID(),
		"nodeInfo", pool.nodeInfo,
	)

	if err := pool.connector.Start(); err != nil {
		return fmt.Errorf("failed to start Connector: %w", err)
	}

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (pool *ConnectionPool) OnStop() {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	peers := pool.peers.Copy()
	for _, peer := range peers {
		if pool.poolected.Has(string(peer.ID())) {
			if err := pool.stopRoutines(peer); err != nil {
				pool.logger.Error("Error stopping rouutines", "err", err, "peer", p)
			}

			pool.connected.Delete(string(peer.ID()))
		}
	}
}

// OnReset implements [service.Service] by resetting the service.
func (pool *ConnectionPool) OnReset(ctx context.Context) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	pool.dialing = cmap.NewCMap()
	pool.connected = cmap.NewCMap()
	pool.peerIdsByChainIds = cmap.NewCMap()
	pool.initTimeByPeerKey = cmap.NewCMap()
	pool.peersForReactors = cmap.NewCMap()
	return nil
}

// ----------------------------------------------------------------------------
// ConnectionManager API implementation

// NodeKey returns the local relay node public key (ed25519), or node ID.
func (pool *ConnectionPool) NodeKey() *cmtp2p.NodeKey {
	return pool.nodeKey
}

// NodeInfo returns the local relay node information.
func (pool *ConnectionPool) NodeInfo() cmtp2p.NodeInfo {
	return pool.nodeInfo
}

// Transport returns the packet transporter.
func (pool *ConnectionPool) Transport() Transport {
	return pool.transport
}

// Dispatcher returns the injected packet dispatcher.
func (pool *ConnectionPool) Dispatcher() cmtp2p.Dispatcher {
	return pool.dispatcher
}

// Connector returns the injected connection dialer.
func (pool *ConnectionPool) Connector() cmtp2p.Connector {
	return pool.connector
}

// Handshaker returns the injected connection handshaker.
func (pool *ConnectionPool) Handshaker() types.Handshaker {
	return pool.handshaker
}

// ----------------------------------------------------------------------------
// cmtp2p.Pool API implementation

// NumPeers returns the number of inbound and outbound peers.
func (pool *ConnectionPool) NumPeers(chainIds ...string) (inbound, outbound, dialing int) {
	pool.mtx.RLock()
	defer pool.mtx.RUnlock()

	var peers []*PeerImpl
	if len(chainIds) > 0 {
		peers = pool.Peers(chainIds...).Copy()
	} else {
		peers = pool.peers.Copy()
	}

	for _, p := range peers {
		if p.IsOutbound() {
			outbound++
		} else {
			inbound++
		}
	}

	dialing = pool.dialing.Size()
	return outbound, inbound, dialing
}

// Peers returns a peerset by its chainID.
func (pool *ConnectionPool) Peers(chainIds ...string) *cmtp2p.PeerSet {
	pool.mtx.RLock()
	defer pool.mtx.RUnlock()

	if len(chainIds) > 0 {
		allPeers := pool.peers.Copy()
		outPeers := make([]*PeerImpl, 0, len(allPeers))

		chainPeerSet := cmtp2p.NewPeerSet()
		for _, chainID := range chainIds {
			if !pool.peerIdsByChainIds.Has(chainID) {
				continue
			}

			peerIds := pool.peerIdsByChainIds.Get(chainID)
			outPeers = append(outPeers, slices.DeleteFunc(allPeers, func(p *PeerImpl) bool {
				return !slices.Contains(peerIds, string(p.ID()))
			}))
		}

		for _, peer := range outPeers {
			chainPeerSet.Add(peer)
		}

		return chainPeerSet
	}

	return pool.peers
}

// AddPeer registers a new peer in the peerset.
func (pool *ConnectionPool) AddPeer(peer *cmtp2p.PeerImpl) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("Adding peer", "peer", peer)

	pool.mtx.RLock()
	defer pool.mtx.RUnlock()

	// In case this peer got stopped before, we must reset it.`
	if peer.IsStopped() {
		peer.Reset(pool.Context())
	}

	pool.peers.Add(peer)

	//XXX reactor.InitPeer

	if !peer.IsRunning() {
		if err := peer.Start(); err != nil {
			pool.logger.Error("Error starting peer", "err", err, "peer", p)
			return err
		}
	}

	//XXX reactor.AddPeer

	if !pool.connected.Has(string(peer.ID())) {
		if err := pool.connector.startRoutines(peer); err != nil {
			pool.logger.Error("Error starting routines", "err", err, "peer", p)
			return err
		}

		pool.connected.Set(string(peer.ID()), peer)
	}

	return nil
}

// RemovePeer removes a peer from the peerset.
func (pool *ConnectionPool) RemovePeer(peerID cmtp2p.ID) error {
	// TODO(midas): remove debug logs
	pool.logger.Debug("Removing peer", "peerId", peerID)

	pool.mtx.RLock()
	defer pool.mtx.RUnlock()

	if !pool.peers.Has(peerID) {
		return nil // Nothing to do
	}

	peer := pool.peers.Get(peerID)

	if pool.connected.Has(string(peerID)) {
		if err := pool.connector.stopRoutines(peer); err != nil {
			pool.logger.Error("Error stopping routines", "err", err, "peer", p)
		}

		pool.connected.Delete(string(peerID))
	}

	pool.peers.Remove(peer)
	return nil
}

// HasPeer returns true if peer is in the PeerSet.
func (pool *ConnectionPool) HasPeer(peer *PeerImpl) bool {
	return pool.peers.HasPeer(peer)
}

// HasPeerID returns true if id is in the PeerSet.
func (pool *ConnectionPool) HasPeerID(id ID) bool {
	return pool.peers.Has(id)
}

// HasPeerIP returns true if ip is in the PeerSet.
func (pool *ConnectionPool) HasPeerIP(ip net.IP) bool {
	return pool.peers.HasIP(ip)
}

// Broadcast sends a message to all peers.
func (pool *ConnectionPool) Broadcast(e Envelope) error {
	peerSet := pool.Peers(e.ChainID)

	if peerSet.Size() == 0 {
		// TODO(midas): remove debug logs
		pool.logger.Error("Skipping broadcast - peerset is empty",
			"chainId", e.ChainID,
			"msg", e.Message,
		)
		return fmt.Errorf("failed to broadcast; peerset is empty for %s", e.ChainID)
	}

	peers := peerSet.Copy()
	for _, p := range peers {
		go func(peer Peer) {
			success := peer.Send(e.ChainID, e)
			_ = success
		}(p)
	}

	return nil
}

// TryBroadcast sends a message to all peers.
func (pool *ConnectionPool) TryBroadcast(e Envelope) error {
	peerSet := pool.Peers(e.ChainID)

	if peerSet.Size() == 0 {
		return fmt.Errorf("failed to try-broadcast; peerset is empty for %s", e.ChainID)
	}

	peers := peerSet.Copy()
	for _, p := range peers {
		go func(peer Peer) {
			success := peer.TrySend(e.ChainID, e)
			_ = success
		}(p)
	}

	return nil
}

// SetPeerForChainID adds peerID to the chainPeers entry for chainID.
func (pool *ConnectionPool) SetPeerForChainID(
	peerID cmtp2p.ID,
	chainID string,
) int {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	peerIds := []string{}
	if pool.peerIdsByChainIds.Has(chainID) {
		peerIds = pool.peerIdsByChainIds.Get(chainID)
	}

	if !slices.Contains(peerIds, string(peerID)) {
		peerIds = append(peerIds, peerID)
		pool.peerIdsByChainIds.Set(chainID, peerIds)
	}
	return len(peerIds)
}

// InitPeerForChainID calls InitPeer(peerID) for reactors of chainID.
func (pool *ConnectionPool) InitPeerForChainID(
	peerID cmtp2p.ID,
	chainID string,
) (peerForReactor *cmtp2p.PeerImpl) {
	peerKey := strings.Join([]string{string(peerID), chainID}, ":")

	pool.mtx.Lock()
	var lastInitTz time.Time
	if pool.initTimeByPeerKey.Has(peerKey) {
		lastInitTz = pool.initTimeByPeerKey.Get(peerKey)
	}
	pool.mtx.Unlock()

	peer := pool.peers.Get(peerID)
	peerForReactor = peer

	reactors := pool.dispatcher.Reactors(chainID)
	for _, reactor := range reactors {
		// The reactor must have started before the peer, otherwise re-init.
		if lastInitTz.IsZero() || reactor.StartedAt().After(lastInitTz) {
			peerForReactor = reactor.InitPeer(peerForReactor)

			pool.mtx.Lock()
			pool.peersForReactors.Set(peerKey, peerForReactor)
			pool.initTimeByPeerKey.Set(peerKey, time.Now())
			pool.mtx.Unlock()
		}
	}

	return peerForReactor
}

// AddPeerForChainID calls AddPeer(peerID) for reactors of chainID.
func (pool *ConnectionPool) AddPeerForChainID(
	peerID cmtp2p.ID,
	chainID string,
) (added bool) {
	peerKey := strings.Join([]string{string(peerID), chainID}, ":")

	var peerForReactor *PeerImpl
	peer := pool.peers.Get(peerID)

	if !pool.peersForReactors.Has(peerKey) {
		peerForReactor = pool.InitPeerForChainID(peerID, chainID)
	}

	reactors := pool.dispatcher.Reactors(chainID)
	for _, reactor := range reactors {
		reactor.AddPeer(peerForReactor)
		added = true
	}

	return // added
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (c *ConnectionPool) SetOptions(options ...ConnectionPoolOption) {
	for _, option := range options {
		option(c)
	}
}

// Logger returns the logger instance.
func (c *ConnectionPool) Logger() cmtlog.Logger {
	return c.logger
}
