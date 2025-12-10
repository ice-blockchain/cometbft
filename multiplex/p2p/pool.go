package p2p

import (
	"context"
	"fmt"
	"net"
	"slices"
	"strings"
	"sync"
	"time"

	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	"github.com/ice-blockchain/cometbft/internal/cmap"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmtconn "github.com/ice-blockchain/cometbft/p2p/conn"
)

// ConnectionPool defines a connection pool.
type ConnectionPool struct {
	service.BaseService
	mtx *sync.Mutex

	// Services
	transport  *cmtp2p.MultiplexTransport
	connector  *PeerConnector
	dispatcher cmtp2p.Dispatcher
	handshaker cmtp2p.Handshaker
	runtimeMgr types.RuntimeManager

	// Resources
	nodeInfo *MultiNetworkNodeInfo
	nodeKey  *cmtp2p.NodeKey
	peers    *cmtp2p.PeerSet
	// dialing contains *cmtp2p.PeerImpl mapped by cmtp2p.ID (string) keys.
	dialing *cmap.CMap

	// peerIdsByChainIds contains cmtp2p.ID mapped by ChainID keys.
	peerIdsByChainIds *cmap.CMap
	// initTimeByPeerKey contains timestamps by peer keys `peerID:chainID`.
	initTimeByPeerKey *cmap.CMap
	// peersForReactors contains *cmtp2p.PeerImpl by peer keys `peerID:chainID`.
	peersForReactors *cmap.CMap
	// peerConnections contains *cmtconn.MConnection by cmtp2p.ID (string) keys.
	peerConnections *cmap.CMap

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
	transport *cmtp2p.MultiplexTransport,
	resourceManager types.ResourceManager,
	logger cmtlog.Logger,
	options ...ConnectionPoolOption,
) *ConnectionPool {
	nodeInfo := transport.NodeInfo().(*MultiNetworkNodeInfo)
	dispatcher := NewDispatcher(ctx, nodeInfo, resourceManager, logger)
	connector := NewConnector(ctx, transport, dispatcher, logger)

	// Use our custom handshaker for all transports.
	handshaker := NewHandshaker(ctx, nodeInfo, logger)
	transport.SetHandshaker(handshaker)
	transport.SetLogger(logger.With("module", "p2p"))

	pool := &ConnectionPool{
		mtx:      new(sync.Mutex),
		nodeKey:  nodeKey,
		nodeInfo: nodeInfo,

		handshaker: NewHandshaker(ctx, nodeInfo, logger),
		dispatcher: dispatcher,
		transport:  transport,
		connector:  connector,

		peers:             cmtp2p.NewPeerSet(),
		dialing:           cmap.NewCMap(),
		peerIdsByChainIds: cmap.NewCMap(),
		initTimeByPeerKey: cmap.NewCMap(),
		peersForReactors:  cmap.NewCMap(),
		peerConnections:   cmap.NewCMap(),

		// Options
		logger: logger,
	}

	// Use option helpers
	pool.SetOptions(options...)
	pool.BaseService = *service.NewBaseService(ctx, logger, "ConnectionPool", pool)

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

// ConnectionPoolWithConnector injects a custom peer connector instance.
func ConnectionPoolWithConnector(c *PeerConnector) ConnectionPoolOption {
	return func(pool *ConnectionPool) {
		pool.connector = c
	}
}

// ConnectionPoolWithNodeKey injects a custom [cmtp2p.NodeKey] instance.
func ConnectionPoolWithNodeKey(k *cmtp2p.NodeKey) ConnectionPoolOption {
	return func(pool *ConnectionPool) {
		pool.nodeKey = k
	}
}

// ConnectionPoolWithNodeInfo injects a custom [cmtp2p.NodeInfo] instance.
func ConnectionPoolWithNodeInfo(i *MultiNetworkNodeInfo) ConnectionPoolOption {
	return func(pool *ConnectionPool) {
		pool.nodeInfo = i
	}
}

// ConnectionPoolWithAutoDialBack configures the PeerConnector to dial back.
func ConnectionPoolWithAutoDialBack(b bool) ConnectionPoolOption {
	return func(pool *ConnectionPool) {
		pool.connector.dialBackInbounds = true
	}
}

// ----------------------------------------------------------------------------
// ConnectionPool implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (pool *ConnectionPool) OnStart(ctx context.Context) (err error) {
	// TODO(midas): remove debug logs
	pool.logger.Debug("Starting connection pool",
		"nodeId", pool.nodeKey.ID(),
		"nodeInfo", pool.nodeInfo,
	)

	if err := pool.connector.Start(); err != nil && err != service.ErrAlreadyStarted {
		return fmt.Errorf("failed to start Connector: %w", err)
	}

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (pool *ConnectionPool) OnStop() {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	// TODO(midas): remove debug logs
	pool.logger.Debug("Stopping connection pool",
		"nodeId", pool.nodeKey.ID(),
		"nodeInfo", pool.nodeInfo,
	)

	peers := pool.peers.Copy()
	for _, peer := range peers {
		if pool.peerConnections.Has(string(peer.ID())) {
			if err := pool.stopRoutines(peer); err != nil && err != service.ErrAlreadyStopped {
				pool.logger.Error("Error stopping routines", "err", err, "peer", peer)
			}

			pool.peerConnections.Delete(string(peer.ID()))
		}
	}

	if err := pool.connector.Stop(); err != nil && err != service.ErrAlreadyStopped {
		pool.logger.Error("failed to stop PeerConnector",
			"err", err,
		)
	}

	pool.transport.Close()
}

// OnReset implements [service.Service] by resetting the service.
func (pool *ConnectionPool) OnReset(ctx context.Context) error {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	pool.dialing = cmap.NewCMap()
	pool.peerIdsByChainIds = cmap.NewCMap()
	pool.initTimeByPeerKey = cmap.NewCMap()
	pool.peersForReactors = cmap.NewCMap()
	pool.peerConnections = cmap.NewCMap()

	// TODO(midas): remove debug logs
	pool.logger.Debug("Connection pool reset",
		"nodeId", pool.nodeKey.ID(),
		"nodeInfo", pool.nodeInfo,
	)
	return nil
}

// ----------------------------------------------------------------------------
// ConnectionManager API implementation

// NodeInfo returns the local relay node information.
func (pool *ConnectionPool) NodeInfo() cmtp2p.NodeInfo {
	return pool.nodeInfo
}

// NodeKey returns the local relay node public key (ed25519), or node ID.
func (pool *ConnectionPool) NodeKey() *cmtp2p.NodeKey {
	return pool.nodeKey
}

// Transport returns the packet transporter.
func (pool *ConnectionPool) Transport() *cmtp2p.MultiplexTransport {
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
func (pool *ConnectionPool) Handshaker() cmtp2p.Handshaker {
	return pool.handshaker
}

// ----------------------------------------------------------------------------
// cmtp2p.Pool API implementation

// NumPeers returns the number of inbound and outbound peers.
// Return order: inbound, outbound, dialing.
func (pool *ConnectionPool) NumPeers(chainIds ...string) (inbound, outbound, dialing int) {
	var peers []*cmtp2p.PeerImpl
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
	return // inbound, outbound, dialing
}

// Peers returns a peerset by its chainID.
func (pool *ConnectionPool) Peers(chainIds ...string) *cmtp2p.PeerSet {
	if len(chainIds) == 0 {
		return pool.peers
	}

	allPeers := pool.peers.Copy()
	outPeers := make([]*cmtp2p.PeerImpl, 0, len(allPeers))

	chainPeerSet := cmtp2p.NewPeerSet()
	for _, chainID := range chainIds {
		if !pool.peerIdsByChainIds.Has(chainID) {
			continue
		}

		peerIds := pool.peerIdsByChainIds.Get(chainID).([]string)
		outPeers = append(outPeers, slices.DeleteFunc(allPeers, func(p *cmtp2p.PeerImpl) bool {
			return !slices.Contains(peerIds, string(p.ID()))
		})...)
	}

	for _, peer := range outPeers {
		chainPeerSet.Add(peer)
	}

	return chainPeerSet
}

// AddPeer registers a new peer in the peerset.
//
// Calling this method starts the internal MConnection routines if they
// have not been started yet, i.e. creates cmtconn.MConnection and Start() it.
// Called by [PeerConnector#Listen] and [PeerConnector#Dial] upon accepting
// inbound/outbound peer connections, respectively.
func (pool *ConnectionPool) AddPeer(peer *cmtp2p.PeerImpl) error {
	if pool.peers.Has(peer.ID()) {
		return nil // Nothing to do
	}

	peerLogger := peer.Logger
	if peerLogger == nil {
		peerLogger = pool.logger.With(
			"self", string(pool.nodeInfo.ID())).With("peer", peer)
		peer.SetLogger(peerLogger)
	}

	peerLogger.Info("Adding peer")

	if !peer.IsRunning() {
		// peer.Start does *not* start a MConnection anymore,
		// instead the connection is started with startRoutines.
		if err := peer.Start(); err != nil && err != service.ErrAlreadyStarted {
			peerLogger.Error("Error starting peer", "err", err)
			return err
		}
	}

	// Add to PeerSet and start MConnection if necessary.
	pool.peers.Add(peer)
	err := func() error {
		pool.mtx.Lock()
		defer pool.mtx.Unlock()

		if !pool.peerConnections.Has(string(peer.ID())) {
			mconn, err := pool.startRoutines(peer)
			if err != nil {
				return fmt.Errorf("failed to AddPeer: %w", err)
			}
			pool.peerConnections.Set(string(peer.ID()), mconn)

			// TODO(midas): remove debug logs.
			peerLogger.Debug("ConnectionPool#AddPeer; connected to peer",
				"mconn", mconn,
			)
		}

		return nil
	}()
	if err != nil {
		return err
	}

	// If we have a one-way connector, or an outbound peer, we don't need
	// to force-add it to the consensus reactor.
	if peer.IsOutbound() || !pool.connector.dialBackInbounds {
		return nil
	}

	// Force the peer to be added to consensus reactor.
	chainIds := pool.runtimeMgr.Composer().GetComposedNetworks()
	for _, chainID := range chainIds {
		conR := pool.dispatcher.Reactor(chainID, "CONSENSUS")
		if conR == nil {
			continue
		}

		pool.logger.Debug("ADDING PEER (consensus!)", "runtimes", chainIds, "peer", peer)
		pfr := conR.InitPeer(peer)
		conR.AddPeer(pfr)
		pool.logger.Debug("ADDED PEER (consensus!)", "chainId", chainID, "peer", peer)
	}

	return nil
}

// RemovePeer removes a peer from the peerset.
//
// Calling this method stops the internal MConnection routines if they
// are currently running. Calls MConnection#Stop and MultiplexTransport#Cleanup.
func (pool *ConnectionPool) RemovePeer(peerID cmtp2p.ID) error {
	if !pool.peers.Has(peerID) {
		return nil // Nothing to do
	}

	peer := pool.peers.Get(peerID)

	peerLogger := peer.Logger
	if peerLogger == nil {
		peerLogger = pool.logger.With(
			"self", string(pool.nodeInfo.ID())).With("peer", peer)
		peer.SetLogger(peerLogger)
	}

	pool.mtx.Lock()
	if pool.peerConnections.Has(string(peerID)) {
		if err := pool.stopRoutines(peer); err != nil {
			peerLogger.Error("Error stopping routines", "err", err, "peer", peer)
		}
		pool.peerConnections.Delete(string(peerID))

		// TODO(midas): remove debug logs.
		peerLogger.Debug("ConnectionPool#RemovePeer; disconnected from peer",
			"peer", peer,
		)
	}
	defer pool.mtx.Unlock()

	pool.peers.Remove(peer)
	return nil
}

// HasPeer returns true if peer is in the PeerSet.
func (pool *ConnectionPool) HasPeer(peer *cmtp2p.PeerImpl) bool {
	return pool.peers.HasPeer(peer)
}

// HasPeerID returns true if id is in the PeerSet.
func (pool *ConnectionPool) HasPeerID(id cmtp2p.ID) bool {
	return pool.peers.Has(id)
}

// HasPeerIP returns true if ip is in the PeerSet.
func (pool *ConnectionPool) HasPeerIP(ip net.IP) bool {
	return pool.peers.HasIP(ip)
}

// GetPeer returns the *cmtp2p.PeerImpl instance by peerID, if available.
func (pool *ConnectionPool) GetPeer(peerID cmtp2p.ID) *cmtp2p.PeerImpl {
	if !pool.peers.Has(peerID) {
		return nil // Nothing to do
	}

	peer := pool.peers.Get(peerID)
	return peer
}

// Broadcast sends a message to all peers.
func (pool *ConnectionPool) Broadcast(e cmtp2p.Envelope) error {
	peerSet := pool.Peers(e.ChainID)

	if peerSet.Size() == 0 {
		pool.logger.Error("Skipping broadcast - peerset is empty",
			"chainId", e.ChainID,
			"msg", e.Message,
		)
		return fmt.Errorf("failed to broadcast; peerset is empty for %s", e.ChainID)
	}

	// TODO(midas): remove debug logs.
	pool.logger.Debug("ConnectionPool#Broadcast",
		"numPeers", peerSet.Size(),
		"peers", peerSet.Copy(),
		"msg", e.Message,
	)

	peers := peerSet.Copy()
	sentWg := sync.WaitGroup{}
	sentWg.Add(len(peers))
	for _, p := range peers {
		go func(peer *cmtp2p.PeerImpl) {
			defer sentWg.Done()

			// TODO(midas): remove debug logs.
			pool.logger.Debug("ConnectionPool#Broadcast; peer.Send()",
				"peer", peer,
				"msg", e.Message,
			)

			if success := peer.Send(e.ChainID, e); !success {
				pool.logger.Error("Failed to broadcast message to peer",
					"peer", p,
					"chainId", e.ChainID,
					"msg", e.Message,
				)
			}
		}(p)
	}

	// Block this thread until all sent.
	sentWg.Wait()

	return nil
}

// TryBroadcast sends a message to all peers.
func (pool *ConnectionPool) TryBroadcast(e cmtp2p.Envelope) error {
	peerSet := pool.Peers(e.ChainID)

	if peerSet.Size() == 0 {
		return fmt.Errorf("failed to try-broadcast; peerset is empty for %s", e.ChainID)
	}

	// TODO(midas): remove debug logs.
	pool.logger.Debug("ConnectionPool#TryBroadcast",
		"numPeers", peerSet.Size(),
		"peers", peerSet.Copy(),
		"msg", e.Message,
	)

	peers := peerSet.Copy()
	sentWg := sync.WaitGroup{}
	sentWg.Add(len(peers))
	for _, p := range peers {
		go func(peer *cmtp2p.PeerImpl) {
			defer sentWg.Done()

			// TODO(midas): remove debug logs.
			pool.logger.Debug("ConnectionPool#TryBroadcast; peer.TrySend()",
				"peer", peer,
				"msg", e.Message,
			)

			if success := peer.TrySend(e.ChainID, e); !success {
				pool.logger.Error("Failed to broadcast message to peer",
					"peer", p,
					"chainId", e.ChainID,
					"msg", e.Message,
				)
			}
		}(p)
	}

	// Block this thread until all sent.
	sentWg.Wait()

	return nil
}

// HasConnection returns true if a connection entry exists for peerID.
func (pool *ConnectionPool) HasConnection(
	peerID cmtp2p.ID,
) bool {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	return pool.peerConnections.Has(string(peerID))
}

// Connection returns the MConnection instance for peerID.
func (pool *ConnectionPool) Connection(peerID cmtp2p.ID) *cmtconn.MConnection {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if !pool.peerConnections.Has(string(peerID)) {
		return nil
	}

	return pool.peerConnections.Get(string(peerID)).(*cmtconn.MConnection)
}

// HasPeerForChainID returns true if the peerID has been added to the
// chainPeers entry for chainID.
func (pool *ConnectionPool) HasPeerForChainID(
	peerID cmtp2p.ID,
	chainID string,
) bool {
	pool.mtx.Lock()
	defer pool.mtx.Unlock()

	if !pool.peerIdsByChainIds.Has(chainID) {
		return false
	}

	peerIds := pool.peerIdsByChainIds.Get(chainID).([]string)
	return slices.Contains(peerIds, string(peerID))
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
		peerIds = pool.peerIdsByChainIds.Get(chainID).([]string)
	}

	if !slices.Contains(peerIds, string(peerID)) {
		peerIds = append(peerIds, string(peerID))
		pool.peerIdsByChainIds.Set(chainID, peerIds)
	}
	return len(peerIds)
}

// InitPeerForChainID calls InitPeer(peerID) for reactors of chainID.
func (pool *ConnectionPool) InitPeerForChainID(
	peerID cmtp2p.ID,
	chainID string,
) (peerForReactor *cmtp2p.PeerImpl) {
	if !pool.peers.Has(peerID) {
		pool.logger.Error("failed to initialize peer; unknown peer ID",
			"chainId", chainID,
			"peerId", peerID,
		)
		return nil
	}

	pool.SetPeerForChainID(peerID, chainID)

	peerKey := strings.Join([]string{string(peerID), chainID}, ":")

	pool.mtx.Lock()
	var lastInitTz time.Time
	if pool.initTimeByPeerKey.Has(peerKey) {
		lastInitTz = pool.initTimeByPeerKey.Get(peerKey).(time.Time)
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

	var peerForReactor *cmtp2p.PeerImpl
	if !pool.peersForReactors.Has(peerKey) {
		peerForReactor = pool.InitPeerForChainID(peerID, chainID)
	} else {
		peerForReactor = pool.peersForReactors.Get(peerKey).(*cmtp2p.PeerImpl)
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

// SetRuntimeManager sets a custom idle manager instance.
func (c *ConnectionPool) SetRuntimeManager(mgr types.RuntimeManager) {
	c.runtimeMgr = mgr
}

// RuntimeManager returns the idle manager instance.
func (c *ConnectionPool) RuntimeManager() types.RuntimeManager {
	return c.runtimeMgr
}

// ----------------------------------------------------------------------------

// startRoutines starts the send and receive routines for peer.
// The mutex must be locked by the caller.
func (pool *ConnectionPool) startRoutines(peer *cmtp2p.PeerImpl) (
	*cmtconn.MConnection,
	error,
) {
	pool.logger.Debug("ConnectionPool#startRoutines",
		"dispatcher", helpers.ReflectTypeName(pool.dispatcher))

	mconn := cmtconn.NewMConnection(pool.Context(),
		string(pool.nodeKey.ID()),
		string(peer.ID()),
		peer.Conn(),
		pool.dispatcher,
		// onReceive:
		func(chainID string, chID byte, msgBytes []byte) {
			pool.dispatcher.Dispatch(peer, tmp2p.PacketMsg{
				ChainID:   chainID,
				ChannelID: int32(chID),
				Data:      msgBytes,
			})
		},
		func(reason any) {
			pool.stopPeerForError(peer, reason)
		},
	)
	mconn.SetLogger(pool.logger)

	if err := mconn.Start(); err != nil && err != service.ErrAlreadyStarted {
		pool.logger.Error("failed to start routines",
			"peer", peer,
			"err", err,
			"running", mconn.IsRunning(),
			"started", mconn.IsStarted(),
			"stopped", mconn.IsStopped(),
			"routines", mconn.HasStartedRoutines(),
		)
	}

	pool.logger.Debug("Storing mconn for connected peer", "peerID", peer.ID(), "mconn", mconn)
	return mconn, nil
}

// stopRoutines stops the send and receive routines for peer.
// The mutex must be locked by the caller.
func (pool *ConnectionPool) stopRoutines(peer *cmtp2p.PeerImpl) error {
	if !pool.peerConnections.Has(string(peer.ID())) {
		return nil
	}

	mconn := pool.peerConnections.Get(string(peer.ID())).(*cmtconn.MConnection)
	if err := mconn.Stop(); err != nil && err != service.ErrAlreadyStopped {
		pool.logger.Error("failed to stop routines",
			"peer", peer,
			"err", err,
		)
	}

	pool.transport.Cleanup(peer)
	return nil
}

// isPersistent returns false because multiplex doesn't allow persistent peers.
func (pool *ConnectionPool) isPersistent(*cmtp2p.NetAddress) bool {
	return false
}

// stopPeerForError removes a peer from the peer set after an error happened.
func (pool *ConnectionPool) stopPeerForError(p *cmtp2p.PeerImpl, r any) {
	pool.logger.Error("Stopping peer for error", "peer", p, "err", p.GetError(), "reason", r)

	pool.RemovePeer(p.ID())
}
