package p2p

import (
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	"github.com/cosmos/gogoproto/proto"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/cmap"
	"github.com/ice-blockchain/cometbft/internal/rand"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
	"github.com/ice-blockchain/cometbft/p2p/conn"
)

const (
	// wait a random amount of time from this interval
	// before dialing peers or reconnecting to help prevent DoS.
	dialRandomizerIntervalMilliseconds = 3000

	// repeatedly try to reconnect for a few minutes
	// ie. 5 * 20 = 100s.
	reconnectAttempts = 20
	reconnectInterval = 5 * time.Second

	// then move into exponential backoff mode for ~1day
	// ie. 3**10 = 16hrs.
	reconnectBackOffAttempts    = 10
	reconnectBackOffBaseSeconds = 3

	ScopeForDiscovery   = "discovery"
	replicationChannel  = byte(0x90)
	ackBroadcastChannel = byte(0x91)
	runtimeChannel      = byte(0x92)
)

var cometMxChannels = []byte{
	ackBroadcastChannel,
	runtimeChannel,
}

// MConnConfig returns an MConnConfig with fields updated
// from the P2PConfig.
func MConnConfig(cfg *config.P2PConfig) conn.MConnConfig {
	mConfig := conn.DefaultMConnConfig()
	mConfig.FlushThrottle = cfg.FlushThrottleTimeout
	mConfig.SendRate = cfg.SendRate
	mConfig.RecvRate = cfg.RecvRate
	mConfig.MaxPacketMsgPayloadSize = cfg.MaxPacketMsgPayloadSize
	mConfig.TestFuzz = cfg.TestFuzz
	mConfig.TestFuzzConfig = cfg.TestFuzzConfig
	return mConfig
}

// -----------------------------------------------------------------------------

// An AddrBook represents an address book from the pex package, which is used
// to store peer addresses.
type AddrBook interface {
	AddAddress(addr *NetAddress, src *NetAddress) error
	AddPrivateIDs(ids []string)
	AddOurAddress(addr *NetAddress)
	OurAddress(addr *NetAddress) bool
	MarkGood(id ID)
	RemoveAddress(addr *NetAddress)
	HasAddress(addr *NetAddress) bool
	Save()
	Size() int
}

// PeerFilterFunc to be implemented by filter hooks after a new Peer has been
// fully setup.
type PeerFilterFunc func(IPeerSet, Peer) error

// -----------------------------------------------------------------------------

// Switch handles peer connections and exposes an API to receive incoming messages
// on `Reactors`.  Each `Reactor` is responsible for handling incoming messages of one
// or more `Channels`.  So while sending outgoing messages is typically performed on the peer,
// incoming messages are received on the reactor.
type Switch struct {
	service.BaseService

	config        *config.P2PConfig
	reactorsMtx   *sync.Mutex
	reactors      map[string]map[string]Reactor
	chDescs       map[string][]*conn.ChannelDescriptor
	reactorsByCh  map[string]map[byte]Reactor
	msgTypeByChID map[string]map[byte]proto.Message

	dialing      *cmap.CMap
	reconnecting *cmap.CMap

	networksMtx *sync.Mutex
	nodeInfo    NodeInfo // our node info
	nodeKey     *NodeKey // our node privkey
	addrBook    AddrBook
	// peers addresses with whom we'll maintain constant connection
	persistentPeersAddrs []*NetAddress
	unconditionalPeerIDs map[ID]struct{}

	transport Transport

	filterTimeout time.Duration
	peerFilters   []PeerFilterFunc

	rng *rand.Rand // seed for randomizing dial times and orders

	metrics *Metrics

	peersMtx     *cmtsync.RWMutex
	peersByScope map[string]*PeerSet
	Typ          string

	runtimesMtx       *cmtsync.RWMutex
	startTz           time.Time
	runtimesChainIds  map[string]struct{}
	reactorPeerTimes  map[string]map[string]time.Time
	totalOpenChannels uint32 // atomic
}

type peerOption = func(p Peer)

// NetAddress returns the address the switch is listening on.
func (sw *Switch) NetAddress() *NetAddress {
	addr := sw.transport.NetAddress()
	return &addr
}

// SwitchOption sets an optional parameter on the Switch.
type SwitchOption func(*Switch)

// NewSwitch creates a new Switch with the given config.
func NewSwitch(
	cfg *config.P2PConfig,
	transport Transport,
	options ...SwitchOption,
) *Switch {
	sw := &Switch{
		config:               cfg,
		reactorsMtx:          new(sync.Mutex),
		networksMtx:          new(sync.Mutex),
		reactors:             make(map[string]map[string]Reactor),
		chDescs:              make(map[string][]*conn.ChannelDescriptor),
		reactorsByCh:         make(map[string]map[byte]Reactor),
		msgTypeByChID:        make(map[string]map[byte]proto.Message),
		peersMtx:             new(cmtsync.RWMutex),
		runtimesMtx:          new(cmtsync.RWMutex),
		runtimesChainIds:     make(map[string]struct{}),
		reactorPeerTimes:     make(map[string]map[string]time.Time),
		dialing:              cmap.NewCMap(),
		reconnecting:         cmap.NewCMap(),
		metrics:              NopMetrics(),
		transport:            transport,
		filterTimeout:        defaultFilterTimeout,
		persistentPeersAddrs: make([]*NetAddress, 0),
		unconditionalPeerIDs: make(map[ID]struct{}),
	}

	sw.peersMtx.Lock()
	sw.peersByScope = map[string]*PeerSet{ScopeForDiscovery: NewPeerSet()}
	sw.peersMtx.Unlock()

	sw.reactorsMtx.Lock()
	sw.reactors[conn.SharedChannelsNamespace] = make(map[string]Reactor)
	sw.chDescs[conn.SharedChannelsNamespace] = make([]*conn.ChannelDescriptor, 0)
	sw.reactorsByCh[conn.SharedChannelsNamespace] = make(map[byte]Reactor)
	sw.msgTypeByChID[conn.SharedChannelsNamespace] = make(map[byte]proto.Message)
	sw.reactorsMtx.Unlock()
	atomic.StoreUint32(&sw.totalOpenChannels, 0)

	// Ensure we have a completely undeterministic PRNG.
	sw.rng = rand.NewRand()

	sw.BaseService = *service.NewBaseService(nil, "P2P Switch", sw)

	for _, option := range options {
		option(sw)
	}
	sw.Transport().SetLogger(sw.Logger)
	return sw
}

func (sw *Switch) SetLogger(l cmtlog.Logger) {
	sw.Logger = l.With("typ", sw.Typ)
	sw.Transport().SetLogger(sw.Logger)
}

// SwitchFilterTimeout sets the timeout used for peer filters.
func SwitchFilterTimeout(timeout time.Duration) SwitchOption {
	return func(sw *Switch) { sw.filterTimeout = timeout }
}

// SwitchPeerFilters sets the filters for rejection of new peers.
func SwitchPeerFilters(filters ...PeerFilterFunc) SwitchOption {
	return func(sw *Switch) { sw.peerFilters = filters }
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *Metrics) SwitchOption {
	return func(sw *Switch) { sw.metrics = metrics }
}

// ---------------------------------------------------------------------
// Switch setup

// GetActiveRuntimes returns the list of active ChainIDs.
// thread safe.
func (sw *Switch) GetActiveRuntimes() []string {
	sw.runtimesMtx.RLock()
	defer sw.runtimesMtx.RUnlock()

	runtimes := make([]string, 0, len(sw.runtimesChainIds))
	for chainID, _ := range sw.runtimesChainIds {
		runtimes = append(runtimes, chainID)
	}

	return runtimes
}

// AddRuntime adds a ChainID
// thread safe.
func (sw *Switch) AddActiveRuntime(chainID string) {
	sw.runtimesMtx.RLock()
	_, hasRuntime := sw.runtimesChainIds[chainID]
	sw.runtimesMtx.RUnlock()

	if !hasRuntime {
		sw.runtimesMtx.Lock()
		sw.runtimesChainIds[chainID] = struct{}{}
		sw.runtimesMtx.Unlock()
	}
}

// RemoveRuntime adds a ChainID
// thread safe.
func (sw *Switch) RemoveActiveRuntime(chainID string) {
	sw.runtimesMtx.RLock()
	_, hasRuntime := sw.runtimesChainIds[chainID]
	sw.runtimesMtx.RUnlock()

	if hasRuntime {
		sw.runtimesMtx.Lock()
		delete(sw.runtimesChainIds, chainID)
		sw.runtimesMtx.Unlock()
	}
}

// AddReactor adds the given reactor to the switch.
func (sw *Switch) AddReactor(chainID string, name string, reactor Reactor) Reactor {
	// JiT initialize the map of peers by ChainID because when the switch
	// is created, we may not know about all (or any) ChainID.
	sw.peersMtx.RLock()
	_, hasPeerSet := sw.peersByScope[chainID]
	sw.peersMtx.RUnlock()

	if !hasPeerSet {
		sw.peersMtx.Lock()
		sw.peersByScope[chainID] = NewPeerSet()
		sw.peersMtx.Unlock()
	}

	sw.AddActiveRuntime(chainID)

	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	if _, ok := sw.reactors[chainID]; !ok {
		sw.reactors[chainID] = make(map[string]Reactor)
	}
	if _, ok := sw.chDescs[chainID]; !ok {
		sw.chDescs[chainID] = make([]*conn.ChannelDescriptor, 0)
	}
	if _, ok := sw.reactorsByCh[chainID]; !ok {
		sw.reactorsByCh[chainID] = make(map[byte]Reactor)
	}
	if _, ok := sw.msgTypeByChID[chainID]; !ok {
		sw.msgTypeByChID[chainID] = make(map[byte]proto.Message)
	}

	for _, chDesc := range reactor.GetChannels() {
		chID := chDesc.ID

		// No two reactors can share the same channel.
		// TODO(midas): removed panic must be evaluated by callers, i.e. (Reactor, err).
		if _, exists := sw.reactorsByCh[chainID][chID]; exists {
			sw.Logger.Error("Failed to add reactor: channel has multiple reactors",
				"chID", fmt.Sprintf("%d (0x%X)", chID, chID),
				"chain_id", chainID,
				"reactor_1", sw.reactorsByCh[chainID][chID],
				"reactor_2", reactor,
			)
			return reactor
		}

		sw.chDescs[chainID] = append(sw.chDescs[chainID], chDesc)
		sw.reactorsByCh[chainID][chID] = reactor
		sw.msgTypeByChID[chainID][chID] = chDesc.MessageType
	}
	sw.reactors[chainID][name] = reactor
	reactor.SetSwitch(sw)
	return reactor
}

// RemoveReactor removes the given Reactor from the Switch.
func (sw *Switch) RemoveReactor(chainID string, name string, reactor Reactor) {
	sw.RemoveActiveRuntime(chainID)

	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	for _, chDesc := range reactor.GetChannels() {
		// remove channel description
		for i := 0; i < len(sw.chDescs[chainID]); i++ {
			if chDesc.ID == sw.chDescs[chainID][i].ID {
				sw.chDescs[chainID] = append(sw.chDescs[chainID][:i], sw.chDescs[chainID][i+1:]...)
				break
			}
		}
		delete(sw.reactorsByCh[chainID], chDesc.ID)
		delete(sw.msgTypeByChID[chainID], chDesc.ID)
	}
	delete(sw.reactors, name)
	reactor.SetSwitch(nil)
}

// Reactors returns a map of reactors registered on the switch.
func (sw *Switch) Reactors(chainID string) map[string]Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	return sw.reactors[chainID]
}

// Reactor returns the reactor with the given name.
func (sw *Switch) Reactor(chainID string, name string) Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	return sw.reactors[chainID][name]
}

// SetNodeInfo sets the switch's NodeInfo for checking compatibility and handshaking with other nodes.
func (sw *Switch) SetNodeInfo(nodeInfo NodeInfo) {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	sw.nodeInfo = nodeInfo
}

// NodeInfo returns the switch's NodeInfo.
func (sw *Switch) NodeInfo() NodeInfo {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	return sw.nodeInfo
}

// SetNodeKey sets the switch's private key for authenticated encryption.
func (sw *Switch) SetNodeKey(nodeKey *NodeKey) {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	sw.nodeKey = nodeKey
}

// GetPeerConfig returns the peer configuration object.
func (sw *Switch) GetPeerConfig() peerConfig {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	// Check if we have a multiplex reactor that can provide additional ChainIDs
	multiplexReactor := sw.getMultiplexReactor()
	if multiplexReactor != nil {
		// Use type assertion to access GetNetworks method
		if mxReactor, ok := multiplexReactor.(interface{ GetNetworks() []string }); ok {
			// Get all known networks from multiplex reactor
			allNetworks := mxReactor.GetNetworks()

			// Ensure all networks have channels and reactors
			sw.ensureChannelsForNetworks(allNetworks)
		}
	}

	return peerConfig{
		chDescs:       sw.chDescs,
		onPeerError:   sw.StopPeerForError,
		isPersistent:  sw.IsPeerPersistent,
		reactorsByCh:  sw.reactorsByCh,
		msgTypeByChID: sw.msgTypeByChID,
		metrics:       sw.metrics,
	}
}

// Transport returns the switch's Transport.
func (sw *Switch) Transport() *MultiplexTransport {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	if sw.transport != nil {
		return sw.transport.(*MultiplexTransport)
	}

	return nil
}

// Metrics returns the p2p metrics.
func (sw *Switch) Metrics() *Metrics {
	return sw.metrics
}

// getMultiplexReactor returns the multiplex reactor if it exists
func (sw *Switch) getMultiplexReactor() Reactor {
	// Look for multiplex reactor in shared channels namespace
	if reactors, ok := sw.reactors[conn.SharedChannelsNamespace]; ok {
		if multiplexReactor, ok := reactors["MULTIPLEX"]; ok {
			return multiplexReactor
		}
	}
	return nil
}

// ensureChannelsForNetworks ensures that all networks have proper channels and reactors
func (sw *Switch) ensureChannelsForNetworks(networks []string) {
	for _, chainID := range networks {
		// Skip if channels already exist for this chainID
		if _, exists := sw.chDescs[chainID]; exists {
			continue
		}

		// Initialize empty maps if they don't exist
		if sw.chDescs[chainID] == nil {
			sw.chDescs[chainID] = []*conn.ChannelDescriptor{}
		}
		if sw.reactorsByCh[chainID] == nil {
			sw.reactorsByCh[chainID] = make(map[byte]Reactor)
		}
		if sw.msgTypeByChID[chainID] == nil {
			sw.msgTypeByChID[chainID] = make(map[byte]proto.Message)
		}
	}
}

// ---------------------------------------------------------------------
// Service start/stop

// OnStart implements BaseService. It starts all the reactors and peers.
func (sw *Switch) OnStart() error {
	sw.runtimesMtx.Lock()
	sw.startTz = time.Now()
	sw.runtimesMtx.Unlock()

	sw.reactorsMtx.Lock()
	safeReactors := sw.reactors
	sw.reactorsMtx.Unlock()

	// Start reactors
	for _, reactors := range safeReactors {
		for _, reactor := range reactors {
			if !reactor.IsRunning() {
				if err := reactor.Start(); err != nil {
					return fmt.Errorf("failed to start %v: %w", reactor, err)
				}
			}
		}
	}

	// Start accepting Peers.
	go sw.acceptRoutine()

	return nil
}

// OnStop implements BaseService. It stops all peers and reactors.
func (sw *Switch) OnStop() {
	// Stop all peers
	peerSets := sw.PeersByScopes()

	// stopAndRemove locks the mutex
	for _, peerSet := range peerSets {
		peers := peerSet.Copy()
		for _, p := range peers {
			sw.stopAndRemovePeer(p, nil)
		}
	}

	sw.reactorsMtx.Lock()
	safeReactors := sw.reactors
	sw.reactorsMtx.Unlock()

	// Stop reactors
	sw.Logger.Debug("Switch: Stopping reactors")
	for _, reactors := range safeReactors {
		for _, reactor := range reactors {
			if reactor.IsRunning() {
				if err := reactor.Stop(); err != nil && err != service.ErrAlreadyStopped {
					sw.Logger.Error("error while stopped reactor", "reactor", reactor, "err", err)
				}
			}
		}
	}
}

// ---------------------------------------------------------------------
// Peers

// Broadcast runs a go routine for each attempted send, which will block trying
// to send for defaultSendTimeoutSeconds.
//
// NOTE: Broadcast uses goroutines, so order of broadcast may not be preserved.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) Broadcast(chainID string, e Envelope) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	if peerSet, ok := sw.peersByScope[chainID]; ok {
		peers := peerSet.Copy()
		for _, p := range peers {
			go func(peer Peer) {
				success := peer.Send(chainID, e)
				_ = success
			}(p)
		}
	}
}

// TryBroadcast runs a go routine for each attempted send.
// If the send queue of the destination channel and peer are full, the message will not be sent. To make sure that messages are indeed sent to all destination, use `Broadcast`.
//
// NOTE: TryBroadcast uses goroutines, so order of broadcast may not be preserved.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) TryBroadcast(chainID string, e Envelope) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	if peerSet, ok := sw.peersByScope[chainID]; ok {
		peers := peerSet.Copy()
		for _, p := range peers {
			go func(peer Peer) {
				peer.TrySend(chainID, e)
			}(p)
		}
	}
}

// NumPeers returns the count of outbound/inbound and outbound-dialing peers.
// unconditional peers are not counted here.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) NumPeers(chainID string) (outbound, inbound, dialing int) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	if peerSet, ok := sw.peersByScope[chainID]; ok {
		peerSet.ForEach(func(p *PeerImpl) {
			if p.IsOutbound() && !sw.IsPeerUnconditional(p.ID()) {
				outbound++
			} else if !sw.IsPeerUnconditional(p.ID()) {
				inbound++
			}
		})
	}

	dialing = sw.dialing.Size()
	return outbound, inbound, dialing
}

// TotalNumPeers returns the total count of outbound/inbound and outbound-dialing
// peers across all ChainIDs. Unconditional peers are not counted here.
func (sw *Switch) TotalNumPeers() (outbound, inbound, dialing int) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		peerSet.ForEach(func(p *PeerImpl) {
			if p.IsOutbound() && !sw.IsPeerUnconditional(p.ID()) {
				outbound++
			} else if !sw.IsPeerUnconditional(p.ID()) {
				inbound++
			}
		})
	}

	dialing = sw.dialing.Size()
	return outbound, inbound, dialing
}

func (sw *Switch) IsPeerUnconditional(id ID) bool {
	_, ok := sw.unconditionalPeerIDs[id]
	return ok
}

// MaxNumOutboundPeers returns a maximum number of outbound peers.
func (sw *Switch) MaxNumOutboundPeers() int {
	return sw.config.MaxNumOutboundPeers
}

func (sw *Switch) PeersByScopes() map[string]*PeerSet {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	return sw.peersByScope
}

// AllPeers returns a flattened slice of Peer.
func (sw *Switch) AllPeers() []*PeerImpl {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	allPeers := []*PeerImpl{}
	for _, peerSet := range sw.peersByScope {
		allPeers = append(allPeers, peerSet.Copy()...)
	}

	return allPeers
}

// Peers returns the set of peers that are connected to the switch.
// Requires a scope to filter the returned peers instances, note that
// the scope may contain a ChainID, or "discovery".
func (sw *Switch) Peers(scope string) *PeerSet {
	sw.peersMtx.RLock()
	if peerSet, ok := sw.peersByScope[scope]; ok {
		defer sw.peersMtx.RUnlock()
		return peerSet
	}
	sw.peersMtx.RUnlock()

	peerSet := NewPeerSet()
	sw.peersMtx.Lock()
	defer sw.peersMtx.Unlock()
	sw.peersByScope[scope] = peerSet

	return peerSet
}

func (sw *Switch) HasPeer(peer *PeerImpl) bool {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		if peerSet.HasPeer(peer) {
			return true
		}
	}

	return false
}

func (sw *Switch) HasPeerInOrOut(id ID) (bool, string) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for scope, peerSet := range sw.peersByScope {
		if peerSet.Has(id) {
			return true, scope
		}
	}

	return false, ""
}

// HasPeerID iterates peersByScope to find a peer by its ID.
func (sw *Switch) HasPeerID(id ID, outbound bool) bool {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		if outbound && peerSet.HasOutbound(id) {
			return true
		} else if !outbound && peerSet.HasInbound(id) {
			return true
		}
	}

	return false
}

// HasPeerIP iterates peersByScope to find a peer by its IP.
func (sw *Switch) HasPeerIP(peerIP net.IP) bool {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		if peerSet.HasIP(peerIP) {
			return true
		}
	}

	return false
}

// InitPeerForScope adds peers to the reactors. This is necessary when the
// switch is already running and peers must work with new ChainID values.
//
// TODO(midas): Add to subset of reactors as requested or necessary (missing).
func (sw *Switch) InitPeerForScope(peer *PeerImpl, scope string) {
	if !peer.IsRunning() {
		return
	}

	var reactors map[string]Reactor
	switch {
	case scope == ScopeForDiscovery: // "discovery" => _shared_channels
		reactors = sw.Reactors(conn.SharedChannelsNamespace)
	default:
		reactors = sw.Reactors(scope)
	}

	for rname, reactor := range reactors {
		if !sw.IsPeerActiveInReactor(peer, scope, rname) {
			reactor.InitPeer(peer)
		}
	}

	sw.Logger.Info("Init peer for reactors",
		"scope", scope,
		"peer", peer,
	)
}

func (sw *Switch) AddPeerForScope(peer *PeerImpl, scope string) {
	if !peer.IsRunning() {
		return
	}

	var reactors map[string]Reactor
	switch {
	case scope == ScopeForDiscovery: // "discovery" => _shared_channels
		reactors = sw.Reactors(conn.SharedChannelsNamespace)
	default:
		reactors = sw.Reactors(scope)
	}

	for rname, reactor := range reactors {
		if !sw.IsPeerActiveInReactor(peer, scope, rname) {
			reactor.AddPeer(peer)
			sw.MarkPeerActiveInReactor(peer, scope, rname)
		}
	}

	sw.Logger.Info("Added peer to reactors",
		"scope", scope,
		"peer", peer,
	)
}

// StopPeerForError disconnects from a peer due to external error.
// If the peer is persistent, it will attempt to reconnect.
// TODO: make record depending on reason.
func (sw *Switch) StopPeerForError(peer *PeerImpl, reason any) {
	if !peer.IsRunning() {
		return
	}

	sw.Logger.Error("Stopping peer for error", "peer", peer, "err", reason)
	sw.stopAndRemovePeer(peer, reason)

	if peer.IsPersistent() {
		var addr *NetAddress
		if peer.IsOutbound() { // socket address for outbound peers
			addr = peer.SocketAddr()
		} else { // self-reported address for inbound peers
			var err error
			addr, err = peer.NodeInfo().NetAddress()
			if err != nil {
				sw.Logger.Error("Wanted to reconnect to inbound peer, but self-reported address is wrong",
					"peer", peer, "err", err)
				return
			}
		}
		go sw.reconnectToPeer(addr)
	}
}

// StopPeerGracefully disconnects from a peer gracefully.
// TODO: handle graceful disconnects.
func (sw *Switch) StopPeerGracefully(peer *PeerImpl) {
	sw.Logger.Info("Stopping peer gracefully", "peer", peer)
	sw.stopAndRemovePeer(peer, nil)
}

// stopPeer calls the Stop method on a peer, then cleans up
// the transport instance and removes the peer from all reactors.
func (sw *Switch) stopPeer(peer *PeerImpl, reason any) error {
	// Check if peer is already stopped to prevent "already stopped" errors
	if !peer.IsRunning() {
		sw.Logger.Debug("Peer already stopped, skipping stop operation", "peer", peer.ID())
		return nil
	}

	if err := peer.Stop(); err != nil {
		return fmt.Errorf(
			"error stopping peer for ID %s: %w", string(peer.ID()), err,
		)
	}

	//sw.transport.Cleanup(peer)

	sw.reactorsMtx.Lock()
	safeReactors := sw.reactors
	sw.reactorsMtx.Unlock()
	for _, reactors := range safeReactors {
		for _, reactor := range reactors {
			reactor.RemovePeer(peer, reason)
		}
	}

	return nil
}

// removePeer removes the peer from all PeerSet instances.
func (sw *Switch) removePeer(peer *PeerImpl, reason any) error {
	relevantScopes := map[string]bool{}
	relevantScopes[conn.SharedChannelsNamespace] = true
	relevantScopes[ScopeForDiscovery] = true

	sw.peersMtx.RLock()
	peersByScope := sw.peersByScope
	sw.peersMtx.RUnlock()

	for scope, peerSet := range peersByScope {
		if !peerSet.HasPeer(peer) {
			continue
		}

		relevantScopes[scope] = true
		if ok := peerSet.Remove(peer); !ok {
			return fmt.Errorf(
				"failed to remove peer#%s for scope %s: %v", string(peer.ID()), scope, reason,
			)
		}
	}

	remainingChannelsForPeer := peer.MConn().GetChannelsIdx()
	for chScope, _ := range remainingChannelsForPeer {
		relevantScopes[chScope] = true
	}

	// We may need to remove channels we added for this peer.
	// CAUTION: This updates MConnection.channelsIdx.
	var connCleanupFn func(*conn.MConnection)
	connCleanupFn = sw.CloseChannelsForScopes(func(scopes map[string]bool) (out []string) {
		out = make([]string, 0, len(scopes))
		for scope, _ := range scopes {
			out = append(out, scope)
		}
		return // out
	}(relevantScopes))

	connCleanupFn(peer.MConn())

	sw.Logger.Debug("Removed all channels for peer", "peer", peer, "scopes", relevantScopes)

	sw.metrics.Peers.Add(float64(-1))

	return nil
}

// stopAndRemovePeer first stops the peer, then removes it from all PeerSet
// instances. Returns early from stopping the peer in case it errors.
func (sw *Switch) stopAndRemovePeer(peer *PeerImpl, reason any) {
	// Returning early if the peer is already stopped prevents data races because
	// this function may be called from multiple places at once.
	if err := sw.stopPeer(peer, reason); err != nil {
		sw.Logger.Error("error stopping peer", "peer", peer.ID(), "err", err)
		if errors.Is(err, service.ErrAlreadyStopped) {
			// Make sure.
			if err := sw.removePeer(peer, reason); err != nil {
				sw.Logger.Debug("error on peer removal", "peer", peer.ID(), "err", err)
				return
			}
		}
		return
	}

	// Removing a peer should go last to avoid a situation where a peer
	// reconnect to our node and the switch calls InitPeer before
	// RemovePeer is finished.
	// https://github.com/tendermint/tendermint/issues/3338
	if err := sw.removePeer(peer, reason); err != nil {
		// Removal of the peer has failed. The function above sets a flag within the peer to mark this.
		// We keep this message here as information to the developer.
		sw.Logger.Debug("error on peer removal", "peer", peer.ID(), "err", err)
		return
	}
}

// reconnectToPeer tries to reconnect to the addr, first repeatedly
// with a fixed interval (approximately 2 minutes), then with
// exponential backoff (approximately close to 24 hours).
// If no success after all that, it stops trying, and leaves it
// to the PEX/Addrbook to find the peer with the addr again
// NOTE: this will keep trying even if the handshake or auth fails.
// TODO: be more explicit with error types so we only retry on certain failures
//   - ie. if we're getting ErrDuplicatePeer we can stop
//     because the addrbook got us the peer back already
func (sw *Switch) reconnectToPeer(addr *NetAddress) {
	if sw.reconnecting.Has(string(addr.ID)) {
		return
	}
	sw.reconnecting.Set(string(addr.ID), addr)
	defer sw.reconnecting.Delete(string(addr.ID))

	start := time.Now()
	sw.Logger.Info("Reconnecting to peer", "addr", addr)

	for i := 0; i < reconnectAttempts; i++ {
		if !sw.IsRunning() {
			return
		}

		err := sw.DialPeerWithAddress(addr)
		if err == nil {
			return // success
		} else if _, ok := err.(ErrCurrentlyDialingOrExistingAddress); ok {
			return
		}

		sw.Logger.Info("Error reconnecting to peer. Trying again", "tries", i, "err", err, "addr", addr)
		// sleep a set amount
		sw.randomSleep(reconnectInterval)
		continue
	}

	sw.Logger.Error("Failed to reconnect to peer. Beginning exponential backoff",
		"addr", addr, "elapsed", time.Since(start))
	for i := 1; i <= reconnectBackOffAttempts; i++ {
		if !sw.IsRunning() {
			return
		}

		// sleep an exponentially increasing amount
		sleepIntervalSeconds := math.Pow(reconnectBackOffBaseSeconds, float64(i))
		sw.randomSleep(time.Duration(sleepIntervalSeconds) * time.Second)

		err := sw.DialPeerWithAddress(addr)
		if err == nil {
			return // success
		} else if _, ok := err.(ErrCurrentlyDialingOrExistingAddress); ok {
			return
		}
		sw.Logger.Info("Error reconnecting to peer. Trying again", "tries", i, "err", err, "addr", addr)
	}
	sw.Logger.Error("Failed to reconnect to peer. Giving up", "addr", addr, "elapsed", time.Since(start))
}

// SetAddrBook allows to set address book on Switch.
func (sw *Switch) SetAddrBook(addrBook AddrBook) {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	sw.addrBook = addrBook
}

// GetAddrBook returns the [p2p.AddrBook] instance associated with the Switch.
func (sw *Switch) GetAddrBook() AddrBook {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	return sw.addrBook
}

// MarkPeerAsGood marks the given peer as good when it did something useful
// like contributed to consensus.
func (sw *Switch) MarkPeerAsGood(peer Peer) {
	sw.networksMtx.Lock()
	defer sw.networksMtx.Unlock()

	if sw.addrBook != nil {
		sw.addrBook.MarkGood(peer.ID())
	}
}

// ---------------------------------------------------------------------
// Dialing

type privateAddr interface {
	PrivateAddr() bool
}

func isPrivateAddr(err error) bool {
	e, ok := err.(privateAddr)
	return ok && e.PrivateAddr()
}

// DialPeersAsync dials a list of peers asynchronously in random order.
// Used to dial peers from config on startup or from unsafe-RPC (trusted sources).
// It ignores ErrNetAddressLookup. However, if there are other errors, first
// encounter is returned.
// Nop if there are no peers.
func (sw *Switch) DialPeersAsync(peers []string) error {
	netAddrs, errs := NewNetAddressStrings(peers)
	// report all the errors
	for _, err := range errs {
		sw.Logger.Error("Error in peer's address", "err", err)
	}
	// return first non-ErrNetAddressLookup error
	for _, err := range errs {
		if _, ok := err.(ErrNetAddressLookup); ok {
			continue
		}
		return err
	}
	sw.dialPeersAsync(netAddrs)
	return nil
}

func (sw *Switch) dialPeersAsync(netAddrs []*NetAddress) {
	ourAddr := sw.NetAddress()

	// TODO: this code feels like it's in the wrong place.
	// The integration tests depend on the addrBook being saved
	// right away but maybe we can change that. Recall that
	// the addrBook is only written to disk every 2min
	sw.networksMtx.Lock()
	if sw.addrBook != nil {
		// add peers to `addrBook`
		for _, netAddr := range netAddrs {
			// do not add our address or ID
			if !netAddr.Same(ourAddr) {
				if err := sw.addrBook.AddAddress(netAddr, ourAddr); err != nil {
					if isPrivateAddr(err) {
						sw.Logger.Debug("Won't add peer's address to addrbook", "err", err)
					} else {
						sw.Logger.Error("Can't add peer's address to addrbook", "err", err)
					}
				}
			}
		}
		// Persist some peers to disk right away.
		// NOTE: integration tests depend on this
		sw.addrBook.Save()
	}
	sw.networksMtx.Unlock()

	// permute the list, dial them in random order.
	perm := sw.rng.Perm(len(netAddrs))
	for i := 0; i < len(perm); i++ {
		go func(i int) {
			j := perm[i]
			addr := netAddrs[j]

			if addr.Same(ourAddr) {
				sw.Logger.Debug("Ignore attempt to connect to ourselves", "addr", addr, "ourAddr", ourAddr)
				return
			}

			sw.randomSleep(0)

			err := sw.DialPeerWithAddress(addr)
			if err != nil {
				switch err.(type) {
				case ErrSwitchConnectToSelf, ErrSwitchDuplicatePeerID, ErrCurrentlyDialingOrExistingAddress:
					sw.Logger.Debug("Error dialing peer", "err", err)
				default:
					sw.Logger.Error("Error dialing peer", "err", err)
				}
			}
		}(i)
	}
}

// DialPeerWithAddress dials the given peer and runs sw.addPeer if it connects
// and authenticates successfully.
// If we're currently dialing this address or it belongs to an existing peer,
// ErrCurrentlyDialingOrExistingAddress is returned.
func (sw *Switch) DialPeerWithAddress(addr *NetAddress) error {
	if sw.IsDialingOrExistingAddress(addr) {

		return ErrCurrentlyDialingOrExistingAddress{addr.String()}
	}

	sw.dialing.Set(string(addr.ID), addr)
	defer sw.dialing.Delete(string(addr.ID))

	return sw.addOutboundPeerWithConfig(addr, sw.config, "")
}
func (sw *Switch) DialPeerWithAddressAndChain(addr *NetAddress, chainID string) error {
	if sw.IsDialingOrExistingAddress(addr) {
		var p *PeerImpl
		if sw.HasPeerID(addr.ID, true) {
			all := sw.AllPeers()
			for _, ap := range all {
				if ap.ID() == addr.ID && ap.outbound {
					p = ap
					break
				}
			}
		} else {
			netAddr, err := net.ResolveTCPAddr("", addr.DialString())
			if err != nil {
				return err
			}
			p = sw.FindMatchingPeer(netAddr)
		}
		peerSet := sw.Peers(chainID)
		if p != nil && !peerSet.HasPeer(p) {
			if err := peerSet.Add(p); err != nil {
				if _, ok := err.(ErrPeerRemoval); ok {
					sw.Logger.Error("Error starting peer ",
						"err", "Peer has already errored and removal was attempted.",
						"peer", p)
				}
				return err // err
			}
		}
		return ErrCurrentlyDialingOrExistingAddress{addr.String()}
	}

	sw.dialing.Set(string(addr.ID), addr)
	defer sw.dialing.Delete(string(addr.ID))

	return sw.addOutboundPeerWithConfig(addr, sw.config, chainID)
}

// sleep for interval plus some random amount of ms on [0, dialRandomizerIntervalMilliseconds].
func (sw *Switch) randomSleep(interval time.Duration) {
	r := time.Duration(sw.rng.Int63n(dialRandomizerIntervalMilliseconds)) * time.Millisecond
	time.Sleep(r + interval)
}

// IsDialingOrExistingAddress returns true if switch has a peer with the given
// address or dialing it at the moment.
func (sw *Switch) IsDialingOrExistingAddress(addr *NetAddress) bool {
	return sw.dialing.Has(string(addr.ID)) ||
		sw.HasPeerID(addr.ID, true) ||
		(!sw.config.AllowDuplicateIP && sw.HasPeerIP(addr.IP))
}

// AddPersistentPeers allows you to set persistent peers. It ignores
// ErrNetAddressLookup. However, if there are other errors, first encounter is
// returned.
func (sw *Switch) AddPersistentPeers(addrs []string) error {
	sw.Logger.Info("Adding persistent peers", "addrs", addrs)
	netAddrs, errs := NewNetAddressStrings(addrs)
	// report all the errors
	for _, err := range errs {
		sw.Logger.Error("Error in peer's address", "err", err)
	}
	// return first non-ErrNetAddressLookup error
	for _, err := range errs {
		if _, ok := err.(ErrNetAddressLookup); ok {
			continue
		}
		return err
	}
	sw.persistentPeersAddrs = netAddrs
	return nil
}

func (sw *Switch) AddUnconditionalPeerIDs(ids []string) error {
	sw.Logger.Info("Adding unconditional peer ids", "ids", ids)
	for i, id := range ids {
		err := validateID(ID(id))
		if err != nil {
			return fmt.Errorf("wrong ID #%d: %w", i, err)
		}
		sw.unconditionalPeerIDs[ID(id)] = struct{}{}
	}
	return nil
}

func (sw *Switch) AddPrivatePeerIDs(ids []string) error {
	validIDs := make([]string, 0, len(ids))
	for i, id := range ids {
		err := validateID(ID(id))
		if err != nil {
			return fmt.Errorf("wrong ID #%d: %w", i, err)
		}
		validIDs = append(validIDs, id)
	}

	sw.networksMtx.Lock()
	sw.addrBook.AddPrivateIDs(validIDs)
	sw.networksMtx.Unlock()

	return nil
}

func (sw *Switch) IsPeerPersistent(na *NetAddress) bool {
	for _, pa := range sw.persistentPeersAddrs {
		if pa.Equals(na) {
			return true
		}
	}
	return false
}

func (sw *Switch) acceptRoutine() {
	outbound, inbound, _ := sw.TotalNumPeers()
	numPeers := outbound + inbound
	for {
		safePeerConfig := sw.GetPeerConfig()
		p, err := sw.transport.Accept(safePeerConfig)
		if err != nil {
			if !IsDialError(err) {
				break
			}

			// If Close() was called, exit silently
			if sw.transport.IsClosing() {
				break
			}

			switch err := err.(type) {
			case ErrRejected:
				if err.IsSelf() {
					// Remove the given address from the address book and add to our addresses
					// to avoid dialing in the future.
					addr := err.Addr()

					sw.networksMtx.Lock()
					sw.addrBook.RemoveAddress(&addr)
					sw.addrBook.AddOurAddress(&addr)
					sw.networksMtx.Unlock()
				}

				sw.Logger.Info(
					"Inbound Peer rejected",
					"err", err,
					"addr", err.addr.String(),
					"peerID", err.id,
					"numPeers", numPeers,
				)

				continue
			case ErrFilterTimeout:
				sw.Logger.Error(
					"Peer filter timed out",
					"err", err,
				)

				continue
			case ErrTransportClosed:
				sw.Logger.Error(
					"Stopped accept routine, as transport is closed",
					"numPeers", numPeers,
				)
			default:
				sw.Logger.Error(
					"Accept on transport errored",
					"err", err,
					"numPeers", numPeers,
				)
				// We could instead have a retry loop around the acceptRoutine,
				// but that would need to stop and let the node shutdown eventually.
				// So might as well panic and let process managers restart the node.
				// There's no point in letting the node run without the acceptRoutine,
				// since it won't be able to accept new connections.
				panic(fmt.Sprintf("accept routine exited: %v", err))
			}

			break
		}

		// BREAKING:
		// NOTE(midas): We disable MaxNumInboundPeers here because a limit on the
		// number of peers is undesired for a multiplex of nodes with many ChainIDs.

		// if !sw.IsPeerUnconditional(p.NodeInfo().ID()) {
		// 	// Ignore connection if we already have enough peers.
		// 	_, in, _ := sw.NumUniquePeers()
		// 	if in >= sw.config.MaxNumInboundPeers {
		// 		sw.Logger.Info(
		// 			"Ignoring inbound connection: already have enough inbound peers",
		// 			"address", p.SocketAddr(),
		// 			"have", in,
		// 			"max", sw.config.MaxNumInboundPeers,
		// 		)

		// 		sw.transport.Cleanup(p)

		// 		continue
		// 	}
		// }

		if err := sw.addPeer(p); err != nil {
			sw.transport.Cleanup(p)
			if p.IsRunning() {
				_ = p.Stop()
			}
			sw.Logger.Info(
				"Ignoring inbound connection: error while adding peer",
				"err", err,
				"id", p.ID(),
			)
		}
	}
}

// IsDialError returns true given a non-acceptable dial error. Acceptable
// dial errors include "currently-dialing", "existing-address" and
// errors marked as duplicates.
func IsDialError(err error) bool {
	switch err.(type) {
	case ErrCurrentlyDialingOrExistingAddress:
		return false
	case ErrRejected:
		return !err.(ErrRejected).IsDuplicate()
	}

	return true
}

// dial the peer; make secret connection; authenticate against the dialed ID;
// add the peer.
// if dialing fails, start the reconnect loop. If handshake fails, it's over.
// If peer is started successfully, reconnectLoop will start when
// StopPeerForError is called.
func (sw *Switch) addOutboundPeerWithConfig(
	addr *NetAddress,
	cfg *config.P2PConfig,
	chainID string,
) error {
	sw.Logger.Debug("Dialing peer", "address", addr)

	// XXX(xla): Remove the leakage of test concerns in implementation.
	if cfg.TestDialFail {
		go sw.reconnectToPeer(addr)
		return errors.New("dial err (peerConfig.DialFail == true)")
	}

	safePeerConfig := sw.GetPeerConfig()
	p, err := sw.transport.Dial(*addr, safePeerConfig)
	if err != nil {
		if !IsDialError(err) {
			if chainID != "" {
				peerSet := sw.Peers(chainID)
				//TODO: peer lookup by addr, or proper linking between net conns / peer
				if duplConn, ok := err.(ErrRejected); ok {
					p = sw.FindMatchingPeer(duplConn.conn.RemoteAddr())
				}
				if p != nil && !peerSet.HasPeer(p) {
					if err = peerSet.Add(p); err != nil {
						if _, ok := err.(ErrPeerRemoval); ok {
							sw.Logger.Error("Error starting peer ",
								"err", "Peer has already errored and removal was attempted.",
								"peer", p)
						}
						return err // err
					}
				}
			}
		}
		if e, ok := err.(ErrRejected); ok {
			if e.IsSelf() {
				sw.networksMtx.Lock()
				// Remove the given address from the address book and add to our addresses
				// to avoid dialing in the future.
				sw.addrBook.RemoveAddress(addr)
				sw.addrBook.AddOurAddress(addr)
				sw.networksMtx.Unlock()

				return err
			}
		}

		// retry persistent peers after
		// any dial error besides IsSelf()
		if sw.IsPeerPersistent(addr) {
			go sw.reconnectToPeer(addr)
		}

		return err
	}

	//// Applies custom options to Peer object
	//for _, option := range options {
	//	option(p)
	//}

	if err := sw.addPeer(p); err != nil {
		sw.transport.Cleanup(p)
		if p.IsRunning() {
			_ = p.Stop()
		}
		return err
	}

	return nil
}

// TODO: peer lookup by addr, or proper linking between net conns / peer
func (sw *Switch) FindMatchingPeer(addr net.Addr) (p *PeerImpl) {
	allPeers := sw.AllPeers()
	for _, ap := range allPeers {
		if ap.conn.RemoteAddr() == addr {
			p = ap
			break
		}
	}
	return p
}

func (sw *Switch) filterPeer(p *PeerImpl) error {
	// NOTE(midas):
	// We don't need to throw a rejection error on duplicate peers at this
	// stage because we skip known peers in [PeerSet#Add].
	//
	// if sw.peers.Has(p.ID()) {
	// 	return ErrRejected{id: p.ID(), isDuplicate: true}
	// }

	errc := make(chan error, len(sw.peersByScope)*len(sw.peerFilters))

	sw.peersMtx.RLock()
	for _, peerSet := range sw.peersByScope {
		for _, f := range sw.peerFilters {
			go func(f PeerFilterFunc, p Peer, errc chan<- error) {
				errc <- f(peerSet, p)
			}(f, p, errc)
		}
	}
	sw.peersMtx.RUnlock()

	for i := 0; i < cap(errc); i++ {
		select {
		case err := <-errc:
			if err != nil {
				return ErrRejected{id: p.ID(), err: err, isFiltered: true}
			}
		case <-time.After(sw.filterTimeout):
			return ErrFilterTimeout{}
		}
	}

	return nil
}

func (sw *Switch) GetPeerActiveChainID(p *PeerImpl) ([]string, error) {
	var (
		commonChainIds []string
		activeChainIds []string
		err            error
	)

	// Find ChainID values that we share with p.
	if commonChainIds, err = sw.NodeInfo().GetCommonChains(p.NodeInfo()); err != nil {
		return []string{}, fmt.Errorf(
			"failed to fetch common chains with peer %s: %w", p.ID(), err)
	}

	// Find ChainID values that are currently active or being created,
	// i.e. when creating new networks, they are not yet in NodeInfo.
	runtimeChainIds := sw.GetActiveRuntimes()
	activeChainIds = slices.DeleteFunc(runtimeChainIds, func(aid string) bool {
		return slices.Contains(commonChainIds, aid)
	})

	// Relevant ChainIDs are all networks we and p may know about.
	numRelevant := len(commonChainIds) + len(runtimeChainIds)
	relevantChainIds := make([]string, 0, numRelevant)
	relevantChainIds = append(relevantChainIds, commonChainIds...)
	relevantChainIds = append(relevantChainIds, activeChainIds...)

	return relevantChainIds, nil
}

// addPeer starts up the Peer and adds it to the Switch. Error is returned if
// the peer is filtered out or failed to start or can't be added.
func (sw *Switch) addPeer(p *PeerImpl) (err error) {
	if err = sw.filterPeer(p); err != nil {
		return
	}

	pubAddr, _ := p.NodeInfo().NetAddress()
	peerLogger := sw.Logger.With("conn", p.SocketAddr()).With("addr", pubAddr.String()).With("peer", p)
	p.SetLogger(peerLogger)

	// Handle the shut down case where the switch has stopped but we're
	// concurrently trying to add a peer.
	if !sw.IsRunning() {
		sw.Logger.Error("Won't start a peer - switch is not running", "peer", p)
		return // err
	}

	// Find ChainID values that we share with p.
	relevantChainIds, err := sw.GetPeerActiveChainID(p)
	if err != nil {
		p.GetLogger().Error("Won't start a peer - failed to determine compatibility",
			"err", err,
		)
		return // err
	}

	// For replication channel, we add the peer to _shared_channels reactors.
	// For CometBFT channels, we add the peer to relevantChainIds reactors.
	// reactorsGroup := []string{conn.SharedChannelsNamespace}
	relevantScopes := []string{ScopeForDiscovery} // i.e. sw.Peers(p2p.ScopeForDiscovery)
	if sw.Typ != "discovery" {
		// reactorsGroup = relevantChainIds[:]
		relevantScopes = relevantChainIds[:]                       // i.e. sw.Peers(ChainID)
		relevantScopes = append(relevantScopes, ScopeForDiscovery) // AckTransactionBroadcast
	}

	// We may need to add new channels to accept messages from this new peer.
	// CAUTION: This updates MConnection.channelsIdx.
	var connUpdaterFn func(*conn.MConnection)
	connUpdaterFn = sw.OpenChannelsForScopes(relevantScopes)
	connUpdaterFn(p.MConn())

	// Add the peer to our internal PeerSet storage.
	for _, peerSetScope := range relevantScopes {
		peerSet := sw.Peers(peerSetScope)
		if !peerSet.HasPeer(p) {
			if err = peerSet.Add(p); err != nil {
				if _, ok := err.(ErrPeerRemoval); ok {
					peerLogger.Error("Error starting peer ",
						"err", "Peer has already errored and removal was attempted.",
						"peer", p)
				}
				return // err
			}

			sw.metrics.Peers.Add(float64(1))
		}
	}

	// Init all the reactor protocols with this peer.
	for _, relevantScope := range relevantScopes {
		sw.InitPeerForScope(p, relevantScope)
	}

	// Start the peer's send/recv routines.
	// Must start it before adding it to the peer set
	// to prevent Start and Stop from being called concurrently.
	if err := p.Start(); err != nil {
		// Should never happen
		sw.Logger.Error("Error starting peer", "err", err, "peer", p)
		return err
	}

	// Start all the reactor protocols on the peer.
	for _, relevantScope := range relevantScopes {
		sw.AddPeerForScope(p, relevantScope)
	}

	peerLogger.Debug("Added peer",
		"peer", p,
	)

	return nil
}

func (sw *Switch) peerReactorLookupKey(p *PeerImpl, reactor string) string {
	if p.IsOutbound() {
		return reactor + string(p.ID()) + "_out"
	}

	return reactor + string(p.ID()) + "_in"
}

// IsPeerActiveInReactor returns true if the peer has been initialized
// after the switch started running.
func (sw *Switch) IsPeerActiveInReactor(p *PeerImpl, scope string, reactor string) bool {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeerTimes[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		return false
	}

	lookupKey := sw.peerReactorLookupKey(p, reactor)

	sw.runtimesMtx.RLock()
	peerInitTimeTz,
		hasPeerTime := sw.reactorPeerTimes[scope][lookupKey]
	swStartTimeTz := sw.startTz
	sw.runtimesMtx.RUnlock()

	if !sw.IsRunning() || !hasPeerTime {
		return false
	}

	// The switch must have started before the peer, otherwise consider inactive.
	return swStartTimeTz.Before(peerInitTimeTz)
}

func (sw *Switch) MarkPeerActiveInReactor(p *PeerImpl, scope string, reactor string) {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeerTimes[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		sw.runtimesMtx.Lock()
		sw.reactorPeerTimes[scope] = make(map[string]time.Time)
		sw.runtimesMtx.Unlock()
	}

	lookupKey := sw.peerReactorLookupKey(p, reactor)

	sw.runtimesMtx.Lock()
	sw.reactorPeerTimes[scope][lookupKey] = time.Now()
	sw.runtimesMtx.Unlock()
}

func (sw *Switch) CleanupChannels() {
	relevantScopes := map[string]bool{}
	relevantScopes[conn.SharedChannelsNamespace] = true
	relevantScopes[ScopeForDiscovery] = true

	activeRuntimes := sw.GetActiveRuntimes()
	for _, activeChainID := range activeRuntimes {
		relevantScopes[activeChainID] = true
	}

	//cleanupWg := new(sync.WaitGroup)

	remainingPeerSets := sw.PeersByScopes()
	for _, peerSet := range remainingPeerSets {
		//cleanupWg.Add(peerSet.Size())
		peers := peerSet.Copy()
		for _, peer := range peers {
			//go func(p *PeerImpl, wg *sync.WaitGroup) {
			go func(p *PeerImpl) {
				mconn := p.MConn()
				relevantScopesForPeer := map[string]bool{}
				remainingChannelsForPeer := mconn.GetChannelsIdx()
				for chScope, _ := range relevantScopes {
					relevantScopesForPeer[chScope] = true
				}
				for chScope, _ := range remainingChannelsForPeer {
					relevantScopesForPeer[chScope] = true
				}

				// We may need to remove channels we added for this peer.
				// CAUTION: This updates MConnection.channelsIdx.
				var connCleanupFn func(*conn.MConnection)
				connCleanupFn = sw.CloseChannelsForScopes(func(scopes map[string]bool) (out []string) {
					out = make([]string, 0, len(scopes))
					for scope, _ := range scopes {
						out = append(out, scope)
					}
					return // out
				}(relevantScopesForPeer))

				connCleanupFn(mconn)

				sw.Logger.Debug("Removed all channels for peer from cleanup", "peer", p, "scopes", relevantScopes)
			}(peer)
		}
		// cleanupWg.Wait()
	}
}

func (sw *Switch) CloseChannelsForScopes(scopes []string) func(mconn *conn.MConnection) {
	return func(mconn *conn.MConnection) {
		channelsIdx := mconn.GetChannelsIdx()
		sw.runtimesMtx.Lock()
		defer sw.runtimesMtx.Unlock()

		for _, scope := range scopes {
			reactorsScope := scope // ChainID or "discovery"
			if scope == ScopeForDiscovery {
				reactorsScope = conn.SharedChannelsNamespace // "_shared_channels"
			}

			reactorsByScope := sw.Reactors(reactorsScope)
			for _, r := range reactorsByScope {
				channels := r.GetChannels()
				for _, chDesc := range channels {
					channelRemoved := mconn.RemoveChannel(scope, chDesc)
					if channelRemoved && atomic.LoadUint32(&sw.totalOpenChannels) > 0 {
						atomic.AddUint32(&sw.totalOpenChannels, ^uint32(0)) // -1
					}
				}
			}

			// Close remaining channels from outbound peers.
			if scope != ScopeForDiscovery {
				for _, channels := range channelsIdx {
					replChannel := channels[replicationChannel]
					ackChannel := channels[ackBroadcastChannel]
					runChannel := channels[runtimeChannel]

					shutdownChannels := []*conn.Channel{
						replChannel,
						ackChannel,
						runChannel,
					}
					for _, channel := range shutdownChannels {
						if channel == nil {
							continue
						}

						channelRemoved := mconn.RemoveChannel(scope, channel.Desc())
						if channelRemoved && atomic.LoadUint32(&sw.totalOpenChannels) > 0 {
							atomic.AddUint32(&sw.totalOpenChannels, ^uint32(0)) // -1
						}
					}
				}
			}
		}

		// TODO(midas): remove debug logs
		sw.Logger.Debug("Removed connection channels",
			"scopes", scopes,
			"conn", mconn.SocketAddr().String(),
			"num_chs", atomic.LoadUint32(&sw.totalOpenChannels),
		)
	}
}

func (sw *Switch) OpenChannelsForScopes(scopes []string) func(mconn *conn.MConnection) {
	return func(mconn *conn.MConnection) {
		sw.runtimesMtx.Lock()
		defer sw.runtimesMtx.Unlock()

		for _, scope := range scopes {
			reactorsGroup := scope // ChainID or "discovery"
			if scope == ScopeForDiscovery {
				reactorsGroup = conn.SharedChannelsNamespace // "_shared_channels"
			}

			reactorsByScope := sw.Reactors(reactorsGroup)
			for name, r := range reactorsByScope {
				channels := r.GetChannels()
				for _, chDesc := range channels {
					if sw.Typ == "discovery" && chDesc.ID != replicationChannel {
						// Discovery switch needs only ReplicationChannel
						continue
					} else if sw.Typ == "cometBFT" && name == "MULTIPLEX" {
						// CometBFT should add only relevant multiplex channels
						if !slices.Contains(cometMxChannels, chDesc.ID) {
							continue
						}
					}

					_, channelAdded := mconn.AddChannel(scope, chDesc)
					if channelAdded {
						atomic.AddUint32(&sw.totalOpenChannels, uint32(1))
					}
				}
			}
		}

		// TODO(midas): remove debug logs
		sw.Logger.Debug("Added connection channels",
			"scopes", scopes,
			"conn", mconn.SocketAddr().String(),
			"num_chs", atomic.LoadUint32(&sw.totalOpenChannels),
		)
	}
}
