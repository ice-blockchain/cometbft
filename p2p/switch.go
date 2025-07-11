package p2p

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strings"
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
	mempoolChannel      = byte(0x30)
)

var cometMxChannels = []byte{
	ackBroadcastChannel,
	runtimeChannel,
	mempoolChannel,
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
	reactorPeersAdded map[string]map[string]time.Time
	reactorPeersInit  map[string]map[string]time.Time
	peersForReactors  map[string]map[string]*PeerImpl
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
	ctx context.Context,
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
		reactorPeersAdded:    make(map[string]map[string]time.Time),
		reactorPeersInit:     make(map[string]map[string]time.Time),
		peersForReactors:     make(map[string]map[string]*PeerImpl),
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

	sw.BaseService = *service.NewBaseService(ctx, nil, "P2P Switch", sw)

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

func (sw *Switch) NumActiveRuntimes() int {
	sw.runtimesMtx.RLock()
	defer sw.runtimesMtx.RUnlock()

	return len(sw.runtimesChainIds)
}

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
	sw.Logger.Debug("Adding reactor", "chainId", chainID, "name", name, "reactor", reactor)

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

		if _, exists := sw.reactorsByCh[chainID][chID]; !exists {
			sw.chDescs[chainID] = append(sw.chDescs[chainID], chDesc)
			sw.msgTypeByChID[chainID][chID] = chDesc.MessageType
		}

		sw.reactorsByCh[chainID][chID] = reactor
	}
	sw.reactors[chainID][name] = reactor
	reactor.SetSwitch(sw)

	sw.Logger.Debug("Added reactor", "chainId", chainID, "name", name, "reactor", reactor)
	return reactor
}

// RemoveReactor removes the given Reactor from the Switch.
func (sw *Switch) RemoveReactor(chainID string, name string, reactor Reactor) {
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
	delete(sw.reactors, chainID)
}

func (sw *Switch) RemoveReactors(chainID string) {
	reactors := sw.Reactors(chainID)
	for name, r := range reactors {
		sw.RemoveReactor(chainID, name, r)
	}
}

// Reactors returns a map of reactors registered on the switch.
func (sw *Switch) Reactors(chainID string) map[string]Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	// Return empty if missing.
	if _, ok := sw.reactors[chainID]; !ok {
		return map[string]Reactor{}
	}

	return sw.reactors[chainID]
}

// Reactor returns the reactor with the given name.
func (sw *Switch) Reactor(chainID string, name string) Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	// Return nil if missing.
	if _, ok := sw.reactors[chainID]; !ok {
		return nil
	}
	if _, ok := sw.reactors[chainID][name]; !ok {
		return nil
	}

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

// GetMultiplexReactor returns the multiplex reactor if it exists.
// Locks the reactors mutex, for usage from outside.
func (sw *Switch) GetMultiplexReactor() Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	return sw.getMultiplexReactor()
}

// getMultiplexReactor returns the multiplex reactor if it exists.
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

// OnStart implements BaseService.
func (sw *Switch) OnStart(ctx context.Context) error {
	sw.runtimesMtx.Lock()
	sw.startTz = time.Now()
	sw.runtimesMtx.Unlock()

	sw.peersMtx.Lock()
	sw.peersByScope = map[string]*PeerSet{ScopeForDiscovery: NewPeerSet()}
	sw.peersMtx.Unlock()

	// BREAKING
	// NOTE(midas): We removed the startup of reactors from this method because
	// in a multiplex of nodes, the active node runtimes (ChainID) have a short
	// lifecycle that is controlled fully by the multiplex reactor.

	// Start accepting Peers.
	go sw.acceptRoutine(ctx)

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
	} else {
		// TODO(midas): remove debug logs
		sw.Logger.Debug("Skipping broadcast - peerset is empty",
			"chain_id", chainID,
			"msg", e.Message,
		)
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

// InitPeerForScope calls InitPeer of reactors for peers. This is necessary
// when the switch is already running and peers must work with new ChainID values.
//
// IMPORTANT: The InitPeer() method of reactors is called only once per peer ID,
// without making a distinction about inbound/outbound. This is because this
// distinction does not matter for CometBFT reactors.
func (sw *Switch) InitPeerForScope(peer *PeerImpl, scope string) {
	var reactors map[string]Reactor
	switch {
	case scope == ScopeForDiscovery: // "discovery" => _shared_channels
		reactors = sw.Reactors(conn.SharedChannelsNamespace)
	default:
		reactors = sw.Reactors(scope)
	}

	for rname, reactor := range reactors {
		if !sw.IsPeerInitialized(peer, scope, rname) {
			peerForReactor := reactor.InitPeer(peer)
			sw.MarkPeerInitialized(peerForReactor, scope, rname)

			// TODO(midas): remove debug logs
			sw.Logger.Info("Init peer for reactor",
				"scope", scope,
				"reactor", rname,
				"peer", peer,
				"reactorRunning", reactor.IsRunning(),
				"peerRunning", peer.IsRunning(),
			)
		}
	}
}

// AddPeerForScope adds peers to the reactors. This is necessary when the
// switch is already running and peers must work with new ChainID values.
//
// The reactor's AddPeer method may be called more than once per peer ID,
// as CometBFT reactors do not distinguish between inbound/outbound.
func (sw *Switch) AddPeerForScope(peer *PeerImpl, scope string) {
	if !peer.IsRunning() {
		return
	}

	// Add the peer to our internal PeerSet storage.
	peerSet := sw.Peers(scope)
	if !peerSet.HasPeer(peer) {
		peerSet.Add(peer)
	}

	var reactors map[string]Reactor
	switch {
	case scope == ScopeForDiscovery: // "discovery" => _shared_channels
		reactors = sw.Reactors(conn.SharedChannelsNamespace)
	default:
		reactors = sw.Reactors(scope)
	}

	for rname, reactor := range reactors {
		var peerForReactor *PeerImpl
		if !sw.IsPeerInitialized(peer, scope, rname) {
			peerForReactor = reactor.InitPeer(peer)
			sw.MarkPeerInitialized(peerForReactor, scope, rname)
		} else {
			// reactor, peer.ID(), in or out
			lookupKey := sw.peerScopeReactorKey(peer, scope, rname)
			sw.runtimesMtx.RLock()
			peerForReactor = sw.peersForReactors[scope][lookupKey] // as returned by reactor.InitPeer()
			sw.runtimesMtx.RUnlock()
		}

		// TODO(midas): remove debug logs
		sw.Logger.Info("Adding peer to reactor",
			"scope", scope,
			"reactor", rname,
			"peer", peer,
			"reactorRunning", reactor.IsRunning(),
			"peerRunning", peer.IsRunning(),
		)

		if scope != ScopeForDiscovery {
			for _, chDesc := range reactor.GetChannels() {
				_, channelAdded := peer.mconn.AddChannel(sw.BaseService.Context(), scope, chDesc)
				if channelAdded {
					atomic.AddUint32(&sw.totalOpenChannels, uint32(1))
				}
			}

			sw.Logger.Info("Added peer connection channels",
				"scope", scope,
				"reactor", rname,
				"peer", peer,
				"conn", peer.mconn.SocketAddr().String(),
				"reactorRunning", reactor.IsRunning(),
				"peerRunning", peer.IsRunning(),
				"num_chs", atomic.LoadUint32(&sw.totalOpenChannels),
			)
		}

		reactor.AddPeer(peerForReactor)
	}
}

// StopPeerForError disconnects from a peer due to external error.
// If the peer is persistent, it will attempt to reconnect.
// TODO: make record depending on reason.
func (sw *Switch) StopPeerForError(peer *PeerImpl, reason any) {
	sw.Logger.Error("Stopping peer for error", "peer", peer, "err", reason)
	sw.stopAndRemovePeer(peer, reason)

	// BREAKING(midas):
	//
	// We disable re-connection to persistent peers to permit having
	// more control over the lifecycle of running peers.

	// if peer.IsPersistent() {
	// 	var addr *NetAddress
	// 	if peer.IsOutbound() { // socket address for outbound peers
	// 		addr = peer.SocketAddr()
	// 	} else { // self-reported address for inbound peers
	// 		var err error
	// 		addr, err = peer.NodeInfo().NetAddress()
	// 		if err != nil {
	// 			sw.Logger.Error("Wanted to reconnect to inbound peer, but self-reported address is wrong",
	// 				"peer", peer, "err", err)
	// 			return
	// 		}
	// 	}
	// 	go sw.reconnectToPeer(addr)
	// }
}

// StopPeerGracefully disconnects from a peer gracefully.
// TODO: handle graceful disconnects.
func (sw *Switch) StopPeerGracefully(peer *PeerImpl) {
	sw.Logger.Info("Stopping peer gracefully", "peer", peer)
	sw.stopAndRemovePeer(peer, nil)
}

func (sw *Switch) RemovePeerScope(peer *PeerImpl, chainID string) {
	sw.Logger.Info("Removing peer for scope", "peer", peer, "chainID", chainID)

	sw.reactorsMtx.Lock()
	chainReactors := sw.reactors[chainID]
	sw.reactorsMtx.Unlock()
	for _, reactor := range chainReactors {
		reactor.RemovePeer(peer, nil) // reason=nil
	}

	sw.runtimesMtx.RLock()
	peersInitTimes := sw.reactorPeersInit[chainID]
	peersForReactors := sw.peersForReactors[chainID]
	sw.runtimesMtx.RUnlock()

	sw.runtimesMtx.Lock()
	for lookupKey := range peersInitTimes {
		peerKey := sw.peerKey(peer)
		if strings.Contains(lookupKey, peerKey) {
			delete(sw.reactorPeersInit[chainID], lookupKey)
		}
	}

	for lookupKey := range peersForReactors {
		peerKey := sw.peerKey(peer)
		if strings.Contains(lookupKey, peerKey) {
			delete(sw.peersForReactors[chainID], lookupKey)
		}
	}
	sw.runtimesMtx.Unlock()

	sw.peersMtx.RLock()
	peersByScope := sw.peersByScope[chainID]
	sw.peersMtx.RUnlock()
	if !peersByScope.HasPeer(peer) {
		return
	}

	_ = peersByScope.RemovePeer(peer)

	sw.Logger.Info("Removed peer for scope",
		"peer", peer,
		"chainId", chainID,
		"reactors", chainReactors)

	sw.CloseChannelsForScopes([]string{chainID})(peer.MConn())
}

func (sw *Switch) StopAllPeersAndCleanup() error {
	allPeers := sw.PeersByScopes()
	sw.Logger.Info("Stopping all peers gracefully",
		"num_scopes", len(allPeers))

	// Then stop all peer objects / connections.
	flatPeers := []*PeerImpl{}
	for _, peerSet := range allPeers {
		peers := peerSet.Copy()
		flatPeers = append(flatPeers, peers...)
	}

	cleanupWg := new(sync.WaitGroup)
	cleanupWg.Add(len(flatPeers))

	for _, p := range flatPeers {
		go func(peer *PeerImpl) {
			defer func() {
				defer cleanupWg.Done()
				_ = recover() // ignore peer error during shutdown
			}()

			sw.StopPeerGracefully(peer)
		}(p)
	}
	cleanupWg.Wait()

	// Cleanup any remaining MConnection.channelsIdx
	sw.CleanupChannels()

	// Ping timer must be killed for outbound peers.
	// NOTE(midas): ForEach() and Close() both lock the conn.
	conns := []net.Conn{}
	sw.Transport().Conns().ForEach(func(c net.Conn) {
		conns = append(conns, c)
	})
	for _, c := range conns {
		c.Close()
	}

	// Must stop listening for P2P messages on broadcast port
	if ts := sw.Transport(); ts != nil {
		ts.Close()
	}

	return nil
}

// stopPeer calls the Stop method on a peer, then cleans up
// the transport instance and removes the peer from all reactors.
func (sw *Switch) stopPeer(peer *PeerImpl, reason any) error {
	// Check if peer is already stopped to prevent "already stopped" errors
	if !peer.IsRunning() && peer.IsStopped() {
		sw.Logger.Debug("Peer already stopped, skipping stop operation", "peer", peer.ID())
		return nil
	}

	if err := peer.Stop(); err != nil {
		sw.Logger.Error("error stopping peer", "peer", peer, "err", err)
	}

	sw.transport.Cleanup(peer)

	sw.reactorsMtx.Lock()
	safeReactors := sw.reactors
	sw.reactorsMtx.Unlock()
	for _, reactors := range safeReactors {
		for _, reactor := range reactors {
			reactor.RemovePeer(peer, reason)
		}
	}

	// TODO(midas): remove debug logs
	sw.Logger.Debug("Peer removed from reactors", "peer", peer, "reason", reason)
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
		// Removes the peer from the registry for peers of reactors.
		sw.runtimesMtx.RLock()
		peersInitTimes := sw.reactorPeersInit[scope]
		peersForReactors := sw.peersForReactors[scope]
		sw.runtimesMtx.RUnlock()

		sw.runtimesMtx.Lock()
		for lookupKey := range peersInitTimes {
			peerKey := sw.peerKey(peer)
			if strings.Contains(lookupKey, peerKey) {
				delete(sw.reactorPeersInit[scope], lookupKey)
			}
		}

		for lookupKey := range peersForReactors {
			peerKey := sw.peerKey(peer)
			if strings.Contains(lookupKey, peerKey) {
				delete(sw.peersForReactors[scope], lookupKey)
			}
		}
		sw.runtimesMtx.Unlock()

		if !peerSet.HasPeer(peer) {
			continue
		}

		relevantScopes[scope] = true
		_ = peerSet.Remove(peer)
	}

	remainingChannelsForPeer := peer.MConn().GetChannelsIdx()
	for chScope, _ := range remainingChannelsForPeer {
		relevantScopes[chScope] = true
	}

	// We may need to remove channels we added for this peer.
	// CAUTION: This updates MConnection.channelsIdx.
	var connCleanupFn func(*conn.MConnection)
	scopesToCleanup := valuesFromMapKeys(relevantScopes)
	connCleanupFn = sw.CloseChannelsForScopes(scopesToCleanup)
	connCleanupFn(peer.MConn())

	sw.metrics.Peers.Add(float64(-1))

	// TODO(midas): remove debug logs
	sw.Logger.Debug("Removed peer", "peer", peer, "scopes", relevantScopes, "reason", reason)
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

	return sw.addOutboundPeerWithConfig(sw.BaseService.Context(), addr, sw.config, "")
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

func (sw *Switch) acceptRoutine(ctx context.Context) {
	for {
		outbound, inbound, dialing := sw.TotalNumPeers()
		numPeers := outbound + inbound

		// Early shutdown detection
		switch {
		case sw.transport.IsClosing():
			return
		default: // proceed to Accept
		}

		safePeerConfig := sw.GetPeerConfig()
		p, err := sw.transport.Accept(ctx, safePeerConfig)
		if p != nil {
			// TODO(midas): remove debug logs
			sw.Logger.Debug("Accepting peer connection request",
				"num_peers", numPeers,
				"num_dials", dialing,
				"peer", p,
				"err", err,
			)
		}

		// If Close() was called, exit silently
		if err != nil && sw.transport.IsClosing() {
			break
		}

		// If we are accepting a conn from a known peer, we try to update
		// reactors with the peer by remote address, don't error here.
		if err != nil && !IsDialError(err) {
			// If it's a duplicate, we must initialize and add it to reactors,
			// otherwise if it's dialing/already existing, do nothing.
			if errDupl, ok := err.(ErrRejected); ok {
				// TODO(midas): remove debug logs
				sw.Logger.Debug("Duplicate inbound peer - adding to reactors",
					"num_peers", numPeers,
					"num_dials", dialing,
					"conn", errDupl.conn,
					"addr", errDupl.conn.RemoteAddr().String(),
				)

				// Find peer by remote address and add for active ChainIDs.
				// Calls sw.addPeer() if a INBOUND peer matches the remote address.
				if err = sw.addPeerByRemoteAddress(errDupl.conn.RemoteAddr(), "", false); err != nil {
					sw.Logger.Error("duplicate inbound peer rejected",
						"num_peers", numPeers,
						"num_dials", dialing,
						"conn", errDupl.conn,
						"addr", errDupl.conn.RemoteAddr().String(),
						"err", err,
					)
				}

				continue
			}
			// else: ErrCurrentlyDialingOrExistingAddress

			// TODO(midas): add peer to reactors if EXISTING, get id from dialing or peerSets.
			// TODO(midas): for peers currently dialing, we may need to later add to reactors.

			// TODO(midas): remove debug logs
			sw.Logger.Debug("Duplicate inbound peer ignored - already added",
				"num_peers", numPeers,
				"num_dials", dialing,
				"err", err,
			)
			continue
		} else if err != nil {
			if ok := sw.handleErrorGracefully(err); !ok {
				break
			}

			sw.Logger.Error(
				"Inbound peer rejected",
				"num_peers", numPeers,
				"num_dials", dialing,
				"err", err,
			)

			continue
		}

		// BREAKING:
		// NOTE(midas): We removed MaxNumInboundPeers here because a limit on the
		// number of peers is undesired for a multiplex of nodes with many ChainIDs.

		// Add this peer to peersets, reactors and add connection channels.
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

func (sw *Switch) handleErrorGracefully(err error) bool {
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

		return true
	case ErrFilterTimeout:
		sw.Logger.Error("Peer filter timed out",
			"err", err,
		)

		return true
	case ErrTransportClosed:
		sw.Logger.Error("Stopped accept routine, as transport is closed",
			"err", err,
		)
	default:
		sw.Logger.Error("Accept on transport errored - accept routine exited",
			"err", err,
		)
	}

	return false
}

func (sw *Switch) addPeerByRemoteAddress(
	remoteAddr net.Addr,
	optionalPeerID ID,
	outbound bool,
) error {
	// TODO(midas): remove debug logs
	sw.Logger.Debug("Looking up peer by remote address",
		"addr", remoteAddr,
		"peer_id", optionalPeerID,
		"outbound", outbound,
	)

	// Search for the peer.mconn.SocketAddr() with remoteAddr.
	peerSearched := sw.FindMatchingPeerByRemoteAddress(remoteAddr, outbound)
	if peerSearched == nil && len(optionalPeerID) > 0 {
		// TODO(midas): remove debug logs
		sw.Logger.Debug("Looking up peer by ID - address not found",
			"addr", remoteAddr.String(),
			"peer_id", optionalPeerID,
			"outbound", outbound,
		)

		peerSearched = sw.FindMatchingPeerByID(optionalPeerID, outbound)
	}

	if peerSearched == nil {
		// Reaching here may need a conn cleanup.
		// return ErrConnCleanup{}
		return fmt.Errorf(
			"failed to find duplicate peer for addr %s", remoteAddr)
	}

	// TODO(midas): remove debug logs
	sw.Logger.Debug("Found peer by remote address",
		"addr", remoteAddr.String(),
		"peer_id", optionalPeerID,
		"peer", peerSearched,
		"is_outbound", peerSearched.IsOutbound(),
		"is_running", peerSearched.IsRunning(),
	)

	// Add this peer to peersets, reactors and add connection channels.
	if err := sw.addPeer(peerSearched); err != nil {
		sw.transport.Cleanup(peerSearched)
		if peerSearched.IsRunning() {
			_ = peerSearched.Stop()
		}
		return err
	}

	return nil
}

// dial the peer; make secret connection; authenticate against the dialed ID;
// add the peer.
// if dialing fails, start the reconnect loop. If handshake fails, it's over.
// If peer is started successfully, reconnectLoop will start when
// StopPeerForError is called.
func (sw *Switch) addOutboundPeerWithConfig(
	ctx context.Context,
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

	outbound, inbound, dialing := sw.TotalNumPeers()
	numPeers := outbound + inbound

	safePeerConfig := sw.GetPeerConfig()
	p, err := sw.transport.Dial(ctx, *addr, safePeerConfig)
	// TODO(midas): remove debug logs
	sw.Logger.Debug("Sending outbound peer connection request",
		"num_peers", numPeers,
		"num_dials", dialing,
		"addr", addr.String(),
		"peer_id", addr.ID,
		"err", err,
	)

	// If we are dialing a conn from a known peer, we try to update
	// reactors with the peer by remote address, don't error here.
	if err != nil && !IsDialError(err) {
		// If it's a duplicate, we must initialize and add it to reactors,
		// otherwise if it's dialing/already existing, do nothing.
		if errDupl, ok := err.(ErrRejected); ok {
			// TODO(midas): remove debug logs
			sw.Logger.Debug("Duplicate outbound peer - adding to reactor",
				"num_peers", numPeers,
				"num_dials", dialing,
				"peer_id", addr.ID,
				"conn", errDupl.conn,
				"addr", errDupl.conn.RemoteAddr().String(),
				"err", err,
			)

			// Find peer by remote address and add for active ChainIDs.
			// Calls sw.addPeer() if a OUTBOUND peer matches the remote address or the ID.
			if err = sw.addPeerByRemoteAddress(errDupl.conn.RemoteAddr(), addr.ID, true); err != nil {
				sw.Logger.Error("duplicate outbound peer rejected",
					"num_peers", numPeers,
					"num_dials", dialing,
					"conn", errDupl.conn,
					"addr", errDupl.conn.RemoteAddr().String(),
					"peer_id", addr.ID,
					"err", err,
				)
			}

			return nil
		}

		// Find the matching (duplicate) peer by ID, must be added to reactors.
		if sw.HasPeerID(addr.ID, true) {
			p = sw.FindMatchingPeerByID(addr.ID, true) // outbound=true
		} else {
			// TODO(midas): remove debug logs
			sw.Logger.Debug("Duplicate outbound peer ignored - already dialing",
				"num_peers", numPeers,
				"num_dials", dialing,
				"addr", addr.DialString(),
				"peer_id", addr.ID,
				"err", err,
			)
			return nil
		}
	} else if err != nil {
		sw.handleErrorGracefully(err)

		sw.Logger.Error("Outbound peer rejected",
			"num_peers", numPeers,
			"num_dials", dialing,
			"addr", addr.DialString(),
			"peer_id", addr.ID,
			"err", err,
		)

		// retry persistent peers after
		// any dial error besides IsSelf()
		if e, ok := err.(ErrRejected); !ok || !e.IsSelf() {
			if sw.IsPeerPersistent(addr) {
				go sw.reconnectToPeer(addr)
			}
		}

		return err
	}

	// Add this peer to peersets, reactors and add connection channels.
	if err := sw.addPeer(p); err != nil {
		sw.transport.Cleanup(p)
		if p.IsRunning() {
			_ = p.Stop()
		}
		return err
	}

	return nil
}

func (sw *Switch) filterPeer(p *PeerImpl) error {
	// NOTE(midas):
	// We don't need to throw a rejection error on duplicate peers.
	//
	// if sw.peers.Has(p.ID()) {
	// 	return ErrRejected{id: p.ID(), isDuplicate: true}
	// }

	sw.peersMtx.RLock()
	errc := make(chan error, len(sw.peersByScope)*len(sw.peerFilters))
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

// addPeer starts up the Peer and adds it to the Switch. Error is returned if
// the peer is filtered out or failed to start or can't be added.
func (sw *Switch) addPeer(p *PeerImpl) (err error) {
	if err = sw.filterPeer(p); err != nil {
		return
	}

	pubAddr, _ := p.NodeInfo().NetAddress()
	peerLogger := sw.Logger.With("peer", p).With("conn", p.SocketAddr()).With("addr", pubAddr.String())
	p.SetLogger(peerLogger)

	// Handle the shut down case where the switch has stopped but we're
	// concurrently trying to add a peer.
	if !sw.IsRunning() {
		sw.Logger.Error("Won't start a peer - switch is not running", "peer", p)
		return // err
	}

	// In case this peer got stopped before, we must reset it.`
	if p.IsStopped() {
		// TODO(midas): remove debug logs
		sw.Logger.Debug("Resetting peer", "peer", p)
		p.Reset(sw.Context())
	}

	// TODO(midas): remove debug logs
	peerLogger.Debug("Adding peer")

	// Find ChainID values that we share with p.
	relevantChainIds, err := sw.GetPeerActiveChainID(p)
	if err != nil {
		peerLogger.Error("Won't start a peer - failed to determine compatibility",
			"err", err,
		)
		return // err
	}

	// For replication channel, we add the peer to _shared_channels reactors.
	// For CometBFT channels, we add the peer to relevantChainIds reactors.
	relevantScopes := []string{ScopeForDiscovery} // i.e. sw.Peers(p2p.ScopeForDiscovery)
	if sw.Typ != "discovery" {
		relevantScopes = relevantChainIds[:]                       // i.e. sw.Peers(ChainID)
		relevantScopes = append(relevantScopes, ScopeForDiscovery) // AckTransactionBroadcast
	}

	// We may need to add new channels to accept messages from this new peer.
	// CAUTION: This updates MConnection.channelsIdx.
	var connUpdaterFn func(*conn.MConnection)
	connUpdaterFn = sw.OpenChannelsForScopes(relevantScopes)
	connUpdaterFn(p.MConn())

	// Init all the reactor protocols with this peer.
	for _, relevantScope := range relevantScopes {
		// Add the peer to our internal PeerSet storage.
		peerSet := sw.Peers(relevantScope)
		if !peerSet.HasPeer(p) {
			peerSet.Add(p)
		}

		sw.InitPeerForScope(p, relevantScope)
	}

	// Start the peer's send/recv routines.
	// Must start it before adding it to the peer set
	// to prevent Start and Stop from being called concurrently.
	if !p.IsRunning() {
		// TODO(midas): remove debug logs
		sw.Logger.Debug("Starting peer",
			"peer", p,
		)

		if err := p.Start(); err != nil {
			// Should never happen
			sw.Logger.Error("Error starting peer", "err", err, "peer", p)
			return err
		}
	} else {
		// TODO(midas): remove debug logs
		sw.Logger.Debug("Not starting peer - already running",
			"peer", p,
		)
	}

	// Start all the reactor protocols on the peer.
	for _, relevantScope := range relevantScopes {
		// TODO(midas): remove debug logs
		peerLogger.Debug("Adding peer for scope",
			"running", p.IsRunning(),
			"stopped", p.IsStopped(),
			"scope", relevantScope,
		)

		sw.AddPeerForScope(p, relevantScope)
	}

	sw.metrics.Peers.Add(float64(1))

	// TODO(midas): remove debug logs
	peerLogger.Debug("Added peer")
	return nil
}

// ----------------------------------------------------------------------------

func (sw *Switch) FindMatchingPeerByID(id ID, outbound bool) (p *PeerImpl) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		if peerById := peerSet.GetInOrOut(id, outbound); peerById != nil {
			p = peerById
			return // p
		}
	}
	return // nil
}

// TODO: peer lookup by addr, or proper linking between net conns / peer
func (sw *Switch) FindMatchingPeerByRemoteAddress(addr net.Addr, outbound bool) (p *PeerImpl) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	for _, peerSet := range sw.peersByScope {
		if p = peerSet.GetByAddr(addr, outbound); p != nil {
			break
		}
	}
	return // p
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
	numRelevant := len(commonChainIds) + len(activeChainIds)
	relevantChainIds := make([]string, 0, numRelevant)
	relevantChainIds = append(relevantChainIds, commonChainIds...)
	relevantChainIds = append(relevantChainIds, activeChainIds...)

	return relevantChainIds, nil
}

// ----------------------------------------------------------------------------

func (sw *Switch) peerKey(peer *PeerImpl) string {
	peerKey := string(peer.ID())
	if peer.IsOutbound() {
		peerKey += "_out"
	} else {
		peerKey += "_in"
	}
	return peerKey
}

// peerScopeReactorKey returns a combination of a scope, a reactor name,
// the peer ID and the connection type, i.e. outbound or inbound.
func (sw *Switch) peerScopeReactorKey(p *PeerImpl, scope, reactor string) string {
	scopedPeerKey := strings.Join([]string{
		scope,
		reactor,
		string(p.ID()),
	}, "_")

	if p.IsOutbound() {
		return scopedPeerKey + "_out"
	}

	return scopedPeerKey + "_in"
}

// IsPeerInitInReactor returns true if p has been initialized for a pair
// of scope and reactor name.
func (sw *Switch) IsPeerInitialized(p *PeerImpl, scope string, reactor string) bool {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeersInit[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		return false
	}

	// scope, reactor, peer.ID(), in or out
	lookupKey := sw.peerScopeReactorKey(p, scope, reactor)

	sw.runtimesMtx.RLock()
	peerInitTimeTz,
		hasPeerTime := sw.reactorPeersInit[scope][lookupKey]
	swStartTimeTz := sw.startTz
	sw.runtimesMtx.RUnlock()

	if !sw.IsRunning() || !hasPeerTime {
		return false
	}

	// The switch must have started before the peer, otherwise consider inactive.
	return swStartTimeTz.Before(peerInitTimeTz)
}

// MarkPeerInitialized marks p as initialized for a pair of scope and
// reactor name. Note that p should contain the initialized peer as
// returned by reactor.InitPeer().
func (sw *Switch) MarkPeerInitialized(p *PeerImpl, scope string, reactor string) {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeersInit[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		sw.runtimesMtx.Lock()
		sw.reactorPeersInit[scope] = make(map[string]time.Time)
		sw.peersForReactors[scope] = make(map[string]*PeerImpl)
		sw.runtimesMtx.Unlock()
	}

	// scope, reactor, peer.ID(), in or out
	lookupKey := sw.peerScopeReactorKey(p, scope, reactor)

	sw.runtimesMtx.Lock()
	sw.reactorPeersInit[scope][lookupKey] = time.Now()
	sw.peersForReactors[scope][lookupKey] = p // as returned by reactor.InitPeer()
	sw.runtimesMtx.Unlock()
}

// IsPeerAddedToReactor returns true if the peer has been initialized for
// a pair of scope and reactor name, after the switch started running.
func (sw *Switch) IsPeerAddedToReactor(p *PeerImpl, scope string, reactor string) bool {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeersAdded[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		return false
	}

	// scope, reactor, peer.ID(), in or out
	lookupKey := sw.peerScopeReactorKey(p, scope, reactor)

	sw.runtimesMtx.RLock()
	peerInitTimeTz,
		hasPeerTime := sw.reactorPeersAdded[scope][lookupKey]
	swStartTimeTz := sw.startTz
	sw.runtimesMtx.RUnlock()

	if !hasPeerTime {
		return false
	}

	// The switch must have started before the peer, otherwise consider inactive.
	return swStartTimeTz.Before(peerInitTimeTz)
}

// MarkPeerAddedToReactor marks p added for a pair of scope and reactor name.
// Note that p should contain the initialized peer as returned by reactor.InitPeer().
func (sw *Switch) MarkPeerAddedToReactor(p *PeerImpl, scope string, reactor string) {
	sw.runtimesMtx.RLock()
	_, hasReactorsForChain := sw.reactorPeersAdded[scope]
	sw.runtimesMtx.RUnlock()

	if !hasReactorsForChain {
		sw.runtimesMtx.Lock()
		sw.reactorPeersAdded[scope] = make(map[string]time.Time)
		sw.runtimesMtx.Unlock()
	}

	// scope, reactor, peer.ID(), in or out
	lookupKey := sw.peerScopeReactorKey(p, scope, reactor)

	// Mark addition time and remove extra allocation.
	sw.runtimesMtx.Lock()
	sw.reactorPeersAdded[scope][lookupKey] = time.Now()
	sw.peersForReactors[scope][lookupKey] = nil
	delete(sw.peersForReactors[scope], lookupKey)
	sw.runtimesMtx.Unlock()
}

// ----------------------------------------------------------------------------

func (sw *Switch) CleanupChannels() {
	relevantScopes := map[string]bool{}
	relevantScopes[conn.SharedChannelsNamespace] = true
	relevantScopes[ScopeForDiscovery] = true

	activeRuntimes := sw.GetActiveRuntimes()
	for _, activeChainID := range activeRuntimes {
		relevantScopes[activeChainID] = true
	}

	cleanupWg := new(sync.WaitGroup)

	remainingPeerSets := sw.PeersByScopes()
	for _, peerSet := range remainingPeerSets {
		peers := peerSet.Copy()
		cleanupWg.Add(len(peers))

		for _, peer := range peers {
			go func(p *PeerImpl) {
				defer cleanupWg.Done()

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
				scopesToCleanup := valuesFromMapKeys(relevantScopesForPeer)
				connCleanupFn = sw.CloseChannelsForScopes(scopesToCleanup)
				connCleanupFn(mconn)

				sw.Logger.Debug("Removed all channels for peer from cleanup", "peer", p, "scopes", relevantScopes)
			}(peer)
		}
	}
	cleanupWg.Wait()
}

func (sw *Switch) CloseChannelsForScopes(scopes []string) func(mconn *conn.MConnection) {
	return func(mconn *conn.MConnection) {
		for _, scope := range scopes {
			reactorsScope := scope // ChainID or "discovery"
			if scope == ScopeForDiscovery {
				reactorsScope = conn.SharedChannelsNamespace // "_shared_channels"
			}

			reactorsByScope := sw.Reactors(reactorsScope)
			for _, r := range reactorsByScope {
				channels := r.GetChannels()

				sw.runtimesMtx.Lock()
				for _, chDesc := range channels {
					channelRemoved := mconn.RemoveChannel(scope, chDesc)
					if channelRemoved && atomic.LoadUint32(&sw.totalOpenChannels) > 0 {
						atomic.AddUint32(&sw.totalOpenChannels, ^uint32(0)) // -1
					}
				}
				sw.runtimesMtx.Unlock()
			}

			// Close remaining channels from outbound peers.
			channelsIdx := mconn.GetChannelsIdx()
			channels, ok := channelsIdx[scope]
			if !ok {
				continue // move on to next scope
			}

			replChannel := channels[replicationChannel]
			ackChannel := channels[ackBroadcastChannel]
			runChannel := channels[runtimeChannel]

			sw.runtimesMtx.Lock()
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
			sw.runtimesMtx.Unlock()
		}

		channelsIdx := mconn.GetChannelsIdx()

		// TODO(midas): remove debug logs
		sw.Logger.Debug("Removed connection channels",
			"scopes", scopes,
			"conn", mconn.SocketAddr().String(),
			"num_chs", atomic.LoadUint32(&sw.totalOpenChannels),
			"remains", channelsIdx,
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

					_, channelAdded := mconn.AddChannel(sw.BaseService.Context(), scope, chDesc)
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

// ----------------------------------------------------------------------------

func valuesFromMapKeys(in map[string]bool) (out []string) {
	out = make([]string, 0, len(in))
	for s, _ := range in {
		out = append(out, s)
	}
	return // out
}
