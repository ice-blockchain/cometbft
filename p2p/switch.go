package p2p

import (
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"sync"
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
)

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
	peersByChain map[string]*PeerSet
	uniquePeers  *PeerSet
	Typ          string
}

type peerOption = func(p Peer)

const runtimeChainKey = "runtimeChainID"

func PeerWithChainID(chainID string) func(p Peer) {
	return func(p Peer) {
		p.Set(runtimeChainKey, chainID)
	}
}

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
		peersByChain:         map[string]*PeerSet{"": NewPeerSet()},
		uniquePeers:          NewPeerSet(),
		dialing:              cmap.NewCMap(),
		reconnecting:         cmap.NewCMap(),
		metrics:              NopMetrics(),
		transport:            transport,
		filterTimeout:        defaultFilterTimeout,
		persistentPeersAddrs: make([]*NetAddress, 0),
		unconditionalPeerIDs: make(map[ID]struct{}),
	}

	sw.reactorsMtx.Lock()
	sw.reactors[conn.SharedChannelsNamespace] = make(map[string]Reactor)
	sw.chDescs[conn.SharedChannelsNamespace] = make([]*conn.ChannelDescriptor, 0)
	sw.reactorsByCh[conn.SharedChannelsNamespace] = make(map[byte]Reactor)
	sw.msgTypeByChID[conn.SharedChannelsNamespace] = make(map[byte]proto.Message)
	sw.reactorsMtx.Unlock()

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

// AddReactor adds the given reactor to the switch.
// NOTE: Not goroutine safe.
func (sw *Switch) AddReactor(chainID string, name string, reactor Reactor) Reactor {
	// JiT initialize the map of peers by ChainID because when the switch
	// is created, we may not know about all (or any) ChainID.
	sw.peersMtx.RLock()
	_, hasPeerSet := sw.peersByChain[chainID]
	sw.peersMtx.RUnlock()

	if !hasPeerSet {
		sw.peersMtx.Lock()
		sw.peersByChain[chainID] = NewPeerSet()
		sw.peersMtx.Unlock()
	}

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
				"chID", fmt.Sprintf("%X", chID),
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
// NOTE: Not goroutine safe.
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
	delete(sw.reactors, name)
	reactor.SetSwitch(nil)
}

// Reactors returns a map of reactors registered on the switch.
// NOTE: Not goroutine safe.
func (sw *Switch) Reactors(chainID string) map[string]Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	return sw.reactors[chainID]
}

// Reactor returns the reactor with the given name.
// NOTE: Not goroutine safe.
func (sw *Switch) Reactor(chainID string, name string) Reactor {
	sw.reactorsMtx.Lock()
	defer sw.reactorsMtx.Unlock()

	return sw.reactors[chainID][name]
}

// SetNodeInfo sets the switch's NodeInfo for checking compatibility and handshaking with other nodes.
// NOTE: Not goroutine safe.
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

// ---------------------------------------------------------------------
// Service start/stop

// OnStart implements BaseService. It starts all the reactors and peers.
func (sw *Switch) OnStart() error {
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
	sw.peersMtx.RLock()
	peers := sw.uniquePeers.Copy()
	sw.peersMtx.RUnlock()

	// stopAndRemove locks the mutex
	for _, p := range peers {
		sw.stopAndRemovePeer(p, nil)
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

	if peerSet, ok := sw.peersByChain[chainID]; ok {
		peerSet.ForEach(func(p Peer) {
			go func(peer Peer) {
				success := peer.Send(chainID, e)
				_ = success
			}(p)
		})
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

	if peerSet, ok := sw.peersByChain[chainID]; ok {
		peerSet.ForEach(func(p Peer) {
			go func(peer Peer) {
				peer.TrySend(chainID, e)
			}(p)
		})
	}
}

// NumPeers returns the count of outbound/inbound and outbound-dialing peers.
// unconditional peers are not counted here.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) NumPeers(chainID string) (outbound, inbound, dialing int) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	if peerSet, ok := sw.peersByChain[chainID]; ok {
		peerSet.ForEach(func(peer Peer) {
			if peer.IsOutbound() && !sw.IsPeerUnconditional(peer.ID()) {
				outbound++
			} else if !sw.IsPeerUnconditional(peer.ID()) {
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

	for _, peerSet := range sw.peersByChain {
		peerSet.ForEach(func(peer Peer) {
			if peer.IsOutbound() && !sw.IsPeerUnconditional(peer.ID()) {
				outbound++
			} else if !sw.IsPeerUnconditional(peer.ID()) {
				inbound++
			}
		})
	}

	dialing = sw.dialing.Size()
	return outbound, inbound, dialing
}

// NumUniquePeers returns the count of unique outbound/inbound and outbound-dialing
// peers across all ChainIDs. Unconditional peers are not counted here.
func (sw *Switch) NumUniquePeers() (outbound, inbound, dialing int) {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()

	sw.uniquePeers.ForEach(func(peer Peer) {
		if peer.IsOutbound() && !sw.IsPeerUnconditional(peer.ID()) {
			outbound++
		} else if !sw.IsPeerUnconditional(peer.ID()) {
			inbound++
		}
	})

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

// Peers returns the set of peers that are connected to the switch.
// Requires a chainID to filter the returned peers instances.
func (sw *Switch) Peers(chainID string) *PeerSet {
	sw.peersMtx.RLock()
	if peerSet, ok := sw.peersByChain[chainID]; ok {
		defer sw.peersMtx.RUnlock()
		return peerSet
	}
	sw.peersMtx.RUnlock()

	peerSet := NewPeerSet()
	sw.peersMtx.Lock()
	defer sw.peersMtx.Unlock()
	sw.peersByChain[chainID] = peerSet

	return peerSet
}

// UniquePeers returns the set of unique peer that are connected to the switch.
func (sw *Switch) UniquePeers() *PeerSet {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()
	return sw.uniquePeers
}

// HasPeerID iterates peersByChain to find a peer by its ID.
func (sw *Switch) HasPeerID(id ID) bool {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()
	return sw.uniquePeers.Has(id)
}

// HasPeerIP iterates peersByChain to find a peer by its IP.
func (sw *Switch) HasPeerIP(peerIP net.IP) bool {
	sw.peersMtx.RLock()
	defer sw.peersMtx.RUnlock()
	return sw.uniquePeers.HasIP(peerIP)
}

// InitPeerForChain adds peers to the reactors. This is necessary when the
// switch is already running and peers must work with new ChainID values.
//
// TODO(midas): Add to subset of reactors as requested or necessary (missing).
func (sw *Switch) InitPeerForChain(peer Peer, chainID string) {
	if !peer.IsRunning() {
		return
	}

	for _, reactor := range sw.Reactors(chainID) {
		peerForReactor := reactor.InitPeer(peer)
		reactor.AddPeer(peerForReactor)
	}

	sw.Logger.Info("Added peer to reactors", "id", string(peer.ID()))
}

// StopPeerForError disconnects from a peer due to external error.
// If the peer is persistent, it will attempt to reconnect.
// TODO: make record depending on reason.
func (sw *Switch) StopPeerForError(peer Peer, reason any) {
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
func (sw *Switch) StopPeerGracefully(peer Peer) {
	sw.Logger.Info("Stopping peer gracefully")
	sw.stopAndRemovePeer(peer, nil)
}

// stopPeer calls the Stop method on a peer, then cleans up
// the transport instance and removes the peer from all reactors.
func (sw *Switch) stopPeer(peer Peer, reason any) error {
	if err := peer.Stop(); err != nil {
		return fmt.Errorf(
			"error stopping peer for ID %s: %w", string(peer.ID()), err,
		)
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

	return nil
}

// removePeer removes the peer from all PeerSet instances.
func (sw *Switch) removePeer(peer Peer, reason any) error {
	sw.peersMtx.Lock()
	defer sw.peersMtx.Unlock()

	if err := sw.uniquePeers.RemoveByAddr(peer.RemoteAddr()); err != nil {
		if extra, ok := err.(ErrHasExtraPeer); ok {
			err = sw.stopPeer(extra.Peer, reason)
			if errors.Is(err, service.ErrAlreadyStopped) {
				err = nil
			}
		}
		if err != nil {
			return fmt.Errorf(
				"error on unique peer removal for ID %s: %w", string(peer.ID()), err,
			)
		}
	}

	for _, peerSet := range sw.peersByChain {
		if peerSet.Has(peer.ID()) {
			if err := peerSet.RemoveByAddr(peer.RemoteAddr()); err != nil {
				if extra, ok := err.(ErrHasExtraPeer); ok {
					err = sw.stopPeer(extra.Peer, reason)
					if errors.Is(err, service.ErrAlreadyStopped) {
						err = nil
					}
				}
				if err != nil {
					return fmt.Errorf(
						"error on peer removal for ID %s: %w", string(peer.ID()), err,
					)
				}
			}
			if peerSet.Has(peer.ID()) {
				peerSet.Remove(peer)
			}
		}
	}

	return nil
}

// stopAndRemovePeer first stops the peer, then removes it from all PeerSet
// instances. Returns early from stopping the peer in case it errors.
func (sw *Switch) stopAndRemovePeer(peer Peer, reason any) {
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

	sw.metrics.Peers.Add(float64(-1))
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

	return sw.addOutboundPeerWithConfig(addr, sw.config)
}

// DialPeerWithAddressAndChainID dials the given peer and adds a runtimeChainID
// to its configuration before it runs sw.addPeer if it connects and
// authenticates successfully.
// If we're currently dialing this address or it belongs to an existing peer,
// ErrCurrentlyDialingOrExistingAddress is returned.
func (sw *Switch) DialPeerWithAddressAndChainID(addr *NetAddress, chainID string) error {
	if sw.IsDialingOrExistingAddress(addr) {
		return ErrCurrentlyDialingOrExistingAddress{addr.String()}
	}

	sw.dialing.Set(string(addr.ID), addr)
	defer sw.dialing.Delete(string(addr.ID))

	return sw.addOutboundPeerWithConfig(addr, sw.config, PeerWithChainID(chainID))
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
		sw.HasPeerID(addr.ID) ||
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
	outbound, inbound, _ := sw.NumUniquePeers()
	numPeers := outbound + inbound
	for {
		safePeerConfig := sw.GetPeerConfig()
		p, err := sw.transport.Accept(safePeerConfig)
		if err != nil {
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

		if !sw.IsPeerUnconditional(p.NodeInfo().ID()) {
			// Ignore connection if we already have enough peers.
			_, in, _ := sw.NumUniquePeers()
			if in >= sw.config.MaxNumInboundPeers {
				sw.Logger.Info(
					"Ignoring inbound connection: already have enough inbound peers",
					"address", p.SocketAddr(),
					"have", in,
					"max", sw.config.MaxNumInboundPeers,
				)

				sw.transport.Cleanup(p)

				continue
			}
		}

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
	options ...peerOption,
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

	// Applies custom options to Peer object
	for _, option := range options {
		option(p)
	}

	if err := sw.addPeer(p); err != nil {
		sw.transport.Cleanup(p)
		if p.IsRunning() {
			_ = p.Stop()
		}
		return err
	}

	return nil
}

func (sw *Switch) filterPeer(p Peer) error {
	// NOTE(midas):
	// We don't need to throw a rejection error on duplicate peers at this
	// stage because we skip known peers in [PeerSet#Add].
	//
	// if sw.peers.Has(p.ID()) {
	// 	return ErrRejected{id: p.ID(), isDuplicate: true}
	// }

	errc := make(chan error, len(sw.peersByChain)*len(sw.peerFilters))

	sw.peersMtx.RLock()
	for _, peerSet := range sw.peersByChain {
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
func (sw *Switch) addPeer(p Peer) error {
	if err := sw.filterPeer(p); err != nil {
		return err
	}

	p.SetLogger(sw.Logger.With("peer", p.SocketAddr()))

	// Handle the shut down case where the switch has stopped but we're
	// concurrently trying to add a peer.
	if !sw.IsRunning() {
		// XXX should this return an error or just log and terminate?
		sw.Logger.Error("Won't start a peer - switch is not running", "peer", p)
		return nil
	}

	relevantChainIds, err := sw.NodeInfo().GetCommonChains(p.NodeInfo())
	if err != nil {
		sw.Logger.Error("Won't start a peer - error with common chains",
			"peer", p,
			"err", err,
		)
		return nil
	}

	// If we don't have common chains with p, we are creating a network.
	runtimeChainID := p.Get(runtimeChainKey)
	if len(relevantChainIds) == 0 && runtimeChainID != nil {
		relevantChainIds = []string{runtimeChainID.(string)}
	} else if runtimeChainID != nil && !slices.Contains(relevantChainIds, runtimeChainID.(string)) {
		relevantChainIds = append(relevantChainIds, runtimeChainID.(string))
	}

	// Add some data to the peer, which is required by reactors.
	for _, chainID := range relevantChainIds {
		reactors := sw.Reactors(chainID)
		for _, reactor := range reactors {
			p = reactor.InitPeer(p)
		}
	}

	// Update this peer's MConnection.channelsIdx.
	sw.reactorsMtx.Lock()
	localAvailableChainIds := []string{}
	for chainID := range sw.chDescs {
		localAvailableChainIds = append(localAvailableChainIds, chainID)
	}
	sw.reactorsMtx.Unlock()

	sw.UpdateChannelsForMConn(localAvailableChainIds, []byte{})(p.MConn())

	// Start the peer's send/recv routines.
	// Must start it before adding it to the peer set
	// to prevent Start and Stop from being called concurrently.
	if err := p.Start(); err != nil {
		// Should never happen
		sw.Logger.Error("Error starting peer", "err", err, "peer", p)
		return err
	}

	// First we add it to the unique peers if necessary
	sw.peersMtx.Lock()
	if !sw.uniquePeers.Has(p.ID()) {
		if err := sw.uniquePeers.Add(p); err != nil {
			if _, ok := err.(ErrPeerRemoval); ok {
				sw.Logger.Error("Error starting peer ",
					" err ", "Peer has already errored and removal was attempted.",
					"peer", p.ID())
			}
			sw.peersMtx.Unlock()
			return err
		}
	}
	sw.peersMtx.Unlock()

	// Then we add it to all internal peersets by ChainID.
	sw.peersMtx.Lock()
	for chainID, peerSet := range sw.peersByChain {
		if !slices.Contains(localAvailableChainIds, chainID) {
			continue
		}

		// Add the peer to PeerSet. Do this before starting the reactors
		// so that if Receive errors, we will find the peer and remove it.
		// Add should not err since we already checked peers.Has().
		if err := peerSet.Add(p); err != nil {
			if _, ok := err.(ErrPeerRemoval); ok {
				sw.Logger.Error("Error starting peer ",
					" err ", "Peer has already errored and removal was attempted.",
					"peer", p.ID())
			}
			sw.peersMtx.Unlock()
			return err
		}
	}
	sw.peersMtx.Unlock()

	sw.metrics.Peers.Add(float64(1))

	// Start all the reactor protocols on the peer.
	for _, chainID := range relevantChainIds {
		reactors := sw.Reactors(chainID)
		for _, reactor := range reactors {
			reactor.AddPeer(p)
		}
	}

	sw.Logger.Debug("Added peer", "peer", p)

	return nil
}

func (sw *Switch) UpdateChannelsForMConn(chainIds []string, channels []byte) func(mconn *conn.MConnection) {
	return func(mconn *conn.MConnection) {
		// TODO(midas): Refactor this, importing server here won't work.
		const (
			replicationChannel  = byte(0x90)
			ackBroadcastChannel = byte(0x91)
			runtimeChannel      = byte(0x92)
		)

		cometMxChannels := []byte{
			ackBroadcastChannel,
			runtimeChannel,
		}

		for _, chainID := range chainIds {
			for name, r := range sw.Reactors(chainID) {
				for _, chDesc := range r.GetChannels() {
					if len(channels) > 0 && !slices.Contains(channels, chDesc.ID) {
						continue
					} else if sw.Typ == "discovery" && chDesc.ID != replicationChannel {
						// Discovery switch needs only ReplicationChannel
						continue
					} else if sw.Typ == "cometBFT" && name == "MULTIPLEX" {
						// CometBFT should add only relevant multiplex channels
						if !slices.Contains(cometMxChannels, chDesc.ID) {
							continue
						}
					}

					// TODO(midas): remove debug logs
					sw.Logger.Debug("Adding connection channel",
						"chain_id", chainID,
						"reactor", name,
						"chID", chDesc.ID,
						"peer", mconn.SocketAddr().String(),
					)

					mconn.AddChannel(chainID, *chDesc)
				}
			}
		}
	}
}
