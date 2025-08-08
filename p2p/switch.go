package p2p

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/cosmos/gogoproto/proto"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/rand"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
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

	mempoolChannel      = byte(0x30)
	replicationChannel  = byte(0x90)
	ackBroadcastChannel = byte(0x91)
	runtimeChannel      = byte(0x92)
)

// MultiplexChannels contains channels that are processed by CometBFT.
var MultiplexChannels = []byte{
	ackBroadcastChannel,
	runtimeChannel,
	mempoolChannel,
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
	mtx *sync.Mutex

	pool Pool

	config        *config.P2PConfig
	reactorsByCh  map[byte]string // reactor name
	chDescs       map[byte]*conn.ChannelDescriptor
	msgTypeByChID map[byte]proto.Message

	nodeInfo NodeInfo // our node info
	nodeKey  *NodeKey // our node privkey
	addrBook AddrBook // our peers addrs

	rng *rand.Rand // seed for randomizing dial times and orders

	metrics *Metrics
}

type peerOption = func(p Peer)

// NetAddress returns the address the switch is listening on.
func (sw *Switch) NetAddress() *NetAddress {
	addr := sw.pool.Transport().NetAddress()
	return &addr
}

// SwitchOption sets an optional parameter on the Switch.
type SwitchOption func(*Switch)

// NewSwitch creates a new Switch with the given config.
func NewSwitch(
	ctx context.Context,
	cfg *config.P2PConfig,
	pool Pool,
	options ...SwitchOption,
) *Switch {
	sw := &Switch{
		mtx: new(sync.Mutex),

		pool: pool,

		config:        cfg,
		chDescs:       make(map[byte]*conn.ChannelDescriptor),
		reactorsByCh:  make(map[byte]string),
		msgTypeByChID: make(map[byte]proto.Message),
		metrics:       NopMetrics(),
	}

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
	sw.Logger = l
	sw.Transport().SetLogger(sw.Logger)
}

// WithMetrics sets the metrics.
func WithMetrics(metrics *Metrics) SwitchOption {
	return func(sw *Switch) { sw.metrics = metrics }
}

// ---------------------------------------------------------------------
// Switch setup

// AddReactor adds the given reactor to the switch.
func (sw *Switch) AddReactor(chainID string, name string, reactor Reactor) Reactor {
	sw.Logger.Debug("AddReactor", "chainId", chainID, "name", name, "reactor", reactor)

	// NOTE(midas): Nothing to do here anymore. The Switch's capacity to manage
	// reactors has been removed in favor of Dispatcher and Pool interfaces.

	return reactor
}

// RemoveReactor removes the given Reactor from the Switch.
func (sw *Switch) RemoveReactor(chainID string, name string, reactor Reactor) {
	sw.Logger.Debug("RemoveReactor", "chainId", chainID, "name", name, "reactor", reactor)

	// NOTE(midas): Nothing to do here anymore. The Switch's capacity to manage
	// reactors has been removed in favor of Dispatcher and Pool interfaces.
}

// Reactors returns a map of reactors registered on the switch.
func (sw *Switch) Reactors(chainID string) map[string]Reactor {
	return sw.pool.Dispatcher().Reactors(chainID)
}

// Reactor returns the reactor with the given name.
func (sw *Switch) Reactor(chainID string, name string) Reactor {
	return sw.pool.Dispatcher().Reactor(chainID, name)
}

func (sw *Switch) Connector() Connector {
	return sw.pool.Connector()
}

// NodeInfo returns the switch's NodeInfo.
func (sw *Switch) NodeInfo() NodeInfo {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	return sw.nodeInfo
}

// SetNodeInfo sets the switch's NodeInfo for checking compatibility and handshaking with other nodes.
func (sw *Switch) SetNodeInfo(nodeInfo NodeInfo) {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	sw.nodeInfo = nodeInfo
}

// NodeKey returns the switch's NodeKey.
func (sw *Switch) NodeKey() *NodeKey {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	return sw.nodeKey
}

// SetNodeKey sets the switch's private key for authenticated encryption.
func (sw *Switch) SetNodeKey(nodeKey *NodeKey) {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	sw.nodeKey = nodeKey
}

// GetPeerConfig returns the peer configuration object.
func (sw *Switch) GetPeerConfig() PeerConfig {
	return PeerConfig{
		dispatcher:  sw.pool.Dispatcher(),
		onPeerError: sw.StopPeerForError,
		// multiplex disables persistent peers.
		isPersistent: func(na *NetAddress) bool { return false },
		metrics:      sw.metrics,
	}
}

// Transport returns the switch's Transport.
func (sw *Switch) Transport() *MultiplexTransport {
	return sw.pool.Transport().(*MultiplexTransport)
}

// Metrics returns the p2p metrics.
func (sw *Switch) Metrics() *Metrics {
	return sw.metrics
}

// GetMultiplexReactor returns the multiplex reactor if it exists.
// Locks the reactors mutex, for usage from outside.
func (sw *Switch) GetMultiplexReactor() Reactor {
	return sw.pool.Dispatcher().GetMultiplexReactor()
}

// ---------------------------------------------------------------------
// Service start/stop

// OnStart implements BaseService.
func (sw *Switch) OnStart(ctx context.Context) error {

	// BREAKING
	// NOTE(midas): We removed the startup of reactors from this method because
	// in a multiplex of nodes, the active node runtimes (ChainID) have a short
	// lifecycle that is controlled fully by the multiplex reactor.

	return nil
}

// OnStop implements BaseService. It stops all peers and reactors.
func (sw *Switch) OnStop() {}

// ---------------------------------------------------------------------
// Peers

// SetAddrBook allows to set address book on Switch.
func (sw *Switch) SetAddrBook(addrBook AddrBook) {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	sw.addrBook = addrBook
}

// GetAddrBook returns the [p2p.AddrBook] instance associated with the Switch.
func (sw *Switch) GetAddrBook() AddrBook {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	return sw.addrBook
}

// MarkPeerAsGood marks the given peer as good when it did something useful
// like contributed to consensus.
func (sw *Switch) MarkPeerAsGood(peer Peer) {
	sw.mtx.Lock()
	defer sw.mtx.Unlock()

	if sw.addrBook != nil {
		sw.addrBook.MarkGood(peer.ID())
	}
}

// Broadcast runs a go routine for each attempted send, which will block trying
// to send for defaultSendTimeoutSeconds.
//
// NOTE: Broadcast uses goroutines, so order of broadcast may not be preserved.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) Broadcast(_ string, e Envelope) error {
	return sw.pool.Broadcast(e)
}

// TryBroadcast runs a go routine for each attempted send.
// If the send queue of the destination channel and peer are full, the message will not be sent. To make sure that messages are indeed sent to all destination, use `Broadcast`.
//
// NOTE: TryBroadcast uses goroutines, so order of broadcast may not be preserved.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) TryBroadcast(_ string, e Envelope) error {
	return sw.pool.TryBroadcast(e)
}

// NumPeers returns the count of outbound/inbound and outbound-dialing peers.
// unconditional peers are not counted here.
// NOTE(midas): Uses the PeerSet instance corresponding to chainID.
func (sw *Switch) NumPeers(chainIds ...string) (outbound, inbound, dialing int) {
	return sw.pool.NumPeers(chainIds...)
}

// Peers returns the set of peers that are connected to the switch.
// Requires a scope to filter the returned peers instances, note that
// the scope may contain a ChainID, or "discovery".
func (sw *Switch) Peers(chainIds ...string) *PeerSet {
	return sw.pool.Peers(chainIds...)
}

func (sw *Switch) HasPeer(peer *PeerImpl) bool {
	return sw.pool.HasPeer(peer)
}

// HasPeerID iterates peersByScope to find a peer by its ID.
func (sw *Switch) HasPeerID(id ID) bool {
	return sw.pool.HasPeerID(id)
}

// HasPeerIP iterates peersByScope to find a peer by its IP.
func (sw *Switch) HasPeerIP(ip net.IP) bool {
	return sw.pool.HasPeerIP(ip)
}

// InitPeerForScope calls InitPeer of reactors for peers. This is necessary
// when the switch is already running and peers must work with new ChainID values.
//
// IMPORTANT: The InitPeer() method of reactors is called only once per peer ID,
// without making a distinction about inbound/outbound. This is because this
// distinction does not matter for CometBFT reactors.
func (sw *Switch) InitPeerForScope(peer *PeerImpl, chainID string) {
	sw.pool.InitPeerForChainID(peer.ID(), chainID)
}

// AddPeerForScope adds peers to the reactors. This is necessary when the
// switch is already running and peers must work with new ChainID values.
//
// The reactor's AddPeer method may be called more than once per peer ID,
// as CometBFT reactors do not distinguish between inbound/outbound.
func (sw *Switch) AddPeerForScope(peer *PeerImpl, chainID string) {
	sw.pool.AddPeerForChainID(peer.ID(), chainID)
}

// StopPeerForError disconnects from a peer due to external error.
// If the peer is persistent, it will attempt to reconnect.
// TODO: make record depending on reason.
func (sw *Switch) StopPeerForError(peer *PeerImpl, reason any) {
	sw.Logger.Error("Stopping peer for error", "peer", peer, "err", reason)

	if err := sw.pool.RemovePeer(peer.ID()); err != nil {
		sw.Logger.Error("failed to remove peer",
			"peer", peer,
			"err", err,
		)
	}

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

// // StopPeerGracefully disconnects from a peer gracefully.
// // TODO: handle graceful disconnects.
func (sw *Switch) StopPeerGracefully(peer *PeerImpl) {
	sw.Logger.Info("Stopping peer gracefully", "peer", peer)

	if err := sw.pool.RemovePeer(peer.ID()); err != nil {
		sw.Logger.Error("failed to remove peer",
			"peer", peer,
			"err", err,
		)
	}
}

// ---------------------------------------------------------------------
// Dialing

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
	for _, netAddr := range netAddrs {
		sw.pool.Connector().Dial(netAddr)
	}
	return nil
}

// DialPeerWithAddress dials the given peer and runs sw.addPeer if it connects
// and authenticates successfully.
// If we're currently dialing this address or it belongs to an existing peer,
// ErrCurrentlyDialingOrExistingAddress is returned.
func (sw *Switch) DialPeerWithAddress(addr *NetAddress) (err error) {
	if sw.pool.HasPeerID(addr.ID) {
		return ErrCurrentlyDialingOrExistingAddress{addr.String()}
	}

	var peer *PeerImpl
	if peer, err = sw.pool.Connector().Dial(addr); err == nil {
		return sw.pool.AddPeer(peer)
	}

	return err
}
