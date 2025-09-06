package p2p

import (
	"net"
	"strconv"

	cmtrand "github.com/ice-blockchain/cometbft/internal/rand"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
)

// IPeerSet has a (immutable) subset of the methods of PeerSet.
type IPeerSet interface {
	// Has returns true if the set contains the peer referred to by this key.
	Has(key ID) bool
	// HasIP returns true if the set contains the peer referred to by this IP.
	HasIP(ip net.IP) bool
	// HasPeer returns true if the set contains the peer referred to by p.
	HasPeer(p *PeerImpl) bool

	// Get returns the peer with the given key, or nil if not found.
	Get(key ID) *PeerImpl
	// GetByAddr returns peer with the given RemoteAddr, or nil if not found.
	GetByAddr(addr net.Addr)
	// Add adds p to the set or returns an error.
	Add(peer *PeerImpl) error
	// Remove returns true if p was removed from the set.
	Remove(peer *PeerImpl) bool

	// Copy returns a copy of the peers list.
	Copy() []*PeerImpl
	// Size returns the number of peers in the PeerSet.
	Size() int
	// ForEach iterates over the PeerSet and calls the given function for each peer.
	ForEach(peer func(*PeerImpl))
	// Random returns a random peer from the PeerSet.
	Random() *PeerImpl
}

// -----------------------------------------------------------------------------

// PeerSet is a special thread-safe structure for keeping a table of peers.
type PeerSet struct {
	mtx    cmtsync.Mutex
	list   []*PeerImpl
	lookup map[string]*peerSetItem
	addrs  map[string]*peerSetItem
}

type peerSetItem struct {
	peer  *PeerImpl
	index int
}

// NewPeerSet creates a new peerSet with a list of initial capacity of 256 items.
func NewPeerSet() *PeerSet {
	return &PeerSet{
		lookup: make(map[string]*peerSetItem),
		addrs:  make(map[string]*peerSetItem),
		list:   make([]*PeerImpl, 0, 256),
	}
}

// Add adds the peer to the PeerSet.
// It returns an error carrying the reason, if the peer is already present.
func (ps *PeerSet) Add(peer *PeerImpl) error {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.lookup[ps.KeyForPeer(peer)] != nil {
		// NOTE(midas):
		// In a multiplex environment, it is common to discover peers with IDs
		// that we already know about because we are using AddrBook with the
		// same NodeID. Note that these AddrBook may contain different peers.
		return nil
	}

	index := len(ps.list)
	item := &peerSetItem{peer, index}

	// Appending is safe even with other goroutines
	// iterating over the ps.list slice.
	ps.list = append(ps.list, peer)
	ps.lookup[ps.KeyForPeer(peer)] = item

	// Uses [net.Conn#RemoteAddr].
	address := peer.RemoteAddr().String()
	ps.addrs[address] = item
	return nil
}

func (ps *PeerSet) Key(id ID, outbound bool) string {
	return ps.lookupKey(id, outbound)
}

func (ps *PeerSet) KeyForOutbound(id ID) string {
	return ps.lookupKey(id, true)
}

func (ps *PeerSet) KeyForInbound(id ID) string {
	return ps.lookupKey(id, false)
}

func (ps *PeerSet) KeyForPeer(p *PeerImpl) string {
	return ps.peerLookupKey(p)
}

// -----------------------------------------------------------------------------
// implements Peer

// Has returns true if the set contains the peer referred to by this
// peerID, otherwise false.
func (ps *PeerSet) Has(peerID ID) bool {
	return ps.HasInbound(peerID) || ps.HasOutbound(peerID)
}

// HasIP returns true if the set contains the peer referred to by this IP
// address, otherwise false.
func (ps *PeerSet) HasIP(peerIP net.IP) bool {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	for _, peer := range ps.list {
		if peer.RemoteIP().Equal(peerIP) {
			return true
		}
	}

	return false
}

// Get looks up a peer by the provided peerID. Returns nil if peer is not
// found. Outbound peer entries have precedence.
func (ps *PeerSet) Get(peerID ID) *PeerImpl {
	getterFn := ps.getInbound
	if ps.HasOutbound(peerID) {
		getterFn = ps.getOutbound
	}

	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return getterFn(peerID)
}

// GetInbound looks up a peer byt the provided peerID and IN direction.
func (ps *PeerSet) GetInbound(peerID ID) *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return ps.getInbound(peerID)
}

// GetOutbound looks up a peer byt the provided peerID and OUT direction.
func (ps *PeerSet) GetOutbound(peerID ID) *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	return ps.getOutbound(peerID)
}

// GetByAddr returns peer with the given RemoteAddr, or nil if not found.
func (ps *PeerSet) GetByAddr(addr net.Addr) *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if item, ok := ps.addrs[addr.String()]; ok {
		return item.peer
	}
	return nil
}

// Size returns the number of unique items in the peerSet.
func (ps *PeerSet) Size() int {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()
	return len(ps.list)
}

// Copy returns the copy of the peers list.
//
// Note: there are no guarantees about the thread-safety of Peer objects.
func (ps *PeerSet) Copy() []*PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	c := make([]*PeerImpl, len(ps.list))
	copy(c, ps.list)
	return c
}

// ForEach iterates over the PeerSet and calls the given function for each peer.
func (ps *PeerSet) ForEach(fn func(peer *PeerImpl)) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	for _, item := range ps.lookup {
		fn(item.peer)
	}
}

// Random returns a random peer from the PeerSet.
func (ps *PeerSet) Random() *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if len(ps.list) == 0 {
		return nil
	}

	return ps.list[cmtrand.Int()%len(ps.list)]
}

// -----------------------------------------------------------------------------

// HasPeer returns true if the set contains the peer referred to by this
// instance of [PeerImpl], i.e. takes account of inbound/outbound.
func (ps *PeerSet) HasPeer(p *PeerImpl) bool {
	if p.IsOutbound() {
		return ps.HasOutbound(p.ID())
	}

	return ps.HasInbound(p.ID())
}

// HasInbound returns true if the set contains the peer as inbound.
func (ps *PeerSet) HasInbound(peerID ID) bool {
	ps.mtx.Lock()
	_, ok := ps.lookup[ps.KeyForInbound(peerID)]
	ps.mtx.Unlock()
	return ok
}

// HasOutbound returns true if the set contains the peer as inbound.
func (ps *PeerSet) HasOutbound(peerID ID) bool {
	ps.mtx.Lock()
	_, ok := ps.lookup[ps.KeyForOutbound(peerID)]
	ps.mtx.Unlock()
	return ok
}

// Remove removes the peer from the PeerSet. Note that this may remove
// up to 2 entries (inbound/outbound) for one peer ID.
func (ps *PeerSet) Remove(peer *PeerImpl) bool {
	ps.mtx.Lock()
	if len(ps.list) == 0 {
		ps.mtx.Unlock()
		return false
	}
	ps.mtx.Unlock()

	ok1 := ps.remove(peer.ID(), peer.IsOutbound())
	ok2 := ps.remove(peer.ID(), !peer.IsOutbound())
	if !(ok1 || ok2) {
		return false
	}
	return ok1 || ok2
}

// RemovePeer removes the peer from the PeerSet. Note that this removes
// always 1 entry (inbound or outbound) for one peer ID.
func (ps *PeerSet) RemovePeer(peer *PeerImpl) bool {
	ps.mtx.Lock()
	if len(ps.list) == 0 {
		ps.mtx.Unlock()
		return false
	}
	ps.mtx.Unlock()

	ok := ps.remove(peer.ID(), peer.IsOutbound())
	if !ok {
		return false
	}
	return true
}

// RemoveByAddr removes the peer from the PeerSet given a net.Addr.
// Note that this may remove up to 2 entries (inbound/outbound) for one peer ID.
func (ps *PeerSet) RemoveByAddr(addr net.Addr) error {
	p := ps.GetByAddr(addr)
	if p == nil {
		return nil // Nothing to do
	}

	if ok := ps.Remove(p); !ok {
		return ErrPeerRemoval{}
	}
	return nil
}

// -----------------------------------------------------------------------------

// getInbound returns an inbound Peer with ID peerKey or nil.
func (ps *PeerSet) getInbound(peerID ID) *PeerImpl {
	if item, ok := ps.lookup[ps.KeyForInbound(peerID)]; ok {
		return item.peer
	}
	return nil
}

// getOutbound returns an outbound Peer with ID peerKey or nil.
func (ps *PeerSet) getOutbound(peerID ID) *PeerImpl {
	if item, ok := ps.lookup[ps.KeyForOutbound(peerID)]; ok {
		return item.peer
	}
	return nil
}

func (ps *PeerSet) remove(id ID, outbound bool) bool {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.Key(id, outbound)]
	if !ok || len(ps.list) == 0 {
		return false
	}

	index := item.index

	// Remove from ps.lookup.
	delete(ps.lookup, ps.Key(id, outbound))
	// Remove from ps.addrs.
	delete(ps.addrs, item.peer.RemoteAddr().String())

	// If it's not the last item.
	if index != len(ps.list)-1 {
		// Swap it with the last item.
		lastPeer := ps.list[len(ps.list)-1]
		item := ps.lookup[ps.KeyForPeer(lastPeer)]
		item.index = index
		ps.list[index] = item.peer
	}

	// Remove the last item from ps.list.
	ps.list[len(ps.list)-1] = nil // nil the last entry of the slice to shorten, so it isn't reachable & can be GC'd.
	ps.list = ps.list[:len(ps.list)-1]

	return true
}

func (ps *PeerSet) lookupKey(id ID, outbound bool) string {
	return string(id) + strconv.FormatBool(outbound)
}

func (ps *PeerSet) peerLookupKey(p *PeerImpl) string {
	return ps.lookupKey(p.ID(), p.IsOutbound())
}
