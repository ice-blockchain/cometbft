package p2p

import (
	"fmt"
	"net"
	"strconv"

	cmtrand "github.com/ice-blockchain/cometbft/internal/rand"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
)

type ErrHasExtraPeer struct {
	Peer Peer
}

func (e ErrHasExtraPeer) Error() string {
	return fmt.Sprintf("has same peer inbound / outbound conn (%v)", e.Peer.String())
}

// IPeerSet has a (immutable) subset of the methods of PeerSet.
type IPeerSet interface {
	// Has returns true if the set contains the peer referred to by this key.
	Has(key ID) bool
	// HasIP returns true if the set contains the peer referred to by this IP
	HasIP(ip net.IP) bool
	// Get returns the peer with the given key, or nil if not found.
	Get(key ID) Peer
	GetByAddr(key net.Addr) Peer
	// Copy returns a copy of the peers list.
	Copy() []Peer
	// Size returns the number of peers in the PeerSet.
	Size() int
	// ForEach iterates over the PeerSet and calls the given function for each peer.
	ForEach(peer func(Peer))
	// Random returns a random peer from the PeerSet.
	Random() Peer
}

// -----------------------------------------------------------------------------

// PeerSet is a special thread-safe structure for keeping a table of peers.
type PeerSet struct {
	mtx        cmtsync.Mutex
	lookup     map[string]*peerSetItem
	addrLookup map[string]*peerSetItem
	list       []Peer
}

type peerSetItem struct {
	peer  Peer
	index int
}

// NewPeerSet creates a new peerSet with a list of initial capacity of 256 items.
func NewPeerSet() *PeerSet {
	return &PeerSet{
		lookup:     make(map[string]*peerSetItem),
		list:       make([]Peer, 0, 256),
		addrLookup: make(map[string]*peerSetItem),
	}
}

// Add adds the peer to the PeerSet.
// It returns an error carrying the reason, if the peer is already present.
func (ps *PeerSet) Add(peer Peer) error {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if ps.lookup[ps.peerLookupKey(peer)] != nil {
		// NOTE(midas):
		// In a multiplex environment, it is common to discover peers with IDs
		// that we already know about because we are using AddrBook with the
		// same NodeID. Note that these AddrBook may contain different peers.
		index := len(ps.list)
		ps.addrLookup[peer.RemoteAddr().String()] = &peerSetItem{peer, index}
		return nil
	}
	if peer.GetRemovalFailed() {
		return ErrPeerRemoval{}
	}

	index := len(ps.list)
	// Appending is safe even with other goroutines
	// iterating over the ps.list slice.
	ps.list = append(ps.list, peer)
	ps.lookup[ps.peerLookupKey(peer)] = &peerSetItem{peer, index}
	ps.addrLookup[peer.RemoteAddr().String()] = &peerSetItem{peer, index}
	return nil
}

// Has returns true if the set contains the peer referred to by this
// peerKey, otherwise false.
func (ps *PeerSet) Has(peerKey ID) bool {
	ps.mtx.Lock()
	_, ok1 := ps.lookup[ps.lookupKey(peerKey, true)]
	_, ok2 := ps.lookup[ps.lookupKey(peerKey, false)]
	ps.mtx.Unlock()
	return ok1 || ok2
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

func (ps *PeerSet) GetInbound(peerKey ID) Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.lookupKey(peerKey, false)]
	if ok {
		return item.peer
	}
	return nil
}

func (ps *PeerSet) GetOutbound(peerKey ID) Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.lookupKey(peerKey, true)]
	if ok {
		return item.peer
	}
	return nil
}

// Get looks up a peer by the provided peerKey. Returns nil if peer is not
// found.
func (ps *PeerSet) Get(peerKey ID) Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.lookupKey(peerKey, true)]
	if ok {
		return item.peer
	}
	item, ok = ps.lookup[ps.lookupKey(peerKey, false)]
	if ok {
		return item.peer
	}
	return nil
}

func (ps *PeerSet) GetByAddr(addr net.Addr) Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.addrLookup[addr.String()]
	if ok {
		return item.peer
	}
	return nil
}

func (ps *PeerSet) lookupKey(id ID, outbound bool) string {
	return string(id) + strconv.FormatBool(outbound)
}

func (ps *PeerSet) peerLookupKey(p Peer) string {
	return ps.lookupKey(p.ID(), p.IsOutbound())
}

// Remove removes the peer from the PeerSet.
func (ps *PeerSet) Remove(peer Peer) bool {
	ok1 := ps.remove(peer.ID(), peer.IsOutbound())
	ok2 := ps.remove(peer.ID(), !peer.IsOutbound())
	if (!(ok1 || ok2)) || len(ps.list) == 0 {
		// Removing the peer has failed so we set a flag to mark that a removal was attempted.
		// This can happen when the peer add routine from the switch is running in
		// parallel to the receive routine of MConn.
		// There is an error within MConn but the switch has not actually added the peer to the peer set yet.
		// Setting this flag will prevent a peer from being added to a node's peer set afterwards.
		peer.SetRemovalFailed()
		return false
	}
	return ok1 || ok2
}

func (ps *PeerSet) RemoveByAddr(addr net.Addr) error {
	p := ps.GetByAddr(addr)
	if p == nil {
		return nil // Nothing to do
	}
	removed := ps.remove(p.ID(), p.IsOutbound())
	extraPeer := ps.Get(p.ID())
	if extraPeer != nil {
		return ErrHasExtraPeer{Peer: extraPeer}
	}
	if !removed {
		p.SetRemovalFailed()
		return ErrPeerRemoval{}
	}
	return nil
}

func (ps *PeerSet) remove(id ID, outbound bool) bool {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.lookupKey(id, outbound)]
	if !ok || len(ps.list) == 0 {
		return false
	}

	index := item.index

	// Remove from ps.lookup.
	delete(ps.lookup, ps.lookupKey(id, outbound))

	// If it's not the last item.
	if index != len(ps.list)-1 {
		// Swap it with the last item.
		lastPeer := ps.list[len(ps.list)-1]
		item := ps.lookup[ps.peerLookupKey(lastPeer)]
		item.index = index
		ps.list[index] = item.peer
	}

	// Remove the last item from ps.list.
	ps.list[len(ps.list)-1] = nil // nil the last entry of the slice to shorten, so it isn't reachable & can be GC'd.
	ps.list = ps.list[:len(ps.list)-1]

	return true
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
func (ps *PeerSet) Copy() []Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	c := make([]Peer, len(ps.list))
	copy(c, ps.list)
	return c
}

// ForEach iterates over the PeerSet and calls the given function for each peer.
func (ps *PeerSet) ForEach(fn func(peer Peer)) {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	for _, item := range ps.lookup {
		fn(item.peer)
	}
}

// Random returns a random peer from the PeerSet.
func (ps *PeerSet) Random() Peer {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	if len(ps.list) == 0 {
		return nil
	}

	return ps.list[cmtrand.Int()%len(ps.list)]
}
