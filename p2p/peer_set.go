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
	// HasIP returns true if the set contains the peer referred to by this IP
	HasIP(ip net.IP) bool
	// Get returns the outbound peer with the given key, or nil if not found.
	GetOutbound(key ID) *PeerImpl
	// Get returns the inbound peer with the given key, or nil if not found.
	GetInbound(key ID) *PeerImpl
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
	lookup map[string]*peerSetItem
	list   []*PeerImpl
}

type peerSetItem struct {
	peer  *PeerImpl
	index int
}

// NewPeerSet creates a new peerSet with a list of initial capacity of 256 items.
func NewPeerSet() *PeerSet {
	return &PeerSet{
		lookup: make(map[string]*peerSetItem),
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
	if peer.GetRemovalFailed() {
		return ErrPeerRemoval{}
	}

	index := len(ps.list)
	// Appending is safe even with other goroutines
	// iterating over the ps.list slice.
	ps.list = append(ps.list, peer)
	ps.lookup[ps.KeyForPeer(peer)] = &peerSetItem{peer, index}
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

// Has returns true if the set contains the peer referred to by this
// peerID, otherwise false.
func (ps *PeerSet) Has(peerID ID) bool {
	return ps.HasInbound(peerID) || ps.HasOutbound(peerID)
}

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

// GetInbound returns an inbound Peer with ID peerKey or nil.
func (ps *PeerSet) GetInbound(peerID ID) *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.KeyForInbound(peerID)]
	if ok {
		return item.peer
	}
	return nil
}

// GetOutbound returns an outbound Peer with ID peerKey or nil.
func (ps *PeerSet) GetOutbound(peerID ID) *PeerImpl {
	ps.mtx.Lock()
	defer ps.mtx.Unlock()

	item, ok := ps.lookup[ps.KeyForOutbound(peerID)]
	if ok {
		return item.peer
	}
	return nil
}

// GetInOrOut returns an inbound OR outbound Peer with ID peerKey,
// or nil. If outboundFirst is true, we check for outbound first, otherwise
// inbound first.
func (ps *PeerSet) GetInOrOut(peerID ID, outboundFirst ...bool) *PeerImpl {
	isOutboundFirst := true
	if len(outboundFirst) > 0 && !outboundFirst[0] {
		isOutboundFirst = false
	}

	var peerExists *PeerImpl
	if isOutboundFirst {
		peerExists = ps.GetOutbound(peerID)
	} else {
		peerExists = ps.GetInbound(peerID)
	}

	if peerExists != nil {
		return peerExists
	}

	if isOutboundFirst {
		peerExists = ps.GetInbound(peerID)
	} else {
		peerExists = ps.GetOutbound(peerID)
	}

	return peerExists
}

// Remove removes the peer from the PeerSet.
func (ps *PeerSet) Remove(peer Peer) bool {
	ps.mtx.Lock()
	if len(ps.list) == 0 {
		peer.SetRemovalFailed()
		ps.mtx.Unlock()
		return false
	}
	ps.mtx.Unlock()

	ok1 := ps.remove(peer.ID(), peer.IsOutbound())
	ok2 := ps.remove(peer.ID(), !peer.IsOutbound())
	if !(ok1 || ok2) {
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

func (ps *PeerSet) lookupKey(id ID, outbound bool) string {
	return string(id) + strconv.FormatBool(outbound)
}

func (ps *PeerSet) peerLookupKey(p *PeerImpl) string {
	return ps.lookupKey(p.ID(), p.IsOutbound())
}
