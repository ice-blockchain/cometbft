package p2p

import (
	"net"

	"github.com/cosmos/gogoproto/proto"

	tmp2p "github.com/ice-blockchain/cometbft/api/cometbft/p2p/v1"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/ice-blockchain/cometbft/types"
)

type (
	ChannelDescriptor = conn.ChannelDescriptor
	ConnectionStatus  = conn.ConnectionStatus
)

// Envelope contains a message with sender routing info.
type Envelope struct {
	Src       *PeerImpl     // sender (empty if outbound)
	Message   proto.Message // message payload
	ChannelID byte
	ChainID   string
}

var (
	_ types.Wrapper = &tmp2p.PexRequest{}
	_ types.Wrapper = &tmp2p.PexAddrs{}
)

// Dispatcher defines the contract for a [tmp2p.PacketMsg] dispatcher.
type Dispatcher interface {
	// Target returns the target reactor to process packet.
	Target(packet tmp2p.PacketMsg) Reactor
	// Dispatch forwards the packet to the target reactor.
	Dispatch(sourcePeer *PeerImpl, packet tmp2p.PacketMsg) error
}

// Connector defines the contract for peer connectors.
type Connector interface {
	// Dial dials addr or returns an error.
	Dial(addr *NetAddress) (*PeerImpl, error)
	// Listen listens for peer connections.
	Listen() error

	// Read reads a packet from peerID and returns its contents in bytes.
	Read(peerID ID, packet tmp2p.PacketMsg) (Envelope, error)

	// Send sends a packet to peerID.
	Send(e Envelope) error
	// TrySend tries to send a packet to peerID (no failure).
	TrySend(e Envelope) error
	// Broadcast sends a message to all peers.
	Broadcast(e Envelope) error
	// TryBroadcast sends a message to all peers.
	TryBroadcast(e Envelope) error
}

// Pool defines the contract for a connection pool.
type Pool interface {
	// Transport returns the packet transporter.
	Transport() Transport
	// Connector returns a peer connector.
	Connector() Connector
	// Dispatcher returns a packet dispatcher.
	Dispatcher() Dispatcher

	// NumPeers returns the number of inbound and outbound peers.
	NumPeers() (inbound, outbound, dialing int)
	// Peers returns the PeerSet to which we broadcast.
	Peers() *PeerSet
	// AddPeer registers a new peer in the peerset.
	AddPeer(peer *PeerImpl) error
	// RemovePeer removes a peer from the peerset.
	RemovePeer(peerID ID) error
	// HasPeer returns true if peer is in the PeerSet.
	HasPeer(peer *PeerImpl) bool
	// HasPeerID returns true if id is in the PeerSet.
	HasPeerID(id ID) bool
	// HasPeerIP returns true if ip is in the PeerSet.
	HasPeerIP(ip net.IP) bool
}
