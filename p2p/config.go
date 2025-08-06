package p2p

import (
	"github.com/cosmos/gogoproto/proto"

	"github.com/ice-blockchain/cometbft/p2p/conn"
)

// MultiplexReactorsMap maps a ChainID to a map of reactors by channel ID.
type MultiplexReactorsMap map[string]map[byte]Reactor

// MultiplexMessagesMap maps a ChainID to a map of message types by channel ID.
type MultiplexMessagesMap map[string]map[byte]proto.Message

// MultiplexChannelDesc maps a ChainID to a list of channel descriptors.
type MultiplexChannelDesc map[string][]*conn.ChannelDescriptor

// PeerConfig is used to bundle data we need to fully setup a Peer with an
// MConn, provided by the caller of Accept and Dial (currently the Switch). This
// a temporary measure until reactor setup is less dynamic and we introduce the
// concept of PeerBehaviour to communicate about significant Peer lifecycle
// events.
// TODO(xla): Refactor out with more static Reactor setup and PeerBehaviour.
// TODO(midas): does peerConfig need to be private? Maybe split into public/private parts.
type PeerConfig struct {
	dispatcher Dispatcher

	// isPersistent allows you to set a function, which, given socket address
	// (for outbound peers) OR self-reported address (for inbound peers), tells
	// if the peer is persistent or not.
	isPersistent func(*NetAddress) bool
	outbound     bool

	metrics     *Metrics
	onPeerError func(*PeerImpl, any)
}
