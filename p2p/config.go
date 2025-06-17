package p2p

import (
	"github.com/cosmos/gogoproto/proto"

	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
	"github.com/ice-blockchain/cometbft/p2p/conn"
)

// PublicPeerConfig describes a wrapper around peer configuration structs.
type PublicPeerConfig struct {
	peerConfig
}

// GetConfig returns the internal peerConfig configuration struct.
func (c PublicPeerConfig) GetConfig() peerConfig {
	return c.peerConfig
}

// MultiplexReactorsMap maps a ChainID to a map of reactors by channel ID.
type MultiplexReactorsMap map[string]map[byte]Reactor

// MultiplexMessagesMap maps a ChainID to a map of message types by channel ID.
type MultiplexMessagesMap map[string]map[byte]proto.Message

// MultiplexChannelDesc maps a ChainID to a list of channel descriptors.
type MultiplexChannelDesc map[string][]*conn.ChannelDescriptor

// peerConfig is used to bundle data we need to fully setup a Peer with an
// MConn, provided by the caller of Accept and Dial (currently the Switch). This
// a temporary measure until reactor setup is less dynamic and we introduce the
// concept of PeerBehaviour to communicate about significant Peer lifecycle
// events.
// TODO(xla): Refactor out with more static Reactor setup and PeerBehaviour.
// TODO(midas): does peerConfig need to be private? Maybe split into public/private parts.
type peerConfig struct {
	mtx cmtsync.Mutex

	reactorsByCh  MultiplexReactorsMap
	msgTypeByChID MultiplexMessagesMap
	chDescs       MultiplexChannelDesc

	// isPersistent allows you to set a function, which, given socket address
	// (for outbound peers) OR self-reported address (for inbound peers), tells
	// if the peer is persistent or not.
	isPersistent func(*NetAddress) bool
	outbound     bool

	metrics     *Metrics
	onPeerError func(*PeerImpl, any)
}

// GetReactors returns the reactors map.
func (c peerConfig) GetReactors() MultiplexReactorsMap {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.reactorsByCh
}

// GetMessages returns the message types map.
func (c peerConfig) GetMessages() MultiplexMessagesMap {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.msgTypeByChID
}

// GetChannels returns the channel descriptors map.
func (c peerConfig) GetChannels() MultiplexChannelDesc {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	return c.chDescs
}

// AddReactor registers a new reactor by ChainID and channelID.
func (c peerConfig) AddReactor(chainID string, channelID byte, reactor Reactor) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.reactorsByCh[chainID][channelID] = reactor
}

// AddMessage registers a new message type by ChainID and channelID.
func (c peerConfig) AddMessage(chainID string, channelID byte, msg proto.Message) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	c.msgTypeByChID[chainID][channelID] = msg
}

// AddChannels registers a new channel descriptor for ChainID.
func (c peerConfig) AddChannels(chainID string, chDescs []*conn.ChannelDescriptor) {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if _, ok := c.chDescs[chainID]; !ok {
		c.chDescs[chainID] = make([]*conn.ChannelDescriptor, 0, len(chDescs))
	}

	c.chDescs[chainID] = append(c.chDescs[chainID], chDescs...)
}
