package p2p

import (
	"context"

	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/p2p/conn"
)

// Reactor is responsible for handling incoming messages on one or more
// Channel. Switch calls GetChannels when reactor is added to it. When a new
// peer joins our node, InitPeer and AddPeer are called. RemovePeer is called
// when the peer is stopped. Receive is called when a message is received on a
// channel associated with this reactor.
//
// Peer#Send or Peer#TrySend should be used to send the message to a peer.
type Reactor interface {
	service.Service // Start, Stop

	// SetSwitch allows setting a switch.
	SetSwitch(sw *Switch)

	// GetChannels returns the list of MConnection.ChannelDescriptor. Make sure
	// that each ID is unique across all the reactors added to the switch.
	GetChannels() []*conn.ChannelDescriptor

	// InitPeer is called by the switch before the peer is started. Use it to
	// initialize data for the peer (e.g. peer state).
	//
	// NOTE: The switch won't call AddPeer nor RemovePeer if it fails to start
	// the peer. Do not store any data associated with the peer in the reactor
	// itself unless you don't want to have a state, which is never cleaned up.
	InitPeer(peer *PeerImpl) *PeerImpl

	// AddPeer is called by the switch after the peer is added and successfully
	// started. Use it to start goroutines communicating with the peer.
	AddPeer(peer *PeerImpl)

	// RemovePeer is called by the switch when the peer is stopped (due to error
	// or other reason).
	RemovePeer(peer *PeerImpl, reason any)

	// Receive is called by the switch when an envelope is received from any connected
	// peer on any of the channels registered by the reactor
	Receive(e Envelope)
}

// --------------------------------------

type BaseReactor struct {
	service.BaseService // Provides Start, Stop, .Quit
	Switch              *Switch
}

func NewBaseReactor(ctx context.Context, name string, impl Reactor) *BaseReactor {
	return &BaseReactor{
		BaseService: *service.NewBaseService(ctx, nil, name, impl),
		Switch:      nil,
	}
}

func (br *BaseReactor) SetSwitch(sw *Switch) {
	br.Switch = sw
}
func (*BaseReactor) GetChannels() []*conn.ChannelDescriptor { return nil }
func (*BaseReactor) AddPeer(*PeerImpl)                      {}
func (*BaseReactor) RemovePeer(*PeerImpl, any)              {}
func (*BaseReactor) Receive(Envelope)                       {}
func (*BaseReactor) InitPeer(peer *PeerImpl) *PeerImpl      { return peer }
