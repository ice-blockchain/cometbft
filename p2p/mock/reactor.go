package mock

import (
	"github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
)

type Reactor struct {
	p2p.BaseReactor

	Channels []*conn.ChannelDescriptor
}

func NewReactor() *Reactor {
	r := &Reactor{}
	r.BaseReactor = *p2p.NewBaseReactor("Mock-PEX", r)
	r.SetLogger(log.TestingLogger())
	return r
}

func (r *Reactor) GetChannels() []*conn.ChannelDescriptor { return r.Channels }
func (*Reactor) AddPeer(_ *p2p.PeerImpl)                  {}
func (*Reactor) RemovePeer(_ *p2p.PeerImpl, _ any)        {}
func (*Reactor) Receive(_ p2p.Envelope)                   {}
