package p2p

import (
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
