package p2p

import (
	"context"

	bc "github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmtmock "github.com/ice-blockchain/cometbft/p2p/mock"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/statesync"

	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// GetRuntimeChannels returns a slice with ChannelID values mapped by reactor
// name, required for relays to accept CometBFT runtime messages, e.g. consensus.
func GetRuntimeChannels() map[string][]byte {
	return map[string][]byte{
		"BLOCKSYNC": []byte{bc.BlocksyncChannel},
		"CONSENSUS": []byte{
			cs.StateChannel,
			cs.DataChannel,
			cs.VoteChannel,
			cs.VoteSetBitsChannel,
		},
		"MEMPOOL":   []byte{mempl.MempoolChannel},
		"EVIDENCE":  []byte{evidence.EvidenceChannel},
		"STATESYNC": []byte{statesync.SnapshotChannel, statesync.ChunkChannel},
		"PEX":       []byte{pex.PexChannel},

		"MULTIPLEX": []byte{
			// AckBroadcastChannel may be used to send AckTransactionBroadcast messages.
			types.AckBroadcastChannel,
			// RuntimeChannel may be used to send ChainReplicationComplete messages.
			types.RuntimeChannel,
		},
	}
}

// GetDiscoveryChannels returns a slice with ChannelID values mapped by reactor
// name, required for relays to accept discovery messages, e.g. ChainReplicationRequest.
func GetDiscoveryChannels() map[string][]byte {
	return map[string][]byte{
		"MULTIPLEX": []byte{types.ReplicationChannel},
	}
}

// GetChannelsIds returns a slice of ChannelID values.
func GetChannelIds(channels map[string][]byte) []byte {
	out := []byte{}
	for _, channelIds := range channels {
		out = append(out, channelIds...)
	}
	return out
}

// GetChannelDescriptors returns a map of channel descriptors by channel ID.
func GetChannelDescriptors() map[byte]*cmtp2p.ChannelDescriptor {
	reactor := cmtmock.NewReactor(context.Background())

	reactorImpls := map[string]cmtp2p.Reactor{
		"BLOCKSYNC": reactor.(*bc.Reactor),
		"CONSENSUS": reactor.(*cs.Reactor),
		"MEMPOOL":   reactor.(*mempl.Reactor),
		"EVIDENCE":  reactor.(*evidence.Reactor),
		"STATESYNC": reactor.(*statesync.Reactor),
		"PEX":       reactor.(*pex.Reactor),
		// TODO(midas): missing MULTIPLEX
	}

	chDescs := map[byte]*cmtp2p.ChannelDescriptor{}
	for _, impl := range reactorImpls {
		channelDescs := impl.GetChannels()
		for _, chDesc := range channelDescs {
			chDescs[chDesc.ID] = chDesc
		}
	}

	return chDescs
}
