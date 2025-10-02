package runtime_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeMessagePoolAddIncoming(t *testing.T) {
	testPool := runtime.NewMessageManager(cmtlog.NewNopLogger())
	require.NotNil(t, testPool)

	testAckEnvelope := makeAckTransactionBroadcast("test-hash-0", "test-node-id", "test-chain-0")
	testPool.AddIncoming(testAckEnvelope)

	// Test that we add to correct message stores.
	byPeer := testPool.IncomingByPeer("test-node-id")
	byType := testPool.IncomingByType("AckTransactionBroadcast")
	byChain := testPool.IncomingByChainID("test-chain-0")

	assert.Len(t, byPeer, 1)
	assert.Len(t, byType, 1)
	assert.Len(t, byChain, 1)

	// Test concurrent AddIncoming calls, must succeed
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			withChainID := "test-chain-1"
			withTxHash := "test-hash-1"
			if c%2 == 0 {
				withChainID = "test-chain-2"
				withTxHash = "test-hash-2"
			}

			testAckEnvelope := makeAckTransactionBroadcast(withTxHash, "test-node-id-2", withChainID)
			testPool.AddIncoming(testAckEnvelope)
		}(i + 1)
	}
	waitAll.Wait()

	byPeer = testPool.IncomingByPeer("test-node-id-2")
	byType = testPool.IncomingByType("AckTransactionBroadcast")
	byChain0 := testPool.IncomingByChainID("test-chain-0")
	byChain1 := testPool.IncomingByChainID("test-chain-1")
	byChain2 := testPool.IncomingByChainID("test-chain-2")

	assert.Len(t, byPeer, 100) // test-chain-1, -2
	assert.Len(t, byType, 101) // test-chain-0, -1, -2
	assert.Len(t, byChain0, 1)
	assert.Len(t, byChain1, 50)
	assert.Len(t, byChain2, 50)

	// Test Reset method for incoming messages, must be empty after.
	err := testPool.Reset()
	assert.NoError(t, err)

	byPeer = testPool.IncomingByPeer("test-node-id")
	byType = testPool.IncomingByType("AckTransactionBroadcast")
	byChain = testPool.IncomingByChainID("test-chain-0")

	assert.Len(t, byPeer, 0)
	assert.Len(t, byType, 0)
	assert.Len(t, byChain, 0)

	// Test other message type with ChainReplicationRequest
	testReplEnvelope := makeChainReplicationRequest("test-node-id", "test-chain-3")
	testPool.AddIncoming(testReplEnvelope)

	byPeer = testPool.IncomingByPeer("test-node-id")
	byType = testPool.IncomingByType("ChainReplicationRequest")
	byChain = testPool.IncomingByChainID("test-chain-3")

	assert.Len(t, byPeer, 1)
	assert.Len(t, byType, 1)
	assert.Len(t, byChain, 1)
}

func TestMultiplexRuntimeMessagePoolAddOutgoing(t *testing.T) {
	testPool := runtime.NewMessageManager(cmtlog.NewNopLogger())
	require.NotNil(t, testPool)

	testAckEnvelope := makeAckTransactionBroadcast("test-hash-0", "test-node-id", "test-chain-0")
	testPool.AddOutgoing(cmtp2p.ID("peer-0"), testAckEnvelope)

	// Test that we add to correct message stores.
	byPeer := testPool.OutgoingByPeer("peer-0")
	byType := testPool.OutgoingByType("AckTransactionBroadcast")
	byChain := testPool.OutgoingByChainID("test-chain-0")

	assert.Len(t, byPeer, 1)
	assert.Len(t, byType, 1)
	assert.Len(t, byChain, 1)

	// Act: concurrent AddOutgoing calls must succeed
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			withChainID := "test-chain-1"
			withTxHash := "test-hash-1"
			if c%2 == 0 {
				withChainID = "test-chain-2"
				withTxHash = "test-hash-2"
			}

			testAckEnvelope := makeAckTransactionBroadcast(withTxHash, "node-id", withChainID)
			testPool.AddOutgoing(cmtp2p.ID("peer-1"), testAckEnvelope)
		}(i + 1)
	}
	waitAll.Wait()

	byPeer = testPool.OutgoingByPeer("peer-1")
	byType = testPool.OutgoingByType("AckTransactionBroadcast")
	byChain0 := testPool.OutgoingByChainID("test-chain-0")
	byChain1 := testPool.OutgoingByChainID("test-chain-1")
	byChain2 := testPool.OutgoingByChainID("test-chain-2")

	assert.Len(t, byPeer, 100) // test-chain-1, -2
	assert.Len(t, byType, 101) // test-chain-0, -1, -2
	assert.Len(t, byChain0, 1)
	assert.Len(t, byChain1, 50)
	assert.Len(t, byChain2, 50)

	// Test Reset method for outgoing messages, must be empty after.
	err := testPool.Reset()
	assert.NoError(t, err)

	byPeer = testPool.OutgoingByPeer("test-node-id")
	byType = testPool.OutgoingByType("AckTransactionBroadcast")
	byChain = testPool.OutgoingByChainID("test-chain-0")

	assert.Len(t, byPeer, 0)
	assert.Len(t, byType, 0)
	assert.Len(t, byChain, 0)

	// Test other message type with ChainReplicationRequest
	testReplEnvelope := makeChainReplicationRequest("peer-3", "test-chain-3")
	testPool.AddOutgoing(cmtp2p.ID("peer-3"), testReplEnvelope)

	byPeer = testPool.OutgoingByPeer("peer-3")
	byType = testPool.OutgoingByType("ChainReplicationRequest")
	byChain = testPool.OutgoingByChainID("test-chain-3")

	assert.Len(t, byPeer, 1)
	assert.Len(t, byType, 1)
	assert.Len(t, byChain, 1)
}

// ----------------------------------------------------------------------------

func makeAckTransactionBroadcast(txHash, nodeId, chainId string) cmtp2p.Envelope {
	return cmtp2p.Envelope{
		Src:       cmtp2p.NewPeerWithoutConn(cmtp2p.ID(nodeId)),
		ChainID:   chainId,
		ChannelID: types.AckBroadcastChannel,
		Message: &mxp2p.Receipt{
			Sum: &mxp2p.Receipt_AckTransactionBroadcast{
				AckTransactionBroadcast: &mxp2p.AckTransactionBroadcast{
					TxHash:  []byte(txHash),
					NodeId:  nodeId,
					ChainID: chainId,
				},
			},
		},
	}
}

func makeChainReplicationRequest(nodeId, chainId string) cmtp2p.Envelope {
	return cmtp2p.Envelope{
		Src:       cmtp2p.NewPeerWithoutConn(cmtp2p.ID(nodeId)),
		ChainID:   chainId,
		ChannelID: types.ReplicationChannel,
		Message: &mxp2p.Message{
			Sum: &mxp2p.Message_ChainReplicationRequest{
				ChainReplicationRequest: &mxp2p.ChainReplicationRequest{
					ChainID:     chainId,
					ChainParams: &mxp2p.ChainParams{},
					Relays:      []string{},
				},
			},
		},
	}
}

func makeChainReplicationResponse(nodeId, chainId string) cmtp2p.Envelope {
	return cmtp2p.Envelope{
		Src:       cmtp2p.NewPeerWithoutConn(cmtp2p.ID(nodeId)),
		ChainID:   chainId,
		ChannelID: types.ReplicationChannel,
		Message: &mxp2p.Message{
			Sum: &mxp2p.Message_ChainReplicationResponse{
				ChainReplicationResponse: &mxp2p.ChainReplicationResponse{
					ChainID: chainId,
					NodeId:  nodeId,
				},
			},
		},
	}
}

func makeChainReplicationComplete(nodeId, chainId string) cmtp2p.Envelope {
	return cmtp2p.Envelope{
		Src:       cmtp2p.NewPeerWithoutConn(cmtp2p.ID(nodeId)),
		ChainID:   chainId,
		ChannelID: types.ReplicationChannel,
		Message: &mxp2p.Message{
			Sum: &mxp2p.Message_ChainReplicationComplete{
				ChainReplicationComplete: &mxp2p.ChainReplicationComplete{
					ChainID: chainId,
					NodeId:  nodeId,
				},
			},
		},
	}
}
