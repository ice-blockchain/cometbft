package snapsapp

import (
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/multiplex/replay"
	cmttypes "github.com/ice-blockchain/cometbft/types"
)

// TxAcceptor defines the implementation contract for the mempool
// transaction verification done with [Mempool#CheckTx].
type TxAcceptor interface {
	// Returns true if CheckTx was called for tx.
	TxAccepted(tx cmttypes.Tx) bool
}

// Backend defines the implementation contract for the multiplex
// runtime manager that is used to retrieve state stores and mempool.
type Backend interface {
	// HasNetwork should return true if a ChainID is known to a node.
	HasNetwork(chainID string) bool
	// GetNetworks should return a slice of ChainID values known to a node.
	GetNetworks() []string

	// StateStore should return the state store for ChainID.
	StateStore(chainID string) sm.Store
	// Mempool should return the mempool for ChainID.
	Mempool(chainID string) TxAcceptor

	// ReplayPool returns a [replay.ReplayPool] which contains transactions
	// batches to be replayed. These batches may contain one or many txes
	// that will be forwarded to [Acceptor#ReplayBroadcastTxBatch].
	ReplayPool() *replay.ReplayPool

	// Quit returns a channel, which is closed once service is stopped.
	Quit() <-chan struct{}
}
