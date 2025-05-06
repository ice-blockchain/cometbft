package snapsapp

import (
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/multiplex/server"
)

// Reactor defines the implementation contract for the multiplex reactor
// that is used to retrieve `stateStore` instances and networks.
type Reactor interface {
	// HasNetwork should return true if a ChainID is known to a node.
	HasNetwork(chainID string) bool

	// GetNetworks should return a slice of ChainID values known to a node.
	GetNetworks() []string

	// GetStoragePaths should return storage paths mapped by ChainID.
	GetStoragePaths() map[string]string

	// GetStateStore should return a pointer to a [sm.Store].
	GetStateStore(chainID string) sm.Store

	// GetReplayPool returns a [server.ReplayPool] which contains transactions
	// batches to be replayed. These batches may contain one or many txes
	// that will be forwarded to [Acceptor#ReplayBroadcastTxBatch].
	GetReplayPool() *server.ReplayPool

	// Quit returns a channel, which is closed once service is stopped.
	Quit() <-chan struct{}
}
