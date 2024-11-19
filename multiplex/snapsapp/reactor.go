package snapsapp

import (
	"github.com/ice-blockchain/cometbft/multiplex/snapshots"
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

	// GetStateStore should return a pointer to a [snapshots.StateSnapshotter].
	GetStateStore(chainID string) snapshots.StateSnapshotter
}
