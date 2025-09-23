package helpers

import "fmt"

// Type-assertions to validate custom structs satisfy error interface.
var _ error = (*ErrBroadcastCancelled)(nil)
var _ error = (*ErrMissingGenesisDocSet)(nil)

// ErrSetupSnapsapp indicates that [snapsapp.Snapsapp] failed during setup.
type ErrSetupSnapsapp error

// ErrSetupRuntime indicates that [runtime.Registry] failed during setup.
type ErrSetupRuntime error

// ErrSetupDiscovery indicates that [cmtp2p.Switch] failed during setup.
type ErrSetupDiscovery error

// ErrSetupCometBFT indicates that [cmtp2p.Switch] failed during setup.
type ErrSetupCometBFT error

// ErrBroadcastCancelled indicates that the broadcast of TxHash got cancelled.
type ErrBroadcastCancelled struct {
	TxHash string
}

func (e ErrBroadcastCancelled) Error() string {
	return fmt.Sprintf("broadcast cancelled - other relays failed to accept transaction %s", e.TxHash)
}

// ErrMissingGenesisDocSet indicates that a genesis file could not be read.
type ErrMissingGenesisDocSet struct {
	Path string
}

func (e ErrMissingGenesisDocSet) Error() string {
	return fmt.Sprintf("couldn't read GenesisDocSet file: %s", e.Path)
}

// ErrEmptyGenesisDocSet indicates that the [GenesisDocSet] is empty.
type ErrEmptyGenesisDocSet error
