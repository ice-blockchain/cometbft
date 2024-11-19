package types

import (
	"fmt"

	proto "github.com/cosmos/gogoproto/proto"

	abcitypes "github.com/ice-blockchain/cometbft/api/cometbft/abci/v1"
)

// ----------------------------------------------------------------------------
// Snapshot
//
// Snapshot defines a custom wrapper for snapshot metadata as used during the
// [statesync] process.
//
// This wrapper is necessary to dynamically type the Metadata field.

// SnapshotFromABCI converts an ABCI snapshot to a snapshot.
func SnapshotFromABCI(in *abcitypes.Snapshot) (Snapshot, error) {
	snapshot := Snapshot{
		Height: in.Height,
		Format: in.Format,
		Chunks: in.Chunks,
		Hash:   in.Hash,
	}
	err := proto.Unmarshal(in.Metadata, &snapshot.Metadata)
	if err != nil {
		return Snapshot{}, fmt.Errorf("failed to unmarshal snapshot metadata: %w", err)
	}
	return snapshot, nil
}

// ToABCI converts a Snapshot to its ABCI representation.
func (s Snapshot) ToABCI() (abcitypes.Snapshot, error) {
	out := abcitypes.Snapshot{
		Height: s.Height,
		Format: s.Format,
		Chunks: s.Chunks,
		Hash:   s.Hash,
	}
	var err error
	out.Metadata, err = proto.Marshal(&s.Metadata)
	if err != nil {
		return abcitypes.Snapshot{}, fmt.Errorf("failed to marshal snapshot metadata: %w", err)
	}
	return out, nil
}
