package client

import (
	"context"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	v1 "github.com/ice-blockchain/cometbft/api/cometbft/types/v1"
	"github.com/ice-blockchain/cometbft/config"
)

// ContextKey defines a string-based context value key.
type ContextKey string

// We will attach a user address and ChainID inside the context.
const (
	KeyAddress ContextKey = "Address"
	KeyChainID ContextKey = "ChainID"
)

// ----------------------------------------------------------------------------
// Configuration

// SyncConfigExtensionFn provides an interface for state-sync config extensions
// that are used in [InjectSyncConfig] to delegate the retrieval of state-sync
// configuration objects to potential extensions.
//
// This method accepts a state-sync configuration [config.StateSyncConfig].
// Implementations should return the prevailing state-sync configuration object
// as a mutated [config.StateSyncConfig] object.
//
// This extension is executed by [mx.NewChainRegistry].
// See also: [DefaultSyncConfigExtension].
type SyncConfigExtensionFn func(
	context.Context,
	*config.StateSyncConfig,
) *config.StateSyncConfig

// SeedConfigExtensionFn provides an interface for seed nodes config extensions
// that are used in [InjectChainSeeds] to delegate the retrieval of seed nodes
// configuration strings to potential extensions.
//
// Note that the seed nodes configuration is a mere `string` which consist of
// a comma-separated list of seed nodes with the format: `id@host:port`.
//
// This method accepts a string with a comma-separated list of seed nodes.
// Implementations should return the prevailing seed nodes comma-separated list
// as a mutated string object.
//
// This extension is executed by [mx.NewChainRegistry].
// See also: [DefaultSeedConfigExtension].
type SeedConfigExtensionFn func(
	context.Context,
	string,
) string

// ----------------------------------------------------------------------------
// Consensus

// ValidatorUpdateExtensionFn provides an interface for validator set updates
// extensions that are used in [InjectValidatorUpdate] to delegate the
// processing of validator set updates, to potential extensions.
//
// This method accepts a *validator update set* as a `[]abci.ValidatorUpdate`.
// Implementations should return nil or an error if auditing fails.
//
// This extension is executed when blocks are finalized with a non-empty
// validator updates set in the ABCI [abci.FinalizeBlockResponse] object.
// See also: [DefaultValidatorUpdateExtension].
type ValidatorUpdateExtensionFn func(
	context.Context,
	[]abci.ValidatorUpdate,
) error

// ConsensusUpdateExtensionFn provides an interface for consensus params
// updates extensions that are used in [InjectConsensusUpdate] to delegate
// the processing of consensus parameter updates, to potential extensions.
//
// This method accepts a *consensus parameter set* as a `v1.ConsensusParams`.
// Implementations should return nil or an error if auditing fails.
//
// This extension is executed when blocks are finalized with a non-empty
// consensus parameter update in the ABCI [abci.FinalizeBlockResponse] object.
// See also: [DefaultConsensusUpdateExtension].
type ConsensusUpdateExtensionFn func(
	context.Context,
	*v1.ConsensusParams,
) error

// ----------------------------------------------------------------------------
// Snapshots

// SnapshotMutationExtensionFn provides an interface for state mutation extensions
// that are used in [InjectSnapshotMutation] to delegate the processing of state
// and the mutations of data, to potential extensions.
//
// This method accepts a *state instance* as a `[]byte` slice.
// Implementations should return the mutated state as a `[]byte` slice.
//
// This extension is executed when snapshots are taken.
// See also: [DefaultSnapshotMutationExtension].
type SnapshotMutationExtensionFn func(
	context.Context,
	[]byte,
) []byte

// CheckMutationResultExtensionFn provides an interface for auditing the result
// of state mutation extensions that are used in [InjectSnapshotMutation]. This
// extension notably permits to audit data such as ensure data consistency.
//
// This method accepts a *state instance* as a `[]byte` slice.
// Implementations should return an error if the audit of data fails.
//
// This extension is executed when snapshots are taken.
// See also: [DefaultCheckMutationResuultExtension].
type CheckMutationResultExtensionFn func(
	context.Context,
	[]byte,
) error

// SnapshotRestoreExtensionFn provides an interface for snapshot restoration
// extensions that are used in [InjectSnapshotRestore] to delegate the
// processing of mutated snapshots, to potential extensions.
//
// This method accepts a *snapshot instance* as a `[]byte` slice.
// Implementations should return the mutated snapshot as a `[]byte` slice.
//
// This extension is executed when snapshots are restored.
// See also: [DefaultSnapshotRestoreExtension].
type SnapshotRestoreExtensionFn func(
	context.Context,
	[]byte,
) []byte

// ----------------------------------------------------------------------------
// Transactions / Blocks

// CheckTxExtensionFn provides an interface for transaction checks extensions
// that are used in [InjectCheckTx] to delegate the validation of transactions
// to potential extensions.
//
// CAUTION: Expensive operations must not be run here but rather in the
// commitment stage(s) of the blocks proposal process.
//
// This method accepts a *raw transaction* as a `[]byte` slice.
// Implementations should return nil or an error if a transaction is invalid.
//
// This extension may be executed by any of PrepareProposal, ProcessProposal
// or FinalizeBlock methods, and should not execute expensive operations.
// See also: [DefaultCheckTxExtension].
type CheckTxExtensionFn func(
	context.Context,
	[]byte,
) error

// PrepareProposalExtensionFn provides an interface for transactions mutation
// extensions that are used in [InjectPrepareProposal] to delegate the
// pre-processing of transactions data, to potential extensions.
//
// Note, transactions *may* be discarded by this handler so that they are
// not included in the next block of the network.
//
// This method accepts a *transactions slice* as a `[][]byte` slice.
// Implementations should return the mutated slice as a `[][]byte` slice.
//
// This extension is executed as the **1st** stage in proposing blocks.
// See also: [DefaultPrepareProposalExtension].
type PrepareProposalExtensionFn func(
	context.Context,
	[][]byte,
) [][]byte

// ProcessProposalExtensionFn provides an interface for transactions mutation
// extensions that are used in [InjectProcessProposal] to delegate the
// post-processing of transactions data, to potential extensions.
//
// This method accepts a *transactions slice* as a `[][]byte` slice.
// Implementations should return the mutated slice as a `[][]byte` slice.
//
// This extension is executed as the **2nd** stage in proposing blocks.
// See also: [DefaultProcessProposalExtension].
type ProcessProposalExtensionFn func(
	context.Context,
	[][]byte,
) [][]byte

// FinalizeBlockExtensionFn provides an interface for blocks mutation extensions
// that are used in [InjectFinalizeBlock] to delegate the post-processing of
// state transactions data, to potential extensions.
//
// Note that transactions *may* be discarded or updated by this handler so that
// the triggered [abcitypes.Event] contains a *mutated* slice of transactions
// before finalizing the block - and thus use the mutated data.
//
// This method accepts a *transactions slice* as a `[][]byte` slice.
// Implementations should return the mutated slice as a `[][]byte` slice.
//
// This extension is executed as the **3rd** stage in proposing blocks.
// See also: [DefaultFinalizeBlockExtension].
type FinalizeBlockExtensionFn func(
	context.Context,
	[][]byte,
) [][]byte

// CommitExtensionFn provides an interface for blocks auditing extensions
// that are used in [InjectCommit] to delegate the post-processing of
// finalized blocks, to potential extensions.
//
// This method accepts a *committed block height* as a `uint64`.
// Implementations should return nil or an error if a block is invalid.
//
// This extension is executed as the **4th** stage in proposing blocks.
// See also: [DefaultCommitExtension].
type CommitExtensionFn func(
	context.Context,
	uint64,
) error
