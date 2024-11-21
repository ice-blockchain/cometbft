package client

import (
	"context"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	v1 "github.com/ice-blockchain/cometbft/api/cometbft/types/v1"
	"github.com/ice-blockchain/cometbft/config"
)

// ----------------------------------------------------------------------------
// Extensions
//
// We hereby provide *example* implementations for the configuration extensions
// that may be used to *inject configuration* from the client of this library.

// Type-assertions ensure the compatibility of this implementation with the
// multiplex client contract defined in this package.
var (
	_ SyncConfigExtensionFn       = DefaultSyncConfigExtension
	_ SeedConfigExtensionFn       = DefaultSeedConfigExtension
	_ ValidatorUpdateExtensionFn  = DefaultValidatorUpdateExtension
	_ ConsensusUpdateExtensionFn  = DefaultConsensusUpdateExtension
	_ SnapshotMutationExtensionFn = DefaultSnapshotMutationExtension
	_ SnapshotRestoreExtensionFn  = DefaultSnapshotRestoreExtension
	_ CheckTxExtensionFn          = DefaultCheckTxExtension
	_ PrepareProposalExtensionFn  = DefaultPrepareProposalExtension
	_ ProcessProposalExtensionFn  = DefaultProcessProposalExtension
	_ FinalizeBlockExtensionFn    = DefaultFinalizeBlockExtension
	_ CommitExtensionFn           = DefaultCommitExtension
)

// ----------------------------------------------------------------------------
// Configuration

// DefaultSyncConfigExtension is an example implementation for the state-sync
// config extension [SyncConfigExtensionFn]. A state-sync config extension may
// be used to *overwrite* state-sync configuration for a particular network.
//
// Fields that may be provided from an external process may include:
// - `StateSyncConfig.TrustPeriod`
// - `StateSyncConfig.TrustHeight`
// - `StateSyncConfig.TrustHash`
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultSyncConfigExtension = func(
	_ context.Context, // ctx
	baseSyncConf *config.StateSyncConfig,
) *config.StateSyncConfig {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// Deep-copy the StateSyncConfig object
	nextStateSyncConfig := &config.StateSyncConfig{
		Enable:              baseSyncConf.Enable,              // bool
		TempDir:             baseSyncConf.TempDir,             // string
		RPCServers:          baseSyncConf.RPCServers,          // []string
		TrustPeriod:         baseSyncConf.TrustPeriod,         // time.Duration
		TrustHeight:         baseSyncConf.TrustHeight,         // int64
		TrustHash:           baseSyncConf.TrustHash,           // string
		DiscoveryTime:       baseSyncConf.DiscoveryTime,       // time.Duration
		ChunkRequestTimeout: baseSyncConf.ChunkRequestTimeout, // time.Duration
		ChunkFetchers:       baseSyncConf.ChunkFetchers,       // int32
	}

	return nextStateSyncConfig
}

// DefaultSeedConfigExtension is an example implementation for the seed nodes
// extension [SeedConfigExtensionFn]. A seed nodes config extension may
// be used to *overwrite* seed nodes configuration for a particular network.
//
// i.e. An extension may be implemented to retrieve seed nodes from a custom
// remote server, or to mutate the available baseSeeds from config.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultSeedConfigExtension = func(
	_ context.Context, // ctx
	baseSeeds string,
) string {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextSeeds := baseSeeds
	return nextSeeds
}

// ----------------------------------------------------------------------------
// Consensus

// DefaultValidatorUpdateExtension is an example implementation for the
// validator updates extension [ValidatorUpdateExtensionFn]. A custom auditing
// and/or reporting unit may be used to evaluate the validator updates set.
//
// i.e. An extension may be implemented to report validator set updates to
// a remove server, or to audit the validator updates.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultValidatorUpdateExtension = func(
	_ context.Context, // ctx
	validatorUpdates []abci.ValidatorUpdate,
) error {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// e.g. You may audit the validatorUpdates or report to a remote server.

	return nil
}

// DefaultConsensusUpdateExtension is an example implementation for the
// consensus updates extension [ConsensusUpdateExtensionFn]. A custom auditing
// and/or reporting unit may be used to evaluate the consensus parameters.
//
// i.e. An extension may be implemented to report consensus parameters to
// a remove server, or to audit the parameter updates.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultConsensusUpdateExtension = func(
	_ context.Context, // ctx
	consensusParams *v1.ConsensusParams,
) error {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// e.g. You may audit the consensusParams or report to a remote server.

	return nil
}

// ----------------------------------------------------------------------------
// Snapshots

// DefaultSnapshotMutationExtension is an example implementation for the state
// mutation extension [SnapshotMutationExtensionFn]. A state mutation extension
// may be used to *process* or *mutate* state raw bytes for a particular network.
//
// i.e. An extension may be implemented to store a full state machine's bytes
// representation in a separate database instance.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultSnapshotMutationExtension = func(
	_ context.Context, // ctx
	baseState []byte,
) []byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextState := baseState
	return nextState
}

// DefaultCheckMutationResultExtension is an example implementation for the
// mutation audit extension [CheckMutationResultExtensionFn]. A mutation audit
// extension may be used to *verify* state mutation results (data consistency).
//
// i.e. An extension may be implemented to audit the mutated state machine
// bytes representation by verifying the latest block hash attached.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultCheckMutationResultExtension = func(
	_ context.Context, // ctx
	mutatedState []byte,
) error {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	return nil
}

// DefaultSnapshotRestoreExtension is an example implementation for the state
// restoration extension [SnapshotRestoreExtensionFn]. A state restoration
// extension may be used to *process* restored state for a particular network.
//
// i.e. An extension may be implemented to store the restored state machine
// bytes representation in a separate database instance.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultSnapshotRestoreExtension = func(
	_ context.Context, // ctx
	baseState []byte,
) []byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextState := baseState
	return nextState
}

// ----------------------------------------------------------------------------
// Transactions / Blocks

// DefaultCheckTxExtension is an example implementation for the transactions
// auditing extension [CheckTxExtensionFn]. A custom auditing and/or reporting
// unit may be used to evaluate the transaction.
//
// CAUTION: Expensive operations must not be run here but rather in the
// commitment stage(s) of the blocks proposal process.
//
// i.e. An extension may be implemented to report transaction bytes to
// a remove server, or to audit the transaction before it is added.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultCheckTxExtension = func(
	_ context.Context, // ctx
	transactionBytes []byte,
) error {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// e.g. You may audit the transactionBytes.
	// CAUTION: Expensive operations must not be run here.

	return nil
}

// DefaultPrepareProposalExtension is an example implementation for the
// transactions mutation extension [PrepareProposalExtensionFn]. A transactions
// mutation extension may be used to *pre-process* or *mutate* transactions raw
// bytes before they are added to a proposal for a particular network.
//
// i.e. An extension may be implemented to store transaction data bytes
// representation in a separate database instance.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extension callback, this example does not make use of it.
var DefaultPrepareProposalExtension = func(
	_ context.Context, // ctx
	baseTransactions [][]byte,
) [][]byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextTransactions := baseTransactions
	return nextTransactions
}

// DefaultProcessProposalExtension is an example implementation for the
// transactions mutation extension [ProcessProposalExtensionFn]. A transactions
// mutation extension may be used to *post-process* or *mutate* transactions
// raw bytes as they are added to a proposal for a particular network.
//
// i.e. An extension may be implemented to store transaction data bytes
// representation in a separate database instance.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extension callback, this example does not make use of it.
var DefaultProcessProposalExtension = func(
	_ context.Context, // ctx
	baseTransactions [][]byte,
) [][]byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextTransactions := baseTransactions
	return nextTransactions
}

// DefaultFinalizeBlockExtension is an example implementation for the
// transactions mutation extension [FinalizeBlockExtensionFn]. A transactions
// mutation extension may be used to *post-process* or *mutate* transactions
// raw bytes as they are added to a finalize block for a particular network.
//
// i.e. An extension may be implemented to store transaction data bytes
// representation in a separate database instance.
//
// Note that we inject `ChainID` in the Context before calling the proposed
// extension callback, this example does not make use of it.
var DefaultFinalizeBlockExtension = func(
	_ context.Context, // ctx
	baseTransactions [][]byte,
) [][]byte {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	nextTransactions := baseTransactions
	return nextTransactions
}

// DefaultCommitExtension is an example implementation for the committed blocks
// auditing extension [CommitExtensionFn]. A custom auditing and/or reporting
// unit may be used to evaluate the committed block height.
//
// i.e. An extension may be implemented to report confirmed block heights to
// a remove server, or to audit the committed block.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extension callback, this example does not make use of these.
var DefaultCommitExtension = func(
	_ context.Context, // ctx
	blockHeight uint64,
) error {
	// e.g. You may interpret/use the UserAddress and ChainID in extensions.
	//
	// userAddress := ctx.Value("Address").(string)
	// chainId := ctx.Value("ChainID").(string)

	// e.g. You may audit the blockHeight or report to a remote server.

	return nil
}
