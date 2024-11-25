package multiplex

import (
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// Assert that our implementations satisfy the Client interface.
var _ client.Client = (*ExtendedClient)(nil)

// NewExtendedClient creates an [ExtendedClient] instance based from cli.
// This method permits to extend a client implementation and use specific
// method overwrite instead of the default implementations.
func NewExtendedClient(cli client.Client) *ExtendedClient {
	return &ExtendedClient{
		SyncConfigExtension: cli.GetSyncConfigExtension(),
		SeedConfigExtension: cli.GetSeedConfigExtension(),

		ValidatorUpdateExtension: cli.GetValidatorUpdateExtension(),
		ConsensusUpdateExtension: cli.GetConsensusUpdateExtension(),
		CheckTxExtension:         cli.GetCheckTxExtension(),
		PrepareProposalExtension: cli.GetPrepareProposalExtension(),
		ProcessProposalExtension: cli.GetProcessProposalExtension(),
		FinalizeBlockExtension:   cli.GetFinalizeBlockExtension(),
		CommitExtension:          cli.GetCommitExtension(),

		CheckMutationResultExtension: cli.GetCheckMutationResultExtension(),
	}
}

// ----------------------------------------------------------------------------
// ExtendedClient

// ExtendedClient implements the [Client] interface using the provided client
// extension functions.
type ExtendedClient struct {
	// Configuration extensions
	SyncConfigExtension client.SyncConfigExtensionFn
	SeedConfigExtension client.SeedConfigExtensionFn

	// Consensus & Data extensions
	ValidatorUpdateExtension client.ValidatorUpdateExtensionFn
	ConsensusUpdateExtension client.ConsensusUpdateExtensionFn
	CheckTxExtension         client.CheckTxExtensionFn
	PrepareProposalExtension client.PrepareProposalExtensionFn
	ProcessProposalExtension client.ProcessProposalExtensionFn
	FinalizeBlockExtension   client.FinalizeBlockExtensionFn
	CommitExtension          client.CommitExtensionFn

	// Data consistency (audit) extensions
	CheckMutationResultExtension client.CheckMutationResultExtensionFn
}

// GetSyncConfigExtension returns the active configuration extension
// for state-sync. This method returns the SyncConfigExtension as set on the
// client instance and thus overwrites the default client implementation.
func (c ExtendedClient) GetSyncConfigExtension() client.SyncConfigExtensionFn {
	return c.SyncConfigExtension
}

// GetSeedConfigExtension returns the active configuration extension
// for seed nodes. This method returns the SeedConfigExtension as set on the
// client instance and thus overwrites the default client implementation.
func (c ExtendedClient) GetSeedConfigExtension() client.SeedConfigExtensionFn {
	return c.SeedConfigExtension
}

// GetValidatorUpdateExtension returns the active reporting extension
// for validator set updates. This method returns the ValidatorUpdateExtension
// as set on the instance and overwrites the default client implementation.
func (c ExtendedClient) GetValidatorUpdateExtension() client.ValidatorUpdateExtensionFn {
	return c.ValidatorUpdateExtension
}

// GetConsensusUpdateExtension returns the active reporting extension
// for consensus parameter updates. This method returns the ConsensusUpdateExtension
// as set on the instance and overwrites the default client implementation.
func (c ExtendedClient) GetConsensusUpdateExtension() client.ConsensusUpdateExtensionFn {
	return c.ConsensusUpdateExtension
}

// GetCheckTxExtension returns the active audit extension
// for transactions. This method returns the CheckTxExtension
// as set on the instance and overwrites the default client implementation.
func (c ExtendedClient) GetCheckTxExtension() client.CheckTxExtensionFn {
	return c.CheckTxExtension
}

// GetPrepareProposalExtension returns the active data extension
// for preparing block proposals with transactions. This method returns
// the PrepareProposalExtension as set on the instance and overwrites the default
// client implementation.
func (c ExtendedClient) GetPrepareProposalExtension() client.PrepareProposalExtensionFn {
	return c.PrepareProposalExtension
}

// GetProcessProposalExtension returns the default data extension
// for processing block proposals' transactions. This method returns
// the ProcessProposalExtension as set on the instance and overwrites the default
// client implementation.
func (c ExtendedClient) GetProcessProposalExtension() client.ProcessProposalExtensionFn {
	return c.ProcessProposalExtension
}

// GetFinalizeBlockExtension returns the default data extension
// for finalized blocks transactions data. This method returns
// the FinalizeBlockExtension as set on the instance and overwrites the default
// client implementation.
func (c ExtendedClient) GetFinalizeBlockExtension() client.FinalizeBlockExtensionFn {
	return c.FinalizeBlockExtension
}

// GetCommitExtension returns the default audit extension
// for committed blocks. This method returns the CommitExtension as set on
// the instance and overwrites the default client implementation.
func (c ExtendedClient) GetCommitExtension() client.CommitExtensionFn {
	return c.CommitExtension
}

// GetCheckMutationResultExtension returns the active mutation audit extension
// for state mutations. This method returns the CheckMutationResultExtension as set
// on the client instance and thus overwrites the default client implementation.
func (c ExtendedClient) GetCheckMutationResultExtension() client.CheckMutationResultExtensionFn {
	return c.CheckMutationResultExtension
}
