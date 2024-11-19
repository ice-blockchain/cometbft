package snapsapp

import (
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// GetValidatorUpdateExtension returns the active reporting extension
// for validator set updates.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom reporting unit, or extension.
func GetValidatorUpdateExtension() client.ValidatorUpdateExtensionFn {
	return client.DefaultValidatorUpdateExtension
}

// GetConsensusUpdateExtension returns the active reporting extension
// for consensus parameter updates.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom reporting unit, or extension.
func GetConsensusUpdateExtension() client.ConsensusUpdateExtensionFn {
	return client.DefaultConsensusUpdateExtension
}

// GetCheckTxExtension returns the active audit extension for transactions.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data auditing unit, or extension.
func GetCheckTxExtension() client.CheckTxExtensionFn {
	return client.DefaultCheckTxExtension
}

// GetPrepareProposalExtension returns the active data extension for blocks
// of transactions.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetPrepareProposalExtension() client.PrepareProposalExtensionFn {
	return client.DefaultPrepareProposalExtension
}

// GetProcessProposalExtension returns the active data extension for blocks
// of transactions.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetProcessProposalExtension() client.ProcessProposalExtensionFn {
	return client.DefaultProcessProposalExtension
}

// GetFinalizeBlockExtension returns the active data extension for finalized
// blocks transactions data.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom data processing unit, or extension.
func GetFinalizeBlockExtension() client.FinalizeBlockExtensionFn {
	return client.DefaultFinalizeBlockExtension
}

// GetCommitExtension returns the active audit extension for blocks.
//
// Note that you may change this method to activate a different extension,
// i.e. after implementing a custom committed blocks auditing unit.
func GetCommitExtension() client.CommitExtensionFn {
	return client.DefaultCommitExtension
}
