package client

import (
	"context"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	v1 "github.com/ice-blockchain/cometbft/api/cometbft/types/v1"
)

// ReportValidatorUpdate defines a callback that returns nil or an error if
// processing fails for a validators update set.
//
// We provide an example [DefaultValidatorUpdateExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [snapsapp.InitChain] and [snapsapp.FinalizeBlock]
// and may be used to post-process validator updates as they are added to a
// recently initialized chain or a finalized block.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultValidatorUpdateExtension].
func ReportValidatorUpdate(
	chainID string,
	validatorUpdates []abci.ValidatorUpdate,
	extensionFn ValidatorUpdateExtensionFn,
) error {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [ValidatorUpdateExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the  validator set updates to a custom remote server
	// by implementing a custom ValidatorUpdateExtensionFn, an example is
	// available with [DefaultValidatorUpdateExtension].
	return extensionFn(chainContext, validatorUpdates)
}

// ReportConsensusUpdate defines a callback that returns nil or an error if
// processing fails for a consensus parameters update set.
//
// We provide an example [DefaultConsensusUpdateExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [snapsapp.InitChain] and [snapsapp.FinalizeBlock]
// and may be used to post-process consensus updates as they are added to a
// recently initialized chain or a finalized block.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultConsensusUpdateExtension].
func ReportConsensusUpdate(
	chainID string,
	consensusParams *v1.ConsensusParams,
	extensionFn ConsensusUpdateExtensionFn,
) error {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [ValidatorUpdateExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the consensus parameters to a custom remote server
	// by implementing a custom ConsensusUpdateExtensionFn, an example is
	// available with [DefaultConsensusUpdateExtension].
	return extensionFn(chainContext, consensusParams)
}
