package client

import "context"

// DelegateCheckTx defines a callback that returns nil or an error if
// verification fails for a single transaction.
//
// CAUTION: Expensive operations must not be run here but rather in the
// commitment stage(s) of the blocks proposal process.
//
// We provide an example [DefaultCheckTxExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [snapsapp.CheckTx], and may be used to delegate
// the verification of transactions to external processes. Returning an error
// should be considered equivalent to *invalidating a single transaction*.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultCheckTxExtension].
func DelegateCheckTx(
	chainID string,
	transactionBytes []byte,
	extensionFn CheckTxExtensionFn,
) error {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [CheckTxExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transaction data to a custom remote server
	// by implementing a custom CheckTxExtensionFn, an example is
	// available with [DefaultCheckTxExtension].
	return extensionFn(chainContext, transactionBytes)
}

// InjectPrepareProposal defines a callback that returns a pre-processed slice
// of transactions as they will be added to a block proposal.
//
// We provide an example [DefaultPrepareProposalExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.PrepareProposal] and may be used to
// pre-process or discard transactions before they are added to a proposal.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultPrepareProposalExtension].
func InjectPrepareProposal(
	chainID string,
	transactionsData [][]byte,
	extensionFn PrepareProposalExtensionFn,
) [][]byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [PrepareProposalExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transactions slice to a custom remote server
	// by implementing a custom PrepareProposalExtensionFn, an example is
	// available with [DefaultPrepareProposalExtension].
	nextTransactions := extensionFn(chainContext, transactionsData)
	return nextTransactions
}

// InjectProcessProposal defines a callback that returns a post-processed slice
// of transactions as they are added to a prepared block proposal.
//
// We provide an example [DefaultProcessProposalExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.ProcessProposal] and may be used to
// post-process or discard transactions as they are added to a proposal.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultProcessProposalExtension].
func InjectProcessProposal(
	chainID string,
	transactionsData [][]byte,
	extensionFn ProcessProposalExtensionFn,
) [][]byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [ProcessProposalExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transactions slice to a custom remote server
	// by implementing a custom ProcessProposalExtensionFn, an example is
	// available with [DefaultProcessProposalExtension].
	nextTransactions := extensionFn(chainContext, transactionsData)
	return nextTransactions
}

// InjectFinalizeBlock defines a callback that returns a post-processed slice
// of transactions as they will be added to a finalized block.
//
// We provide an example [DefaultFinalizeBlockExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.FinalizeBlock] and may be used to
// post-process transactions as they are added to a finalized block and events.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultFinalizeBlockExtension].
func InjectFinalizeBlock(
	chainID string,
	transactionsData [][]byte,
	extensionFn FinalizeBlockExtensionFn,
) [][]byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [FinalizeBlockExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the transactions slice to a custom remote server
	// by implementing a custom FinalizeBlockExtensionFn, an example is
	// available with [DefaultFinalizeBlockExtension].
	nextTransactions := extensionFn(chainContext, transactionsData)
	return nextTransactions
}

// ReportCommit defines a callback that returns nil or an error if
// reporting fails for a committed block.
//
// We provide an example [DefaultCommitExtension] implementation for the
// extensionFn parameter which only copies the transaction bytes.
//
// This method is called by [snapsapp.Commit] and may be used to
// post-process blocks as they are being committed.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultCommitExtension].
func ReportCommit(
	chainID string,
	chainHeight uint64,
	extensionFn CommitExtensionFn,
) error {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [CommitExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the block or height to a custom remote server
	// by implementing a custom CommitExtensionFn, an example is
	// available with [DefaultCommitExtension].
	return extensionFn(chainContext, chainHeight)
}
