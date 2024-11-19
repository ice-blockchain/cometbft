package client

import "context"

// InjectSnapshotMutation defines a callback that returns a processed/mutated state
// machine bytes representation (raw data) as they are snapshotted.
//
// We provide an example [DefaultSnapshotMutationExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [multiplex.ChainHistoryStore] and may be used to
// mutate state instances *before* they are snapshotted and saved to disk.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultSnapshotMutationExtension].
func InjectSnapshotMutation(
	chainID string,
	stateBytes []byte,
	extensionFn SnapshotMutationExtensionFn,
) []byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [SnapshotMutationExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the content of the state machine to a custom remote server
	// by implementing a custom SnapshotMutationExtensionFn, an example is
	// available with [DefaultSnapshotMutationExtension].
	nextStateBytes := extensionFn(chainContext, stateBytes)
	return nextStateBytes
}

// AuditMutationResult defines a callback that returns an error if the
// audit of the mutated state fails.
//
// We provide an example [DefaultCheckMutationResultExtension] implementation for the
// extensionFn parameter which always returns nil (no errors).
//
// This method is called by [multiplex.ChainHistoryStore] and may be used to
// audit state instance mutations *before* they are snapshotted and saved to disk.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultCheckMutationResultExtension].
func AuditMutationResult(
	chainID string,
	mutatedBytes []byte,
	extensionFn CheckMutationResultExtensionFn,
) error {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [CheckMutationResultExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Auditing the mutated state bytes from a custom remote server
	// by implementing a custom CheckMutationResultExtensionFn, an example is
	// available with [DefaultCheckMutationResultExtension].
	return extensionFn(chainContext, mutatedBytes)
}

// InjectSnapshotRestore defines a callback that returns a restorable state
// machine bytes representation (raw data).
//
// We provide an example [DefaultSnapshotRestoreExtension] implementation for the
// extensionFn parameter which only copies the state machine instance.
//
// This method is called by [multiplex.ChainHistoryStore] and may be used to
// mutate state instances *before* they are restored to the state machine.
//
// Note that we inject `Address` and `ChainID` in the Context before calling
// the proposed extensionFn callback, you can use these in your extension as
// documented with [DefaultSnapshotRestoreExtension].
func InjectSnapshotRestore(
	chainID string,
	stateBytes []byte,
	extensionFn SnapshotRestoreExtensionFn,
) []byte {
	// Injects Address and ChainID to the context in case it is
	// necessary inside the [SnapshotRestoreExtensionFn] extension.
	userAddress := extractAddressFromChainID(chainID)
	chainContext := context.WithValue(context.TODO(), KeyAddress, userAddress)
	chainContext = context.WithValue(chainContext, KeyChainID, chainID)

	// CALLBACK: You may add custom per-user-chain source code here.
	//
	// e.g.: Sending the content of the state machine to a custom remote server
	// by implementing a custom SnapshotRestoreExtensionFn, an example is
	// available with [DefaultSnapshotRestoreExtension].
	nextStateBytes := extensionFn(chainContext, stateBytes)
	return nextStateBytes
}
