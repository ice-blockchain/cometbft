package client

import "context"

// AuditMutationResult defines a callback that returns an error if the
// audit of the mutated state fails.
//
// We provide an example [DefaultCheckMutationResultExtension] implementation for the
// extensionFn parameter which always returns nil (no errors).
//
// This method may be used to audit state instance mutations *before* they
// are replicated and saved to disk.
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
