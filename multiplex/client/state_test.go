package client_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func TestMultiplexClientInjectSnapshotMutation(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()
	baseStateBytes := []byte(`this is just an example, not a sm.State.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectSnapshotMutation []byte //nolint:gosimple
	injectSnapshotMutation = client.InjectSnapshotMutation(
		testChainID,
		baseStateBytes,
		mockSnapshotMutationExtensionHashedState, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectSnapshotMutation, "SnapshotMutationExtensionFn may not return nil")

	// Must return a []byte
	assert.NotEmpty(t, injectSnapshotMutation)

	// The extension should have hashed stated (tmhash) and return it
	assert.Len(t, injectSnapshotMutation, tmhash.Size)
	assert.NotEqual(t, baseStateBytes, injectSnapshotMutation)

	// But it should not have touched the input config object
	assert.Equal(t, baseStateBytes, inputStateBytes)
}

func TestMultiplexClientAuditMutationResult(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()

	// Prepare a base transaction
	baseMutatedBytes := []byte(`this is not a real transaction.`)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errDelegateCheckTx error //nolint:gosimple
	errDelegateCheckTx = client.AuditMutationResult(
		testChainID,
		baseMutatedBytes,
		mockCheckMutationResultExtensionSizeAsError, // default_test.go
	)

	// The extension should have formatted the bytes slice size as an Error
	expectedMessage := fmt.Sprintf("Mutated state bytes: %d", len(baseMutatedBytes))
	assert.Equal(t, expectedMessage, errDelegateCheckTx.Error())
}

func TestMultiplexClientInjectSnapshotRestore(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()
	baseStateBytes := []byte(`this is just an example, not a sm.State.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectSnapshotRestore []byte //nolint:gosimple
	injectSnapshotRestore = client.InjectSnapshotRestore(
		testChainID,
		baseStateBytes,
		mockSnapshotRestoreExtensionHashedState, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectSnapshotRestore, "SnapshotRestoreExtensionFn may not return nil")

	// Must return a []byte
	assert.NotEmpty(t, injectSnapshotRestore)

	// The extension should have hashed stated (tmhash) and return it
	assert.Len(t, injectSnapshotRestore, tmhash.Size)
	assert.NotEqual(t, baseStateBytes, injectSnapshotRestore)

	// But it should not have touched the input config object
	assert.Equal(t, baseStateBytes, inputStateBytes)
}
