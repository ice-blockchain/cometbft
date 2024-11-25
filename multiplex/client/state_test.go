package client_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

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
