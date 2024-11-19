package client_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func TestMultiplexClientDelegateCheckTx(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()

	// Prepare a base transaction
	baseTransaction := []byte(`this is not a real transaction.`)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errDelegateCheckTx error //nolint:gosimple
	errDelegateCheckTx = client.DelegateCheckTx(
		testChainID,
		baseTransaction,
		mockCheckTxExtensionSizeAsError, // default_test.go
	)

	// The extension should have formatted the transaction size as an Error
	expectedMessage := fmt.Sprintf("Transaction bytes: %d", len(baseTransaction))
	assert.Equal(t, expectedMessage, errDelegateCheckTx.Error())
}

func TestMultiplexClientInjectPrepareProposal(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectPrepareProposal [][]byte //nolint:gosimple
	injectPrepareProposal = client.InjectPrepareProposal(
		testChainID,
		baseTransactionsSlice,
		mockPrepareProposalExtensionAppendOneTx, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectPrepareProposal, "PrepareProposalExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectPrepareProposal)

	// The extension should have appended one transaction
	assert.Len(t, injectPrepareProposal, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectPrepareProposal)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}

func TestMultiplexClientInjectProcessProposal(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectProcessProposal [][]byte //nolint:gosimple
	injectProcessProposal = client.InjectProcessProposal(
		testChainID,
		baseTransactionsSlice,
		mockProcessProposalExtensionAppendOneTx, // default_test.go
	)

	// Extension may not return nil
	assert.NotNil(t, injectProcessProposal, "ProcessProposalExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectProcessProposal)

	// The extension should have appended one transaction
	assert.Len(t, injectProcessProposal, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectProcessProposal)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}

func TestMultiplexClientInjectFinalizeBlock(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionBytes := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var injectFinalizeBlock [][]byte //nolint:gosimple
	injectFinalizeBlock = client.InjectFinalizeBlock(
		testChainID,
		baseTransactionsSlice,
		mockFinalizeBlockExtensionAppendHash, // default_test.go
	)

	// The extension should have appended a transactions hash
	actualLastTx := injectFinalizeBlock[len(injectFinalizeBlock)-1]

	// Extension may not return nil
	assert.NotNil(t, injectFinalizeBlock, "FinalizeBlockExtensionFn may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, injectFinalizeBlock)

	// The extension should have appended the transactions hash
	assert.Len(t, injectFinalizeBlock, len(inputTransactionBytes)+1)
	assert.NotEqual(t, baseTransactionsSlice, injectFinalizeBlock)
	assert.Len(t, actualLastTx, tmhash.Size)

	// But it should not have touched the input config object
	assert.Equal(t, baseTransactionsSlice, inputTransactionBytes)
}

func TestMultiplexClientReportCommit(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()

	// Prepare a base block height
	baseBlockHeight := uint64(123)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errReportCommit error //nolint:gosimple
	errReportCommit = client.ReportCommit(
		testChainID,
		baseBlockHeight,
		mockCommitExtensionHeightAsError, // default_test.go
	)

	// The extension should have formatted the block height as an Error
	expectedMessage := fmt.Sprintf("Block height: %v", baseBlockHeight)
	assert.Equal(t, expectedMessage, errReportCommit.Error())
}
