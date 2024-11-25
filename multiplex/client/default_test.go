package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	v1 "github.com/ice-blockchain/cometbft/api/cometbft/types/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

const (
	testSeedNodesExample = "testNodeId@127.0.0.1:123,"
)

// ----------------------------------------------------------------------------
// Mocks

// Type-assertions ensure the compatibility of these mocks with the
// client contract defined in this client package.
var (
	_ client.SyncConfigExtensionFn = mockSyncConfigExtensionMutatesHeight
	_ client.SeedConfigExtensionFn = mockSeedConfigExtensionPrefixOneSeed

	_ client.ValidatorUpdateExtensionFn = mockValidatorUpdateExtensionCountAsError
	_ client.ConsensusUpdateExtensionFn = mockConsensusUpdateExtensionMaxBytesAsError
	_ client.CheckTxExtensionFn         = mockCheckTxExtensionSizeAsError
	_ client.PrepareProposalExtensionFn = mockPrepareProposalExtensionAppendOneTx
	_ client.ProcessProposalExtensionFn = mockProcessProposalExtensionAppendOneTx
	_ client.FinalizeBlockExtensionFn   = mockFinalizeBlockExtensionAppendHash
	_ client.CommitExtensionFn          = mockCommitExtensionHeightAsError

	_ client.CheckMutationResultExtensionFn = mockCheckMutationResultExtensionSizeAsError
)

// mockSyncConfigExtensionMutatesHeight is an implementation that mutates the
// baseSyncConf.TrustHeight and increases it by 1.
func mockSyncConfigExtensionMutatesHeight(
	ctx context.Context,
	baseSyncConf *config.StateSyncConfig,
) *config.StateSyncConfig {
	nextStateSyncConfig := &config.StateSyncConfig{
		Enable:              baseSyncConf.Enable,
		TempDir:             baseSyncConf.TempDir,
		RPCServers:          baseSyncConf.RPCServers,
		TrustPeriod:         baseSyncConf.TrustPeriod,
		TrustHeight:         baseSyncConf.TrustHeight + 1, // mutation
		TrustHash:           baseSyncConf.TrustHash,
		DiscoveryTime:       baseSyncConf.DiscoveryTime,
		ChunkRequestTimeout: baseSyncConf.ChunkRequestTimeout,
		ChunkFetchers:       baseSyncConf.ChunkFetchers,
	}
	return nextStateSyncConfig
}

// mockSeedConfigExtensionPrefixOneSeed is an implementation that mutates the
// baseSeeds and prefixes it by adding "testNodeId@127.0.0.1:123,".
func mockSeedConfigExtensionPrefixOneSeed(
	ctx context.Context,
	baseSeeds string,
) string {
	nextSeeds := testSeedNodesExample + baseSeeds[:]
	return nextSeeds
}

// mockValidatorUpdateExtensionCountAsError is an implementation that reads the
// baseValidators validator set and formats an error with the number of validators.
func mockValidatorUpdateExtensionCountAsError(
	ctx context.Context,
	baseValidators []abci.ValidatorUpdate,
) error {
	return fmt.Errorf("Count validator updates: %d", len(baseValidators))
}

// mockConsensusUpdateExtensionMaxBytesAsError is an implementation that reads the
// consensusParams updates and formats an error with the max bytes content.
func mockConsensusUpdateExtensionMaxBytesAsError(
	ctx context.Context,
	baseConsensusParams *v1.ConsensusParams,
) error {
	return fmt.Errorf("Max bytes: %v", baseConsensusParams.Block.MaxBytes)
}

// mockCheckMutationResultExtensionSizeAsError is an implementation that reads the
// mutated state bytes and formats an error that prints the length of the byte slice.
func mockCheckMutationResultExtensionSizeAsError(
	ctx context.Context,
	tx []byte,
) error {
	return fmt.Errorf("Mutated state bytes: %d", len(tx))
}

// mockCheckTxExtensionSizeAsError is an implementation that reads the
// transaction bytes and formats an error that prints the length of the byte slice.
func mockCheckTxExtensionSizeAsError(
	ctx context.Context,
	tx []byte,
) error {
	return fmt.Errorf("Transaction bytes: %d", len(tx))
}

// mockPrepareProposalExtensionAppendOneTx is an implementation that mutates the
// transactions slice so that it contains one more testable transaction.
// CAUTION: this mock mutates the transactions data.
func mockPrepareProposalExtensionAppendOneTx(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := append(baseTransactions[:], []byte(`test transaction`))
	return nextTransactions
}

// mockProcessProposalExtensionAppendOneTx is an implementation that mutates the
// transactions slice so that it contains one more testable transaction.
// CAUTION: this mock mutates the transactions data.
func mockProcessProposalExtensionAppendOneTx(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := append(baseTransactions[:], []byte(`test transaction`))
	return nextTransactions
}

// mockFinalizeBlockExtensionAppendHash is an implementation that mutates the
// transactions slice so that it contains a transitions hash at the end.
// CAUTION: this mock mutates the transactions data.
func mockFinalizeBlockExtensionAppendHash(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	// We create a sha-256 hash of the flattened transactions bytes
	var hashedTxs []byte
	for _, baseTx := range baseTransactions {
		hashedTxs = append(hashedTxs, baseTx...)
	}

	// And append the hash to the transactions slice
	nextTransactions := append(baseTransactions[:], tmhash.Sum(hashedTxs))
	return nextTransactions
}

// mockCommitExtensionHeightAsError is an implementation that reads the
// block height and formats an error to print it.
func mockCommitExtensionHeightAsError(
	ctx context.Context,
	blockHeight uint64,
) error {
	return fmt.Errorf("Block height: %v", blockHeight)
}

// ----------------------------------------------------------------------------
// Unit tests

func TestMultiplexClientDefaultSyncConfigExtension(t *testing.T) {
	baseTrustHeight := int64(123)
	baseTrustHash := makeDeterministicTrustHash("trust me!")

	baseConf := config.TestConfig()
	baseConf.StateSync.TrustHeight = baseTrustHeight
	baseConf.StateSync.TrustHash = baseTrustHash

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextSyncConf *config.StateSyncConfig //nolint:gosimple
	nextSyncConf = client.DefaultSyncConfigExtension(context.TODO(), baseConf.StateSync)
	// Should deep-copy the object
	// do some mutations to test deep-copy
	nextSyncConf.TrustHeight = baseTrustHeight + 1
	nextSyncConf.TrustHash = makeDeterministicTrustHash("do not trust me!")

	// Extension may not return nil
	assert.NotNil(t, nextSyncConf, "DefaultSyncConfigExtension may not return nil")

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input config object.
	assert.Equal(t, baseTrustHeight, baseConf.StateSync.TrustHeight)
	assert.Equal(t, baseTrustHash, baseConf.StateSync.TrustHash)
}

func TestMultiplexClientDefaultSeedConfigExtension(t *testing.T) {
	baseSeeds := "testNodeId2@192.168.1.1:30001,testNodeId3@192.168.1.2:30001"

	baseConf := config.TestConfig()
	baseConf.P2P.Seeds = baseSeeds

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextChainSeeds string
	nextChainSeeds = client.DefaultSeedConfigExtension(context.TODO(), baseConf.P2P.Seeds)
	// Should deep-copy the string
	// do some mutations to test deep-copy
	nextChainSeeds += ",mutationForTest"

	// Extension may not return nil
	assert.NotNil(t, nextChainSeeds, "DefaultSeedConfigExtension may not return nil")

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input config object.
	assert.Equal(t, baseSeeds, baseConf.P2P.Seeds)
}

func TestMultiplexClientDefaultValidatorUpdateExtension(t *testing.T) {
	// unmarshal a test validator
	testValidator := abci.ValidatorUpdate{}
	err := cmtjson.Unmarshal([]byte(testValidatorJSON), &testValidator)
	require.NoError(t, err, "should unmarshal a test validator from JSON")

	// Prepare a validators update set
	baseValidators := []abci.ValidatorUpdate{testValidator}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errValidatorUpdate error //nolint:gosimple
	errValidatorUpdate = client.DefaultValidatorUpdateExtension(context.TODO(), baseValidators)

	// Default extension returns nil (no error)
	assert.Nil(t, errValidatorUpdate, "DefaultValidatorUpdateExtension must return nil")
}

func TestMultiplexClientDefaultConsensusUpdateExtension(t *testing.T) {
	// Prepare a consensus params instance
	expectedMaxBytes := 123
	baseConsensusParams := &v1.ConsensusParams{
		Block: &v1.BlockParams{
			MaxBytes: int64(expectedMaxBytes),
		},
	}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errConsensusUpdate error //nolint:gosimple
	errConsensusUpdate = client.DefaultConsensusUpdateExtension(context.TODO(), baseConsensusParams)

	// Default extension returns nil (no error)
	assert.Nil(t, errConsensusUpdate, "DefaultConsensusUpdateExtension must return nil")
}

func TestMultiplexClientDefaultCheckMutationResultExtension(t *testing.T) {
	// Prepare a base transaction
	baseStateBytes := []byte(`this is not a real transaction.`)
	inputStateBytes := baseStateBytes[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errCheckMutationResults error //nolint:gosimple
	errCheckMutationResults = client.DefaultCheckMutationResultExtension(context.TODO(), inputStateBytes)

	// Default extension returns nil (no error)
	assert.Nil(t, errCheckMutationResults, "DefaultCheckMutationResultExtension must return nil")
}

func TestMultiplexClientDefaultCheckTxExtension(t *testing.T) {
	// Prepare a base transaction
	baseTransaction := []byte(`this is not a real transaction.`)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errCheckTx error //nolint:gosimple
	errCheckTx = client.DefaultCheckTxExtension(context.TODO(), baseTransaction)

	// Default extension returns nil (no error)
	assert.Nil(t, errCheckTx, "DefaultCheckTxExtension must return nil")
}

func TestMultiplexClientDefaultPrepareProposalExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte //nolint:gosimple
	nextTransactions = client.DefaultPrepareProposalExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultPrepareProposalExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultProcessProposalExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte //nolint:gosimple
	nextTransactions = client.DefaultProcessProposalExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultProcessProposalExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultFinalizeBlockExtension(t *testing.T) {
	baseTransactionsSlice := [][]byte{
		[]byte(`this is just an example.`),
		[]byte(`with multiplex "transactions".`),
	}
	inputTransactionsSlice := baseTransactionsSlice[:]

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var nextTransactions [][]byte //nolint:gosimple
	nextTransactions = client.DefaultFinalizeBlockExtension(context.TODO(), inputTransactionsSlice)

	// Extension may not return nil
	assert.NotNil(t, nextTransactions, "DefaultFinalizeBlockExtension may not return nil")

	// Must return a [][]byte
	assert.NotEmpty(t, nextTransactions)
	assert.Len(t, nextTransactions, len(baseTransactionsSlice))

	// The extension does only a deep-copy, so we test that
	// mutations did not execute on the input bytes slices.
	assert.Equal(t, baseTransactionsSlice, inputTransactionsSlice)
}

func TestMultiplexClientDefaultCommitExtension(t *testing.T) {
	// Prepare a base block height
	baseBlockHeight := uint64(123)

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errCommit error //nolint:gosimple
	errCommit = client.DefaultCommitExtension(context.TODO(), baseBlockHeight)

	// Default extension returns nil (no error)
	assert.Nil(t, errCommit, "DefaultCommitExtension must return nil")
}
