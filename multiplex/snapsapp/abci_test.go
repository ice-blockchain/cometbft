package snapsapp_test

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/ice-blockchain/cometbft/api/cometbft/abci/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

var (
	stateKey = []byte("stateKey")

	testPanicMessage = "A panic which occurs in a ABCI extension"
)

// ----------------------------------------------------------------------------
// Mocks

// Type-assertions ensure the compatibility of these mocks with the
// multiplex client contract defined in the client package.
var (
	_ client.CheckTxExtensionFn         = mockCheckTxExtensionWithError
	_ client.PrepareProposalExtensionFn = mockPrepareProposalExtensionWithError
	_ client.ProcessProposalExtensionFn = mockProcessProposalExtensionWithError
	_ client.FinalizeBlockExtensionFn   = mockFinalizeBlockExtensionWithError
	_ client.CommitExtensionFn          = mockCommitExtensionWithError
)

// mockPrepareProposalExtensionWithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockPrepareProposalExtensionWithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	func(_ [][]byte) {}(nextTransactions)
	panic(errors.New(testPanicMessage))
	return nextTransactions //nolint:govet
}

// mockProcessProposalExtensionWithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockProcessProposalExtensionWithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	func(_ [][]byte) {}(nextTransactions)
	panic(errors.New(testPanicMessage))
	return nextTransactions //nolint:govet
}

// mockFinalizeBlockExtensionWithError is an implementation that deep-copies
// the transactions bytes slices and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockFinalizeBlockExtensionWithError(
	ctx context.Context,
	baseTransactions [][]byte,
) [][]byte {
	nextTransactions := baseTransactions[:]
	func(_ [][]byte) {}(nextTransactions)
	panic(errors.New(testPanicMessage))
	return nextTransactions //nolint:govet
}

// mockCheckTxExtensionWithError is an implementation that deep-copies
// the transaction bytes and then panics.
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockCheckTxExtensionWithError(
	ctx context.Context,
	baseTransaction []byte,
) error {
	nextTransactions := baseTransaction[:]
	func(_ []byte) {}(nextTransactions)
	panic(errors.New(testPanicMessage))
	return nil //nolint:govet
}

// mockCommitExtensionWithError is an implementation which just panics
// CAUTION: this mock is intended to test panic recovery for extensions.
func mockCommitExtensionWithError(
	ctx context.Context,
	_ uint64,
) error {
	panic(errors.New(testPanicMessage))
	return nil //nolint:govet
}

// ----------------------------------------------------------------------------
// Unit tests

func TestABCI_Info(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	// We must cast to ChainHistoryStore for database access
	chainStore := suite.reactor.GetStateStore(testChainID).(*mx.ChainHistoryStore)
	require.NotNil(t, chainStore)

	reqTestInfo := abci.InfoRequest{}

	// Store custom state machine instance
	expectHeight := int64(1500)
	appState, appHash := makeState(t, testChainID, expectHeight)
	err := chainStore.GetDatabase().Set(stateKey, appState.Bytes())
	require.NoError(t, err)

	// Should return empty given invalid ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, "unknown-chain")
	infoRes, err := suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.Nil(t, err, "should not error given Info request")
	assert.Empty(t, infoRes.GetData())

	// Should succeed given valid injected ChainID
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)
	infoRes, err = suite.snapsApp.Info(ctx, &reqTestInfo)
	assert.NoError(t, err, "should not error given Info request")
	assert.Equal(t, testChainID, infoRes.GetData())
	assert.Equal(t, appHash, infoRes.GetLastBlockAppHash())
	assert.Equal(t, expectHeight, infoRes.GetLastBlockHeight())
}

func TestABCI_InitChain(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	// We must cast to ChainHistoryStore for database access
	chainStore := suite.reactor.GetStateStore(testChainID).(*mx.ChainHistoryStore)
	require.NotNil(t, chainStore)

	// Store custom state machine instance
	emptyState := &mx.HistoricalState{State: &sm.State{}, Data: []byte{}}
	err := chainStore.GetDatabase().Set(stateKey, emptyState.Bytes())
	require.NoError(t, err)

	// Should error given unknown ChainID
	initChainRes, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		ChainId: "wrong-chain-id",
	})
	assert.Error(t, err)
	assert.Nil(t, initChainRes)

	// Store custom GENESIS state for InitChain
	valPubKey := ed25519.GenPrivKey().PubKey()
	fakeAppHash := []byte{1, 2, 3}
	genState, err := sm.MakeGenesisState(&types.GenesisDoc{
		GenesisTime:   cmttime.Now(),
		ChainID:       testChainID,
		InitialHeight: 0,
		Validators: []types.GenesisValidator{{
			Address: valPubKey.Address(),
			PubKey:  valPubKey,
			Power:   10,
			Name:    "myval",
		}},
		ConsensusParams: types.DefaultConsensusParams(),
		AppHash:         fakeAppHash,
		AppState:        []byte(`{}`),
	})
	require.NoError(t, err, "should not error creating state machine")

	archiveState := &mx.HistoricalState{
		State: &genState,
		Data:  []byte{},
	}
	err = chainStore.GetDatabase().Set(stateKey, archiveState.Bytes())
	require.NoError(t, err)

	// Should succeed given correct ChainID
	initChainRes, err = suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		AppStateBytes: []byte("{}"),
		ChainId:       testChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, fakeAppHash, initChainRes.AppHash)
}

func TestABCI_InitChain_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       testChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(testChainID))
}

func TestABCI_PrepareProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). PrepareProposal
	bytesTx1 := []byte{1, 2, 3}
	bytesTx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytesTx1, bytesTx2},
	}

	resPrepareProposal, err := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)
	assert.NoError(t, err, "should not error given proposal request (PrepareProposal)")
	assert.Equal(t, 2, len(resPrepareProposal.Txs))
}

func TestABCI_PrepareProposal_ExtensionFailure(t *testing.T) {
	// Forces a FAILING PrepareProposal extension
	suite := NewSnapsAppSuite(t, snapsapp.WithPrepareProposalExtension(
		mockPrepareProposalExtensionWithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// ---------------------
	// Errors
	//
	// - Must error and stop the block proposal process given faulty extension.

	expectedError := "CLIENT PANIC: failing prepare proposal extension: " + testPanicMessage

	// (1). PrepareProposal
	bytesTx1 := []byte{1, 2, 3}
	bytesTx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytesTx1, bytesTx2},
	}

	// Should error given a failing PrepareProposal extension
	_, errPrepareProposal := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)

	// Returning an error means that the proposal process is STOPPED!
	assert.Error(t, errPrepareProposal,
		"should stop blocks proposal process given failing extension")
	assert.Equal(t, expectedError, errPrepareProposal.Error())
}

func TestABCI_ProcessProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). ProcessProposal
	bytesTx1 := []byte{1, 2, 3}
	bytesTx2 := []byte{4, 5, 6}
	mergedTxBytes := [2][]byte{bytesTx1, bytesTx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    mergedTxBytes[:],
		Height: 1,
	}

	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)
}

func TestABCI_ProcessProposal_ExtensionFailure(t *testing.T) {
	// Forces a FAILING ProcessProposal extension
	suite := NewSnapsAppSuite(t, snapsapp.WithProcessProposalExtension(
		mockProcessProposalExtensionWithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// ---------------------
	// Errors
	//
	// - Must *not* influence the processing stage, i.e. should accept proposal

	// (1). ProcessProposal
	bytesTx1 := []byte{1, 2, 3}
	bytesTx2 := []byte{4, 5, 6}
	mergedTxBytes := [2][]byte{bytesTx1, bytesTx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    mergedTxBytes[:],
		Height: 1,
	}

	// Should not error, even with failing extension
	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)
}

func TestABCI_FinalizeBlock(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	assert.NoError(t, err)
	assert.NotNil(t, resFinalizeBlock)
}

func TestABCI_FinalizeBlock_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// Attach an Initial Height
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		InitialHeight: 3,
		AppStateBytes: []byte("{}"),
		ChainId:       testChainID, // must have valid JSON genesis file, even if empty
	})
	require.NoError(t, err)
	require.Equal(t, int64(3), suite.snapsApp.LastBlockHeight(testChainID))

	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	res, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{Height: 4})
	assert.NoError(t, err)
	assert.NotNil(t, res)
}

func TestABCI_FinalizeBlock_ExtensionFailure(t *testing.T) {
	// Forces a FAILING FinalizeBlock extension
	suite := NewSnapsAppSuite(t, snapsapp.WithFinalizeBlockExtension(
		mockFinalizeBlockExtensionWithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// ---------------------
	// Errors
	//
	// - Must error and stop the block finalization process given faulty extension.

	expectedError := "CLIENT PANIC: failing finalize block extension: " + testPanicMessage

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	// Should error given a failing FinalizeBlock extension
	_, errFinalizeBlock := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)

	// Returning an error means that the finalization process is STOPPED!
	assert.Error(t, errFinalizeBlock,
		"should stop finalization process given failing extension")
	assert.Equal(t, expectedError, errFinalizeBlock.Error())
}

func TestABCI_Proposal_HappyPath(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). InitChain
	_, err := suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		ChainId: testChainID,
	})
	assert.NoError(t, err, "should not error given correct ChainID (InitChain)")

	// (2). PrepareProposal
	bytesTx1 := []byte{1, 2, 3}
	bytesTx2 := []byte{4, 5, 6}
	reqPrepareProposal := abci.PrepareProposalRequest{
		MaxTxBytes: 1000,
		Height:     1,
		Txs:        [][]byte{bytesTx1, bytesTx2},
	}

	resPrepareProposal, err := suite.snapsApp.PrepareProposal(ctx, &reqPrepareProposal)
	assert.NoError(t, err, "should not error given proposal request (PrepareProposal)")
	assert.Equal(t, 2, len(resPrepareProposal.Txs))

	// (3). ProcessProposal
	reqProposalMergedTxBytes := [2][]byte{bytesTx1, bytesTx2}
	reqProcessProposal := abci.ProcessProposalRequest{
		Txs:    reqProposalMergedTxBytes[:],
		Height: reqPrepareProposal.Height,
	}

	resProcessProposal, err := suite.snapsApp.ProcessProposal(ctx, &reqProcessProposal)
	assert.NoError(t, err, "should not error given proposal request (ProcessProposal)")
	assert.Equal(t, abci.PROCESS_PROPOSAL_STATUS_ACCEPT, resProcessProposal.Status)

	// (4). FinalizeBlock
	lastBlockHeight := suite.snapsApp.LastBlockHeight(testChainID)
	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{
		Height: lastBlockHeight + 1,
		Txs:    reqProposalMergedTxBytes[:], // same as ProcessProposal
	})
	assert.NoError(t, err, "should not error given correct request (FinalizeBlock)")
	assert.NotEmpty(t, resFinalizeBlock.TxResults)
	assert.Len(t, resFinalizeBlock.TxResults, 2)
}

func TestABCI_CheckTx(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). CheckTx
	testTransaction := []byte{1, 2, 3}
	reqCheckTx := abci.CheckTxRequest{
		Tx: testTransaction,
	}

	resCheckTx, errCheckTx := suite.snapsApp.CheckTx(ctx, &reqCheckTx)
	assert.NoError(t, errCheckTx, "should not error given check request (CheckTx)")
	assert.Equal(t, abci.CodeTypeOK, resCheckTx.Code)
}

func TestABCI_CheckTx_ExtensionFailure(t *testing.T) {
	// Forces a FAILING CheckTx extension
	suite := NewSnapsAppSuite(t, snapsapp.WithCheckTxExtension(
		mockCheckTxExtensionWithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// ---------------------
	// Errors
	//
	// - Must consider a transaction invalid given faulty extension

	// (1). CheckTx
	testTransaction := []byte{1, 2, 3}
	reqCheckTx := abci.CheckTxRequest{
		Tx: testTransaction,
	}

	resCheckTx, errCheckTx := suite.snapsApp.CheckTx(ctx, &reqCheckTx)
	assert.Error(t, errCheckTx, "should error given failing extension (CheckTx)")
	assert.NotEqual(t, abci.CodeTypeOK, resCheckTx.Code)
}

func TestABCI_Commit(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	require.NoError(t, err)
	require.NotNil(t, resFinalizeBlock)

	// (2). Commit
	resCommit, errCommit := suite.snapsApp.Commit(ctx, &abci.CommitRequest{})
	assert.NoError(t, errCommit, "should not error given commit request")
	assert.Equal(t, int64(0), resCommit.RetainHeight,
		"should use 0 as RetainHeight due to disabled pruning")
}

func TestABCI_Commit_ExtensionFailure(t *testing.T) {
	suite := NewSnapsAppSuite(t, snapsapp.WithCommitExtension(
		mockCommitExtensionWithError,
	))
	defer os.RemoveAll(suite.rootDir)

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// (0). Inject ChainID
	ctx := context.TODO()
	ctx = context.WithValue(ctx, client.KeyChainID, testChainID)

	// (1). FinalizeBlock
	reqFinalizeBlock := abci.FinalizeBlockRequest{
		Height: 1,
	}

	resFinalizeBlock, err := suite.snapsApp.FinalizeBlock(ctx, &reqFinalizeBlock)
	require.NoError(t, err)
	require.NotNil(t, resFinalizeBlock)

	// ---------------------
	// Errors
	//
	// - Must *not* influence the commitment stage, i.e. should commit.

	// (2). Commit
	resCommit, errCommit := suite.snapsApp.Commit(ctx, &abci.CommitRequest{})
	assert.NoError(t, errCommit, "should not error given commit request")
	assert.Equal(t, int64(0), resCommit.RetainHeight,
		"should use 0 as RetainHeight due to disabled pruning")
}
