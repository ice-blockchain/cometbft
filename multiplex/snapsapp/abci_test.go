package snapsapp_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/ice-blockchain/cometbft/api/cometbft/abci/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// ----------------------------------------------------------------------------
// Unit tests

func TestMultiplexABCI_Info(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	reqTestInfo := abci.InfoRequest{}

	// Store custom state machine instance
	expectHeight := int64(1500)
	appState, appHash := makeState(t, testChainID, expectHeight)
	err := chainStore.Save(appState)
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

func TestMultiplexABCI_InitChain(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]

	// Check that we have a correct state store
	chainStore := suite.reactor.GetStateStore(testChainID)
	require.NotNil(t, chainStore)

	// Store custom state machine instance
	emptyState := sm.State{}
	err := chainStore.Save(emptyState)
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

	err = chainStore.Save(genState)
	require.NoError(t, err)

	// Should succeed given correct ChainID
	initChainRes, err = suite.snapsApp.InitChain(context.TODO(), &abci.InitChainRequest{
		AppStateBytes: []byte("{}"),
		ChainId:       testChainID, // must have valid JSON genesis file, even if empty
	})
	assert.NoError(t, err)
	assert.Equal(t, fakeAppHash, initChainRes.AppHash)
}

func TestMultiplexABCI_InitChain_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_PrepareProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_ProcessProposal(t *testing.T) {
	suite := NewSnapsAppSuite(t, snapsapp.WithUseMempool(false))
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_FinalizeBlock(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_FinalizeBlock_WithInitialHeight(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_FinalizeBlock_WithAcceptor(t *testing.T) {
	suite := NewSnapsAppSuite(t, snapsapp.WithAcceptor(
		client.NewMockAcceptorImpl(),
	))
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

	// We test using the "first network"
	testChainID := suite.reactor.GetNetworks()[0]
	testAddress := client.GetUserAddress(testChainID)
	testFingerprint := testChainID[len(testChainID)-16:]
	require.NotEmpty(t, testAddress)

	testTransactions := [][]byte{
		client.TransactionToRawTx(client.Transaction{
			Data:        []byte{1, 2, 3},
			Fingerprint: testFingerprint,
		}),
		client.TransactionToRawTx(client.Transaction{
			Data:        []byte{4, 5, 6},
			Fingerprint: testFingerprint,
		}),
	}

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

	// Act
	res, err := suite.snapsApp.FinalizeBlock(ctx, &abci.FinalizeBlockRequest{
		Height: 4,
		Txs:    testTransactions,
	})
	assert.NoError(t, err)
	assert.NotNil(t, res)

	actualAcceptor := suite.snapsApp.GetAcceptor()
	assert.NotNil(t, actualAcceptor)
	testAcceptor := actualAcceptor.(*client.MockAcceptorImpl)

	expectedNumCalls := uint64(2) // InitChain + FinalizeBlock
	assert.Equal(t, expectedNumCalls, testAcceptor.TxCommitCalls.Load())
}

func TestMultiplexABCI_Proposal_HappyPath(t *testing.T) {
	suite := NewSnapsAppSuite(t, snapsapp.WithUseMempool(false))
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_CheckTx(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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

func TestMultiplexABCI_Commit(t *testing.T) {
	suite := NewSnapsAppSuite(t)
	defer func() {
		defer os.RemoveAll(suite.rootDir)
		suite.reactor.Stop() //nolint:errcheck
	}()

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
