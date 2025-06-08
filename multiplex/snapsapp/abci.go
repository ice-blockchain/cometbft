package snapsapp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	abcitypes "github.com/ice-blockchain/cometbft/abci/types"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// ----------------------------------------------------------------------------
// Network

// InitChain initializes the application's state and sets up the initial
// validator set and other consensus parameters.
//
// This method is called once before applying the genesis block and permits
// to overwrite an application's validator set and consensus params, as well
// as to set an initial state to be replicated.
//
// InitChain implements [abcitypes.Application].
func (app *SnapsApp) InitChain(
	ctx context.Context,
	req *abcitypes.InitChainRequest,
) (*abcitypes.InitChainResponse, error) {
	// Retrieve ChainID from request for InitChain
	chainID := req.ChainId

	// Make sure we handle only relevant InitChain routines
	if !app.reactor.HasNetwork(chainID) {
		return nil, fmt.Errorf("invalid chain-id on InitChain: %s is not replicated", chainID)
	}

	// On a new chain, we consider the init chain block height as 0, even though
	// req.InitialHeight is 1 by default.
	app.ihMutex.Lock()
	app.initialHeights[chainID] = req.InitialHeight
	if app.initialHeights[chainID] == 0 { // If initial height is 0, set it to 1
		app.initialHeights[chainID] = 1
	}
	app.ihMutex.Unlock()

	if err := app.setFinalizeBlockHeight(chainID, 0); err != nil {
		return nil, fmt.Errorf("could not update block height for %s from InitChain: %w", chainID, err)
	}

	// if req.InitialHeight is > 1, then we set the initial version on the app
	app.lbMutex.Lock()
	if req.InitialHeight > 1 {
		app.lastBlockHeights[chainID] = req.InitialHeight
	} else {
		app.lastBlockHeights[chainID] = 0
	}
	app.lbMutex.Unlock()
	app.logger.Info("InitChain", "initialHeight", req.InitialHeight, "chainID", req.ChainId)

	// Get the state machine instance to retrieve current AppHash
	stateStore := app.reactor.GetStateStore(chainID)
	stateMachine, err := stateStore.Load()
	if err != nil {
		return nil, fmt.Errorf("could not load state machine for %s from InitChain: %w", chainID, err)
	}

	// NOTE: We don't commit, but FinalizeBlock for block InitialHeight starts from
	// this FinalizeBlockState.
	return &abcitypes.InitChainResponse{
		ConsensusParams: req.ConsensusParams,
		Validators:      req.Validators,
		AppHash:         stateMachine.AppHash,
	}, nil
}

// Info returns information about the application, including the AppHash.
//
// This method is called upon crash-recovery and after executing state-sync
// to verify the loaded AppHash.
//
// Info implements [abcitypes.Application].
func (app *SnapsApp) Info(
	ctx context.Context,
	req *abcitypes.InfoRequest,
) (*abcitypes.InfoResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Make sure we handle only relevant info requests
	if !app.reactor.HasNetwork(chainID) {
		app.logger.Error("received irrelevant snapshot chain identifier (Info)", "chain_id", chainID)
		return &abcitypes.InfoResponse{}, nil
	}

	// Get the state machine instance to retrieve current AppHash
	stateStore := app.reactor.GetStateStore(chainID)
	stateMachine, err := stateStore.Load()
	if err != nil {
		app.logger.Error("could not load state machine (Info)", "chain_id", chainID)
		return &abcitypes.InfoResponse{}, err
	}

	// The ChainID is included in the response data
	return &abcitypes.InfoResponse{
		Data:             chainID,
		Version:          snapsappVersion,
		AppVersion:       AppVersion,
		LastBlockHeight:  stateMachine.LastBlockHeight,
		LastBlockAppHash: stateMachine.AppHash,
	}, nil
}

// TODO(midas): Query not yet supported as of v1
// Query implements [abcitypes.Application].
func (app *SnapsApp) Query(context.Context, *abcitypes.QueryRequest) (*abcitypes.QueryResponse, error) {
	return &abcitypes.QueryResponse{Code: abcitypes.CodeTypeOK}, nil
}

// ----------------------------------------------------------------------------
// Transactions / Blocks

// PrepareProposal implements the PrepareProposal ABCI method and returns a
// ResponsePrepareProposal object to the client. The PrepareProposal method is
// responsible for allowing the block proposer to perform application-dependent
// work in a block before proposing it.
//
// Transactions can be modified, removed, or added by the application. Since the
// application maintains its own local mempool, it will ignore the transactions
// provided to it in RequestPrepareProposal. Instead, it will determine which
// transactions to return based on the mempool's semantics and the MaxTxBytes
// provided by the client's request.
//
// PrepareProposal implements [abcitypes.Application].
func (app *SnapsApp) PrepareProposal(
	ctx context.Context,
	req *abcitypes.PrepareProposalRequest,
) (*abcitypes.PrepareProposalResponse, error) {
	// Retrieve ChainID from context
	// chainID := ctx.Value(client.KeyChainID).(string)

	// CometBFT must never call PrepareProposal with a height of 0.
	//
	// Ref: https://github.com/ice-blockchain/cometbft/blob/059798a4f5b0c9f52aa8655fa619054a0154088c/spec/core/state.md?plain=1#L37-L38
	if req.Height < 1 {
		return nil, errors.New("PrepareProposal called with invalid height")
	}

	// Makes sure that the transaction bytes proposed do not overflow
	// the MaxTxBytes value and discards data if necessary.
	txs := make([][]byte, 0, len(req.Txs))
	var totalBytes int64
	for _, tx := range req.Txs {
		totalBytes += int64(len(tx))
		if totalBytes > req.MaxTxBytes {
			break
		}
		txs = append(txs, tx)
	}

	// Prepare the contract for PrepareProposal
	preparedTxes := txs

	return &abcitypes.PrepareProposalResponse{Txs: preparedTxes}, nil
}

// ProcessProposal implements the ProcessProposal ABCI method and returns a
// ResponseProcessProposal object to the client.
//
// The ProcessProposal method is responsible for allowing execution of
// application-dependent work in a proposed block. Note, the application defines
// the exact implementation details of ProcessProposal. In general, the
// application must at the very least ensure that all transactions are valid.
// If all transactions are valid, then we inform CometBFT that the Status is
// ACCEPT.
// However, the application is also able to implement optimizations such as
// executing the entire proposed block immediately.
//
// ProcessProposal implements [abcitypes.Application].
func (app *SnapsApp) ProcessProposal(
	ctx context.Context,
	req *abcitypes.ProcessProposalRequest,
) (*abcitypes.ProcessProposalResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Make sure we handle only relevant proposals
	if !app.reactor.HasNetwork(chainID) {
		return nil, fmt.Errorf("received irrelevant chain identifier (ProcessProposal): %s", chainID)
	}

	// CometBFT must never call ProcessProposal with a height of 0.
	//
	// Ref: https://github.com/ice-blockchain/cometbft/blob/059798a4f5b0c9f52aa8655fa619054a0154088c/spec/core/state.md?plain=1#L37-L38
	if req.Height < 1 {
		return nil, errors.New("ProcessProposal called with invalid height")
	}

	// Update the current working block height
	app.whMutex.Lock()
	app.workingHeights[chainID] = req.Height
	app.whMutex.Unlock()

	// Update the last block height for this ChainID
	app.lbMutex.Lock()
	app.lastBlockHeights[chainID] = req.Height
	app.lbMutex.Unlock()

	return &abcitypes.ProcessProposalResponse{Status: abcitypes.PROCESS_PROPOSAL_STATUS_ACCEPT}, nil
}

// FinalizeBlock will execute the block proposal provided by FinalizeBlockRequest.
//
// Currently we do not perform any filtering with transactions, thus the
// transactions are all deemed to be "valid".
//
// The finalizeBlockHeights is updated for the relevant chain such that the
// subsequent Commit() ABCI with the same ChainID may know which *height* is
// being finalized.
//
// Note that given a non-nil [client.Acceptor] instance on the app, transaction
// batches will be forwarded to the acceptor's `AcceptTx()` method. We do this
// to ensure that blocks replay and block-sync always persist all batches.
//
// FinalizeBlock implements [abcitypes.Application].
func (app *SnapsApp) FinalizeBlock(
	ctx context.Context,
	req *abcitypes.FinalizeBlockRequest,
) (*abcitypes.FinalizeBlockResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	resp := &abcitypes.FinalizeBlockResponse{TxResults: []*abcitypes.ExecTxResult{}}

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		return resp, fmt.Errorf(
			"invalid chain-id on FinalizeBlock: %s is not replicated", chainID)
	}

	// Prepare the contract for FinalizeBlock
	processedTxs := req.Txs

	// Whenever there is transactions that are included in a *finalized*
	// block, we create an [abcitypes.Event] which contains the transaction
	// bytes as a hexadecimal string, the ChainID and the block height.
	//
	// These events can be subscribed for processing of individual transactions.
	txResults := make([]*abcitypes.ExecTxResult, len(processedTxs))
	for i := range processedTxs {
		txResults[i] = &abcitypes.ExecTxResult{Code: abcitypes.CodeTypeOK, Events: []abcitypes.Event{
			{
				Type: "app",
				Attributes: []abcitypes.EventAttribute{
					{Key: "chain_id", Value: chainID, Index: true},
					{Key: "height", Value: strconv.FormatInt(req.Height, 10), Index: true},
					{Key: "tx", Value: hex.EncodeToString(processedTxs[i]), Index: true},
				},
			},
		}}
	}

	// When we did not process a block proposal for the request's height,
	// we are finalizing a block received through blocksync. In this case,
	// instead of executing the CommitBroadcastTx callback, we forward the
	// transaction batch to the replay pool to execute ReplayBroadcastTxBatch.
	app.whMutex.RLock()
	isReplayMode := false
	if workingHeight, ok := app.workingHeights[chainID]; ok {
		isReplayMode = workingHeight != req.Height
	}
	app.whMutex.RUnlock()

	// Forward the transaction batch to an Acceptor if any is available.
	if app.txAcceptor != nil && len(processedTxs) > 0 {
		hasUncommittedTx := false

		batch := []client.Transaction{}
		txHashes := []string{}
		for _, rawTx := range processedTxs {
			tx := client.RawTxToTransaction(rawTx)
			txHash := fmt.Sprintf("%X", tx.Hash())
			batch = append(batch, tx)
			txHashes = append(txHashes, txHash)

			app.txMutex.RLock()
			_, committed := app.committedTxHashes[txHash]
			app.txMutex.RUnlock()
			if !committed {
				hasUncommittedTx = true
			}
		}

		// In replay-mode, we must make sure the ReplayBroadcastTxBatch
		// callback will succeed. The replay pool calls it continuously
		// until it succeeds (no-error).
		if isReplayMode {
			// TODO(midas): remove debug logs
			app.logger.Debug("Process transaction batch with replay pool",
				"chain_id", chainID,
				"tx_batch", txHashes,
			)

			// Add the transactions to a transaction replay bucket by ChainID.
			if err := app.addToReplayPool(chainID, batch...); err != nil {
				return nil, fmt.Errorf(
					"error adding transaction batch to replay pool for %s: %w", chainID, err)
			}
		} else if hasUncommittedTx {
			// TODO(midas): remove debug logs
			app.logger.Debug("Process transaction batch with acceptor: CommitBroadcastTx",
				"chain_id", chainID,
				"tx_batch", txHashes,
			)

			// In consensus-mode, we should call the CommitBroadcastTx
			// callback once per transaction batch, if the finalized block
			// contains a transaction hash that has not yet been committed.
			if err := app.txAcceptor.CommitBroadcastTx(
				ctx,
				batch...,
			); err != nil {
				app.logger.Error(
					"Acceptor callback CommitBroadcastTx rejected transaction batch",
					"chain_id", chainID,
					"tx_batch", txHashes,
					"err", err,
				)
			} else {
				app.txMutex.Lock()
				for _, tx := range batch {
					txHash := fmt.Sprintf("%X", tx.Hash())
					app.committedTxHashes[txHash] = true
				}
				app.txMutex.Unlock()
			}
		}
	}

	// We can now safely store the finalized block height
	if err := app.setFinalizeBlockHeight(chainID, req.Height); err != nil {
		return nil, fmt.Errorf(
			"could not update block height for %s from FinalizeBlock: %w", chainID, err)
	}

	return &abcitypes.FinalizeBlockResponse{
		TxResults: txResults,
	}, nil
}

func (app *SnapsApp) addToReplayPool(
	chainID string,
	transactions ...client.Transaction,
) error {
	userAddress := client.GetUserAddress(chainID)

	// Register this batch in the reactor's replay pool.
	// Transactions will be added to a transaction bucket by user address.
	replayPool := app.reactor.GetReplayPool()
	replayPool.Add(userAddress, transactions...)
	return nil
}

// CheckTx allows the application to validate transactions and/or discard them.
//
// This method may execute transactions in CheckTx mode, i.e. not actually
// executing messages. Note also that expensive operations should not be run
// here but rather in the commitment stage.
//
// CheckTx implements [abcitypes.Application].
func (app *SnapsApp) CheckTx(
	ctx context.Context,
	req *abcitypes.CheckTxRequest,
) (*abcitypes.CheckTxResponse, error) {
	// Retrieve ChainID from context
	// chainID := ctx.Value(client.KeyChainID).(string)

	// All transactions are valid here.
	return &abcitypes.CheckTxResponse{Code: abcitypes.CodeTypeOK}, nil
}

// Commit may persist the application state if any data is relevant.
//
// This method is called after finalizing blocks.
//
// Commit implements [abcitypes.Application].
func (app *SnapsApp) Commit(
	ctx context.Context,
	req *abcitypes.CommitRequest,
) (*abcitypes.CommitResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	resp := &abcitypes.CommitResponse{
		RetainHeight: 0, // pruning is disabled, so block retention 0.
	}

	// Make sure we handle only relevant commits
	if !app.reactor.HasNetwork(chainID) {
		app.logger.Error("received irrelevant snapshot chain identifier (Commit)", "chain_id", chainID)
		return resp, nil
	}

	app.fbMutex.RLock()
	workingHeight := app.finalizeBlockHeights[chainID]
	app.fbMutex.RUnlock()

	app.logger.Info("Committed block height",
		"height", workingHeight,
		"chain_id", chainID,
	)

	return resp, nil
}

// ExtendVote implements the ExtendVote ABCI method and returns a ResponseExtendVote.
// It calls the application's ExtendVote handler which is responsible for performing
// application-specific business logic when sending a pre-commit for the NEXT
// block height. The extensions response may be non-deterministic but must always
// be returned, even if empty.
//
// Agreed upon vote extensions are made available to the proposer of the next
// height and are committed in the subsequent height, i.e. H+2. An error is
// returned if vote extensions are not enabled or if extendVote fails or panics.
//
// ExtendVote implements [abcitypes.Application].
func (app *SnapsApp) ExtendVote(context.Context, *abcitypes.ExtendVoteRequest) (*abcitypes.ExtendVoteResponse, error) {
	return &abcitypes.ExtendVoteResponse{}, nil
}

// VerifyVoteExtension implements the VerifyVoteExtension ABCI method and returns
// a ResponseVerifyVoteExtension. It calls the applications' VerifyVoteExtension
// handler which is responsible for performing application-specific business
// logic in verifying a vote extension from another validator during the pre-commit
// phase. The response MUST be deterministic. An error is returned if vote
// extensions are not enabled or if verifyVoteExt fails or panics.
// We highly recommend a size validation due to performance degradation,
// see more here https://docs.cometbft.com/v1.0/references/qa/cometbft-qa-38#vote-extensions-testbed
//
// VerifyVoteExtension implements [abcitypes.Application].
func (app *SnapsApp) VerifyVoteExtension(context.Context, *abcitypes.VerifyVoteExtensionRequest) (*abcitypes.VerifyVoteExtensionResponse, error) {
	return &abcitypes.VerifyVoteExtensionResponse{
		Status: abcitypes.VERIFY_VOTE_EXTENSION_STATUS_ACCEPT,
	}, nil
}

// ----------------------------------------------------------------------------
// Snapshotting features are disabled

// ListSnapshots is not supported as state-sync must be disabled.
//
// This method is called when a peer requests for snapshots. Note that the
// response includes only snapshot metadata, not the snapshot chunks.
//
// ListSnapshots implements [abcitypes.Application].
func (app *SnapsApp) ListSnapshots(
	ctx context.Context,
	req *abcitypes.ListSnapshotsRequest,
) (*abcitypes.ListSnapshotsResponse, error) {
	// ListSnapshots is not supported as state-sync must be disabled.
	return &abcitypes.ListSnapshotsResponse{
		Snapshots: []*abcitypes.Snapshot{},
	}, nil
}

// OfferSnapshot is not supported as state-sync must be disabled.
//
// This method is called when a peer received a list of snapshots and chooses
// one to sync. It will initiate the download of snapshot chunks and restore
// the snapshot data. Note that this method does not *apply* the snapshot.
//
// OfferSnapshot implements [abcitypes.Application].
func (app *SnapsApp) OfferSnapshot(
	ctx context.Context,
	req *abcitypes.OfferSnapshotRequest,
) (*abcitypes.OfferSnapshotResponse, error) {
	// OfferSnapshot is not supported as state-sync must be disabled.
	return &abcitypes.OfferSnapshotResponse{
		Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT,
	}, nil
}

// LoadSnapshotChunk is not supported as state-sync must be disabled.
//
// This method is called to retrieve snapshot chunks which may be transported
// to other peers, i.e. asynchronously called when a peer downloads chunks.
//
// LoadSnapshotChunk implements [abcitypes.Application].
func (app *SnapsApp) LoadSnapshotChunk(
	ctx context.Context,
	req *abcitypes.LoadSnapshotChunkRequest,
) (*abcitypes.LoadSnapshotChunkResponse, error) {
	// LoadSnapshotChunk is not supported as state-sync must be disabled.
	return &abcitypes.LoadSnapshotChunkResponse{}, nil
}

// ApplySnapshotChunk is not supported as state-sync must be disabled.
//
// This method is called when a peer received a snapshot chunk (downloaded).
// The downloaded snapshot chunk will be applied with this method, thus
// updating the filesystem. When all snapshot chunks have finished downloading,
// the snapshot will be considered fully applied.
//
// ApplySnapshotChunk implements [abcitypes.Application].
func (app *SnapsApp) ApplySnapshotChunk(
	ctx context.Context,
	req *abcitypes.ApplySnapshotChunkRequest,
) (*abcitypes.ApplySnapshotChunkResponse, error) {
	// ApplySnapshotChunk is not supported as state-sync must be disabled.
	return &abcitypes.ApplySnapshotChunkResponse{
		Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT,
	}, nil
}
