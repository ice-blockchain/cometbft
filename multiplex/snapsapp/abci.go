package snapsapp

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"

	abcitypes "github.com/ice-blockchain/cometbft/abci/types"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	snapshottypes "github.com/ice-blockchain/cometbft/multiplex/snapshots/types"
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

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		return nil, fmt.Errorf("invalid chain-id on InitChain: %s is not replicated", chainID)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		return &abcitypes.InitChainResponse{}, nil
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
	app.chMutex.Lock()
	if req.InitialHeight > 1 {
		app.currentHeights[chainID] = req.InitialHeight
	} else {
		app.currentHeights[chainID] = 0
	}
	app.chMutex.Unlock()
	app.logger.Info("InitChain", "initialHeight", req.InitialHeight, "chainID", req.ChainId)

	// Get the commitment snapshotter instance to retrieve current AppHash
	snapshotter := app.snapshotManagers[req.ChainId].GetSnapshotter()

	// NOTE: We don't commit, but FinalizeBlock for block InitialHeight starts from
	// this FinalizeBlockState.
	return &abcitypes.InitChainResponse{
		ConsensusParams: req.ConsensusParams,
		Validators:      req.Validators,
		AppHash:         snapshotter.AppHash(),
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

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		app.logger.Error("snapshot manager not configured (Info)", "chain_id", chainID)
		return &abcitypes.InfoResponse{}, nil
	}

	snapshotter := app.snapshotManagers[chainID].GetSnapshotter()
	stateMachine, err := snapshotter.GetStateMachine()
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
// Snapshots

// ListSnapshots delegates to the correct snapshot manager to list recent
// snapshots for the requested replicated chain.
//
// This method is called when a peer requests for snapshots. Note that the
// response includes only snapshot metadata, not the snapshot chunks.
//
// ListSnapshots implements [abcitypes.Application].
func (app *SnapsApp) ListSnapshots(
	ctx context.Context,
	req *abcitypes.ListSnapshotsRequest,
) (*abcitypes.ListSnapshotsResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	resp := &abcitypes.ListSnapshotsResponse{Snapshots: []*abcitypes.Snapshot{}}

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		return nil, fmt.Errorf("invalid chain-id on ListSnapshots: %s is not replicated", chainID)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		app.logger.Error("snapshot manager not configured (ListSnapshots)", "chain_id", chainID)
		return resp, nil
	}

	// Read recent snapshots from filesystem
	snapshots, err := app.snapshotManagers[chainID].List()
	if err != nil {
		app.logger.Error("failed to list snapshots", "chain_id", chainID, "err", err)
		return nil, err
	}

	// Marshal snapshots for ABCI transport
	for _, snapshot := range snapshots {
		abciSnapshot, err := snapshot.ToABCI()
		if err != nil {
			app.logger.Error("failed to convert ABCI snapshots", "chain_id", chainID, "err", err)
			return nil, err
		}

		resp.Snapshots = append(resp.Snapshots, &abciSnapshot)
	}

	return resp, nil
}

// OfferSnapshot unmarshals snapshot metadata and starts the restoration of
// snapshot chunks (download).
//
// This method is called when a peer received a list of snapshots and chooses
// one to sync. It will initiate the download of snapshot chunks and restore
// the snapshot data. Note that this method does not *apply* the snapshot.
//
// ListSnapshots implements [abcitypes.Application].
func (app *SnapsApp) OfferSnapshot(
	ctx context.Context,
	req *abcitypes.OfferSnapshotRequest,
) (*abcitypes.OfferSnapshotResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		app.logger.Error("received irrelevant snapshot chain identifier (OfferSnapshot)", "chain_id", chainID)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		app.logger.Error("snapshot manager not configured (OfferSnapshot)", "chain_id", chainID)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}

	// Obviously we do not permit nil-snapshots
	if req.Snapshot == nil {
		app.logger.Error("received nil snapshot", "chain_id", chainID)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil
	}

	// Unmarshal snapshots from ABCI transport
	snapshot, err := snapshottypes.SnapshotFromABCI(req.Snapshot)
	if err != nil {
		app.logger.Error("failed to decode snapshot metadata", "chain_id", chainID, "err", err)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil
	}

	// Verifies snapshot format and start downloading chunks
	err = app.snapshotManagers[chainID].Restore(snapshot)
	switch {
	case err == nil:
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ACCEPT}, nil

	case errors.Is(err, snapshottypes.ErrUnknownFormat):
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT_FORMAT}, nil

	case errors.Is(err, snapshottypes.ErrInvalidMetadata):
		app.logger.Error(
			"rejecting invalid snapshot",
			"chain_id", chainID,
			"height", req.Snapshot.Height,
			"format", req.Snapshot.Format,
			"err", err,
		)
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_REJECT}, nil

	default:
		// CometBFT errors are defined here: https://github.com/ice-blockchain/cometbft/blob/main/statesync/syncer.go
		// It may happen that in case of a CometBFT error, such as a timeout (which occurs after two minutes),
		// the process is aborted. This is done intentionally because deleting the database programmatically
		// can lead to more complicated situations.
		app.logger.Error(
			"failed to restore snapshot",
			"chain_id", chainID,
			"height", req.Snapshot.Height,
			"format", req.Snapshot.Format,
			"err", err,
		)

		// We currently don't support resetting the IAVL stores and retrying a
		// different snapshot, so we ask CometBFT to abort all snapshot restoration.
		return &abcitypes.OfferSnapshotResponse{Result: abcitypes.OFFER_SNAPSHOT_RESULT_ABORT}, nil
	}
}

// LoadSnapshotChunk delegates to the correct snapshot manager to load a chunk
// from the filesystem.
//
// This method is called to retrieve snapshot chunks which may be transported
// to other peers, i.e. asynchronously called when a peer downloads chunks.
//
// LoadSnapshotChunk implements [abcitypes.Application].
func (app *SnapsApp) LoadSnapshotChunk(
	ctx context.Context,
	req *abcitypes.LoadSnapshotChunkRequest,
) (*abcitypes.LoadSnapshotChunkResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		return nil, fmt.Errorf("invalid chain-id on ListSnapshots: %s is not replicated", chainID)
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		return &abcitypes.LoadSnapshotChunkResponse{}, nil
	}

	chunk, err := app.snapshotManagers[chainID].LoadChunk(req.Height, req.Format, req.Chunk)
	if err != nil {
		app.logger.Error(
			"failed to load snapshot chunk",
			"chain_id", chainID,
			"height", req.Height,
			"format", req.Format,
			"chunk", req.Chunk,
			"err", err,
		)
		return nil, err
	}

	return &abcitypes.LoadSnapshotChunkResponse{Chunk: chunk}, nil
}

// ApplySnapshotChunk applies snapshot chunks sequentially.
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
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Make sure we handle only relevant snapshotting routines
	if !app.reactor.HasNetwork(chainID) {
		app.logger.Error("received irrelevant snapshot chain identifier (ApplySnapshotChunk)", "chain_id", chainID)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		app.logger.Error("snapshot manager not configured (ApplySnapshotChunk)", "chain_id", chainID)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}

	// Applies snapshot chunk in order (updates filesystem)
	_, err := app.snapshotManagers[chainID].RestoreChunk(req.Chunk)
	switch {
	case err == nil:
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ACCEPT}, nil

	case errors.Is(err, snapshottypes.ErrChunkHashMismatch):
		app.logger.Error(
			"chunk checksum mismatch; rejecting sender and requesting refetch",
			"chain_id", chainID,
			"chunk", req.Index,
			"sender", req.Sender,
			"err", err,
		)
		return &abcitypes.ApplySnapshotChunkResponse{
			Result:        abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_RETRY,
			RefetchChunks: []uint32{req.Index},
			RejectSenders: []string{req.Sender},
		}, nil

	default:
		app.logger.Error("failed to restore snapshot", "chain_id", chainID, "err", err)
		return &abcitypes.ApplySnapshotChunkResponse{Result: abcitypes.APPLY_SNAPSHOT_CHUNK_RESULT_ABORT}, nil
	}
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
// CAUTION:
// This method executes the [client.PrepareProposalExtensionFn] and catches
// potential panics from the extension's runtime. Importantly, in case of a
// failing PrepareProposal extension, **blocks proposals cannot be created**.
// This is notably to ensure that important client mutations and/or reports
// are *always* prioritized and that data is hereby consistently originating
// or audited/reported from the client (extension/hook) implementation.
// Note, the default (example) implementation for the PrepareProposal extension
// returns a deep-copy of the original request's transaction bytes.
//
// PrepareProposal implements [abcitypes.Application].
func (app *SnapsApp) PrepareProposal(
	ctx context.Context,
	req *abcitypes.PrepareProposalRequest,
) (*abcitypes.PrepareProposalResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

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

	// Prepare the contract for PrepareProposal extensions
	var preparedTxes [][]byte

	// Executes prepare proposal extension and catches potential panics to ensure
	// data consistency, also making a compromise on availability.
	//
	// Catch recovered errors from prepare proposal extension and stop.
	// When a prepare proposal extension fails, the block proposal cannot be
	// created because the extension determines the filtering of transactions
	// in prepared blocks proposals.
	preparedTxes, err := app.runPrepareProposalExtension(chainID, txs)
	if err != nil {
		return nil, fmt.Errorf(
			"CLIENT PANIC: failing prepare proposal extension: %w", err)
	}

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
// If a panic is detected during execution of an application's ProcessProposal
// handler, it will be recovered and we will reject the proposal.
//
// CAUTION:
// This method executes the [client.ProcessProposalExtensionFn] and catches
// potential panics from the extension's runtime. Importantly, in case of a
// failing ProcessProposal extension, this method returns ACCEPT.
// Note, the default (example) implementation for the ProcessProposal extension
// returns a deep-copy of the original request's transaction bytes.
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
	app.chMutex.Lock()
	app.currentHeights[chainID] = req.Height
	app.chMutex.Unlock()

	// Executes process proposal extension and catches potential panics to
	// ensure that any errors *do not* influence the processing stage.
	//
	// The reason for this is that the data written in a *processed* blocks
	// proposal cannot be modified from here, and must be done in PrepareProposal.
	_, err := app.runProcessProposalExtension(chainID, req.Txs)
	if err != nil {
		app.logger.Error("CLIENT PANIC: failing process proposal extension:", "err", err.Error())
		// Proceed with processing stage!
	}

	return &abcitypes.ProcessProposalResponse{Status: abcitypes.PROCESS_PROPOSAL_STATUS_ACCEPT}, nil
}

// FinalizeBlock will execute the block proposal provided by FinalizeBlockRequest.
//
// Currently we do not perform any filtering with transactions, thus the
// transactions are all deemed to be "valid".
//
// The finalizeBlockHeights is updated for the relevant chain such that the
// subsequent Commit() ABCI with the same ChainID may know which *height* is
// being finalized. This height is used to determine whether a snapshot must
// be taken or not.
//
// CAUTION:
// This method executes the [client.FinalizeBlockExtensionFn] and catches
// potential panics from the extension's runtime. Importantly, in case of a
// failing FinalizeBlock extension, **blocks cannot be finalized**.
// This is notably to ensure that important client mutations and/or reports
// are *always* prioritized and that data is hereby consistently originating
// or audited/reported from the client (extension/hook) implementation.
// Note, the default (example) implementation for the FinalizeBlock extension
// returns a deep-copy of the original request's transaction bytes.
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
		return resp, fmt.Errorf("invalid chain-id on FinalizeBlock: %s is not replicated", chainID)
	}

	// Prepare the contract for FinalizeBlock extensions
	var processedTxs [][]byte

	// Executes finalize block extension and catches potential panics to ensure
	// data consistency, also making a compromise on availability.
	//
	// Catch recovered errors from finalize block extension and stop.
	// When a finalize block extension fails, the block cannot be finalized
	// because the extension determines the filtering of transactions in blocks.
	processedTxs, err := app.runFinalizeBlockExtension(chainID, req.Txs)
	if err != nil {
		return resp, fmt.Errorf(
			"CLIENT PANIC: failing finalize block extension: %w", err)
	}

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

	// We can now safely store the finalized block height
	if err := app.setFinalizeBlockHeight(chainID, req.Height); err != nil {
		return nil, fmt.Errorf("could not update block height for %s from FinalizeBlock: %w", chainID, err)
	}

	return &abcitypes.FinalizeBlockResponse{
		TxResults: txResults,
	}, nil
}

// CheckTx allows the application to validate transactions and/or discard them.
//
// This method may execute transactions in CheckTx mode, i.e. not actually
// executing messages. Note also that expensive operations should not be run
// here but rather in the commitment stage.
//
// CAUTION:
// We use [client.DelegateCheckTx] to delegate the CheckTx call to a potential
// extension. It is important to note that if the extension fails, the returned
// response [abcitype.CheckTxResponse] will contain an error code and the
// transaction **will not be accepted**.
// Note, the default (example) implementation for the CheckTx extension
// always returns nil, such that **all transactions are valid**.
//
// CheckTx implements [abcitypes.Application].
func (app *SnapsApp) CheckTx(
	ctx context.Context,
	req *abcitypes.CheckTxRequest,
) (*abcitypes.CheckTxResponse, error) {
	// Retrieve ChainID from context
	chainID := ctx.Value(client.KeyChainID).(string)

	// Executes commit extension and catches potential panics to ensure
	// data consistency. If the extension returns an error or fails,
	// the transaction will be considered *invalid*.
	//
	// When a commit extension fails, the response [abcitype.CheckTxResponse]
	// will contain an error code and the transaction **will not be accepted**.
	err := app.runCheckTxExtension(chainID, req.Tx)
	// If the extension returns an error, we do not accept this transaction.
	if err != nil {
		return &abcitypes.CheckTxResponse{Code: CodeTypeErrCheckTxFailure}, err
	}

	// If the extensions returns nil, we accept the transaction.
	return &abcitypes.CheckTxResponse{Code: abcitypes.CodeTypeOK}, err
}

// Commit may persist the application state if any data is relevant and it must
// also determine whether a snapshot must be created, or not.
//
// This method is called after finalizing blocks. This method uses the snapshot
// manager to determine whether a new snapshot must be taken or not, based on
// the `interval` set in the [config.SnapshotOptions] instance for this chain.
//
// CAUTION:
// We use [client.ReportCommit] to enable auditing or reporting about committed
// blocks from a potential extension. It is important to note that if the
// extension fails, the commitment stage is *unaffected*, i.e. an error in
// auditing or reporting units *does not* affect commitment stage(s).
// Note, the default (example) implementation for the Commit extension
// always returns nil, such that **all committed blocks are accepted**.
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

	// Without snapshotter for this chain, we stop here
	if _, ok := app.snapshotManagers[chainID]; !ok {
		app.logger.Error("snapshot manager not configured (Commit)", "chain_id", chainID)
		return resp, nil
	}

	app.fbMutex.RLock()
	workingHeight := app.finalizeBlockHeights[chainID]
	app.fbMutex.RUnlock()

	// Executes commit extension and catches potential panics to ensure that
	// any errors *do not* influence the commitment stage.
	//
	// The reason for this is that the data written in a *committed* blocks
	// cannot be modified from here and the response is always RetainHeight=0.
	errCommitHook := app.runCommitExtension(chainID, uint64(workingHeight))
	if errCommitHook != nil {
		app.logger.Error("CLIENT PANIC: failing commit extension:", "err", errCommitHook.Error())
		// Proceed with commitment stage!
	}

	app.snapshotManagers[chainID].SnapshotIfApplicable(workingHeight)
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
// Extensions / Hooks

// runCheckTxExtension executes a checktx extension. This method
// uses the checkTxExtension on the instance if available, or otherwise
// it uses the extension configured as the *default*.
//
// Executes checktx extension and catches potential panics to ensure
// data consistency, also making a compromise on availability.
// When a checktx extension fails, the transaction won't be accepted.
//
// See also: [GetCheckTxExtension], [WithCheckTxExtension].
func (app *SnapsApp) runCheckTxExtension(
	chainID string,
	transaction []byte,
) error {
	// Prepare the contract for CheckTx extensions
	var checkTxHook client.CheckTxExtensionFn

	// Uses the default extension or the one configured
	checkTxHook = GetCheckTxExtension()
	if app.checkTxExtension != nil {
		checkTxHook = app.checkTxExtension
	}

	// Executes checktx extension and catches potential panics to ensure
	// data consistency, also making a compromise on availability.
	//
	// When a checktx extension fails, the transaction won't be accepted.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a checktx extension. This recovery ensures *data consistency*
		// in case of failing checktx extensions, and thus breaks the
		// transaction verification process until the checktx extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in prepare proposal extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. all transactions valid
		// see `snapsapp/client.go` to use a custom transactions auditing extension.
		// See also [WithCheckTxExtension].
		err = client.DelegateCheckTx(
			chainID,
			transaction,
			checkTxHook,
		)
		return err
	}()

	return err
}

// runPrepareProposalExtension executes a prepare proposal extension. This method
// uses the prepareProposalExtension on the instance if available, or otherwise
// it uses the extension configured as the *default*.
//
// Executes prepare proposal extension and catches potential panics to ensure
// data consistency, also making a compromise on availability.
// When a prepare proposal extension fails, the block proposal cannot be
// created because the extension determines the filtering of transactions
// in prepared blocks proposals.
//
// See also: [GetPrepareProposalExtension], [WithPrepareProposalExtension].
func (app *SnapsApp) runPrepareProposalExtension(
	chainID string,
	transactions [][]byte,
) ([][]byte, error) {
	// Prepare the contract for finalize block extensions
	var mutatedTransactions [][]byte
	var prepareProposalHook client.PrepareProposalExtensionFn

	// Uses the default extension or the one configured
	prepareProposalHook = GetPrepareProposalExtension()
	if app.prepareProposalExtension != nil {
		prepareProposalHook = app.prepareProposalExtension
	}

	// Executes prepare proposal extension and catches potential panics to ensure
	// data consistency, also making a compromise on availability.
	//
	// When a prepare proposal extension fails, the block proposal cannot be
	// created because the extension determines the filtering of transactions.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a prepare proposal extension. This recovery ensures *data consistency*
		// in case of failing prepare proposal extensions, and thus breaks the
		// blocks proposal process until the prepare proposal extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in prepare proposal extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. return nil (no-error)
		// See `multiplex/client.go` to use a custom prepare proposal extension.
		// See also [WithPrepareProposalExtension].
		mutatedTransactions = client.InjectPrepareProposal(
			chainID,
			transactions,
			prepareProposalHook,
		)

		return err
	}()
	// Catch recovered errors from prepare proposal extension and stop.
	// When a prepare proposal extension fails, the block proposal cannot be
	// created because the extension determines the filtering of transactions.
	if err != nil {
		return [][]byte{}, err
	}

	// No errors happened, we can safely use the mutated transaction bytes
	return mutatedTransactions, nil
}

// runProcessProposalExtension executes a process proposal extension. This method
// uses the processProposalExtension on the instance if available, or otherwise
// it uses the extension configured as the *default*.
//
// Executes process proposal extension and catches potential panics to ensure
// that failing extensions do not influence the processing stage.
// i.e. Failing extensions do not affect the acceptance of a block proposal.
//
// See also: [GetProcessProposalExtension], [WithProcessProposalExtension].
func (app *SnapsApp) runProcessProposalExtension(
	chainID string,
	transactions [][]byte,
) ([][]byte, error) {
	// Prepare the contract for finalize block extensions
	var mutatedTransactions [][]byte
	var processProposalHook client.ProcessProposalExtensionFn

	// Uses the default extension or the one configured
	processProposalHook = GetProcessProposalExtension()
	if app.processProposalExtension != nil {
		processProposalHook = app.processProposalExtension
	}

	// Executes process proposal extension and catches potential panics to ensure
	// that failing extensions do not influence the processing stage.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a process proposal extension.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in process proposal extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. return nil (no-error)
		// See `multiplex/client.go` to use a custom process proposal extension.
		// See also [WithProcessProposalExtension].
		mutatedTransactions = client.InjectProcessProposal(
			chainID,
			transactions,
			processProposalHook,
		)

		return err
	}()
	// Catch recovered errors from process proposal extension.
	if err != nil {
		return [][]byte{}, err
	}

	// No errors happened, we can safely use the mutated transactions bytes slices
	return mutatedTransactions, nil
}

// runFinalizeBlockExtension executes a finalize block extension. This method
// uses the finalizeBlockExtension on the instance if available, or otherwise
// it uses the extension configured as the *default*.
//
// Executes finalize block extension and catches potential panics to ensure
// data consistency, also making a compromise on availability.
// When a finalize block extension fails, the block cannot be finalized
// because the extension determines the filtering of transactions in blocks.
//
// See also: [GetFinalizeBlockExtension], [WithFinalizeBlockExtension].
func (app *SnapsApp) runFinalizeBlockExtension(
	chainID string,
	transactions [][]byte,
) ([][]byte, error) {
	// Prepare the contract for finalize block extensions
	var mutatedTransactions [][]byte
	var finalizeBlockHook client.FinalizeBlockExtensionFn

	// Uses the default extension or the one configured
	finalizeBlockHook = GetFinalizeBlockExtension()
	if app.finalizeBlockExtension != nil {
		finalizeBlockHook = app.finalizeBlockExtension
	}

	// Executes finalize block extension and catches potential panics to ensure
	// data consistency, also making a compromise on availability.
	//
	// When a finalize block extension fails, the block cannot be finalized
	// because the extension determines the filtering of transactions in blocks.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a finalize block extension. This recovery ensures *data consistency*
		// in case of failing finalize block extensions, and thus breaks the
		// blocks finalizing process until the finalize block extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in finalize block extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. return nil (no-error)
		// See `multiplex/client.go` to use a custom finalize block extension.
		// See also [WithFinalizeBlockExtension].
		mutatedTransactions = client.InjectFinalizeBlock(
			chainID,
			transactions,
			finalizeBlockHook,
		)

		return err
	}()
	// Catch recovered errors from finalize block extension and stop.
	// When a finalize block extension fails, the block cannot be finalized
	// because the extension determines the filtering of transactions in blocks.
	if err != nil {
		return [][]byte{}, err
	}

	// No errors happened, we can safely use the mutated transaction bytes
	return mutatedTransactions, nil
}

// runCommitExtension executes a commit extension. This method uses the
// commitExtension on the instance if available, or otherwise it uses the
// extension configured as the *default*.
//
// Executes commit extension and catches potential panics to ensure that
// any errors *do not* influence the commitment stage.
//
// See also: [GetCommitExtension], [WithCommitExtension].
func (app *SnapsApp) runCommitExtension(
	chainID string,
	committedHeight uint64,
) error {
	// Prepare the contract for commit extensions
	var commitHook client.CommitExtensionFn

	// Uses the default extension or the one configured
	commitHook = GetCommitExtension()
	if app.commitExtension != nil {
		commitHook = app.commitExtension
	}

	// Executes commit extension and catches potential panics to ensure that
	// any errors *do not* influence the commitment stage.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a commit extension. This recovery ensures that the reporting of commit
		// does not influence the actual commitment stage.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in commit extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. return nil (no-error)
		// See `multiplex/client.go` to use a custom commit extension.
		// See also [WithCommitExtension].
		err = client.ReportCommit(
			chainID,
			committedHeight,
			commitHook,
		)

		return err
	}()

	// Catch recovered errors from commit extension and return error.
	// When a commit extension fails, it should not affect the commitment stage.
	return err
}
