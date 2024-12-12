package proxy

import (
	"context"

	abcicli "github.com/ice-blockchain/cometbft/abci/client"
	abcitypes "github.com/ice-blockchain/cometbft/abci/types"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// -----------------------------------------------------------------------------------------
// ChainConns

// ChainConns is a breaking upgrade to AppConns which passes a ChainID to
// connection methods such that the right connections are used.
//
// Note that only one shared ABCI client is used by all replicated chains.
// On the other hand, we create x connections with the client, one per
// replicated chain. Each of these connections injects the correct ChainID
// to ABCI requests sent through the shared client.
//
// Return types of methods defined by this interface are compatible with
// [AppConns] to prevent breaking the ABCI integration.
type ChainConns interface {
	service.Service

	// Add a network connection pool
	AddNetwork(chainID string)

	// Mempool connection
	Mempool(chainID string) AppConnMempool
	// Consensus connection
	Consensus(chainID string) AppConnConsensus
	// Query connection
	Query(chainID string) AppConnQuery
	// Snapshot connection
	Snapshot(chainID string) AppConnSnapshot

	// Convert to be AppConns compatible
	ToAppConns(chainID string) AppConns
}

// NewChainConns calls NewMultiplexAppConn.
func NewChainConns(
	chainIds []string,
	clientCreator ClientCreator,
	metrics *Metrics,
) ChainConns {
	return NewMultiplexAppConn(chainIds, clientCreator, metrics)
}

// -----------------------------------------------------------------------------------------
// Implements AppConnConsensus (subset of abcicli.Client)

type chainConnConsensus struct {
	ChainID string

	metrics *Metrics
	appConn abcicli.Client
}

var _ AppConnConsensus = (*chainConnConsensus)(nil)

func NewChainConnConsensus(
	chainID string,
	appConn abcicli.Client,
	metrics *Metrics,
) AppConnConsensus {
	return &chainConnConsensus{
		ChainID: chainID,
		metrics: metrics,
		appConn: appConn,
	}
}

func (conn *chainConnConsensus) Error() error {
	return conn.appConn.Error()
}

func (conn *chainConnConsensus) InitChain(
	ctx context.Context,
	req *abcitypes.InitChainRequest,
) (*abcitypes.InitChainResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "init_chain", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.InitChain(ctx, req)
}

func (conn *chainConnConsensus) PrepareProposal(ctx context.Context,
	req *abcitypes.PrepareProposalRequest,
) (*abcitypes.PrepareProposalResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "prepare_proposal", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.PrepareProposal(ctx, req)
}

func (conn *chainConnConsensus) ProcessProposal(ctx context.Context, req *abcitypes.ProcessProposalRequest) (*abcitypes.ProcessProposalResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "process_proposal", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.ProcessProposal(ctx, req)
}

func (conn *chainConnConsensus) ExtendVote(ctx context.Context, req *abcitypes.ExtendVoteRequest) (*abcitypes.ExtendVoteResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "extend_vote", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.ExtendVote(ctx, req)
}

func (conn *chainConnConsensus) VerifyVoteExtension(ctx context.Context, req *abcitypes.VerifyVoteExtensionRequest) (*abcitypes.VerifyVoteExtensionResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "verify_vote_extension", "type", "sync"))()
	// TODO(midas): currently VerifyVoteExtension does not support ChainID
	return conn.appConn.VerifyVoteExtension(ctx, req)
}

func (conn *chainConnConsensus) FinalizeBlock(ctx context.Context, req *abcitypes.FinalizeBlockRequest) (*abcitypes.FinalizeBlockResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "finalize_block", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.FinalizeBlock(ctx, req)
}

func (conn *chainConnConsensus) Commit(ctx context.Context) (*abcitypes.CommitResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "commit", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.Commit(ctx, &abcitypes.CommitRequest{})
}

// ------------------------------------------------
// Implements AppConnMempool (subset of abcicli.Client)

type chainConnMempool struct {
	ChainID string
	metrics *Metrics
	appConn abcicli.Client
}

var _ AppConnMempool = (*chainConnMempool)(nil)

func NewChainConnMempool(
	chainID string,
	appConn abcicli.Client,
	metrics *Metrics,
) AppConnMempool {
	return &chainConnMempool{
		ChainID: chainID,
		metrics: metrics,
		appConn: appConn,
	}
}

// Deprecated: Do not use.
func (conn *chainConnMempool) SetResponseCallback(cb abcicli.Callback) {
	conn.appConn.SetResponseCallback(cb)
}

func (conn *chainConnMempool) Error() error {
	return conn.appConn.Error()
}

func (conn *chainConnMempool) Flush(ctx context.Context) error {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "flush", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.Flush(ctx)
}

func (conn *chainConnMempool) CheckTx(ctx context.Context, req *abcitypes.CheckTxRequest) (*abcitypes.CheckTxResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "check_tx", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.CheckTx(ctx, req)
}

func (conn *chainConnMempool) CheckTxAsync(ctx context.Context, req *abcitypes.CheckTxRequest) (*abcicli.ReqRes, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "check_tx", "type", "async"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.CheckTxAsync(ctx, req)
}

// ------------------------------------------------
// Implements AppConnQuery (subset of abcicli.Client)

type chainConnQuery struct {
	ChainID string
	metrics *Metrics
	appConn abcicli.Client
}

var _ AppConnQuery = (*chainConnQuery)(nil)

func NewChainConnQuery(
	chainID string,
	appConn abcicli.Client,
	metrics *Metrics,
) AppConnQuery {
	return &chainConnQuery{
		ChainID: chainID,
		metrics: metrics,
		appConn: appConn,
	}
}

func (conn *chainConnQuery) Error() error {
	return conn.appConn.Error()
}

func (conn *chainConnQuery) Echo(ctx context.Context, msg string) (*abcitypes.EchoResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "echo", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.Echo(ctx, msg)
}

func (conn *chainConnQuery) Info(ctx context.Context, req *abcitypes.InfoRequest) (*abcitypes.InfoResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "info", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.Info(ctx, req)
}

func (conn *chainConnQuery) Query(ctx context.Context, req *abcitypes.QueryRequest) (*abcitypes.QueryResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "query", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.Query(ctx, req)
}

// ------------------------------------------------
// Implements AppConnSnapshot (subset of abcicli.Client)

type chainConnSnapshot struct {
	ChainID string
	metrics *Metrics
	appConn abcicli.Client
}

var _ AppConnSnapshot = (*chainConnSnapshot)(nil)

func NewChainConnSnapshot(
	chainID string,
	appConn abcicli.Client,
	metrics *Metrics,
) AppConnSnapshot {
	return &chainConnSnapshot{
		ChainID: chainID,
		metrics: metrics,
		appConn: appConn,
	}
}

func (conn *chainConnSnapshot) Error() error {
	return conn.appConn.Error()
}

func (conn *chainConnSnapshot) ListSnapshots(ctx context.Context, req *abcitypes.ListSnapshotsRequest) (*abcitypes.ListSnapshotsResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "list_snapshots", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.ListSnapshots(ctx, req)
}

func (conn *chainConnSnapshot) OfferSnapshot(ctx context.Context, req *abcitypes.OfferSnapshotRequest) (*abcitypes.OfferSnapshotResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "offer_snapshot", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.OfferSnapshot(ctx, req)
}

func (conn *chainConnSnapshot) LoadSnapshotChunk(ctx context.Context, req *abcitypes.LoadSnapshotChunkRequest) (*abcitypes.LoadSnapshotChunkResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "load_snapshot_chunk", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.LoadSnapshotChunk(ctx, req)
}

func (conn *chainConnSnapshot) ApplySnapshotChunk(ctx context.Context, req *abcitypes.ApplySnapshotChunkRequest) (*abcitypes.ApplySnapshotChunkResponse, error) {
	defer addTimeSample(conn.metrics.MethodTimingSeconds.With("method", "apply_snapshot_chunk", "type", "sync"))()
	ctx = context.WithValue(ctx, client.KeyChainID, conn.ChainID)
	return conn.appConn.ApplySnapshotChunk(ctx, req)
}
