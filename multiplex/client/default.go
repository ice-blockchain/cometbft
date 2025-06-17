package client

import (
	"context"
	"sync/atomic"
)

type (
	DefaultAcceptor struct{}
	DefaultClient   struct{}
	DefaultServer   struct{}
)

var (
	_ Acceptor = (*DefaultAcceptor)(nil)
	_ Acceptor = (*MockAcceptorImpl)(nil)
	_ Client   = (*DefaultClient)(nil)
)

// AcceptBroadcastTx returns an error if any of the transactions
// should not be accepted, or if the batch must not be broadcast.
func (DefaultAcceptor) AcceptBroadcastTx(
	_ context.Context,
	_ ...Transaction,
) error {
	return nil
}

// CommitBroadcastTx returns an error if any of the transactions
// should not be committed.
func (DefaultAcceptor) CommitBroadcastTx(
	_ context.Context,
	_ ...Transaction,
) error {
	return nil
}

// ReplayBroadcastTxBatch returns an error if any of the transactions
// could not be added to a replay batch or if the replay fails.
func (DefaultAcceptor) ReplayBroadcastTxBatch(
	ctx context.Context,
	transactions ...Transaction,
) error {
	return nil
}

// AcceptBroadcastTxRemoval returns an error if any of the transactions
// should not be accepted, or if the batch must not be broadcast.
func (DefaultAcceptor) AcceptBroadcastTxRemoval(
	_ context.Context,
	_ ...Transaction,
) error {
	return nil
}

// RollbackTx should execute custom business logic such as removing data
// previously committed for a transaction batch that is being rollbacked.
func (DefaultAcceptor) RollbackTx(
	_ context.Context,
	_ ...Transaction,
) error {
	return nil
}

// RollbackTxRemoval should execute custom business logic such as removing
// data previously committed for a removal operation that is rollbacked.
func (DefaultAcceptor) RollbackTxRemoval(
	_ context.Context,
	_ ...Transaction,
) error {
	return nil
}

// BroadcastTx sends a status to a notifier if any of the transactions
// fails basic verification, or if we fail to get a majority approval
// for the broadcast operation from healthy relays.
func (DefaultClient) BroadcastTx(
	_ context.Context,
	_ string,
	_ []string,
	_ chan<- BroadcastStatus,
	_ ...Transaction,
) {
}

// BroadcastTxRemoval sends a status to a notifier if any of the removal
// operations fail verification, or if we fail to get a majority approval
// for the broadcast operation from healthy relays.
func (DefaultClient) BroadcastTxRemoval(
	_ context.Context,
	_ string,
	_ []string,
	_ chan<- BroadcastStatus,
	_ ...Transaction,
) {
}

// Close implements io.Closer
func (DefaultServer) Close() error {
	return nil
}

// GetAcceptor returns the injected [Acceptor] implementation.
func (DefaultServer) GetAcceptor() Acceptor {
	return &DefaultAcceptor{}
}

// MustStart must start a replication backend or return an error.
func (DefaultServer) MustStart() {}

// ----------------------------------------------------------------------------
// Mocks

type MockAcceptorImpl struct {
	TxAcceptCalls atomic.Uint64
	TxCommitCalls atomic.Uint64
	TxReplayCalls atomic.Uint64
	TxRemoveCalls atomic.Uint64
	RbAcceptCalls atomic.Uint64
	RbRemoveCalls atomic.Uint64
}

func NewMockAcceptorImpl() *MockAcceptorImpl {
	return &MockAcceptorImpl{}
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTx(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.TxAcceptCalls.Add(uint64(len(batch)))
	return nil
}

// CommitBroadcastTx returns an error if any of the transactions
// should not be committed.
func (acceptor *MockAcceptorImpl) CommitBroadcastTx(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.TxCommitCalls.Add(uint64(len(batch)))
	return nil
}

// ReplayBroadcastTxBatch returns an error if any of the transactions
// could not be added to a replay batch or if the replay fails.
func (acceptor *MockAcceptorImpl) ReplayBroadcastTxBatch(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.TxReplayCalls.Add(uint64(len(batch)))
	return nil
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTxRemoval(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.TxRemoveCalls.Add(uint64(len(batch)))
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTx(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.RbAcceptCalls.Add(uint64(len(batch)))
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTxRemoval(
	_ context.Context,
	batch ...Transaction,
) error {
	acceptor.RbRemoveCalls.Add(uint64(len(batch)))
	return nil
}
