package client

import "context"

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
	TxAcceptCalls int
	TxCommitCalls int
	TxReplayCalls int
	TxRemoveCalls int
	RbAcceptCalls int
	RbRemoveCalls int
}

func NewMockAcceptorImpl() *MockAcceptorImpl {
	return &MockAcceptorImpl{
		TxAcceptCalls: 0,
		TxCommitCalls: 0,
		TxReplayCalls: 0,
		TxRemoveCalls: 0,
		RbAcceptCalls: 0,
		RbRemoveCalls: 0,
	}
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTx(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.TxAcceptCalls++
	return nil
}

// CommitBroadcastTx returns an error if any of the transactions
// should not be committed.
func (acceptor *MockAcceptorImpl) CommitBroadcastTx(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.TxCommitCalls++
	return nil
}

// ReplayBroadcastTxBatch returns an error if any of the transactions
// could not be added to a replay batch or if the replay fails.
func (acceptor *MockAcceptorImpl) ReplayBroadcastTxBatch(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.TxReplayCalls++
	return nil
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTxRemoval(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.TxRemoveCalls++
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTx(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.RbAcceptCalls++
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTxRemoval(
	_ context.Context,
	_ ...Transaction,
) error {
	acceptor.RbRemoveCalls++
	return nil
}
