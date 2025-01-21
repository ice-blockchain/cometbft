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
	_ string,
	_ ...Transaction,
) error {
	return nil
}

// AcceptBroadcastTxRemoval returns an error if any of the transactions
// should not be accepted, or if the batch must not be broadcast.
func (DefaultAcceptor) AcceptBroadcastTxRemoval(
	_ context.Context,
	_ string,
	_ ...Transaction,
) error {
	return nil
}

// RollbackTx should execute custom business logic such as removing data
// previously committed for a transaction batch that is being rollbacked.
func (DefaultAcceptor) RollbackTx(
	_ context.Context,
	_ string,
	_ ...Transaction,
) error {
	return nil
}

// RollbackTxRemoval should execute custom business logic such as removing
// data previously committed for a removal operation that is rollbacked.
func (DefaultAcceptor) RollbackTxRemoval(
	_ context.Context,
	_ string,
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
	TxAcceptCallsByAddress map[string]int
	TxAcceptBytesByAddress map[string][][]byte

	TxRemoveCallsByAddress map[string]int
	TxRemoveBytesByAddress map[string][][]byte

	RbAcceptCallsByAddress map[string]int
	RbAcceptBytesByAddress map[string][][]byte

	RbRemoveCallsByAddress map[string]int
	RbRemoveBytesByAddress map[string][][]byte
}

func NewMockAcceptorImpl() *MockAcceptorImpl {
	return &MockAcceptorImpl{
		TxAcceptCallsByAddress: map[string]int{},
		TxAcceptBytesByAddress: map[string][][]byte{},

		TxRemoveCallsByAddress: map[string]int{},
		TxRemoveBytesByAddress: map[string][][]byte{},

		RbAcceptCallsByAddress: map[string]int{},
		RbAcceptBytesByAddress: map[string][][]byte{},

		RbRemoveCallsByAddress: map[string]int{},
		RbRemoveBytesByAddress: map[string][][]byte{},
	}
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTx(
	_ context.Context,
	userAddress string,
	transactions ...Transaction,
) error {
	acceptor.TxAcceptCallsByAddress = acceptor.addCall(
		acceptor.TxAcceptCallsByAddress,
		userAddress,
	)
	acceptor.TxAcceptBytesByAddress = acceptor.addBytes(
		acceptor.TxAcceptBytesByAddress,
		userAddress,
		transactions...,
	)
	return nil
}

func (acceptor *MockAcceptorImpl) AcceptBroadcastTxRemoval(
	_ context.Context,
	userAddress string,
	transactions ...Transaction,
) error {
	acceptor.TxRemoveCallsByAddress = acceptor.addCall(
		acceptor.TxRemoveCallsByAddress,
		userAddress,
	)
	acceptor.TxRemoveBytesByAddress = acceptor.addBytes(
		acceptor.TxRemoveBytesByAddress,
		userAddress,
		transactions...,
	)
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTx(
	_ context.Context,
	userAddress string,
	transactions ...Transaction,
) error {
	acceptor.RbAcceptCallsByAddress = acceptor.addCall(
		acceptor.RbAcceptCallsByAddress,
		userAddress,
	)
	acceptor.RbAcceptBytesByAddress = acceptor.addBytes(
		acceptor.RbAcceptBytesByAddress,
		userAddress,
		transactions...,
	)
	return nil
}

func (acceptor *MockAcceptorImpl) RollbackTxRemoval(
	_ context.Context,
	userAddress string,
	transactions ...Transaction,
) error {
	acceptor.RbRemoveCallsByAddress = acceptor.addCall(
		acceptor.RbRemoveCallsByAddress,
		userAddress,
	)
	acceptor.RbRemoveBytesByAddress = acceptor.addBytes(
		acceptor.RbRemoveBytesByAddress,
		userAddress,
		transactions...,
	)
	return nil
}

func (acceptor *MockAcceptorImpl) addCall(
	target map[string]int,
	userAddress string,
) map[string]int {
	if _, ok := target[userAddress]; !ok {
		target[userAddress] = 0
	}

	target[userAddress]++
	return target
}

func (acceptor *MockAcceptorImpl) addBytes(
	target map[string][][]byte,
	userAddress string,
	transactions ...Transaction,
) map[string][][]byte {
	if _, ok := target[userAddress]; !ok {
		target[userAddress] = [][]byte{}
	}

	for _, tx := range transactions {
		rawTx := TransactionToRawTx(tx)
		target[userAddress] = append(
			target[userAddress],
			rawTx.Hash(),
		)
	}

	return target
}
