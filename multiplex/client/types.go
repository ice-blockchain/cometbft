package client

import (
	"context"
)

// ContextKey defines a string-based context value key.
type ContextKey string

// We will attach a user address and ChainID inside the context.
const (
	KeyAddress ContextKey = "Address"
	KeyChainID ContextKey = "ChainID"
)

// Transaction defines a wrapper for data attached to a fingerprint.
//
// Note that transaction fingerprints *may* hold plaintext scope names,
// e.g. "posts", "likes". Privacy shall not be a concern here because the
// ChainID is built using the user address and a *fingerprint hash*.
type Transaction struct {
	Data        []byte
	Fingerprint string
}

// BroadcastStatus defines a wrapper for transaction broadcast status
// which may contain an error and metadata about the current step.
type BroadcastStatus struct {
	Error    error
	TxHashes [][]byte
}

// Error pushes a [BroadcastStatus] on the channel and attaches the error.
func Error(
	ch chan<- BroadcastStatus,
	err error,
) {
	ch <- BroadcastStatus{
		Error:    err,
		TxHashes: [][]byte{},
	}
}

// Success pushes a [BroadcastStatus] on the channel and attaches a nil-error
// and a list of accepted transaction hashes. The transaction hashes attached
// are guaranteed to have been included in a block by a consensus instance.
func Success(
	ch chan<- BroadcastStatus,
	txHashes [][]byte,
) {
	ch <- BroadcastStatus{
		Error:    nil,
		TxHashes: txHashes,
	}
}

// Acceptor defines the contract for client-side transactions verification.
//
// An acceptor instance is injected in a [Client] to perform pre-committing
// verification of transactions data. If the acceptor method returns an error,
// the transactions data must be discarded entirely.
//
// The acceptor shall always receive a batch of transactions which may concern
// one or more than one independent cometbft network.
//
// See also: [Client].
type Acceptor interface {
	// AcceptBroadcastTx returns an error if any of the transactions
	// should not be accepted, or if the batch must not be broadcast.
	AcceptBroadcastTx(
		ctx context.Context,
		userAddress string,
		transactions ...Transaction,
	) error

	// AcceptBroadcastTxRemoval returns an error if any of the transactions
	// should not be accepted, or if the batch must not be broadcast.
	AcceptBroadcastTxRemoval(
		ctx context.Context,
		userAddress string,
		transactions ...Transaction,
	) error

	// RollbackTx should execute custom business logic such as removing data
	// previously committed for a transaction batch that is being rollbacked.
	RollbackTx(
		ctx context.Context,
		userAddress string,
		transactions ...Transaction,
	) error

	// RollbackTxRemoval should execute custom business logic such as removing
	// data previously committed for a removal operation that is rollbacked.
	RollbackTxRemoval(
		ctx context.Context,
		userAddress string,
		transactions ...Transaction,
	) error
}

// Client defines the contract for multiplex client implementations.
//
// A client instance may be used to perform on-demand consensus instances
// using a predefined list of active relays running a cometbft network.
//
// Transactions that are broadcast using a client instance will be broadcast
// to other relays, then verified, before they are committed to a network.
//
// See also: [Acceptor].
type Client interface {
	// BroadcastTx sends an error to a notifier if any of the transactions
	// fails basic verification, or if we fail to get a majority approval
	// for the broadcast operation from healthy relays.
	//
	// This method should broadcast the transactions to all other relays and
	// iff the calls to [Acceptor#AcceptBroadcastTx] by relays are successful,
	// it should commit the transactions data.
	// If commitment does not succeed for any reason outside of the scope of
	// acceptance, e.g. hd failure, this method calls [Acceptor#RollbackTx]
	// and broadcasts a RollbackTxs message to other relays' mempool reactor.
	//
	// If any error happens during the broadcast process, the transactions
	// data must be discarded.
	// Otherwise, it should eventually persist the transactions data using
	// active relays of one or many networks. The ChainID used for persisting
	// data must be built using the userAddress and the [Transaction#Fingerprint].
	BroadcastTx(
		ctx context.Context,
		userAddress string,
		relays []string,
		notifier chan<- BroadcastStatus,
		transactions ...Transaction,
	)

	// BroadcastTxRemoval sends an error to a notifier if any of the removal
	// operations fail verification, or if we fail to get a majority approval
	// for the broadcast operation from healthy relays.
	//
	// This method should broadcast the remove operations to all other relays
	// and iff the calls to [Acceptor#AcceptBroadcastTxRemoval] by relays are
	// successful, it should completely remove the persisted data.
	//
	// If any error happens during the broadcast process or during verification,
	// the removal operations data must be discarded.
	// Otherwise, it should eventually persist the removal operations data using
	// active relays of so-called *removal history networks*. The ChainID used
	// for persisting removal operations must be built using the userAddress
	// and the [Transaction#Fingerprint] will be prefixed with `delete_`.
	BroadcastTxRemoval(
		ctx context.Context,
		userAddress string,
		relays []string,
		notifier chan<- BroadcastStatus,
		transactions ...Transaction,
	)
}
