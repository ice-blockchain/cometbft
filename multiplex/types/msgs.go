package types

// AckTransactionResult describes a remote transaction receipt.
// Used by [MultiplexBackend#WaitForRelaysAckTransactionBatch].
type AckTransactionResult struct {
	// Contains relay IDs of relays that acknowledged TxHash.
	Relays []string

	// Contains a transaction hash in hexadecimal.
	TxHash string

	// May contain an error
	Error error
}

// AckReplicationResult describes a remote replication receipt.
// Used by [MultiplexBackend#WaitForRelaysAckChainReplications].
type AckReplicationResult struct {
	// Contains relay IDs of relays that acknowledged ChainID.
	Relays  []string
	ChainID string

	// May contain an error
	Error error
}

// RuntimeUpdateResult describes a remote status update result.
// Used by [MultiplexBackend#WaitForRelaysReplicationCompleted].
type RuntimeUpdateResult struct {
	// Contains relay IDs of relays that have announced the completion
	// of their replication for ChainID.
	Relays  []string
	ChainID string

	// May contain an error
	Error error
}

// TransactionEventResult describes a transaction event result.
// Used by [MultiplexBackend#WaitForTransactionsEvents].
type TransactionEventResult struct {
	// Contains transaction hashes in hex format for transactions
	// which have been included in a block on ChainID.
	TxHashes []string
	ChainID  string

	// May contain an error
	Error error
}
