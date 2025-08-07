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
