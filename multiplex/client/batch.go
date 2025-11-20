package client

import (
	"crypto/sha256"
	"sync"

	"github.com/ice-blockchain/cometbft/crypto/merkle"
)

// TransactionBatch defines a goroutine-safe wrapper for batches
// of transactions which contains a merkle hash of all transactions.
type TransactionBatch struct {
	mtx  sync.Mutex
	txes []Transaction
	hash []byte
}

func NewTransactionBatch(txes [][]byte) TransactionBatch {
	cliTxes := make([]Transaction, 0, len(txes))
	for _, rawTx := range txes {
		cliTxes = append(cliTxes, RawTxToTransaction(rawTx))
	}

	batch := TransactionBatch{
		txes: cliTxes,
	}
	batch.computeMerkleHash()
	return batch
}

func (b TransactionBatch) Hash() []byte {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	if len(b.hash) > 0 {
		return b.hash
	}

	return b.computeMerkleHash()
}

func (b TransactionBatch) computeMerkleHash() []byte {
	hashes := make([][]byte, 0, len(b.txes)*sha256.Size)
	for _, tx := range b.txes {
		hashes = append(hashes, tx.Hash())
	}

	b.hash = make([]byte, sha256.Size)
	b.hash = merkle.HashFromByteSlices(hashes)
	return b.hash
}
