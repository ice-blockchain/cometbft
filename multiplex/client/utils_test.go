package client_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexClientUtilsTransactionToRawTx(t *testing.T) {
	testRawTxBytes := []byte{1, 2, 3}
	expectedLength := len(testRawTxBytes) + 16 // 8-bytes fingerprint

	testFingerprints := []string{
		"Posts", "Likes", "Media", "Images",
		"12345", "_'-W1", "etc. etc. etc. ",
	}

	for _, testFingerprint := range testFingerprints {
		testTransaction := client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		}

		actualTx := client.TransactionToRawTx(testTransaction)
		assert.Len(t, actualTx, expectedLength)
	}
}

func TestMultiplexClientUtilsRawTxToTransaction(t *testing.T) {
	testRawTxBytes := []byte{1, 2, 3}
	expectedHashLen := 16 // 8-bytes fingerprint

	testFingerprints := []string{
		"Posts", "Likes", "Media", "Images",
		"12345", "_'-W1", "etc. etc. etc. ",
	}

	for _, testFingerprint := range testFingerprints {
		testTransaction := client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		}

		rawTx := client.TransactionToRawTx(testTransaction)
		actualTransaction := client.RawTxToTransaction(rawTx)
		assert.Len(t, actualTransaction.Data, len(testRawTxBytes))
		assert.Len(t, actualTransaction.Fingerprint, expectedHashLen)
	}
}

func TestMultiplexClientUtilsGetBroadcastID(t *testing.T) {
	// Test with one transaction hash
	testRawTxBytes := []byte{1, 2, 3}
	testTransaction := client.Transaction{
		Data:        testRawTxBytes,
		Fingerprint: "Posts",
	}

	expectedBroadcastID := fmt.Sprintf("%X", tmhash.Sum(testTransaction.Hash()))
	actualBroadcastID := client.GetBroadcastID(testTransaction)

	assert.Equal(t, expectedBroadcastID, actualBroadcastID)

	// Test with unsorted transaction hashes with same fingerprints
	testFingerprints1 := []string{
		"Posts", "Likes", "Media", "Images",
		"12345", "_'-W1", "etc. etc. etc. ",
	}
	testFingerprints2 := []string{
		"Likes", "Posts", "Images", "Media",
		"_'-W1", "etc. etc. etc. ", "12345",
	}

	testTransactions1 := make([]client.Transaction, 0, len(testFingerprints1))
	testTransactions2 := make([]client.Transaction, 0, len(testFingerprints2))

	for _, testFingerprint := range testFingerprints1 {
		testTransactions1 = append(testTransactions1, client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		})
	}

	for _, testFingerprint := range testFingerprints2 {
		testTransactions2 = append(testTransactions2, client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		})
	}

	actualBroadcastID1 := client.GetBroadcastID(testTransactions1...)
	actualBroadcastID2 := client.GetBroadcastID(testTransactions2...)

	assert.Equal(t, actualBroadcastID1, actualBroadcastID2)

	// Test with different unsorted transaction hashes (one character diff)
	testFingerprints3 := []string{
		"Posts", "Likes", "Media", "Images",
		"12345", "_'-W1", "etc. etc. etc.", // <- one space removed
	}

	testTransactions3 := make([]client.Transaction, 0, len(testFingerprints3))
	for _, testFingerprint := range testFingerprints3 {
		testTransactions3 = append(testTransactions3, client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		})
	}

	actualBroadcastID3 := client.GetBroadcastID(testTransactions3...)
	assert.NotEqual(t, actualBroadcastID1, actualBroadcastID3)

	// Test with different unsorted transaction hashes (one character diff)
	testFingerprint4 := []string{
		"Posts", "Likes", "Media", "Images",
		"12345", "_'-W1", // <- one fingerprint removed
	}

	testTransactions4 := make([]client.Transaction, 0, len(testFingerprint4))
	for _, testFingerprint := range testFingerprint4 {
		testTransactions4 = append(testTransactions4, client.Transaction{
			Data:        testRawTxBytes,
			Fingerprint: testFingerprint,
		})
	}

	actualBroadcastID4 := client.GetBroadcastID(testTransactions4...)
	assert.NotEqual(t, actualBroadcastID1, actualBroadcastID4)
	assert.NotEqual(t, actualBroadcastID3, actualBroadcastID4)
}
