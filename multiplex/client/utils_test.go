package client_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

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
