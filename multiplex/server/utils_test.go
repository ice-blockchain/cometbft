package server_test

import (
	"math/rand"
	"testing"
	"time"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func makeAddress() crypto.Address {
	return ed25519.GenPrivKey().PubKey().Address()
}

func makeClientTransactions(
	tb testing.TB,
	fingerprint string,
	numTransactions int,
) []client.Transaction {
	tb.Helper()

	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		randomData := randomizer.Intn(999999999)
		testTransactions = append(testTransactions, client.Transaction{
			Data:        []byte{byte(i), byte(i + 1), byte(i + 2), byte(randomData)},
			Fingerprint: fingerprint,
		})
	}

	return testTransactions
}
