package helpers

import (
	"encoding/hex"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

func MakeChainID(input string) string {
	ext := NewExtendedChainID(
		MakeAddress().String(),
		MakeFingerprint(input),
	)
	return ext.String()
}

func MakeAddress() crypto.Address {
	return ed25519.GenPrivKey().PubKey().Address()
}

func MakeFingerprint(input string) string {
	return strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(input))[:DefaultFingerprintSize], // 8 bytes only
	))
}

func MakeClientTransaction(
	tb testing.TB,
	fingerprint string,
	data []byte,
) client.Transaction {
	tb.Helper()

	return client.Transaction{
		Data:        data,
		Fingerprint: fingerprint,
	}
}

func MakeClientTransactions(
	tb testing.TB,
	fingerprint string,
	numTransactions int,
) []client.Transaction {
	tb.Helper()

	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	testTransactions := []client.Transaction{}
	for i := 0; i < numTransactions; i++ {
		randomData := randomizer.Intn(999999999)

		testTransactions = append(testTransactions, MakeClientTransaction(tb,
			fingerprint,
			[]byte{byte(i), byte(i + 1), byte(i + 2), byte(randomData)},
		))
	}

	return testTransactions
}
