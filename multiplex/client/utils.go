package client

import (
	"encoding/hex"
	"regexp"
	"strings"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtcodec "github.com/ice-blockchain/cometbft/crypto/encoding"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/types"
)

// GetFingerprint returns a hash which consists of the first 8-bytes
// of a SHA-256 of the plaintext input.
func GetFingerprint(plaintext string) string {
	_, hexErr := hex.DecodeString(plaintext)
	if len(plaintext) == 16 && hexErr == nil {
		return strings.ToUpper(plaintext)
	}

	return strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(plaintext))[:8], // 8-bytes
	))
}

// GetUserAddress extracts the user address part of a ChainID.
func GetUserAddress(chainID string) string {
	// Extract using regexp
	extractor := regexp.MustCompile(`(.*)\-([A-F0-9]+)\-([A-F0-9]+)`)
	if !extractor.MatchString(chainID) {
		return ""
	}

	matches := extractor.FindStringSubmatch(chainID)
	return matches[2]
}

// GetChainID returns ChainID from a user address and fingerprint.
func GetChainID(userAddress string, fingerprint string) string {
	fingerprintHash := GetFingerprint(fingerprint)

	return strings.Join([]string{
		"mx-chain",
		userAddress,
		fingerprintHash,
	}, "-")
}

// PubKeyToAddress parses a master public key (ed25519) and returns
// the resulting compressed address format (20 bytes).
//
// An error is returned if the master public key cannot be parsed as
// a [crypto.PubKey] instance. Note that we forcefully use `ed25519`
// public keys to permit batch verifications down the line.
func PubKeyToAddress(masterPubKey string) (string, error) {
	pubKey, err := cmtcodec.PubKeyFromTypeAndBytes(
		ed25519.KeyType,
		[]byte(masterPubKey),
	)
	if err != nil {
		return "", err
	}

	return pubKey.Address().String(), nil
}

// TransactionToRawTx converts a [Transaction] to a cometbft transaction
// base type `types.Tx` which consists of the raw transaction with its hash.
func TransactionToRawTx(transaction Transaction) types.Tx {
	// Compute SHA-256 fingerprint from transaction
	fingerprintHash := GetFingerprint(transaction.Fingerprint)
	fingerprintBytes := []byte(fingerprintHash)

	// Concatenate transaction body and fingerprint
	txRaw := []byte{}
	txRaw = append(txRaw, transaction.Data...)
	txRaw = append(txRaw, fingerprintBytes...)

	// Uses the cometbft/types transaction encoder
	return types.ToTxs([][]byte{txRaw})[0]
}

// RawTxToTransaction converts a cometbft transaction to a [Transaction].
func RawTxToTransaction(transaction types.Tx) Transaction {
	fpIndex := len(transaction) - 16
	fingerprint := string(transaction[fpIndex:])

	return Transaction{
		Data:        transaction[:fpIndex],
		Fingerprint: fingerprint,
	}
}
