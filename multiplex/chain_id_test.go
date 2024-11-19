package multiplex_test

import (
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	mx "github.com/ice-blockchain/cometbft/multiplex"
)

const (
	exampleAddress     = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	exampleFingerprint = "1A63C0E60122F9BB"
)

// formalizeChainID replaces the "test-chain" prefixes for "mx-chain" prefixes
// such that it matches the format of a [ExtendedChainID].
func formalizeChainID(opts ...string) string {
	chainID := opts[0]
	prefix := "test-chain"

	if len(opts) > 1 && len(opts[1]) > 0 {
		prefix = opts[1]
	}

	// ExtendedChainID uses a *statically* compiled prefix for chain
	// identifiers which cannot be changed.
	return strings.Replace(chainID, prefix, "mx-chain", 1)
}

func makeChainID(input string) string {
	ext, _ := mx.NewExtendedChainID(
		makeAddress().String(),
		makeFingerprint(input),
	)
	return ext.String()
}

func makeFingerprint(input string) string {
	return strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(input))[:8], // 8 bytes only
	))
}

func makeAddress() crypto.Address {
	return ed25519.GenPrivKey().PubKey().Address()
}

func TestMultiplexExtendedChainIDNewExtendedChainID(t *testing.T) {
	// ----------------
	// Errors
	addressFailCases := []string{
		"",
		"1234",
		"#000000000000000000000000000000000000000",
		"CC8E6555A3F401FF61DA098F94D325E7041BC4",     // too short
		"CC8E6555A3F401FF61DA098F94D325E7041BC43AAB", // too long
	}

	for _, failCaseAddress := range addressFailCases {
		_, err := mx.NewExtendedChainID(failCaseAddress, exampleFingerprint)
		assert.Error(t, err)
	}

	fingerprintFailCases := []string{
		"",
		"1234",
		"#0000000000000",
		"1A63C0E60122F9",     // too short
		"1A63C0E60122F9BBAB", // too long
	}

	for _, failCaseFingerprint := range fingerprintFailCases {
		_, err := mx.NewExtendedChainID(exampleAddress, failCaseFingerprint)
		assert.Error(t, err)
	}

	// ----------------
	// Successes
	addressTestCases := []string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A",
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0",
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB",
		makeAddress().String(), // random ed25519 priv
		makeAddress().String(), // random ed25519 priv
		makeAddress().String(), // random ed25519 priv
	}

	for _, testCaseAddress := range addressTestCases {
		ext, err := mx.NewExtendedChainID(testCaseAddress, exampleFingerprint)
		assert.NoError(t, err)
		assert.Equal(t, testCaseAddress, ext.GetUserAddress())
	}

	fingerprintTestCases := []string{
		"1A63C0E60122F9BB",
		"D1ED2B487F2E93CC",
		"79F77E672C1DB0BC",
		makeFingerprint("Just a test"),
		makeFingerprint("another test"),
		makeFingerprint(makeFingerprint("complexity")),
		makeFingerprint("Using a bit more text."),
	}

	for _, testCaseFingerprint := range fingerprintTestCases {
		ext, err := mx.NewExtendedChainID(exampleAddress, testCaseFingerprint)
		assert.NoError(t, err)
		assert.Equal(t, testCaseFingerprint, ext.GetFingerprint())
	}

	pairingTestCases := [][]string{
		{"CC8E6555A3F401FF61DA098F94D325E7041BC43A", "1A63C0E60122F9BB"},
		{"CC8E6555A3F401FF61DA098F94D325E7041BC43A", "D1ED2B487F2E93CC"},
		{"FF1410CEEB411E55487701C4FEE65AACE7115DC0", "79F77E672C1DB0BC"},
		{makeAddress().String(), makeFingerprint("abc")},
		{makeAddress().String(), makeFingerprint("def")},
		{makeAddress().String(), makeFingerprint("ghi")},
	}

	for _, pair := range pairingTestCases {
		testCaseAddress := pair[0]
		testCaseFingerprint := pair[1]

		ext, err := mx.NewExtendedChainID(testCaseAddress, testCaseFingerprint)
		assert.NoError(t, err)
		assert.Equal(t, testCaseAddress, ext.GetUserAddress())
		assert.Equal(t, testCaseFingerprint, ext.GetFingerprint())
	}
}

func TestMultiplexExtendedChainIDNewExtendedChainIDFromLegacy(t *testing.T) {
	// ----------------
	// Errors
	failCases := []string{
		"invalid-chain-id",
		"cosmoshub-4",
		"mx-chain-1234-5678",
		// invalid addresses
		"mx-chain-#000000000000000000000000000000000000000-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC4-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43AAB-1A63C0E60122F9BB",
		// invalid fingerprints
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-#000000000000000",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BBCC",
	}

	for _, failCaseChainID := range failCases {
		_, err := mx.NewExtendedChainIDFromLegacy(failCaseChainID)
		assert.Error(t, err)
	}

	// ----------------
	// Successes
	testCases := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
		makeChainID("Posts"),
		makeChainID("Likes"),
		makeChainID("Media"),
	}

	for _, testCaseChainID := range testCases {
		ext, err := mx.NewExtendedChainIDFromLegacy(testCaseChainID)
		assert.NoError(t, err)
		assert.Equal(t, testCaseChainID, ext.String())
		assert.Equal(t, testCaseChainID, ext.Format())
	}
}
