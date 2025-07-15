package helpers_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

const (
	exampleAddress     = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	exampleFingerprint = "1A63C0E60122F9BB"
)

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
		testObj := helpers.NewExtendedChainID(failCaseAddress, exampleFingerprint)
		assert.Nil(t, testObj)
	}

	fingerprintFailCases := []string{
		"",
		"1234",
		"#0000000000000",
		"1A63C0E60122F9",     // too short
		"1A63C0E60122F9BBAB", // too long
	}

	for _, failCaseFingerprint := range fingerprintFailCases {
		testObj := helpers.NewExtendedChainID(exampleAddress, failCaseFingerprint)
		assert.Nil(t, testObj)
	}

	// ----------------
	// Successes
	addressTestCases := []string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A",
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0",
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB",
		helpers.MakeAddress().String(), // random ed25519 priv
		helpers.MakeAddress().String(), // random ed25519 priv
		helpers.MakeAddress().String(), // random ed25519 priv
	}

	for _, testCaseAddress := range addressTestCases {
		ext := helpers.NewExtendedChainID(testCaseAddress, exampleFingerprint)
		assert.NotNil(t, ext)
		assert.Equal(t, testCaseAddress, ext.GetUserAddress())
	}

	fingerprintTestCases := []string{
		"1A63C0E60122F9BB",
		"D1ED2B487F2E93CC",
		"79F77E672C1DB0BC",
		helpers.MakeFingerprint("Just a test"),
		helpers.MakeFingerprint("another test"),
		helpers.MakeFingerprint(helpers.MakeFingerprint("complexity")),
		helpers.MakeFingerprint("Using a bit more text."),
	}

	for _, testCaseFingerprint := range fingerprintTestCases {
		ext := helpers.NewExtendedChainID(exampleAddress, testCaseFingerprint)
		assert.NotNil(t, ext)
		assert.Equal(t, testCaseFingerprint, ext.GetFingerprint())
	}

	pairingTestCases := [][]string{
		{"CC8E6555A3F401FF61DA098F94D325E7041BC43A", "1A63C0E60122F9BB"},
		{"CC8E6555A3F401FF61DA098F94D325E7041BC43A", "D1ED2B487F2E93CC"},
		{"FF1410CEEB411E55487701C4FEE65AACE7115DC0", "79F77E672C1DB0BC"},
		{helpers.MakeAddress().String(), helpers.MakeFingerprint("abc")},
		{helpers.MakeAddress().String(), helpers.MakeFingerprint("def")},
		{helpers.MakeAddress().String(), helpers.MakeFingerprint("ghi")},
	}

	for _, pair := range pairingTestCases {
		testCaseAddress := pair[0]
		testCaseFingerprint := pair[1]

		ext := helpers.NewExtendedChainID(testCaseAddress, testCaseFingerprint)
		assert.NotNil(t, ext)
		assert.Equal(t, testCaseAddress, ext.GetUserAddress())
		assert.Equal(t, testCaseFingerprint, ext.GetFingerprint())
	}
}

func TestMultiplexExtendedChainIDNewExtendedChainIDFromString(t *testing.T) {
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
		testObj := helpers.NewExtendedChainIDFromString(failCaseChainID)
		assert.Nil(t, testObj)
	}

	// ----------------
	// Successes
	testCases := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
		helpers.MakeChainID("Posts"),
		helpers.MakeChainID("Likes"),
		helpers.MakeChainID("Media"),
	}

	for _, testCaseChainID := range testCases {
		ext := helpers.NewExtendedChainIDFromString(testCaseChainID)
		assert.NotNil(t, ext)
		assert.Equal(t, testCaseChainID, ext.String())
		assert.Equal(t, testCaseChainID, ext.Format())
	}
}
