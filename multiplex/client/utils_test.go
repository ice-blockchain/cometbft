package client_test

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// ----------------------------------------------------------------------------
// Helpers

// makeRandomTestChainID creates a random test ChainID using a randomly
// generated validator address and the sha-256 fingerprint of "Posts".
//
// Note, this method always produces the same fingerprint value, whereas the
// user address part of the ChainID is randomly generated.
func makeRandomTestChainID() string {
	// address and fingerprint added to ChainID
	userPubKey := ed25519.GenPrivKey().PubKey()
	userAddress := userPubKey.Address().String()
	fingerprint := strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
	))

	return fmt.Sprintf("test-chain-%s-%s", userAddress, fingerprint)
}

// ----------------------------------------------------------------------------
// Tests

func TestMultiplexClientUtilsRegExpChainID(t *testing.T) {
	// Tests the regular expression to extract parts of a ChainID
	extractor, err := regexp.Compile(client.RegExpChainID)
	assert.NoError(t, err, "RegExpChainID regular expression must compile")

	// Should match 4 groups: "full match", "prefix", "address", "ChainID"
	expectedMatches := 4

	// Should match a general "prefix" concept
	testCases := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"prefix-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"abc-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"123-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
	}

	for _, testChainID := range testCases {
		matches := extractor.FindStringSubmatch(testChainID)
		assert.Len(t, matches, expectedMatches)
		assert.NotEmpty(t, matches[1], "RegExpChainID must extract prefix")  // prefix
		assert.NotEmpty(t, matches[2], "RegExpChainID must extract address") // address
		assert.NotEmpty(t, matches[3], "RegExpChainID must extract ChainID") // ChainID
	}
}
