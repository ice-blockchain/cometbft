package client

import (
	"regexp"
)

const (
	RegExpChainID = `(.*)\-([A-F0-9]+)\-([A-F0-9]+)`
)

// extractAddressFromChainID returns the user address extracted from a ChainID.
func extractAddressFromChainID(chainID string) string {
	extractor := regexp.MustCompile(RegExpChainID)
	matches := extractor.FindStringSubmatch(chainID)

	// Returns empty given non-compatible ChainID
	if len(matches) == 0 || len(matches) < 3 {
		return ""
	}

	// Returns the USER ADDRESS part of the ChainID
	return matches[2]
}
