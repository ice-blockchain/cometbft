package multiplex

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	sm "github.com/ice-blockchain/cometbft/state"
)

// ----------------------------------------------------------------------------
// Utils

func onlyValidatorIsUs(state sm.State, pubKey crypto.PubKey) bool {
	if state.Validators.Size() > 1 {
		return false
	}
	addr, _ := state.Validators.GetByIndex(0)
	return bytes.Equal(pubKey.Address(), addr)
}

// splitAndTrimEmpty slices s into all subslices separated by sep and returns a
// slice of the string s with all leading and trailing Unicode code points
// contained in cutset removed. If sep is empty, SplitAndTrim splits after each
// UTF-8 sequence. First part is equivalent to strings.SplitN with a count of
// -1.  also filter out empty strings, only return non-empty strings.
func splitAndTrimEmpty(s, sep, cutset string) []string {
	if s == "" {
		return []string{}
	}

	spl := strings.Split(s, sep)
	nonEmptyStrings := make([]string, 0, len(spl))
	for i := 0; i < len(spl); i++ {
		element := strings.Trim(spl[i], cutset)
		if element != "" {
			nonEmptyStrings = append(nonEmptyStrings, element)
		}
	}
	return nonEmptyStrings
}

// removeDuplicates filters out duplicate strings in a string slice.
func removeDuplicates(input []string) (output []string) {
	dedupl := map[string]bool{}
	for _, v := range input {
		dedupl[v] = true
	}

	output = make([]string, 0, len(dedupl))
	for v := range dedupl {
		output = append(output, v)
	}
	return
}

// txHashesToHex returns a string-slice with transaction hashes in hex format.
func txHashesToHex(transactions ...client.Transaction) []string {
	txHashes := make([]string, 0, len(transactions))
	for _, tx := range transactions {
		txHashes = append(txHashes, fmt.Sprintf("%X", tx.Hash()))
	}
	return txHashes
}

// chainIdsFromTransactions accepts a userAddress and transactions,
// and it returns a slice of ChainID values.
func chainIdsFromTransactions(
	userAddress string,
	transactions ...client.Transaction,
) []string {
	chainIds := make([]string, 0, len(transactions))
	for _, tx := range transactions {
		chainID := client.GetChainID(userAddress, tx.Fingerprint)
		chainIds = append(chainIds, chainID)
	}
	return removeDuplicates(chainIds)
}
