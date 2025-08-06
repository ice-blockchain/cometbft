package multiplex

import (
	"encoding/hex"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/types"
)

// ----------------------------------------------------------------------------
// Utils

// overwriteListenPort replaces the port in a service listen address.
func overwriteListenPort(laddr string, port int) string {
	re := regexp.MustCompile(`(.*)(\:\d+)(.*)`)
	newPort := ":" + strconv.Itoa(port)
	return re.ReplaceAllString(laddr, `$1`+newPort+`$3`)
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

// txBytesToHashes returns a string-slice with transaction hashes in hex format.
func txBytesToHashes(protoTxs [][]byte) []string {
	txHashes := make([]string, 0, len(protoTxs))
	for _, bzTx := range protoTxs {
		txHashes = append(txHashes, bytesToHex(types.Tx(bzTx).Hash()))
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

// mapTransactionsByChainID maps transactions by their ChainID.
func mapTransactionsByChainID(
	userAddress string,
	transactions ...client.Transaction,
) map[string][]client.Transaction {
	txesByChainID := map[string][]client.Transaction{}
	for _, tx := range transactions {
		txChainID := chainIdsFromTransactions(userAddress, tx)[0]

		if _, ok := txesByChainID[txChainID]; !ok {
			txesByChainID[txChainID] = []client.Transaction{}
		}

		txesByChainID[txChainID] = append(txesByChainID[txChainID], tx)
	}
	return txesByChainID
}

// bytesToHex returns an upper-case hexadecimal format of bytes.
func bytesToHex(
	bytes []byte,
) string {
	return strings.ToUpper(
		hex.EncodeToString(bytes),
	)
}

// pubKeyToHex returns an upper-case hexadecimal format of pubKey.
func pubKeyToHex(
	pubKey crypto.PubKey,
) string {
	return bytesToHex(pubKey.Bytes())
}
