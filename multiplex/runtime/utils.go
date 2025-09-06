package runtime

import (
	"bytes"
	"encoding/hex"
	"regexp"
	"strconv"
	"strings"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	sm "github.com/ice-blockchain/cometbft/state"
)

// overwriteListenPort replaces the port in a service listen address.
func overwriteListenPort(laddr string, port int) string {
	re := regexp.MustCompile(`(.*)(\:\d+)(.*)`)
	newPort := ":" + strconv.Itoa(port)
	return re.ReplaceAllString(laddr, `$1`+newPort+`$3`)
}

// onlyValidatorIsUs returns true if pubKey is the only validator.
func onlyValidatorIsUs(state sm.State, pubKey crypto.PubKey) bool {
	if state.Validators.Size() > 1 {
		return false
	}
	addr, _ := state.Validators.GetByIndex(0)
	return bytes.Equal(pubKey.Address(), addr)
}

// validatorsIncludesUs returns true if pubKey is part of the validator set.
func validatorsIncludesUs(state sm.State, pubKey crypto.PubKey) bool {
	if _, addr := state.Validators.GetByAddress(pubKey.Address()); addr != nil {
		return true
	}

	return false
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

// txHashesToHex returns a string-slice with transaction hashes in hex format.
func txHashesToHex(transactions ...client.Transaction) []string {
	txHashes := make([]string, 0, len(transactions))
	for _, tx := range transactions {
		txHashes = append(txHashes, bytesToHex(tx.Hash()))
	}
	return txHashes
}

// bytesToHex returns an upper-case hexadecimal format of bytes.
func bytesToHex(
	bytes []byte,
) string {
	return strings.ToUpper(
		hex.EncodeToString(bytes),
	)
}
