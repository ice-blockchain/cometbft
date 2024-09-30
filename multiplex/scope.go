package multiplex

import (
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"
	"sync"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
)

// -----------------------------------------------------------------------------
// scopeID
// A private struct to deal with unique pairs of user address and scope.

const (
	fingerprintSize = 8
	scopesSeparator = ":"
)

// scopeID embeds a string and adds a scope hash
type scopeID struct {
	ScopeHash string
	string
}

// Hash creates a SHA256 hash of the embedded string part of a scopeID, encodes
// it in hexadecimal format and returns the uppercase transformed string.
func (s *scopeID) Hash() string {
	if len(s.ScopeHash) == 0 {
		sum256 := tmhash.Sum([]byte(s.string))
		s.ScopeHash = hex.EncodeToString(sum256)
	}

	return strings.ToUpper(s.ScopeHash)
}

// Fingerprint creates an 8-bytes size fingerprint using the SHA256 hash.
func (s *scopeID) Fingerprint() string {
	if len(s.ScopeHash) == 0 {
		s.ScopeHash = s.Hash()
	}

	hashBytes, _ := hex.DecodeString(s.ScopeHash)
	fpBytes := hashBytes[:fingerprintSize]
	return strings.ToUpper(hex.EncodeToString(fpBytes))
}

// String implements Stringer interface
func (s *scopeID) String() string {
	return s.string
}

// Parts returns the two parts of a scope description.
func (s *scopeID) Parts() [2]string {
	if len(s.string) == 0 {
		return [2]string{"", ""}
	}

	// Index returns first instance of token
	separatorAt := strings.Index(s.string, scopesSeparator)
	if separatorAt == -1 {
		return [2]string{s.string, ""}
	}

	return [2]string{
		s.string[:separatorAt],
		s.string[separatorAt+1:],
	}
}

// -----------------------------------------------------------------------------
// ScopeRegistry
// A registry pattern implementation, searchable by user address and scope.

// UserScopeSet maps user addresses to SHA256 scope hashes
type UserScopeSet map[string][]string

// UserScopeMap maps user addresses to pairs of scope (cleartext) and scope hash
type UserScopeMap map[string]map[string]string

// ScopeRegistry wraps searchable scope hashes by user and by user+scope
type ScopeRegistry struct {
	ScopeHashes UserScopeSet
	UsersScopes UserScopeMap
}

// GetScopeHashes returns a slice of all scope hashes
//
// Hashes are always sorted lexicographically.
func (r *ScopeRegistry) GetScopeHashes() []string {
	var allHashes []string
	for _, scopeHashes := range r.ScopeHashes {
		allHashes = append(allHashes, scopeHashes...)
	}

	// Sort hashes lexicographically
	sort.Strings(allHashes)
	return allHashes
}

// GetScopeHashesByUser returns a slice of scope hashes by user address
//
// Hashes are always sorted lexicographically.
func (r *ScopeRegistry) GetScopeHashesByUser(
	userAddress string,
) ([]string, error) {
	if _, ok := r.ScopeHashes[userAddress]; !ok {
		return []string{}, errors.New("no scope hashes found for user: " + userAddress)
	}

	// Sort hashes lexicographically
	sort.Strings(r.ScopeHashes[userAddress])
	return r.ScopeHashes[userAddress], nil
}

// GetScopeHash returns a scope hash by user address and scope (cleartext)
func (r *ScopeRegistry) GetScopeHash(
	userAddress string,
	scope string,
) (string, error) {
	if _, ok := r.UsersScopes[userAddress]; !ok {
		return "", fmt.Errorf("no scope hashes found for user: %s", userAddress)
	}

	if _, ok := r.UsersScopes[userAddress][scope]; !ok {
		return "", fmt.Errorf("no scope '%s' for user: %s", scope, userAddress)
	}

	return r.UsersScopes[userAddress][scope], nil
}

// GetAddress tries to find a user address by scope hash or errors if none is found.
func (r *ScopeRegistry) GetAddress(scopeHash string) (string, error) {
	for userAddress, hashes := range r.ScopeHashes {
		if slices.Contains(hashes, scopeHash) {
			return userAddress, nil
		}
	}

	return "", fmt.Errorf("no address found for scope hash %s", scopeHash)
}

// GetScopeIndex searches for a scope hash and returns its index or -1 if not found.
//
// The index generally represents the index of the replicated chain's scope hash
// in the scope hash slices, e.g. with scope hashes ['A','B','C'], the index for
// the node replicating the chain with scope hash 'B' is 1.
// Hashes are always sorted lexicographically.
func (r *ScopeRegistry) FindScope(scopeHash string) int {
	for i, sh := range r.GetScopeHashes() {
		if sh == scopeHash {
			return i
		}
	}

	return -1
}

// ----------------------------------------------------------------------------
// Builders

func NewScopeID(userAddress string, scope string) *scopeID {
	return &scopeID{
		string: fmt.Sprintf("%s%s%s", userAddress, scopesSeparator, scope),
	}
}

func NewScopeIDFromHash(scopeHash string) *scopeID {
	return &scopeID{
		ScopeHash: strings.ToUpper(scopeHash),
	}
}

func NewScopeIDFromDesc(scopeDesc string) *scopeID {
	return &scopeID{string: scopeDesc}
}

// ----------------------------------------------------------------------------
// Providers

type ScopeHashProvider func(*cfg.UserConfig) (ScopeRegistry, error)

// DefaultScopeHashProvider computes SHA256 hashes by pairing user addresses
// and arbitrary scopes, separated by `:`, such that every combination of user
// address and scope can be referred to by a scope hash.
// This method implementation supports concurrent calls.
// DefaultScopeHashProvider implements ScopeHashProvider
func DefaultScopeHashProvider(config *cfg.UserConfig) (*ScopeRegistry, error) {
	if config.Replication == cfg.SingularReplicationMode() {
		return &ScopeRegistry{}, nil
	}

	cacheableHashProvider := sync.OnceValue(func() *ScopeRegistry {
		registry := &ScopeRegistry{}
		registry.ScopeHashes = UserScopeSet{}
		registry.UsersScopes = UserScopeMap{}

		for userAddress, scopes := range config.UserScopes {
			// allocates per-user-scope hash map
			registry.UsersScopes[userAddress] = map[string]string{}

			// Create SHA256 hashes of the pair of user address and scope
			var userScopeHashes []string
			for _, scope := range scopes {
				scopeId := NewScopeID(userAddress, scope)
				scopeHash := scopeId.Hash()
				userScopeHashes = append(userScopeHashes, scopeHash)

				// Builds a per-user-scope map of scope hashes
				registry.UsersScopes[userAddress][scope] = scopeHash
			}

			// Builds a per-user slice of scope hashes
			registry.ScopeHashes[userAddress] = userScopeHashes
		}

		return registry
	})

	sRegistry := cacheableHashProvider()

	if config.Replication == cfg.PluralReplicationMode() && len(sRegistry.ScopeHashes) == 0 {
		return &ScopeRegistry{}, errors.New("in plural replication mode, at least one user scope is required")
	}

	return sRegistry, nil
}
