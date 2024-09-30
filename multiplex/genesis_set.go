package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"slices"

	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/merkle"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	types "github.com/cometbft/cometbft/types"
)

//------------------------------------------------------------
// core types for a blockchain genesis in plural replication mode
// - struct ChecksummedGenesisDocSet implements IChecksummedGenesisDoc
// - struct UserScopedGenesisDoc embeds GenesisDoc
// - struct GenesisDocSet contains a slice of UserScopedGenesisDoc

// ChecksummedUserScopedGenesisDoc combines a GenesisDoc together with its
// SHA256 checksum and the user address.
type ChecksummedGenesisDocSet struct {
	GenesisDocs    GenesisDocSet
	Sha256Checksum []byte
}

// DefaultGenesisDoc() implements IChecksummedGenesisDoc
func (c *ChecksummedGenesisDocSet) DefaultGenesisDoc() (*types.GenesisDoc, error) {
	if len(c.GenesisDocs.GenesisDocs) > 1 {
		return nil, fmt.Errorf("no default genesis doc available in plural replication mode")
	}

	return &c.GenesisDocs.GenesisDocs[0].GenesisDoc, nil
}

// GenesisDocByScope() implements IChecksummedGenesisDoc
// This method returns an error when the genesis doc cannot be
// found for the userScopeHash SHA256 hash.
func (c *ChecksummedGenesisDocSet) GenesisDocByScope(
	userScopeHash string,
) (*types.GenesisDoc, error) {
	if doc, ok, _ := c.GenesisDocs.SearchGenesisDocByScope(userScopeHash); ok {
		return &doc.GenesisDoc, nil
	}

	return nil, fmt.Errorf("could not find genesis doc for user %s", userScopeHash)
}

// GetChecksum() implements IChecksummedGenesisDoc
func (c *ChecksummedGenesisDocSet) GetChecksum() []byte {
	return c.Sha256Checksum
}

// UserScopedGenesisDoc defines a mapping with a composite key made of a user
// address and an arbitrary string-typed scope, to the initial conditions
// for CometBFT blockchains.
//
// TODO(midas):
// TBI whether UserAddress and Scope are necessary in genesis doc due to the
// Config structs already including these, they may be skipped here if they
// can be filled through configuration files otherwise.
type UserScopedGenesisDoc struct {
	UserAddress crypto.Address   `json:"user_address"`
	Scope       string           `json:"scope"`
	ScopeHash   string           `json:"scope_hash"`
	GenesisDoc  types.GenesisDoc `json:"genesis"`
}

// GenesisDocSet defines the initial conditions for multiple CometBFT blockchains,
// in particular their validator set and the mapping between a UserAddress and ChainID.
// TODO(midas): would a multiplex object be more comfortable? take into account JSON format
type GenesisDocSet struct {
	GenesisDocs []UserScopedGenesisDoc
}

// SaveAs is a utility method for saving GenesisDocSet as a JSON file
func (genDocSet *GenesisDocSet) SaveAs(file string) error {
	genDocSetBytes, err := cmtjson.Marshal(genDocSet.GenesisDocs)
	if err != nil {
		return err
	}
	//fmt.Printf("docSetBytes: %s", genDocSetBytes)
	return cmtos.WriteFile(file, genDocSetBytes, 0644)
}

// ValidatorHash returns the Merkle root hash built using genesis doc's
// validator hashes (as leaves).
func (genDocSet *GenesisDocSet) ValidatorHash() []byte {
	bzs := make([][]byte, len(genDocSet.GenesisDocs))
	for _, genDoc := range genDocSet.GenesisDocs {
		bzs = append(bzs, genDoc.GenesisDoc.ValidatorHash())
	}
	return merkle.HashFromByteSlices(bzs)
}

// ValidateAndComplete checks that all necessary fields are present,
// then fills in defaults for optional fields left empty for each of
// the set's genesis docs and computes composite scope SHA256 hashes.
func (genDocSet *GenesisDocSet) ValidateAndComplete() error {
	if len(genDocSet.GenesisDocs) < 1 {
		return fmt.Errorf("encountered empty GenesisDocSet")
	}

	users := map[string]bool{}
	chains := map[string]bool{}
	for i, userGenDoc := range genDocSet.GenesisDocs {
		userChainID := userGenDoc.GenesisDoc.ChainID

		// Validate the address
		address := userGenDoc.UserAddress
		if len(address) != tmhash.TruncatedSize {
			return fmt.Errorf("incorrect address for user in the genesis file, got %d bytes, expected %d bytes", len(address), tmhash.TruncatedSize)
		}

		// Scope may not be empty
		if len(userGenDoc.Scope) == 0 {
			return fmt.Errorf("user scopes may not be empty in the genesis file")
		}

		// Generate the ScopeHash
		sid := NewScopeID(userGenDoc.UserAddress.String(), userGenDoc.Scope)
		userGenDoc.ScopeHash = sid.Hash()

		if _, ok := users[userGenDoc.ScopeHash]; ok {
			return fmt.Errorf("duplicate scope hash in the genesis file for pair of user %v and scope %s", address, userGenDoc.Scope)
		}

		if _, ok := chains[userChainID]; ok {
			return fmt.Errorf("ChainID already exists in the genesis file for pair of user %v and scope %s", address, userGenDoc.Scope)
		}

		err := userGenDoc.GenesisDoc.ValidateAndComplete()

		if err != nil {
			return err
		}

		// Flag for uniqueness
		users[userGenDoc.ScopeHash] = true
		chains[userChainID] = true
		genDocSet.GenesisDocs[i] = userGenDoc
	}

	return nil
}

// SearchGenesisDocByScope starts a search for a UserScopedGenesisDoc mapped to
// the given user address. A hash of length tmhash.TruncatedSize is used.
func (genDocSet *GenesisDocSet) SearchGenesisDocByScope(
	userScopeHash string,
) (doc UserScopedGenesisDoc, found bool, err error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return UserScopedGenesisDoc{}, false, fmt.Errorf("incorrect scope hash for genesis doc search, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if idx := slices.IndexFunc(genDocSet.GenesisDocs, func(d UserScopedGenesisDoc) bool {
		return d.ScopeHash == userScopeHash
	}); idx > -1 {
		return genDocSet.GenesisDocs[idx], true, nil
	}

	return UserScopedGenesisDoc{}, false, nil
}

//------------------------------------------------------------
// Make genesis set state from file

// GenesisDocSetFromJSON unmarshalls JSON data into a GenesisDocSet.
func GenesisDocSetFromJSON(jsonBlob []byte) (*GenesisDocSet, error) {
	genDocSet := &GenesisDocSet{
		GenesisDocs: make([]UserScopedGenesisDoc, 0),
	}

	err := cmtjson.Unmarshal(jsonBlob, &genDocSet.GenesisDocs)
	if err != nil {
		return nil, err
	}

	if err := genDocSet.ValidateAndComplete(); err != nil {
		return nil, err
	}

	return genDocSet, err
}

// GenesisDocSetFromFile reads JSON data from a file and unmarshalls it into a GenesisDocSet.
func GenesisDocSetFromFile(genDocSetFile string) (*GenesisDocSet, error) {
	jsonBlob, err := os.ReadFile(genDocSetFile)
	if err != nil {
		return nil, fmt.Errorf("couldn't read GenesisDocSet file: %w", err)
	}
	genDocSet, err := GenesisDocSetFromJSON(jsonBlob)
	if err != nil {
		return nil, fmt.Errorf("error reading GenesisDocSet at %s: %w", genDocSetFile, err)
	}
	return genDocSet, nil
}
