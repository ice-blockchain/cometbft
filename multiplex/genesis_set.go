package multiplex

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"slices"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/merkle"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/types"
)

// Note: we use a separate key space for the genesis doc and doc hashes
// to prevent mixing both storages as mxGenesisDocHash holds hash of
// each GenesisDoc in the GenesisDocSet.
var genesisDocHashKey = []byte("mxGenesisDocHash")

// -----------------------------------------------------------------------------
// ChecksummedGenesisDocSet

// Core types for a blockchain genesis in network replication mode:
// - struct ChecksummedGenesisDocSet implements [node.IChecksummedGenesisDoc]
// - struct GenesisDocSet consists of a slice of [types.GenesisDoc]

// ChecksummedGenesisDocSet combines a GenesisDocSet together with its SHA256 checksum.
type ChecksummedGenesisDocSet struct {
	GenesisDocs    GenesisDocSet
	Sha256Checksum []byte
}

var _ node.IChecksummedGenesisDoc = (*ChecksummedGenesisDocSet)(nil)

// DefaultGenesisDoc() implements IChecksummedGenesisDoc.
func (c *ChecksummedGenesisDocSet) DefaultGenesisDoc() (*types.GenesisDoc, error) {
	if len(c.GenesisDocs) > 1 {
		return nil, errors.New("no default genesis doc available in network replication mode")
	}

	return &c.GenesisDocs[0], nil
}

// GenesisDocByChainID() implements IChecksummedGenesisDoc
// This method returns an error when the genesis doc cannot be
// found for the given chainId.
func (c *ChecksummedGenesisDocSet) GenesisDocByChainID(
	chainID string,
) (*types.GenesisDoc, error) {
	if doc, ok, _ := c.GenesisDocs.SearchGenesisDocByChainID(chainID); ok {
		return &doc, nil
	}

	return nil, fmt.Errorf("could not find genesis doc for ChainID %s", chainID)
}

// GetChecksum() implements IChecksummedGenesisDoc.
func (c *ChecksummedGenesisDocSet) GetChecksum() []byte {
	return c.Sha256Checksum
}

// -----------------------------------------------------------------------------
// GenesisDocSet

// GenesisDocSet defines the initial conditions for a CometBFT node multiplex,
// in particular their validator set and ChainID.
type GenesisDocSet []types.GenesisDoc

// SaveAs is a utility method for saving GenesisDocSet as a JSON file.
func (genDocSet GenesisDocSet) SaveAs(file string) error {
	genDocSetBytes, err := cmtjson.Marshal(genDocSet)
	if err != nil {
		return err
	}

	return cmtos.WriteFile(file, genDocSetBytes, 0o644)
}

// ValidatorHash returns the Merkle root hash built using genesis doc's
// validator hashes (as leaves).
func (genDocSet GenesisDocSet) ValidatorHash() []byte {
	bzs := make([][]byte, len(genDocSet))
	for i, genDoc := range genDocSet {
		bzs[i] = genDoc.ValidatorHash()
	}
	return merkle.HashFromByteSlices(bzs)
}

// ValidateAndComplete checks that all necessary fields are present.
func (genDocSet GenesisDocSet) ValidateAndComplete() error {
	if len(genDocSet) < 1 {
		return errors.New("encountered empty GenesisDocSet")
	}

	chains := map[string]bool{}
	for i, userGenDoc := range genDocSet {
		chainID := userGenDoc.ChainID

		if _, ok := chains[chainID]; ok {
			return fmt.Errorf("duplicate ChainID in the genesis file %s", chainID)
		}

		err := userGenDoc.ValidateAndComplete()
		if err != nil {
			return err
		}

		// Flag for uniqueness
		chains[chainID] = true
		genDocSet[i] = userGenDoc
	}

	return nil
}

// SearchGenesisDocByChainID starts a search for a GenesisDoc by its ChainID.
func (genDocSet GenesisDocSet) SearchGenesisDocByChainID(
	chainID string,
) (doc types.GenesisDoc, found bool, err error) {
	if idx := slices.IndexFunc(genDocSet, func(d types.GenesisDoc) bool {
		return d.ChainID == chainID
	}); idx > -1 {
		return genDocSet[idx], true, nil
	}

	return types.GenesisDoc{}, false, nil
}

//------------------------------------------------------------
// Providers

// GenesisDocFromChainParams creates a genesisDoc from transported ChainParams
// which should contain all the genesisDoc properties.
func GenesisDocFromChainParams(params *mxp2p.ChainParams) (types.GenesisDoc, error) {
	// Unmarshal protobuf
	consensusParams := types.ConsensusParamsFromProto(*params.ConsensusParams)
	validatorSet, err := types.ValidatorSetFromProto(&params.Validators)
	if err != nil {
		return types.GenesisDoc{}, err
	}

	genesisValidators := []types.GenesisValidator{}
	for _, validator := range validatorSet.Validators {
		genesisValidators = append(genesisValidators, types.GenesisValidator{
			Address: validator.Address,
			PubKey:  validator.PubKey,
			Power:   validator.VotingPower,
		})
	}

	return types.GenesisDoc{
		GenesisTime:     params.GenesisTime,
		ChainID:         params.ChainID,
		InitialHeight:   int64(params.InitialHeight),
		ConsensusParams: &consensusParams,
		Validators:      genesisValidators,
		AppHash:         params.AppHash,
		AppState:        params.AppStateBytes, // TODO(midas): appStateBytes initialization
	}, nil
}

// GenesisDocSetFromJSON unmarshalls JSON data into a GenesisDocSet.
func GenesisDocSetFromJSON(jsonBlob []byte) (GenesisDocSet, error) {
	genDocSet := GenesisDocSet{}

	err := cmtjson.Unmarshal(jsonBlob, &genDocSet)
	if err != nil {
		return nil, err
	}

	if err := genDocSet.ValidateAndComplete(); err != nil {
		return nil, err
	}

	return genDocSet, err
}

// GenesisDocSetFromFile reads JSON data from a file and unmarshalls it into a GenesisDocSet.
func GenesisDocSetFromFile(genDocSetFile string) (GenesisDocSet, error) {
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

// ChecksumGenesisDoc creates a SHA256 checksum of a [types.GenesisDoc].
func ChecksumGenesisDoc(genesisDoc *types.GenesisDoc) ([]byte, error) {
	jsonBlob, err := cmtjson.Marshal(genesisDoc)
	if err != nil {
		return []byte{}, fmt.Errorf("couldn't marshal GenesisDoc: %w", err)
	}
	return tmhash.Sum(jsonBlob), nil
}

// ValidateGenesisDocChecksum reads a genesis doc hash from the database
// or inserts it if it's the first time. If it's not the first time, this
// method will validate that the produced checksum matches the database one.
func ValidateGenesisDocChecksum(database *ChainDB, genesisDoc *types.GenesisDoc) error {
	// Used for verification after first run
	genDocChecksum, err := ChecksumGenesisDoc(genesisDoc)
	if err != nil {
		return fmt.Errorf(
			"error creating genesis doc set hash: %w", err)
	}

	// Get genesis doc set hash from chain's database
	genDocSetHashFromDB, err := database.Get(genesisDocHashKey)
	if err != nil {
		return fmt.Errorf(
			"error retrieving genesis doc set hash: %w", err)
	}

	if len(genDocSetHashFromDB) == 0 {
		// Save the genDoc hash in the store if it doesn't already exist for future verification
		if err = database.SetSync(genesisDocHashKey, genDocChecksum); err != nil {
			return fmt.Errorf(
				"failed to save genesis doc hash to db: %w", err)
		}
	} else {
		// Validates that the genesis doc hash in database matches
		if !bytes.Equal(genDocSetHashFromDB, genDocChecksum) {
			return errors.New(
				"genesis doc hash in db does not match loaded genesis doc")
		}
	}

	return nil
}

// MultiplexGenesisDocProviderFunc returns a [node.GenesisDocProvider] that loads
// the GenesisDocSet from a config.GenesisFile() on the filesystem.
//
// CAUTION: this method expects the genesis file to contain a GenesisDocSet.
func MultiplexGenesisDocProviderFunc(nodeCfg *config.Config) node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		emptyGenesisDocSet := &ChecksummedGenesisDocSet{
			GenesisDocs:    GenesisDocSet{},
			Sha256Checksum: []byte{},
		}

		jsonBlob, err := os.ReadFile(nodeCfg.GenesisFile())
		if err != nil {
			return emptyGenesisDocSet, fmt.Errorf("couldn't read GenesisDocSet from file: %w", err)
		}

		genDocSet, err := GenesisDocSetFromJSON(jsonBlob)
		if err != nil {
			return emptyGenesisDocSet, err
		}

		incomingChecksum := tmhash.Sum(jsonBlob)
		return &ChecksummedGenesisDocSet{GenesisDocs: genDocSet, Sha256Checksum: incomingChecksum}, nil
	}
}
