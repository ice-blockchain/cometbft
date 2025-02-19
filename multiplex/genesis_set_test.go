package multiplex_test

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// Note: we use a separate key space for the genesis doc and doc hashes
// to prevent mixing both storages as mxGenesisDocHash holds hash of
// each GenesisDoc in the GenesisDocSet.
var genesisDocHashKey = []byte("mxGenesisDocHash")

func TestMultiplexGenesisDocSetBad(t *testing.T) {
	// test some bad ones from raw json
	testCases := [][]byte{
		{},               // empty
		{1},              // junk
		[]byte(`{null}`), // invalid GenesisDocs
		[]byte(`[{}]`),   // invalid GenesisDocs
		[]byte(`[{"chain_id": "", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}]`),   // empty chain_id
		[]byte(`[{"chain_id": null, "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}]`), // nil chain_id
		[]byte(`[
			{"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}},
			{"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}
		]`), // ChainID not unique
	}

	for i, testCase := range testCases {
		_, err := mx.GenesisDocSetFromJSON(testCase)

		assert.Error(t, err, "expected error for invalid genDocSet json at "+strconv.Itoa(i))
	}

	emptyGenesisDocSet := []byte(`[]`)
	_, actualErr := mx.GenesisDocSetFromJSON(emptyGenesisDocSet)
	assert.NoError(t, actualErr, "should not error with empty genesis doc set")
}

func TestMultiplexGenesisDocSetGood(t *testing.T) {
	// test one genesis doc by raw json
	genDocSetBytes := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		}
	]`)
	_, err := mx.GenesisDocSetFromJSON(genDocSetBytes)
	assert.NoError(t, err, "expected no error for correct genesis docs set with single user from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultiple := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		},
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`)
	_, err = mx.GenesisDocSetFromJSON(genDocSetBytesMultiple)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple users from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultipleChains := []byte(`[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Alice"}
		},
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "test-chain-BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB-D1ED2B487F2E93CC",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`)
	_, err = mx.GenesisDocSetFromJSON(genDocSetBytesMultipleChains)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple chains from json")

	// create a GenesisDocSet from struct
	valPubKey := ed25519.GenPrivKey().PubKey()
	baseGenDocSet := mx.GenesisDocSet{
		{
			ChainID: "abc",
			Validators: []types.GenesisValidator{{
				Address: valPubKey.Address(),
				PubKey:  valPubKey,
				Power:   10,
				Name:    "myval",
			}},
		},
	}
	_, err = cmtjson.Marshal(baseGenDocSet)
	assert.NoError(t, err, "error marshaling genDocSet")

	// create multiple types.GenesisDoc from struct
	genDocSetDefault := randomGenesisDocSet(3)
	err = genDocSetDefault.ValidateAndComplete()
	assert.NoError(t, err, "expected no error from random correct genDocSet struct")
}

func TestMultiplexGenesisDocSetSaveAs(t *testing.T) {
	tmpfile, err := os.CreateTemp("", "genesis")
	require.NoError(t, err)
	defer os.Remove(tmpfile.Name())

	genDocSet := randomGenesisDocSet()

	// save
	err = genDocSet.SaveAs(tmpfile.Name())
	require.NoError(t, err)
	stat, err := tmpfile.Stat()
	require.NoError(t, err)
	if err != nil && stat.Size() <= 0 {
		t.Fatalf("SaveAs failed to write any bytes to %v", tmpfile.Name())
	}

	err = tmpfile.Close()
	require.NoError(t, err)

	// load
	genDocSet2, err := mx.GenesisDocSetFromFile(tmpfile.Name())
	require.NoError(t, err)
	assert.EqualValues(t, genDocSet2, genDocSet)
	assert.Equal(t, genDocSet2, genDocSet)
}

func TestMultiplexGenesisDocSetValidatorHash(t *testing.T) {
	genDocSet := randomGenesisDocSet()
	assert.NotEmpty(t, genDocSet.ValidatorHash())
}

func TestMultiplexGenesisDocSetSearchGenesisDocByChainID(t *testing.T) {
	// defines a correct genesis doc
	genDocSetFmt := `[
		{
			"genesis_time": "0001-01-01T00:00:00Z",
			"chain_id": "%s",
			"initial_height": "1000",
			"consensus_params": null,
			"validators": [{
				"pub_key":{"type":"tendermint/PubKeyEd25519","value":"8y+xNWFp1i6PZeu5o9wfmDL6nwfMDxF6tVVJwTOmVjo="},
				"power":"10",
				"name":""
			}],
			"app_hash":"",
			"app_state":{"account_owner":"Bob"}
		}
	]`

	// searching for an existing ChainID
	testCases := []string{
		"abc1",
		"abc2",
	}

	for i, testChainID := range testCases {
		testCaseGenesis := fmt.Sprintf(genDocSetFmt, testChainID)
		testCaseDocSet, err := mx.GenesisDocSetFromJSON([]byte(testCaseGenesis))
		require.NoError(t, err)
		assert.Equal(t, 1, len(testCaseDocSet))

		doc, ok, err := testCaseDocSet.SearchGenesisDocByChainID(testChainID)
		assert.NoError(t, err)
		assert.Equal(t, true, ok, "ChainID should be found at "+strconv.Itoa(i))
		assert.Equal(t, testChainID, doc.ChainID)
	}

	// searching for non-existing ChainID
	testCases = []string{
		"abc3",
		"abc4",
	}

	failCaseGenesis := fmt.Sprintf(genDocSetFmt, "not-abc")
	failCaseDocSet, err := mx.GenesisDocSetFromJSON([]byte(failCaseGenesis))
	require.NoError(t, err)

	for _, failCaseChainID := range testCases {
		doc, ok, err := failCaseDocSet.SearchGenesisDocByChainID(failCaseChainID)
		assert.NoError(t, err)
		assert.Equal(t, false, ok)
		assert.Empty(t, doc)
	}
}

func TestMultiplexGenesisDocSetValidateGenesisDocChecksum(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	genesisDocSet := randomGenesisDocSet(3)
	for i, genesisDoc := range genesisDocSet {
		dbName := "genesis-" + strconv.Itoa(i)
		db, err := dbm.NewDB(dbName, dbm.BackendType("memdb"), rootDir)
		require.NoError(t, err)

		genesisDocJSON, err := cmtjson.Marshal(genesisDoc)
		require.NoError(t, err)
		expectedSha256 := tmhash.Sum(genesisDocJSON)

		err = mx.ValidateGenesisDocChecksum(
			&mx.ChainDB{
				ChainID: "test",
				DB:      db,
			},
			&genesisDoc,
		)
		assert.NoError(t, err)

		// Should save the genesis doc hash in db
		actualSha256, err := db.Get(genesisDocHashKey)
		assert.NoError(t, err)
		assert.Equal(t, true, bytes.Equal(expectedSha256, actualSha256))
	}
}

func TestMultiplexGenesisDocFromChainParams(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err)
	defer os.RemoveAll(rootDir)

	// Test with one validator
	genValidator := ed25519.GenPrivKey()
	cmtConsensus := types.DefaultConsensusParams().ToProto()

	cmtValidators, err := types.NewValidatorSet([]*types.Validator{
		types.NewValidator(genValidator.PubKey(), 10),
	}).ToProto()
	assert.NoError(t, err)
	assert.NotNil(t, cmtValidators)

	chainParams := &mxp2p.ChainParams{
		GenesisTime:     cmttime.Now(),
		ChainID:         "genesis-test1",
		InitialHeight:   1,
		ConsensusParams: &cmtConsensus,
		Validators:      *cmtValidators,
		AppHash:         []byte{1, 2, 3},
		AppStateBytes:   []byte("data"),
	}

	actualGenesisDoc, err := mx.GenesisDocFromChainParams(chainParams)
	assert.NoError(t, err, "should format ChainParams as GenesisDoc")
	assert.NotNil(t, actualGenesisDoc.GenesisTime)
	assert.Equal(t, "genesis-test1", actualGenesisDoc.ChainID)
	assert.NotEmpty(t, actualGenesisDoc.AppHash)
	assert.NotEmpty(t, actualGenesisDoc.AppState)

	// Test with many validators and mutate consensus params
	numValidators := 7
	validatorSetMany := types.NewValidatorSet([]*types.Validator{
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
		types.NewValidator(ed25519.GenPrivKey().PubKey(), 10),
	})
	cmtValidatorsMany, err := validatorSetMany.ToProto()
	assert.NoError(t, err)
	assert.NotNil(t, cmtValidatorsMany)
	assert.Len(t, cmtValidatorsMany.Validators, numValidators)

	expectedConsensusChange := int64(123456789)
	expectedInitialHeight := int64(101)
	expectedAppVersion := uint64(123)

	newConsensus := types.DefaultConsensusParams()
	newConsensus.Block.MaxBytes = expectedConsensusChange
	newConsensus.Block.MaxGas = expectedConsensusChange
	newConsensus.Version.App = expectedAppVersion
	cmtNewConsensus := newConsensus.ToProto()

	chainParamsMany := &mxp2p.ChainParams{
		GenesisTime:     cmttime.Now(),
		ChainID:         "genesis-test2",
		InitialHeight:   expectedInitialHeight,
		ConsensusParams: &cmtNewConsensus,
		Validators:      *cmtValidatorsMany,
		AppHash:         []byte{3, 2, 1},
		AppStateBytes:   []byte("data2"),
	}

	actualGenesisDoc2, err := mx.GenesisDocFromChainParams(chainParamsMany)
	assert.NoError(t, err, "should format ChainParams as GenesisDoc")
	assert.NotNil(t, actualGenesisDoc2.GenesisTime)
	assert.Equal(t, "genesis-test2", actualGenesisDoc2.ChainID)
	assert.Equal(t, expectedInitialHeight, actualGenesisDoc2.InitialHeight)
	assert.Len(t, actualGenesisDoc2.Validators, numValidators)
	assert.NotEmpty(t, actualGenesisDoc2.AppHash)
	assert.NotEmpty(t, actualGenesisDoc2.AppState)

	actualConsensusParams := actualGenesisDoc2.ConsensusParams
	assert.Equal(t, expectedConsensusChange, actualConsensusParams.Block.MaxBytes)
	assert.Equal(t, expectedConsensusChange, actualConsensusParams.Block.MaxGas)
	assert.Equal(t, expectedAppVersion, actualConsensusParams.Version.App)
}

func randomGenesisDocSet(opt ...int) mx.GenesisDocSet {
	cnt := 2
	if len(opt) > 0 {
		cnt = opt[0]
	}

	userGenDocs := make(mx.GenesisDocSet, cnt)
	for i := 0; i < cnt; i++ {
		valPubKey := ed25519.GenPrivKey().PubKey()

		// address and fingerprint added to ChainID
		valAddress := valPubKey.Address().String()
		fingerprint := strings.ToUpper(hex.EncodeToString(
			tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
		))

		mxChainID := "test-chain-" + valAddress + "-" + fingerprint
		userGenDocs[i] = types.GenesisDoc{
			GenesisTime:   cmttime.Now(),
			ChainID:       mxChainID,
			InitialHeight: 1000,
			Validators: []types.GenesisValidator{{
				Address: valPubKey.Address(),
				PubKey:  valPubKey,
				Power:   10,
				Name:    "myval",
			}},
			ConsensusParams: types.DefaultConsensusParams(),
			AppHash:         []byte{1, 2, 3},
			AppState:        []byte(`{"account_owner":"Bob"}`),
		}
	}
	return userGenDocs
}
