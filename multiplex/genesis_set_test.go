package multiplex

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	types "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

func TestMultiplexGenesisDocSetBad(t *testing.T) {
	// test some bad ones from raw json
	testCases := [][]byte{
		{},               // empty
		{1},              // junk
		[]byte(`{null}`), // invalid GenesisDocs
		[]byte(`[]`),     // empty GenesisDocs
		[]byte(`[{}]`),   // invalid GenesisDocs
		[]byte(`[{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF"}]`),                     // missing scope and GenesisDoc fields
		[]byte(`[{"scope": "Default"}]`),                                                             // missing user address and GenesisDoc fields
		[]byte(`[{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default"}]`), // missing GenesisDoc fields
		[]byte(`[{"user_address": null, "scope": "Default", "genesis": {"chain_id": "abc", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}}]`),                                  // nil user address
		[]byte(`[{"user_address": "A0", "scope": "Default", "genesis": {"chain_id": "abc", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}}]`),                                  // junk user address
		[]byte(`[{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": null, "genesis": {"chain_id": "abc", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}}]`), // nil scope
		[]byte(`[
			{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default", "genesis": {"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}},
			{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default", "genesis": {"chain_id": "abc2", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}}
		]`), // scope hash not unique
		[]byte(`[
			{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default" "genesis": {"chain_id": "abc1", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}},
			{"user_address": "FF080888BE0F48DE88927C3F49215B96548273AB", "scope": "Other", "genesis": {"chain_id": "abc2", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Charlie"}}},
			{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default", "genesis": {"chain_id": "abc3", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Dave"}}}
		]`), // scope hash not unique
		[]byte(`[
			{"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF", "scope": "Default", "genesis": {"chain_id": "same-chain", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}},
			{"user_address": "FF080888BE0F48DE88927C3F49215B96548273AB", "scope": "Default", "genesis": {"chain_id": "same-chain", "initial_height": "1", "consensus_params": null, "validators": null,"app_hash":"","app_state":{"account_owner":"Bob"}}}
		]`), // ChainID not unique
	}

	for i, testCase := range testCases {
		_, err := GenesisDocSetFromJSON(testCase)

		assert.Error(t, err, "expected error for invalid genDocSet json at "+strconv.Itoa(i))
	}
}

func TestMultiplexGenesisDocSetGood(t *testing.T) {
	// test one genesis doc by raw json
	genDocSetBytes := []byte(`[
		{
			"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF",
			"scope": "Default",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-QDKdJr",
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
		}
	]`)
	_, err := GenesisDocSetFromJSON(genDocSetBytes)
	assert.NoError(t, err, "expected no error for correct genesis docs set with single user from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultiple := []byte(`[
		{
			"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF",
			"scope": "Default",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-QDKdJr-1",
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
		},
		{
			"user_address": "CC8E6555A3F401FF61DA098F94D325E7041BC43A",
			"scope": "Default",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-QDKdJr-2",
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
		}
	]`)
	_, err = GenesisDocSetFromJSON(genDocSetBytesMultiple)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple users from json")

	// test multiple genesis docs by a correct raw json
	genDocSetBytesMultipleScopes := []byte(`[
		{
			"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF",
			"scope": "Default",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-QDKdJr-1",
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
		},
		{
			"user_address": "FF190888BE0F48DE88927C3F49215B96548273AF",
			"scope": "Another",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-QDKdJr-2",
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
		}
	]`)
	_, err = GenesisDocSetFromJSON(genDocSetBytesMultipleScopes)
	assert.NoError(t, err, "expected no error for correct genesis docs set with multiple scopes from json")

	// create a GenesisDocSet from struct
	userPubKey := ed25519.GenPrivKey().PubKey()
	valPubKey := ed25519.GenPrivKey().PubKey()
	baseGenDocSet := &GenesisDocSet{
		GenesisDocs: []UserScopedGenesisDoc{
			{
				UserAddress: userPubKey.Address(),
				GenesisDoc: types.GenesisDoc{
					ChainID: "abc",
					Validators: []types.GenesisValidator{{
						Address: valPubKey.Address(),
						PubKey:  valPubKey,
						Power:   10,
						Name:    "myval",
					}},
				},
			},
		},
	}
	_, err = cmtjson.Marshal(baseGenDocSet)
	assert.NoError(t, err, "error marshaling genDocSet")

	// create multiple UserScopedGenesisDoc from struct
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
	genDocSet2, err := GenesisDocSetFromFile(tmpfile.Name())
	require.NoError(t, err)
	assert.EqualValues(t, genDocSet2, genDocSet)
	assert.Equal(t, genDocSet2.GenesisDocs, genDocSet.GenesisDocs)
}

func TestMultiplexGenesisDocSetValidatorHash(t *testing.T) {
	genDocSet := randomGenesisDocSet()
	assert.NotEmpty(t, genDocSet.ValidatorHash())
}

func TestMultiplexGenesisDocSetSearchGenesisDocByUser(t *testing.T) {
	// defines a correct genesis doc
	genDocSetFmt := `[
		{
			"user_address": "%s",
			"scope": "%s",
			"genesis": {
				"genesis_time": "0001-01-01T00:00:00Z",
				"chain_id": "test-chain-1",
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
		}
	]`

	// searching for an existing scope
	testCases := [][]string{
		{"FF190888BE0F48DE88927C3F49215B96548273AF", "Default"},
		{"CC8E6555A3F401FF61DA098F94D325E7041BC43A", "Default"},
	}

	for i, testCaseData := range testCases {
		testCaseUserAddress, testCaseScope := testCaseData[0], testCaseData[1]
		testCaseGenesis := fmt.Sprintf(genDocSetFmt, testCaseUserAddress, testCaseScope)
		testCaseDocSet, err := GenesisDocSetFromJSON([]byte(testCaseGenesis))
		require.NoError(t, err)
		assert.Equal(t, 1, len(testCaseDocSet.GenesisDocs))

		testCaseScopeID := NewScopeID(testCaseUserAddress, testCaseScope)
		testCaseScopeHash := testCaseScopeID.Hash()
		assert.NotEmpty(t, testCaseScopeHash, "scopeID hash should not be empty")

		doc, ok, err := testCaseDocSet.SearchGenesisDocByScope(testCaseScopeHash)
		assert.NoError(t, err)
		assert.Equal(t, true, ok, "scope hash should be found at "+strconv.Itoa(i))
		assert.Equal(t, testCaseUserAddress, doc.UserAddress.String())
	}

	// searching for non-existing scopes
	testCases = [][]string{
		{"AA190888BE0F48DE88927C3F49215B96548273AF", "Default"}, // 1-byte address change
		{"FF190888BE0F48DE88927C3F49215B96548273AF", "default"}, // 1-byte scope change
	}

	failCaseGenesis := fmt.Sprintf(genDocSetFmt, "FF190888BE0F48DE88927C3F49215B96548273AF", "Default")
	failCaseDocSet, err := GenesisDocSetFromJSON([]byte(failCaseGenesis))
	require.NoError(t, err)

	for _, failCaseData := range testCases {
		failCaseUserAddress, failCaseScope := failCaseData[0], failCaseData[1]

		failCaseScopeID := NewScopeID(failCaseUserAddress, failCaseScope)
		failCaseScopeHash := failCaseScopeID.Hash()
		assert.NotEmpty(t, failCaseScopeHash, "scopeID hash should not be empty")

		doc, ok, err := failCaseDocSet.SearchGenesisDocByScope(failCaseScopeHash)
		assert.NoError(t, err)
		assert.Equal(t, false, ok)
		assert.Empty(t, doc)
	}

	// searching for invalid scope hashes (sha256)
	testCases = [][]string{
		{""},   // empty
		{"A0"}, // too short
		{"AA190888BE0F48DE88927C3F49215B96548273AFAA190888BE0F48DE88927C3F49215B96548273AF"}, // too long
	}

	for _, failCaseScopeHash := range testCases {
		_, ok, err := failCaseDocSet.SearchGenesisDocByScope(failCaseScopeHash[0])
		assert.Error(t, err)
		assert.Equal(t, false, ok)
	}
}

func randomGenesisDocSet(opt ...int) *GenesisDocSet {
	cnt := 2
	if len(opt) > 0 {
		cnt = opt[0]
	}

	var userGenDocs []UserScopedGenesisDoc
	for i := 0; i < cnt; i++ {
		userPubKey := ed25519.GenPrivKey().PubKey()
		valPubKey := ed25519.GenPrivKey().PubKey()
		scopeId := NewScopeID(userPubKey.Address().String(), "Default")

		userGenDocs = append(userGenDocs, UserScopedGenesisDoc{
			UserAddress: userPubKey.Address(),
			Scope:       "Default",
			ScopeHash:   scopeId.Hash(),
			GenesisDoc: types.GenesisDoc{
				GenesisTime:   cmttime.Now(),
				ChainID:       "test-chain-" + strconv.Itoa(i),
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
			},
		})
	}
	return &GenesisDocSet{
		GenesisDocs: userGenDocs,
	}
}
