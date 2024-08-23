package test

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmttest "github.com/cometbft/cometbft/internal/test"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/privval"
)

func ResetMultiplexPrivValidator(
	cfg config.BaseConfig,
	userAddress string,
	scope string,
	generate bool,
) *privval.FilePV {

	scopeHash := CreateScopeHash(fmt.Sprintf("%s:%s", userAddress, scope))
	scopeFingerprint := CreateFingerprint(scopeHash)

	userConfDir := filepath.Join(cfg.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(cfg.RootDir, config.DefaultDataDir, userAddress)

	folderName := scopeFingerprint
	privValKeyDir := filepath.Join(userConfDir, folderName)
	privValStateDir := filepath.Join(userDataDir, folderName)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(cfg.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(cfg.PrivValidatorStateFile()))

	// fmt.Printf("Resetting priv validator key file: %s\n", privValKeyFile)
	// fmt.Printf("Resetting priv validator state file: %s\n", privValStateFile)

	if !generate {
		// XXX careful this uses always the same priv validator, if a change is made to
		//     it, please also update the genesis.validators option.
		cmttest.ResetTestPrivValidatorFiles(privValKeyFile, privValStateFile)
		return privval.LoadFilePV(privValKeyFile, privValStateFile)
	}

	// IMPORTANT:
	// This generates a random privValidator private key
	filePV := privval.GenFilePV(privValKeyFile, privValStateFile)
	testPrivValidatorKey, _ := cmtjson.MarshalIndent(filePV.Key, "", "  ")
	cmtos.MustWriteFile(privValKeyFile, []byte(testPrivValidatorKey), 0o644)
	cmtos.MustWriteFile(privValStateFile, []byte(testPrivValidatorState), 0o644)

	return filePV
}

func ResetTestRootMultiplexDisabled(testName string, chainID string) *config.Config {
	return cmttest.ResetTestRootWithChainID(testName, chainID)
}

func ResetTestRootMultiplexWithChainIDAndScopes(
	testName string,
	chainID string,
	userScopes map[string][]string,
) *config.Config {

	// Calls EnsureRoot() and EnsureRootMultiplex() to reset filesystem
	baseConfig := resetRootDirWithChainIDAndScopes(testName, chainID, userScopes)
	rootDir := baseConfig.RootDir

	// IMPORTANT:
	// If there is a genesis file at the configured path, read it and expect it
	// to contain a genesis doc set ; otherwise create it with testOneScopedGenesisFmt.

	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)
	if !cmtos.FileExists(genesisFilePath) {
		if chainID == "" {
			chainID = cmttest.DefaultTestChainID
		}
		var testGenesis string
		if len(userScopes) == 0 {
			testGenesis = fmt.Sprintf(testUserGenesisFmt, TestUserAddress, TestScope, chainID, testDefaultGenesisValidator)

			// We overwrite the default priv validator files
			cmttest.ResetTestPrivValidator(rootDir, baseConfig)
		} else {
			testGenesis = `[`
			for i, scopeDesc := range baseConfig.GetScopes() {
				parts := strings.Split(scopeDesc, ":")
				userAddress, scope := parts[0], parts[1]

				// TODO: Right now, the chainID is forcefully overwritten in loop ; it is
				// the network owner's responsibility to provide separate chainIDs.

				// Creates one genesis doc per pair of user address and scope
				perUserChainID := chainID + "-" + strconv.Itoa(i)
				scopedTestGenesis := fmt.Sprintf(testOneScopedGenesisFmt, userAddress, scope, perUserChainID, testDefaultGenesisValidator)
				testGenesis += scopedTestGenesis + ","

				// resets priv validators to default state/key (as present in genesis)
				ResetMultiplexPrivValidator(baseConfig, userAddress, scope, false) // generate=false
			}

			// Removes last comma and closes json array
			testGenesis = testGenesis[:len(testGenesis)-1] + `]`
		}

		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// XXX TBI: put singular genesis docs inside scoped subfolders?

	conf := config.MultiplexTestConfig(baseConfig.Replication, userScopes).SetRoot(rootDir)
	return conf
}

// CAUTION: Calling this method will create a random priv validator and add it
// to the validator set that is present in the genesis doc.
func ResetTestRootMultiplexWithValidators(
	testName string,
	chainID string,
	userScopes map[string][]string, // user_address:[scope1,scope2]
	validators map[string][]string, // scope_hash:[validator1,validator2]
) *config.Config {

	// Calls EnsureRoot() and EnsureRootMultiplex() to reset filesystem
	baseConfig := resetRootDirWithChainIDAndScopes(testName, chainID, userScopes)
	rootDir := baseConfig.RootDir

	// IMPORTANT:
	// If there is a genesis file at the configured path, read it and expect it
	// to contain a genesis doc set ; otherwise create it with testOneScopedGenesisFmt.

	genesisFilePath := filepath.Join(rootDir, baseConfig.Genesis)

	if !cmtos.FileExists(genesisFilePath) {
		if chainID == "" {
			chainID = cmttest.DefaultTestChainID
		}
		var testGenesis string

		testGenesis = `[`

		for i, scopeDesc := range baseConfig.GetScopes() {
			parts := strings.Split(scopeDesc, ":")
			userAddress, scope := parts[0], parts[1]
			scopeHash := CreateScopeHash(scopeDesc)

			// TODO: Right now, the chainID is forcefully overwritten in loop ; it is
			// the network owner's responsibility to provide separate chainIDs.

			// Creates one genesis doc per pair of user address and scope
			perUserChainID := chainID + "-" + strconv.Itoa(i)

			// Resets priv validator to default state (as present in genesis)
			// generate=true means a NEW random priv validator is generated
			validatorSet, ok := validators[scopeHash]
			if !ok {
				// Resets priv validator to default KEY as well
				fmt.Printf("WARNING: no validators found, fallback to default validator set (%s)\n", scopeHash)
				validatorSet = []string{testGenesisValidatorPubKey}
				ResetMultiplexPrivValidator(baseConfig, userAddress, scope, false) // generate=false
			} else {
				// Resets priv validator to newly generated random key
				privVal := ResetMultiplexPrivValidator(baseConfig, userAddress, scope, true) // generate=true
				pvPubKey := base64.StdEncoding.EncodeToString(privVal.Key.PubKey.Bytes())
				validatorSet = append(validatorSet, pvPubKey)
			}

			validatorsJSON := ""
			for i, validatorPubKey := range validatorSet {
				validatorsJSON += fmt.Sprintf(testValidatorFmt, validatorPubKey, 10) // 10=voting power
				if i < len(validatorSet)-1 {
					validatorsJSON += ", "
				}
			}

			scopedTestGenesis := fmt.Sprintf(testOneScopedGenesisFmt, userAddress, scope, perUserChainID, validatorsJSON)
			testGenesis += scopedTestGenesis + ","
		}

		// Removes last comma and closes json array
		testGenesis = testGenesis[:len(testGenesis)-1] + `]`
		cmtos.MustWriteFile(genesisFilePath, []byte(testGenesis), 0o644)
	}

	// XXX TBI: put singular genesis docs inside scoped subfolders?

	conf := config.MultiplexTestConfig(baseConfig.Replication, userScopes).SetRoot(rootDir)
	return conf
}

// ----------------------------------------------------------------------------
// Helpers

func CreateScopeHash(scopeDesc string) string {
	sum256 := tmhash.Sum([]byte(scopeDesc))
	return strings.ToUpper(hex.EncodeToString(sum256))
}

func CreateFingerprint(scopeHash string) string {
	hashBytes, _ := hex.DecodeString(scopeHash)
	return strings.ToUpper(hex.EncodeToString(hashBytes[:8]))
}

func GeneratePrivValidators(testName string, numValidators int) []*privval.FilePV {
	tempDir, err := os.MkdirTemp("", "priv-validators-"+testName)
	if err != nil {
		panic(err)
	}

	privVals := make([]*privval.FilePV, numValidators)
	for i := 0; i < numValidators; i++ {
		indexPrivVal := strconv.Itoa(i)
		privValKeyFile := filepath.Join(tempDir, indexPrivVal+"_priv_validator_key.json")
		privValStateFile := filepath.Join(tempDir, indexPrivVal+"_priv_validator_state.json")

		filePV := privval.LoadOrGenFilePV(privValKeyFile, privValStateFile)
		privVals[i] = filePV
	}

	os.RemoveAll(tempDir)
	return privVals
}

// ----------------------------------------------------------------------------
// Private helpers

func resetRootDirWithChainIDAndScopes(
	testName string,
	chainID string,
	userScopes map[string][]string,
) config.BaseConfig {
	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", fmt.Sprintf("%s-%s_", chainID, testName))
	if err != nil {
		panic(err)
	}

	config.EnsureRoot(rootDir)

	baseConfig := config.MultiplexBaseConfig(
		config.PluralReplicationMode(),
		userScopes,
	)

	baseConfig.RootDir = rootDir

	// XXX:
	// This is not optimal as it is possible that undesired config will
	// be overwritten ; at best we should replace the EnsureRoot call
	// completely with this one. This solution is for further compat.
	config.EnsureRootMultiplex(rootDir, &baseConfig)
	return baseConfig
}

// ----------------------------------------------------------------------------

// TestScopeHash is a SHA256 of "user_address:scope" with TestUserAddress and scope "Default"
var TestScopeHash = "1A63C0E60122F9BB3DF9EA61C8184ED59C3936CAD1A3B35FA1D3BCA272E3F38B"
var TestFolderName = "1A63C0E60122F9BB" // fingerprint of TestScopeHash
var TestScope = "Default"
var TestUserAddress = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
var testUserGenesisFmt = `[` + testOneScopedGenesisFmt + `]`
var testOneScopedGenesisFmt = `{
	"user_address": "%s",
	"scope": "%s",
	"genesis": {
		"genesis_time": "2018-10-10T08:20:13.695936996Z",
		"chain_id": "%s",
		"initial_height": "1",
		"consensus_params": {
			"block": {
				"max_bytes": "22020096",
				"max_gas": "-1",
				"time_iota_ms": "10"
			},
			"synchrony": {
				"message_delay": "500000000",
				"precision": "10000000"
			},
			"evidence": {
				"max_age_num_blocks": "100000",
				"max_age_duration": "172800000000000",
				"max_bytes": "1048576"
			},
			"validator": {
				"pub_key_types": [
					"ed25519"
				]
			},
			"abci": {
				"vote_extensions_enable_height": "0"
			},
			"version": {},
			"feature": {
				"vote_extensions_enable_height": "0",
				"pbts_enable_height": "1"
			}
		},
		"validators": [
			%s
		],
		"app_hash": ""
	}
}`

var testGenesisValidatorPubKey = "AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
var testDefaultGenesisValidator = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"` + testGenesisValidatorPubKey + `"
	},
	"power": "10",
	"name": ""
}`

var testValidatorFmt = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"%s"
	},
	"power": "%d",
	"name": ""
}`

var testPrivValidatorKeyFmt = `{
  "address": "%s",
  "pub_key": {
    "type": "tendermint/PubKeyEd25519",
    "value": "%s"
  },
  "priv_key": {
    "type": "tendermint/PrivKeyEd25519",
    "value": "%s"
  }
}`

var testPrivValidatorState = `{
  "height": "0",
  "round": 0,
  "step": 0
}`
