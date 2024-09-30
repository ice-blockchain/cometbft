package commands

import (
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	kt "github.com/cometbft/cometbft/internal/keytypes"
	cmtos "github.com/cometbft/cometbft/internal/os"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	mx "github.com/cometbft/cometbft/multiplex"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

// InitFilesCmd initializes a fresh CometBFT instance.
var InitFilesCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize CometBFT",
	RunE:  initFiles,
}

func init() {
	InitFilesCmd.Flags().StringVarP(&keyType, "key-type", "k", ed25519.KeyType, fmt.Sprintf("private key type (one of %s)", kt.SupportedKeyTypesStr()))
	InitFilesCmd.Flags().BoolVarP(&enableMultiplex, "multiplex", "m", false, fmt.Sprintf("whether to use plural replication mode"))
	InitFilesCmd.Flags().StringVarP(&scopesFile, "scopes-file", "", "", "path to a JSON file containing the initial user scopes that will be replicated.")
	InitFilesCmd.Flags().StringVarP(&seedsFile, "seeds-file", "", "", "path to a JSON file containing the replicate chains scope hashes and associated seeds.")
}

func initFiles(*cobra.Command, []string) error {
	return initFilesWithConfig(config)
}

func initFilesWithConfig(config *cfg.Config) error {
	// In plural replication mode, we generate more than one
	// private validator and use config.UserConfig.UserScopes.
	if enableMultiplex { // If you change this condition, see L128.
		return initMultiplexFilesWithConfig(config)
	}

	// EnsureRoot removed in cmd/root.go in favor of EnsureFilesystem,
	// so we need to EnsureConfig here to write the config file.
	cfg.EnsureConfig(config.RootDir, cfg.DefaultConfig())

	// private validator
	privValKeyFile := config.PrivValidatorKeyFile()
	privValStateFile := config.PrivValidatorStateFile()
	var pv *privval.FilePV
	if cmtos.FileExists(privValKeyFile) {
		pv = privval.LoadFilePV(privValKeyFile, privValStateFile)
		logger.Info("Found private validator", "keyFile", privValKeyFile,
			"stateFile", privValStateFile)
	} else {
		var err error
		pv, err = privval.GenFilePV(privValKeyFile, privValStateFile, genPrivKeyFromFlag)
		if err != nil {
			return fmt.Errorf("can't generate file pv: %w", err)
		}
		pv.Save()
		logger.Info("Generated private validator", "keyFile", privValKeyFile,
			"stateFile", privValStateFile)
	}

	// node key file
	nodeKeyFile := config.NodeKeyFile()
	if cmtos.FileExists(nodeKeyFile) {
		logger.Info("Found node key", "path", nodeKeyFile)
	} else {
		if _, err := p2p.LoadOrGenNodeKey(nodeKeyFile); err != nil {
			return err
		}
		logger.Info("Generated node key", "path", nodeKeyFile)
	}

	// genesis file
	genFile := config.GenesisFile()
	if cmtos.FileExists(genFile) {
		logger.Info("Found genesis file", "path", genFile)
	} else {
		genDoc := types.GenesisDoc{
			ChainID:         fmt.Sprintf("test-chain-%v", cmtrand.Str(6)),
			GenesisTime:     cmttime.Now(),
			ConsensusParams: types.DefaultConsensusParams(),
		}
		pubKey, err := pv.GetPubKey()
		if err != nil {
			return fmt.Errorf("can't get pubkey: %w", err)
		}
		genDoc.Validators = []types.GenesisValidator{{
			Address: pubKey.Address(),
			PubKey:  pubKey,
			Power:   10,
		}}

		if err := genDoc.SaveAs(genFile); err != nil {
			return err
		}
		logger.Info("Generated genesis file", "path", genFile)
	}

	return nil
}

func initMultiplexFilesWithConfig(config *cfg.Config) error {
	var err error
	genesisDoc := config.GenesisFile()
	if !cmtos.FileExists(genesisDoc) && len(scopesFile) == 0 {
		return errors.New("without a genesis.json the scopes file must be present (--scopes-file)")
	} else if cmtos.FileExists(genesisDoc) && len(seedsFile) == 0 {
		return errors.New("provided a genesis.json the seeds file must be present (--seeds-file)")
	}

	// Generate or re-create the list of user scopes
	userScopes := map[string][]string{}
	userSeeds := map[string]string{}
	if cmtos.FileExists(genesisDoc) {
		// Read the genesis.json file to re-create the map of slices with
		// arbitrary scopes by user address.
		userScopes, err = loadScopesFromGenesisFile(config.GenesisFile())
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}

		userSeeds, err = loadSeedsFromFile(seedsFile)
		if err != nil {
			return fmt.Errorf("failed to load seeds config: %w", err)
		}

		// TODO(midas): remove debug logs
		logger.Info("[DEBUG] found seeds file", "cnt", len(userSeeds))
	} else {
		// Otherwise, read the scopes.json file, it should contain a map of slices
		// with arbitrary scopes (plaintext) by user address.
		// e.g.: { "CC8E6555A3F401FF61DA098F94D325E7041BC43A": ["Default", "Another"] }
		userScopes, err = loadScopesFromFile(scopesFile)
		if err != nil {
			return fmt.Errorf("failed to load scopes config: %w", err)
		}
	}

	// If we can't find a user scopes configuration file or if it is invalid,
	// fallback to legacy node implementation in singular replication mode.
	if len(userScopes) == 0 {
		logger.Info("No user scopes configuration found, fallback to singular replication mode")
		enableMultiplex = false
		return initFilesWithConfig(config)
	}

	// Overwrite the UserConfig part with userScopes (but keep prepared rootDir)
	rootDir := config.RootDir
	config.BaseConfig = cfg.MultiplexBaseConfig(cfg.PluralReplicationMode(), userScopes)
	config.SetRoot(rootDir)

	// Create [ScopedUserConfig] instance for multiplex
	scopedUserConf := mx.NewScopedUserConfig(userScopes, 30001)
	scopeRegistry, err := mx.DefaultScopeHashProvider(&config.UserConfig)
	if err != nil {
		return fmt.Errorf("failed to create scope registry: %w", err)
	}

	if enableMultiplex {
		cfg.EnsureRootMultiplex(config.RootDir, &config.BaseConfig)
	}

	// Generate or load the node key file
	nodeKeyFile := config.NodeKeyFile()

	if cmtos.FileExists(nodeKeyFile) {
		logger.Info("Found node key", "path", nodeKeyFile)
	} else {
		nodeKey, err := p2p.LoadOrGenNodeKey(nodeKeyFile)
		if err != nil {
			return err
		}
		logger.Info("Generated node key", "path", nodeKeyFile, "id", string(nodeKey.ID()))
	}

	// Create as many private validators as there are user scopes (one per chain)
	individualScopeDescs := config.UserConfig.GetScopes()
	privVals := make(map[string]*privval.FilePV, len(individualScopeDescs))
	for _, addressAndScope := range individualScopeDescs {
		scopeId := mx.NewScopeIDFromDesc(addressAndScope)
		scParts := scopeId.Parts()

		mxRootDir := config.RootDir
		mxDataDir := filepath.Join(cfg.DefaultDataDir, scParts[0], scopeId.Fingerprint())

		// Try to find seeds for this network (parsed from seeds.json)
		// If none are set, we are setting up a new network.
		chainSeeds, ok := userSeeds[scopeId.Fingerprint()]
		if !ok {
			chainSeeds = ""
		}

		// TODO(midas): remove debug logs
		logger.Info("[DEBUG] using chain seed", "scope", scopeId.Fingerprint(), "node", chainSeeds)

		privValKeyFile := filepath.Join(mxDataDir, "priv_validator_key.json")
		privValStateFile := filepath.Join(mxDataDir, "priv_validator_state.json")

		filePV, err := privval.LoadOrGenFilePV(
			filepath.Join(mxRootDir, privValKeyFile),
			filepath.Join(mxRootDir, privValStateFile),
			genPrivKeyFromFlag,
		)
		if err != nil {
			return err
		}

		privVals[scopeId.Hash()] = filePV

		// Create listen addresses for this node, create a new
		// WAL in scope filesystem and set on consensus config
		config.PrivValidatorKey = privValKeyFile
		config.PrivValidatorState = privValStateFile
		thisNodeConfig := mx.NewMultiplexNodeConfig(
			config,
			scopedUserConf,
			scopeRegistry,
			scopeId.Hash(),
			chainSeeds,
		)

		// Store node config in config.toml
		configDir := filepath.Join(
			cfg.DefaultConfigDir,
			scParts[0],            // user address
			scopeId.Fingerprint(), // scope fingerprint
		)
		cfg.EnsureConfigFile(filepath.Join(mxRootDir, configDir), thisNodeConfig)
	}

	// Genesis doc set is expected to contain multiple replicated chains
	genFile := config.GenesisFile()
	if cmtos.FileExists(genFile) {
		logger.Info("Found genesis file", "path", genFile)
	} else {
		createInitialGenesisDocSet(config, privVals)
	}

	return nil
}

func createInitialGenesisDocSet(
	config *cfg.Config,
	privValidators map[string]*privval.FilePV,
) error {
	var userGenDocs []mx.UserScopedGenesisDoc
	userConfig := config.BaseConfig.UserConfig
	genDocSetFile := config.GenesisFile()

	for _, addressHex := range userConfig.GetAddresses() {
		userScopes := userConfig.UserScopes[addressHex]
		addressBytes, err := hex.DecodeString(addressHex)
		if err != nil {
			return err
		}

		userAddress := crypto.Address(addressBytes)

		for _, scopeName := range userScopes {
			scopeId := mx.NewScopeID(addressHex, scopeName)
			privVal, ok := privValidators[scopeId.Hash()]
			if !ok {
				return fmt.Errorf("could not find a priv validator for scope hash %s", scopeId.Hash())
			}

			valPubKey, err := privVal.GetPubKey()
			if err != nil {
				return err
			}

			// ChainID contains scope fingerprint
			userGenesisDoc := types.GenesisDoc{
				ChainID:         fmt.Sprintf("test-chain-mx-%v", scopeId.Fingerprint()),
				GenesisTime:     cmttime.Now(),
				ConsensusParams: types.DefaultConsensusParams(),
				Validators: []types.GenesisValidator{{
					Address: valPubKey.Address(),
					PubKey:  valPubKey,
					Power:   10,
				}},
			}

			// Store individual genesis docs (per replicated chain)
			// i.e.: %root%/config/%address%/%fingerprint%/genesis.json
			genFile := filepath.Join(
				config.RootDir,
				cfg.DefaultConfigDir,
				addressHex,
				scopeId.Fingerprint(),
				"genesis.json",
			)

			if err := userGenesisDoc.SaveAs(genFile); err != nil {
				return err
			}

			// Store in memory in genesis doc set
			userGenDocs = append(userGenDocs, mx.UserScopedGenesisDoc{
				UserAddress: userAddress,
				Scope:       scopeName,
				ScopeHash:   scopeId.Hash(),
				GenesisDoc:  userGenesisDoc,
			})

			logger.Info("Generated genesis doc", "scope", scopeId.Fingerprint())
		}
	}

	genDocSet := &mx.GenesisDocSet{
		GenesisDocs: userGenDocs,
	}

	if err := genDocSet.SaveAs(genDocSetFile); err != nil {
		return err
	}

	logger.Info("Generated multiplex genesis file", "path", genDocSetFile)

	return nil
}

func loadScopesFromFile(file string) (map[string][]string, error) {
	if len(file) == 0 || !cmtos.FileExists(file) {
		return map[string][]string{}, fmt.Errorf("could not find scopes file: %s", file)
	}

	logger.Info("Using scopes file", "path", file)
	scopesBytes, err := os.ReadFile(file)
	if err != nil {
		return map[string][]string{}, err
	}

	userScopes := map[string][]string{}
	err = json.Unmarshal(scopesBytes, &userScopes)
	if err != nil {
		return map[string][]string{}, err
	}

	return userScopes, nil
}

func loadScopesFromGenesisFile(genFile string) (map[string][]string, error) {
	if !cmtos.FileExists(genFile) {
		return map[string][]string{}, fmt.Errorf("genesis file does not exist: %s", genFile)
	}

	// CAUTION:
	// Genesis file is expected to contain a set of genesis docs
	genesisDocSet, err := mx.GenesisDocSetFromFile(genFile)
	if err != nil {
		return map[string][]string{}, fmt.Errorf("error unmarshalling GenesisDocSet: %s", err.Error())
	}

	numReplicatedChains := len(genesisDocSet.GenesisDocs)

	// Read the replicated chains scoped genesis docs to find a list of scopes
	// by user address. This map is later used to create a scope registry.
	userScopes := make(map[string][]string, numReplicatedChains)
	for _, userGenDoc := range genesisDocSet.GenesisDocs {
		userAddress := userGenDoc.UserAddress.String()

		if _, ok := userScopes[userAddress]; !ok {
			userScopes[userAddress] = []string{}
		}

		userScopes[userAddress] = append(userScopes[userAddress], userGenDoc.Scope)
	}

	return userScopes, nil
}

func loadSeedsFromFile(file string) (map[string]string, error) {
	if len(file) == 0 || !cmtos.FileExists(file) {
		return map[string]string{}, fmt.Errorf("could not find seeds file: %s", file)
	}

	logger.Info("Using seeds file", "path", file)
	seedsBytes, err := os.ReadFile(file)
	if err != nil {
		return map[string]string{}, err
	}

	userSeeds := map[string]string{}
	err = json.Unmarshal(seedsBytes, &userSeeds)
	if err != nil {
		return map[string]string{}, err
	}

	return userSeeds, nil
}
