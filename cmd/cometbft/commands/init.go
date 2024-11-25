package commands

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	cfg "github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	kt "github.com/ice-blockchain/cometbft/internal/keytypes"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmtrand "github.com/ice-blockchain/cometbft/internal/rand"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// InitFilesCmd initializes a fresh CometBFT instance.
var InitFilesCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize CometBFT",
	RunE:  initFiles,
}

func init() {
	InitFilesCmd.Flags().StringVarP(&keyType, "key-type", "k", ed25519.KeyType, fmt.Sprintf("private key type (one of %s)", kt.SupportedKeyTypesStr()))
	InitFilesCmd.Flags().BoolVarP(&enableMultiplex, "multiplex", "m", false, "whether to enable the multiplex mode to run concurrent nodes")
	InitFilesCmd.Flags().StringVarP(&seedsFile, "seeds-file", "", "", "path to a JSON file containing chain seeds mapped by ChainID.")
	InitFilesCmd.Flags().StringVarP(&usersFile, "users-file", "", "", "path to a JSON file containing ChainID slices by user address.")
}

func initFiles(*cobra.Command, []string) error {
	return initFilesWithConfig(config)
}

func initFilesWithConfig(config *cfg.Config) error {
	// In multiplex mode, we generate more than one private validator
	// and use config.UserChains to generate a GenesisDocSet.
	if enableMultiplex {
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

// initMultiplexFilesWithConfig creates the required files for configuration
// of nodes multiplexes. It reads a genesis.json or users.json file to create
// a [config.MultiplexConfig] instance, then creates the necessary filesystem
// folders with [mx.MultiplexFS].
//
// This method also initializes a [mx.ChainRegistry] instance and multiple
// instances of [types.PrivValidator], as required to run a validating node
// for the supported networks. Finally, a [mx.GenesisDocSet] will be saved
// to disk if it was not present yet, using the above resources.
//
// CAUTION: This method is automatically called when the init command is run
// with the `--multiplex` flag. You should not have to call this method.
func initMultiplexFilesWithConfig(config *cfg.Config) error {
	var err error

	// One of genesis.json or users.json is required.
	genesisFile := config.GenesisFile()
	hasUsersFile := len(usersFile) > 0 && cmtos.FileExists(usersFile)
	if !cmtos.FileExists(genesisFile) && !hasUsersFile {
		return errors.New("missing required configuration: genesis.json or users.json")
	}

	// Generate or re-create the list of user chains
	// - If genesis.json is present, use it to re-create a map
	// - Otherwise, users.json must be present to create a map
	var userChains map[string][]string
	if cmtos.FileExists(genesisFile) {
		// Read the genesis.json file to re-create the map of slices with
		// ChainIDs by user addresses.
		userChains, err = mx.LoadChainsFromGenesisFile(config.GenesisFile())
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}

		logger.Info("Found chains from genesis file", "cnt", len(userChains))
	} else {
		// Read the users.json file to create the map of slices with
		// ChainIDs by user addresses.
		userChains, err = loadChainsFromUsersFile()
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}

		logger.Info("Found chains from users file", "cnt", len(userChains))
	}

	// Parse a --seeds-file option to force some chain seeds by config
	// Note that client extensions prevail and overrule chain seeds config.
	// See also: client.InjectSeedConfig()
	chainSeeds := map[string]string{}
	if len(seedsFile) > 0 && cmtos.FileExists(seedsFile) {
		chainSeeds, err = mx.LoadSeedsFromFile(seedsFile)
		if err != nil {
			return fmt.Errorf("failed to load seeds config: %w", err)
		}
	}

	// If we can't find a user chains configuration file or if it is invalid,
	// fallback to legacy node implementation.
	if len(userChains) == 0 {
		logger.Info("No user chains configuration found, fallback to legacy node implementation")
		enableMultiplex = false
		return initFilesWithConfig(config)
	}

	// Overwrite the UserChains (but keep prepared rootDir)
	rootDir := config.RootDir
	config.Strategy = mx.NetworkReplicationStrategy()
	config.ChainSeeds = chainSeeds
	config.UserChains = userChains
	config.SetRoot(rootDir)

	// Make sure we have /data and /config
	_, err = mx.NewMultiplexFS(config)
	if err != nil {
		return fmt.Errorf("could not create multiplex filesystem: %w", err)
	}

	// Create a ChainRegistry
	// Note that this executes configuration extensions (SyncConfig, SeedConfig)
	// See also: client.InjectSyncConfig(), client.InjectSeedConfig()
	chainRegistry, err := mx.NewChainRegistry(&config.MultiplexConfig)
	if err != nil {
		return err
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

	// Create as many private validators as there are networks and later
	// map each private validator instance to its corresponding ChainID.
	privValidators := map[string]*privval.FilePV{}
	for userAddress, chainIds := range config.UserChains {
		// e.g. /tmp/mx-chain/config/%address%/
		// e.g. /tmp/mx-chain/data/%address%/
		userConfDir := filepath.Join(config.RootDir, cfg.DefaultConfigDir, userAddress)
		userDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir, userAddress)

		for _, chainID := range chainIds {
			// Key file is in config/, State file is in data/
			privValKeyDir := filepath.Join(userConfDir, chainID)
			privValStateDir := filepath.Join(userDataDir, chainID)
			privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(config.PrivValidatorKeyFile()))
			privValStateFile := filepath.Join(privValStateDir, filepath.Base(config.PrivValidatorStateFile()))

			// We just try to load or generate it and saves to file
			filePV, err := privval.LoadOrGenFilePV(
				privValKeyFile,
				privValStateFile,
				genPrivKeyFromFlag,
			)
			if err != nil {
				return err
			}

			// May be necessary for generation of a genesis file
			privValidators[chainID] = filePV

			// Create multiplex configuration overwrite, the returned config
			// object contains the updated listen addresses, WAL file, seed
			// nodes config and state-sync config.
			configOverwrite := mx.NewConfigOverwrite(
				config,
				chainRegistry,
				chainID,
			)

			// Store node config in config/%address%/%ChainID%/config.toml
			configDir := filepath.Join(userConfDir, chainID)
			cfg.EnsureConfigFile(configDir, configOverwrite)
		}
	}

	// Genesis file is expected to contain multiple networks, i.e. [mx.GenesisDocSet]
	// BREAKING: The following block is *not* compatible with the legacy node implementation.
	genFile := config.GenesisFile()
	if cmtos.FileExists(genFile) {
		logger.Info("Found genesis file", "path", genFile)
	} else {
		err := createInitialGenesisDocSet(config, privValidators)
		if err != nil {
			return err
		}
	}

	return nil
}

func createInitialGenesisDocSet(
	config *cfg.Config,
	privValidators map[string]*privval.FilePV,
) error {
	var genesisDocSet mx.GenesisDocSet
	genDocSetFile := config.GenesisFile()

	for userAddress, chainIds := range config.UserChains {
		// e.g. /tmp/mx-chain/config/%address%/
		userConfDir := filepath.Join(config.RootDir, cfg.DefaultConfigDir, userAddress)

		for _, chainID := range chainIds {
			privVal, ok := privValidators[chainID]
			if !ok {
				return fmt.Errorf("could not find a priv validator for ChainID %s", chainID)
			}

			valPubKey, err := privVal.GetPubKey()
			if err != nil {
				return err
			}

			// Create new GenesisDoc with genesis time "now" and validators are
			// set to the priv validators loaded/generated with initMultiplexFilesWithConfig
			// TODO(midas): validators voting power = 10, probably needs change?
			genesisDoc := types.GenesisDoc{
				ChainID:         chainID,
				GenesisTime:     cmttime.Now(),
				ConsensusParams: types.DefaultConsensusParams(),
				Validators: []types.GenesisValidator{{
					Address: valPubKey.Address(),
					PubKey:  valPubKey,
					Power:   10,
				}},
			}

			// Store individual genesis docs (per replicated chain)
			// i.e.: %root%/config/%address%/%ChainID%/genesis.json
			genesisDocFile := filepath.Join(userConfDir, chainID, "genesis.json")
			if err := genesisDoc.SaveAs(genesisDocFile); err != nil {
				return err
			}

			// Store in memory in genesis doc set
			genesisDocSet = append(genesisDocSet, genesisDoc)
			logger.Info("Generated genesis doc", "ChainID", chainID)
		}
	}

	genDocSet := genesisDocSet
	if err := genDocSet.SaveAs(genDocSetFile); err != nil {
		return err
	}

	logger.Info("Generated multiplex genesis file", "path", genDocSetFile)
	return nil
}

// loadChainsFromUsersFile expects a JSON map where keys are user addresses
// and values are *slices* of 8-bytes fingerprints or ChainIDs.
//
// Note that we also accept slices of complete ChainIDs.
func loadChainsFromUsersFile() (map[string][]string, error) {
	if len(usersFile) == 0 || !cmtos.FileExists(usersFile) {
		return nil, fmt.Errorf("could not load chains from users.json at %s", usersFile)
	}

	logger.Info("Using users.json file", "path", usersFile)
	chainsBytes, err := os.ReadFile(usersFile)
	if err != nil {
		return nil, err
	}

	userFingerprints := map[string][]string{}
	err = json.Unmarshal(chainsBytes, &userFingerprints)
	if err != nil {
		return nil, err
	}

	userChains := map[string][]string{}
	for userAddress, fingerprints := range userFingerprints {
		// Drop empty chains/fingerprints list
		if len(fingerprints) == 0 {
			delete(userChains, userAddress)
		}

		// Build a ChainID with user address and fingerprint
		// or parse a complete ChainID, e.g.: mx-chain-...-...
		userChains[userAddress] = make([]string, len(fingerprints))
		for i, fp := range fingerprints {
			var chainID mx.ExtendedChainID

			if strings.HasPrefix(fp, mx.GetMultiplexPrefix()) {
				// Parses a ChainID, must contain address and fingerprint
				chainID, err = mx.NewExtendedChainIDFromLegacy(fp)
				if err != nil {
					return nil, err
				}
			} else {
				// Parses fingerprint, must be 8-bytes in hexadecimal
				chainID, err = mx.NewExtendedChainID(userAddress, fp)
				if err != nil {
					return nil, err
				}
			}

			userChains[userAddress][i] = chainID.String()
		}
	}

	return userChains, nil
}
