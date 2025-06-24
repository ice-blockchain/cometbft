package commands

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	kt "github.com/ice-blockchain/cometbft/internal/keytypes"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// InitMxFilesCmd initializes a fresh CometBFT multiplex instance.
var InitMxFilesCmd = &cobra.Command{
	Use:   "init",
	Short: "Initialize CometBFT multiplex",
	RunE:  initFiles,
}

// init registers custom command line options.
func init() {
	InitMxFilesCmd.Flags().StringVarP(&keyType, "key-type", "k", ed25519.KeyType, fmt.Sprintf("private key type (one of %s)", kt.SupportedKeyTypesStr()))
	InitMxFilesCmd.Flags().StringVarP(&usersFile, "users-file", "", "", "path to a JSON file containing ChainID slices by user address.")
	InitMxFilesCmd.Flags().StringVarP(&seedsFile, "seeds-file", "", "", "path to a JSON file containing seed nodes mapped by ChainID.")
	InitMxFilesCmd.Flags().Uint16VarP(&discoveryPort, "relay-port", "", 30001, "a port number used as the multiplex backend p2p discovery port.")
}

// initFiles uses [initMultiplexFilesWithConfig] using the global
// nodeConfig object populated previously by root.go.
func initFiles(*cobra.Command, []string) error {
	return initMultiplexFilesWithConfig(nodeConfig)
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
func initMultiplexFilesWithConfig(withCfg *config.Config) error {
	var err error

	// One of genesis.json or users.json is required.
	genesisFile := withCfg.GenesisFile()
	hasUsersFile := len(usersFile) > 0 && cmtos.FileExists(usersFile)
	if !cmtos.FileExists(genesisFile) && !hasUsersFile {
		nodeLogger.Info("Skipping import, genesis.json and users.json not found.")
	}

	// Generate or re-create the list of user chains
	// - If genesis.json is present, use it to re-create a map
	// - Otherwise, users.json must be present to create a map
	userChains := map[string][]string{}
	if cmtos.FileExists(genesisFile) {
		// Read the genesis.json file to re-create the map of slices with
		// ChainIDs by user addresses.
		userChains, err = mx.LoadChainsFromGenesisFile(withCfg.GenesisFile())
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}

		nodeLogger.Info("Found chains from genesis file", "cnt", len(userChains))
	} else if hasUsersFile {
		// Read the users.json file to create the map of slices with
		// ChainIDs by user addresses.
		userChains, err = loadChainsFromUsersFile()
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}

		nodeLogger.Info("Found chains from users file", "cnt", len(userChains))
	}

	// Report if we couldn't find a user chains configuration file
	if len(userChains) == 0 {
		nodeLogger.Info("Found 0 chains, creating an empty multiplex backend")
	}

	// Parse a --seeds-file option to force some chain seeds by config
	chainSeeds := map[string]string{}
	if len(seedsFile) > 0 && cmtos.FileExists(seedsFile) {
		chainSeeds, err = mx.LoadSeedsFromFile(seedsFile)
		if err != nil {
			return fmt.Errorf("failed to load seeds config: %w", err)
		}
	}

	// Overwrite the UserChains
	withCfg.Strategy = mx.NetworkReplicationStrategy()
	withCfg.UserChains = userChains
	withCfg.DiscoveryPort = discoveryPort
	withCfg.DBBackend = "goleveldb"

	// Overwrite seed nodes to connect/synchronize with existing networks.
	if len(chainSeeds) > 0 {
		withCfg.ChainSeeds = chainSeeds
	}

	// TODO(midas): Adding all ChainIDs here is for backwards compatibility.
	// TODO(midas): This command may be removed OR adapted for latest runtime impl.
	allChainIds := []string{}
	for _, chainIds := range config.UserChains {
		allChainIds = append(allChainIds, chainIds...)
	}

	// Make sure we have /data and /config
	_, err = mx.NewMultiplexFS(withCfg, allChainIds)
	if err != nil {
		return fmt.Errorf("could not create multiplex filesystem: %w", err)
	}

	// Create a ChainRegistry
	// Note that this executes configuration extensions (SyncConfig, SeedConfig)
	// See also: client.InjectSyncConfig(), client.InjectSeedConfig()
	chainRegistry, err := mx.NewChainRegistry(&withCfg.MultiplexConfig)
	if err != nil {
		return err
	}

	// Generate or load the node key file
	nodeKeyFile := withCfg.NodeKeyFile()

	if cmtos.FileExists(nodeKeyFile) {
		nodeLogger.Info("Found node key", "path", nodeKeyFile)
	} else {
		nodeKey, err := p2p.LoadOrGenNodeKey(nodeKeyFile)
		if err != nil {
			return err
		}
		nodeLogger.Info("Generated node key", "path", nodeKeyFile, "id", string(nodeKey.ID()))
	}

	// Create as many private validators as there are networks and later
	// map each private validator instance to its corresponding ChainID.
	privValidators := map[string]*privval.FilePV{}
	for userAddress, chainIds := range withCfg.UserChains {
		// e.g. /tmp/mx-chain/config/%address%/
		// e.g. /tmp/mx-chain/data/%address%/
		userConfDir := filepath.Join(withCfg.RootDir, config.DefaultConfigDir, userAddress)
		userDataDir := filepath.Join(withCfg.RootDir, config.DefaultDataDir, userAddress)

		for _, chainID := range chainIds {
			// Key file is in config/, State file is in data/
			keyFile := filepath.Base(withCfg.PrivValidatorKeyFile())
			datFile := filepath.Base(withCfg.PrivValidatorStateFile())
			privValKeyFile := filepath.Join(userConfDir, chainID, keyFile)
			privValStateFile := filepath.Join(userDataDir, chainID, datFile)

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
				withCfg,
				chainRegistry,
				chainID,
			)

			// Store node config in config/%address%/%ChainID%/config.toml
			configDir := filepath.Join(userConfDir, chainID)
			config.EnsureConfigFile(configDir, configOverwrite)
		}
	}

	// Genesis file is expected to contain multiple networks,
	// i.e. [mx.GenesisDocSet]
	genFile := withCfg.GenesisFile()
	if cmtos.FileExists(genFile) {
		nodeLogger.Info("Found genesis file", "path", genFile)
	} else {
		err := createInitialGenesisDocSet(withCfg, privValidators)
		if err != nil {
			return err
		}
	}

	return nil
}

// createInitialGenesisDocSet iterates a multiplex configuration to create
// a compatible [mx.GenesisDocSet] instance which also contains the correct
// validators public keys.
//
// TODO(midas): Voting power must be adapted for validity of BFT consensus.
func createInitialGenesisDocSet(
	withCfg *config.Config,
	privValidators map[string]*privval.FilePV,
) error {
	genesisDocSet := mx.GenesisDocSet{}
	genDocSetFile := withCfg.GenesisFile()

	for userAddress, chainIds := range withCfg.UserChains {
		// e.g. /tmp/mx-chain/config/%address%/
		userConfDir := filepath.Join(withCfg.RootDir, config.DefaultConfigDir, userAddress)

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
			genesisDoc := types.GenesisDoc{
				ChainID:         chainID,
				GenesisTime:     cmttime.Now(),
				ConsensusParams: types.DefaultConsensusParams(),
				Validators: []types.GenesisValidator{{
					Address: valPubKey.Address(),
					PubKey:  valPubKey,
					Power:   100, // TODO(midas): `(num_vals x 100) / (num_vals - (num_vals/2))`
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
			nodeLogger.Info("Generated genesis doc", "ChainID", chainID)
		}
	}

	genDocSet := genesisDocSet
	if err := genDocSet.SaveAs(genDocSetFile); err != nil {
		return err
	}

	nodeLogger.Info("Generated multiplex genesis file", "path", genDocSetFile)
	return nil
}

// loadChainsFromUsersFile expects a JSON map where keys are user addresses
// and values are *slices* of 8-bytes fingerprints or ChainIDs.
// e.g.: `{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": ["3E547E3280313019"]}`
//
// Note that we also accept slices of complete ChainIDs.
func loadChainsFromUsersFile() (map[string][]string, error) {
	if len(usersFile) == 0 || !cmtos.FileExists(usersFile) {
		return nil, fmt.Errorf("could not load chains from users.json at %s", usersFile)
	}

	nodeLogger.Info("Using users.json file", "path", usersFile)
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
