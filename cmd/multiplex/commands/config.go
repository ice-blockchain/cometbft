package commands

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
)

var (
	overwrite bool
	numUsers  int
	numChains int

	targetFile string
	seedNode   string
	hostname   string
)

func init() {
	GenUsersCmd.Flags().IntVarP(&numUsers, "num-users", "u", 10, "number of random user addresses")
	GenUsersCmd.Flags().IntVarP(&numChains, "num-chains", "c", 100, "number of ChainIDs to generate")
	GenUsersCmd.Flags().StringVarP(&targetFile, "file", "f", "/tmp/users.json", "file name (.json) that will be saved")
	GenUsersCmd.Flags().BoolVarP(&overwrite, "yes", "y", false, "confirm the overwrite of an existing file")

	GenSeedsCmd.Flags().StringVarP(&seedNode, "with-seed", "s", "", "path to a seed node root folder")
	GenSeedsCmd.Flags().StringVarP(&hostname, "with-host", "H", "localhost:30002", "hostname and port of the seed node")
	GenSeedsCmd.Flags().StringVarP(&targetFile, "file", "f", "/tmp/seeds.json", "file name (.json) that will be saved")
	GenSeedsCmd.Flags().BoolVarP(&overwrite, "yes", "y", false, "confirm the overwrite of an existing file")
}

// GenUsersCmd generates random user addresses and ChainIDs
// depending of the parameters. This command is to be used to
// generate random slices of ChainIDs for many users at once.
var GenUsersCmd = &cobra.Command{
	Use:   "gen-users",
	Short: "Generator for users.json files.",
	RunE: func(cmd *cobra.Command, _ []string) (err error) {
		if numUsers > numChains {
			return fmt.Errorf("number of users cannot exceed %d", numChains)
		}

		if _, err := os.Stat(targetFile); err == nil && !overwrite {
			return fmt.Errorf("a file already exists at %s (use -y to overwrite)", targetFile)
		}

		chainsPerUser := int(numChains / numUsers)
		numRestChains := numChains - (chainsPerUser * numUsers)

		randomChainIDs := make([]string, numChains)
		randomAddresses := make([]string, numUsers)
		randUserChains := make(map[string][]string, numUsers)

		for i := 0; i < int(numUsers); i++ {
			userPubKey := ed25519.GenPrivKey().PubKey()
			userAddress := userPubKey.Address().String()
			randomAddresses[i] = userAddress

			randUserChains[userAddress] = make([]string, chainsPerUser)
			for j := 0; j < chainsPerUser; j++ {
				fingerprint := makeFingerprint("Posts_" + strconv.Itoa(j))

				chainID, err := mx.NewExtendedChainID(userAddress, fingerprint)
				if err != nil {
					return err
				}

				randUserChains[userAddress][j] = chainID.String()
				randomChainIDs = append(randomChainIDs, chainID.String())
			}
		}

		for x := 0; x < numRestChains; x++ {
			userAddress := randomAddresses[len(randomAddresses)-1]
			fingerprint := makeFingerprint("Posts_rest_" + strconv.Itoa(x))

			chainID, err := mx.NewExtendedChainID(userAddress, fingerprint)
			if err != nil {
				return err
			}

			randUserChains[userAddress] = append(randUserChains[userAddress], chainID.String())
			randomChainIDs = append(randomChainIDs, chainID.String())
		}

		jsonBytes, err := json.Marshal(randUserChains)
		if err != nil {
			return fmt.Errorf("could not format to JSON: %w", err)
		}

		if err := os.WriteFile(targetFile, jsonBytes, 0644); err != nil {
			return fmt.Errorf("could not write to file %s: %w", targetFile, err)
		}

		return nil
	},
}

// GenSeedsCmd uses a `users.json` to generate a `seeds.json`
// file that identifies seed nodes per ChainID.
var GenSeedsCmd = &cobra.Command{
	Use:   "gen-seeds",
	Short: "Generator for seeds.json files.",
	RunE: func(cmd *cobra.Command, _ []string) (err error) {
		// --seed is required and must exist
		if _, err := os.Stat(seedNode); err != nil {
			return fmt.Errorf("failed to read node config: %s", seedNode)
		}

		if _, err := os.Stat(targetFile); err == nil && !overwrite {
			return fmt.Errorf("a file already exists at %s (use -y to overwrite)", targetFile)
		}

		// Read the seed node config
		seedCfg := config.DefaultConfig()
		if err := viper.Unmarshal(seedCfg); err != nil {
			return fmt.Errorf("failed to read seed node config: %w", err)
		}
		seedCfg.RootDir = seedNode
		seedCfg.SetRoot(seedCfg.RootDir)

		// Read the genesis.json file to re-create the map of slices with
		// ChainIDs by user addresses.
		userChains, err := mx.LoadChainsFromGenesisFile(seedCfg.GenesisFile())
		if err != nil {
			return fmt.Errorf("failed to load multiplex genesis: %w", err)
		}

		// Read the node_key.json to find the relay's CometBFT node id.
		seedNodeKey, err := p2p.LoadNodeKey(seedCfg.NodeKeyFile())
		if err != nil {
			return fmt.Errorf("failed to load multiplex config: %w", err)
		}
		seedNodeID := p2p.PubKeyToID(seedNodeKey.PubKey())

		chainIDs := []string{}
		seedNodes := map[string]string{}
		for _, chains := range userChains {
			for _, chainID := range chains {
				chainIDs = append(chainIDs, chainID)
				seedNodes[chainID] = string(seedNodeID) + "@" + hostname
			}
		}

		nodeLogger.Info("Found chains from genesis file", "cnt", len(chainIDs))

		jsonBytes, err := json.Marshal(seedNodes)
		if err != nil {
			return fmt.Errorf("could not format to JSON: %w", err)
		}

		if err := os.WriteFile(targetFile, jsonBytes, 0644); err != nil {
			return fmt.Errorf("could not write to file %s: %w", targetFile, err)
		}

		return nil
	},
}

func makeFingerprint(input string) string {
	return strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(input))[:8],
	))
}
