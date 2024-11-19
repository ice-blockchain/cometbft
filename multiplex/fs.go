package multiplex

import (
	"fmt"
	"path/filepath"

	"github.com/ice-blockchain/cometbft/config"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
)

// MultiplexFS maps ChainIDs to filesystem paths (data/...)
type MultiplexFS map[string]string

// ----------------------------------------------------------------------------
// Providers

// NewMultiplexFS returns multiple data filesystem paths using DefaultDataDir
// specified in the Config and uses two levels of subfolders for user and chain.
//
// Note that a separate folder is created for every replicated chain and that
// it is organized under a user address parent folder in the `data/` folder.
func NewMultiplexFS(conf *config.Config) (multiplex MultiplexFS, err error) {
	// When replication is *disabled*, we will create only one data dir
	// This mimics the default behavior of CometBFT blockchain nodes' data dir
	if conf.Strategy == config.DefaultReplicationStrategy() {
		multiplex = make(map[string]string, 1)
		multiplex[""] = config.DefaultDataDir
		return multiplex, nil
	}

	// This multiplex maps ChainIDs to filesystem paths
	multiplex = map[string]string{}

	// Storage is located in ChainID subfolders per each user
	// i.e.: data/%address%/%ChainID%/...
	baseDataDir := filepath.Join(conf.BaseConfig.RootDir, config.DefaultDataDir)
	baseConfDir := filepath.Join(conf.BaseConfig.RootDir, config.DefaultConfigDir)
	for userAddress, chainIds := range conf.UserChains {
		// Uses one subfolder by user in data/ and one in config/
		userDataDir := filepath.Join(baseDataDir, userAddress)
		userConfDir := filepath.Join(baseConfDir, userAddress)

		// .. and one subfolder by ChainID
		for _, chainID := range chainIds {
			chainID, err := NewExtendedChainIDFromLegacy(chainID)
			if err != nil {
				return multiplex, err
			}

			folderName := chainID.String()
			chainDataFolder := filepath.Join(userDataDir, folderName)
			chainConfFolder := filepath.Join(userConfDir, folderName)
			multiplex[chainID.String()] = chainDataFolder

			// Any error here means the directory is not accessible
			if err := cmtos.EnsureDir(chainDataFolder, config.DefaultDirPerm); err != nil {
				return multiplex, fmt.Errorf("missing mandatory data folder %s: %w", chainDataFolder, err)
			}

			// Any error here means the directory is not accessible
			if err := cmtos.EnsureDir(chainConfFolder, config.DefaultDirPerm); err != nil {
				return multiplex, fmt.Errorf("missing mandatory config folder %s: %w", chainConfFolder, err)
			}
		}
	}

	return multiplex, nil
}
