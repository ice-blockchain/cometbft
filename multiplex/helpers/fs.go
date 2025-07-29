package helpers

import (
	"fmt"
	"path/filepath"

	"github.com/ice-blockchain/cometbft/config"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
)

// EnsureNetworkFS creates a filesystem structure for a single network
// with a user folder located in data/ and config/, and which contains
// one subfolder per network, using the ChainID.
//
// Return order is: config folder, data folder, error.
func EnsureNetworkFS(
	chainID ExtendedChainID,
	baseConfDir string,
	baseDataDir string,
) (string, string, error) {
	// Uses one subfolder by user in data/ and one in config/
	userDataDir := filepath.Join(baseDataDir, chainID.GetUserAddress())
	userConfDir := filepath.Join(baseConfDir, chainID.GetUserAddress())

	// .. and one subfolder by ChainID
	chainDataFolder := filepath.Join(userDataDir, chainID.String())
	chainConfFolder := filepath.Join(userConfDir, chainID.String())

	// Any error here means the directory is not accessible
	if err := cmtos.EnsureDir(chainDataFolder, config.DefaultDirPerm); err != nil {
		return "", "", fmt.Errorf(
			"missing mandatory data folder %s: %w", chainDataFolder, err)
	}

	// Any error here means the directory is not accessible
	if err := cmtos.EnsureDir(chainConfFolder, config.DefaultDirPerm); err != nil {
		return "", "", fmt.Errorf(
			"missing mandatory config folder %s: %w", chainConfFolder, err)
	}

	return chainConfFolder, chainDataFolder, nil
}
