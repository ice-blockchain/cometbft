package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	cfg "github.com/cometbft/cometbft/config"
)

// ----------------------------------------------------------------------------
// Multiplex providers

// MultiplexFSProvider returns multiple data filesystem paths using DefaultDataDir
// specified in the Config and uses two levels of subfolders for user and scope.
// The scope-level subfolders are named after the first 8-bytes of the scope hash.
func MultiplexFSProvider(config *cfg.Config) (multiplex MultiplexFS, err error) {

	// When replication is in singular mode, we will create only one data dir
	// This mimics the default behaviour of CometBFT blockchain nodes' data dir
	if config.Replication == cfg.SingularReplicationMode() {
		multiplex = make(map[string]string, 1)
		multiplex[""] = cfg.DefaultDataDir
		return multiplex, nil
	}

	// This multiplex maps scope hashes to scoped data folders
	multiplex = map[string]string{}

	// Uses a singleton scope registry to create SHA256 once
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	if err != nil {
		return nil, err
	}

	// Storage is located in scopes subfolders per each user
	baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	baseConfDir := filepath.Join(config.RootDir, cfg.DefaultConfigDir)
	for _, userAddress := range config.GetAddresses() {
		// Uses one subfolder by user in data/ and one in config/
		userDataDir := filepath.Join(baseDataDir, userAddress)
		userConfDir := filepath.Join(baseConfDir, userAddress)

		// .. and one subfolder by scope
		for _, scope := range config.UserScopes[userAddress] {
			// Query scope hash from registry to avoid re-creating SHA256
			scopeHash, err := scopeRegistry.GetScopeHash(userAddress, scope)
			if err != nil {
				return nil, err
			}

			// The folder name is the hex representation of the fingerprint
			scopeId := NewScopeIDFromHash(scopeHash)
			folderName := scopeId.Fingerprint()

			scopedDataFolder := filepath.Join(userDataDir, folderName)
			scopedConfFolder := filepath.Join(userConfDir, folderName)
			multiplex[scopeHash] = scopedDataFolder

			// Any error here means the directory is not accessible
			if _, err := os.Stat(scopedDataFolder); err != nil {
				return nil, fmt.Errorf("missing mandatory data folder %s: %w", scopedDataFolder, err)
			}

			// Any error here means the directory is not accessible
			if _, err := os.Stat(scopedConfFolder); err != nil {
				return nil, fmt.Errorf("missing mandatory config folder %s: %w", scopedConfFolder, err)
			}
		}
	}

	return multiplex, nil
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedFS tries to find a filesystem path in the FS multiplex using its scope hash
// If an error is produced, the default data dir will be returned alongside the error
func GetScopedFS(
	multiplex MultiplexFS,
	userScopeHash string,
) (string, error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return cfg.DefaultDataDir, fmt.Errorf("incorrect scope hash for state multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedStoragePath, ok := multiplex[userScopeHash]; ok {
		return scopedStoragePath, nil
	}

	return cfg.DefaultDataDir, fmt.Errorf("could not find state in multiplex using scope hash %s", scopeHash)
}
