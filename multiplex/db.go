package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
)

// ScopedDBContext embeds a [cfg.DBContext] instance and adds a scope hash
type ScopedDBContext struct {
	ScopeHash string
	cfg.DBContext
}

// ScopedDB embeds a [dbm.DB] instance and adds a scope hash
type ScopedDB struct {
	ScopeHash string
	dbm.DB
}

// ----------------------------------------------------------------------------
// Multiplex providers

// OpenMultiplexDB returns multiple databases using the DBBackend and DBDir
// specified in the Config and uses two levels of subfolders for user and scope.
func OpenMultiplexDB(ctx *ScopedDBContext) (multiplex MultiplexDB, err error) {
	dbType := dbm.BackendType(ctx.Config.DBBackend)

	// This multiplex maps scope hashes to scoped database folders
	multiplex = MultiplexDB{}

	scopeRegistry, err := DefaultScopeHashProvider(&ctx.Config.UserConfig)
	if err != nil {
		return nil, err
	}

	// Storage is located in scopes subfolders per each user
	for _, userAddress := range ctx.Config.GetAddresses() {
		// Uses one subfolder by user
		dbStorage := filepath.Join(ctx.Config.DBDir(), userAddress)

		for _, scope := range ctx.Config.UserScopes[userAddress] {
			scopeHash, err := scopeRegistry.GetScopeHash(userAddress, scope)
			if err != nil {
				return nil, err
			}

			// The folder name is the hex representation of the fingerprint
			scopeId := NewScopeIDFromHash(scopeHash)
			folderName := scopeId.Fingerprint()

			// .. and one subfolder by scope
			dbStorage = filepath.Join(dbStorage, folderName)

			userDb, err := dbm.NewDB(ctx.ID, dbType, dbStorage)
			if err != nil {
				return nil, err
			}

			usDB := &ScopedDB{
				ScopeHash: scopeHash,
				DB:        userDb,
			}

			multiplex[scopeHash] = usDB
		}
	}

	return multiplex, nil
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedDB tries to find a DB in the multiplex using its scope hash
func GetScopedDB(
	multiplex MultiplexDB,
	userScopeHash string,
) (usDB *ScopedDB, err error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for user scoped DB, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedDB, ok := multiplex[userScopeHash]; ok {
		return scopedDB, nil
	}

	return nil, fmt.Errorf("could not find user scoped DB using scope hash %s", scopeHash)
}
