package multiplex

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// ----------------------------------------------------------------------------
// Multiplex database implementations
// - struct ChainDBContext defines a table name attached to a ChainID
// - struct ChainDB embeds a database instance and adds a ChainID
// - type MultiplexDB maps database instances to ChainID values

// ChainDBContext embeds a [config.DBContext] instance and adds a ChainID.
type ChainDBContext struct {
	ChainID string
	config.DBContext
}

// ChainDB embeds a [dbm.DB] instance and adds a ChainID.
type ChainDB struct {
	ChainID string
	dbm.DB
}

// MultiplexDB maps ChainIDs to database instances.
type MultiplexDB map[string]*ChainDB

// ----------------------------------------------------------------------------
// Providers

// NewMultiplexDB returns multiple databases using the DBBackend and DBDir
// specified in the Config and uses two levels of subfolders for user and chain.
//
// Note that a separate folder is created for every replicated chain and that
// it is organized under a user address parent folder in the `data/` folder.
func NewMultiplexDB(
	ctx *ChainDBContext,
	chainIds []string,
) (multiplex MultiplexDB, err error) {
	dbType := dbm.BackendType(ctx.Config.DBBackend)

	// This multiplex maps ChainID to database instances
	multiplex = MultiplexDB{}

	// Storage is located in ChainID subfolders per each user
	for _, chainID := range chainIds {
		extChainID := helpers.NewExtendedChainIDFromString(chainID)
		if extChainID == nil {
			return nil, errors.New("invalid ChainID")
		}

		// Uses one subfolder by user
		userAddress := extChainID.GetUserAddress()
		dbStorage := filepath.Join(ctx.Config.DBDir(), userAddress)

		// .. and one subfolder by ChainID
		dbStorage = filepath.Join(dbStorage, extChainID.String())
		chainDB, err := dbm.NewDB(ctx.ID, dbType, dbStorage)
		if err != nil {
			return nil, err
		}

		db := &ChainDB{
			ChainID: extChainID.String(),
			DB:      chainDB,
		}

		multiplex[extChainID.String()] = db
	}

	return multiplex, nil
}

// EnsureStartDBService asserts the type of s for it being a DBService and
// makes sure that it will be reset & started if it is necessary.
func EnsureStartDBService(ctx context.Context, s service.Service) error {
	if dbS, ok := s.(*DBService); ok {
		dbS.SetContext(ctx)
		if !dbS.IsRunning() {
			if dbS.IsStopped() {
				dbS.Reset(ctx) // reset stopped flag
			}
			dbS.Start()
		}

		return nil
	}

	return fmt.Errorf("failed to open database: %v", s)
}
