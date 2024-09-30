package multiplex

import (
	"errors"
	"fmt"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/state/indexer"
	blockidxkv "github.com/cometbft/cometbft/state/indexer/block/kv"
	blockidxnull "github.com/cometbft/cometbft/state/indexer/block/null"
	"github.com/cometbft/cometbft/state/indexer/sink/psql"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/state/txindex/kv"
	"github.com/cometbft/cometbft/state/txindex/null"
)

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedIndexer tries to find a db instance in the db multiplex using its scope hash
// and creates instances for the TxIndexer and BlockIndexer.
//
// TODO(midas):
// The scoped indexer functionality is compatible only with the `kv` indexer for now,
// postgresql compatibility must be added. Appending the scope hash to the chainID
// in the NewEventSink() call may be enough to allow multiple indexers instances.
func GetScopedIndexer(
	cfg *config.Config,
	txindexMultiplexDB MultiplexDB,
	chainID string,
	userScopeHash string,
) (txindex.TxIndexer, indexer.BlockIndexer, error) {
	switch cfg.TxIndex.Indexer {
	case "kv":
		store, err := GetScopedDB(txindexMultiplexDB, userScopeHash)
		if err != nil {
			return nil, nil, err
		}

		return kv.NewTxIndex(store), blockidxkv.New(dbm.NewPrefixDB(store, []byte("block_events")), blockidxkv.WithCompaction(cfg.Storage.Compact, cfg.Storage.CompactionInterval)), nil

	case "psql":
		conn := cfg.TxIndex.PsqlConn
		if conn == "" {
			return nil, nil, errors.New("the psql connection settings cannot be empty")
		}
		es, err := psql.NewEventSink(cfg.TxIndex.PsqlConn, chainID)
		if err != nil {
			return nil, nil, fmt.Errorf("creating psql indexer: %w", err)
		}
		return es.TxIndexer(), es.BlockIndexer(), nil

	default:
		return &null.TxIndex{}, &blockidxnull.BlockerIndexer{}, nil
	}
}
