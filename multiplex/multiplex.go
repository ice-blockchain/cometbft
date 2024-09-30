package multiplex

import (
	"github.com/cometbft/cometbft/config"
	cmtlibs "github.com/cometbft/cometbft/libs/service"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/state/txindex"
	types "github.com/cometbft/cometbft/types"
)

// ----------------------------------------------------------------------------
// FILESYSTEM / ENV

// MultiplexFS maps scope hashes to filesystem paths (data/...)
type MultiplexFS map[string]string

// MultiplexNodeConfig maps scope hashes to configuration objects
type MultiplexNodeConfig map[string]*config.Config

// MultiplexGroup maps scope hashes to autofile group instances
type MultiplexGroup map[string]*ScopedGroup

// MultiplexWAL maps scope hashes to scoped WAL instances
type MultiplexWAL map[string]*ScopedWAL

// MultiplexDB maps scope hashes to database instances
type MultiplexDB map[string]*ScopedDB

// ----------------------------------------------------------------------------
// STATE / CONSENSUS

// MultiplexState maps scope hashes to state instances
type MultiplexState map[string]*ScopedState

// MultiplexStateStore maps scope hashes to state stores
type MultiplexStateStore map[string]*ScopedStateStore

// MultiplexBlockStore maps scope hashes to block stores
type MultiplexBlockStore map[string]*ScopedBlockStore

// MultiplexIndexerService maps scope hashes to indexer services
type MultiplexIndexerService map[string]*txindex.IndexerService

// ----------------------------------------------------------------------------
// SERVICES / P2P

// MultiplexService maps scope hashes to maps of services by name
type MultiplexService map[string]map[string]cmtlibs.Service

// MultiplexPrivValidator maps scope hashes to priv validators
type MultiplexPrivValidator map[string]types.PrivValidator

// MultiplexNode maps scope hashes to bootstrapped nodes
type MultiplexNode map[string]*ScopedNode

// MultiplexServiceAddress maps scope hashes to service address maps
type MultiplexServiceAddress map[string]map[string]string

// MultiplexPeerFilterFunc maps scope hashes to one or many peer filter functions
type MultiplexPeerFilterFunc map[string][]p2p.PeerFilterFunc
