package multiplex

import (
	"github.com/cometbft/cometbft/config"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/types"
)

// ----------------------------------------------------------------------------
// FILESYSTEM

// MultiplexFS maps scope hashes to filesystem paths (data/...)
type MultiplexFS map[string]string

// MultiplexNodeConfig maps scope hashes to configuration objects
type MultiplexNodeConfig map[string]*config.Config

// ----------------------------------------------------------------------------
// DATABASE

// MultiplexDB maps scope hashes to database instances
type MultiplexDB map[string]*ScopedDB

// MultiplexAddressBook maps scope hashes to P2P address books
type MultiplexAddressBook map[string]pex.AddrBook

// ----------------------------------------------------------------------------
// STATE / CONSENSUS

// MultiplexFlag maps scope hashes to bool values
type MultiplexFlag map[string]bool

// MultiplexState maps scope hashes to state instances
type MultiplexState map[string]*ScopedState

// MultiplexConsensus maps scope hashes to consensus states
type MultiplexConsensus map[string]*cs.State

// MultiplexStateStore maps scope hashes to state stores
type MultiplexStateStore map[string]*ScopedStateStore

// MultiplexBlockStore maps scope hashes to block stores
type MultiplexBlockStore map[string]*ScopedBlockStore

// MultiplexIndexerService maps scope hashes to indexer services
type MultiplexIndexerService map[string]*txindex.IndexerService

// ----------------------------------------------------------------------------
// SERVICES

// MultiplexPrivValidator maps scope hashes to priv validators
type MultiplexPrivValidator map[string]types.PrivValidator

// MultiplexSwitch maps scope hashes to P2P event switches
type MultiplexSwitch map[string]*ScopedSwitch

// MultiplexEventBus maps scope hashes to event buses
type MultiplexEventBus map[string]*types.EventBus

// MultiplexAppConn maps scope hashes to proxyApp app connections
type MultiplexAppConn map[string]*proxy.AppConns

// MultiplexPruner maps scope hashes to state pruners
type MultiplexPruner map[string]*sm.Pruner

// MultiplexNode maps scope hashes to bootstrapped nodes
type MultiplexNode map[string]*ScopedNode

// ----------------------------------------------------------------------------
// REACTORS

// MultiplexBlockSyncReactor maps scope hashes to blocksync reactors
type MultiplexBlockSyncReactor map[string]*p2p.Reactor

// MultiplexStateSyncReactor maps scope hashes to statesync reactors
type MultiplexStateSyncReactor map[string]*statesync.Reactor

// MultiplexConsensusReactor maps scope hashes to consensus reactors
type MultiplexConsensusReactor map[string]*cs.Reactor

// MultiplexEvidenceReactor maps scope hashes to evidence reactors
type MultiplexEvidenceReactor map[string]*evidence.Reactor

// MultiplexMempoolReactor maps scope hashes to mempool reactors
type MultiplexMempoolReactor map[string]*mempl.Reactor

// ----------------------------------------------------------------------------
// P2P

// MultiplexServiceAddress maps scope hashes to service address maps
type MultiplexServiceAddress map[string]map[string]string

// MultiplexP2PTransport maps scope hashes to p2p multiplex transports
type MultiplexP2PTransport map[string]*p2p.MultiplexTransport

// MultiplexPeerFilterFunc maps scope hashes to one or many peer filter functions
type MultiplexPeerFilterFunc map[string][]p2p.PeerFilterFunc
