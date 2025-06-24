package multiplex

import (
	"fmt"

	"github.com/ice-blockchain/cometbft/config"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
)

// initMultiplexDatabases initializes database tables for each replicated
// chain with table names: blockstore, state, tx_index and evidence.
//
// This method registers instances in the multiplexRegistry:
// - `database/blockstore`: the blockstore databases.
// - `database/state`: the state machine databases.
// - `database/tx_index`: the tx_index databases.
// - `database/evidence`: the evidence databases.
//
// TODO(midas): refactoring with MakeNetworkDatabases.
func (reactor *Reactor) InitMultiplexDatabases(chainIds []string) error {
	nodeConfig := reactor.GetNodeConfig()

	// Create blockstore databases
	bsMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "blockstore", Config: nodeConfig},
	}, chainIds)
	if err != nil {
		return err
	}

	// Create state databases
	stateMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "state", Config: nodeConfig},
	}, chainIds)
	if err != nil {
		return err
	}

	// Create indexer databases
	indexerMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "tx_index", Config: nodeConfig},
	}, chainIds)
	if err != nil {
		return err
	}

	// Create evidence databases
	evidenceMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "evidence", Config: nodeConfig},
	}, chainIds)
	if err != nil {
		return err
	}

	// Register the database instances with the Reactor (thread-safe)
	for _, chainID := range chainIds {
		reactor.RegisterInstance(InstanceKeyDatabaseBlock, chainID, bsMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseState, chainID, stateMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseIndex, chainID, indexerMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseEvidence, chainID, evidenceMultiplexDB[chainID])
	}

	return nil
}

// InitMultiplexStates loads a state multiplex using the reactor's
// instance provider to retrieve a state database instance by ChainID.
//
// First, a state database instance is retrieved, then the GenesisDoc checksum
// is validated against the hash stored in the state database.
// Next, a [sm.Store] instance is created around the state database
// instance for the respective replicated chain.
//
// And finally, this method will *load the state machine* using either the
// database or the GenesisDoc.
//
// This method also registers instances in the multiplexRegistry:
// - `state`: the [sm.State] state machine instances.
// - `stateStore`: the [sm.Store] instance attached to the database.
func (reactor *Reactor) InitMultiplexStates(chainIds []string) error {
	if len(chainIds) == 0 {
		// Nothing to do for now
		return nil
	}

	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving GenesisDoc instance by chain
	genesisDocProvider := reactor.GetGenesisProvider()

	// Used for retrieving state database instance by chain
	stateMachineProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseState)

	// Validate genesis configuration
	// Then get genesis doc set hashes from dbs or update
	for _, chainID := range chainIds {
		// Load genesis doc
		genesisDoc, genesisErr := genesisDocProvider(chainID)
		if genesisErr != nil {
			return genesisErr
		}

		// Validate per-chain genesis doc
		if err := genesisDoc.ValidateAndComplete(); err != nil {
			return fmt.Errorf("error in genesis doc for ChainID %s: %w", chainID, err)
		}

		// Retrieve this chain's state database instance
		stateDB := stateMachineProvider(chainID).(*ChainDB)

		// Validate the genesis doc hash vs. database
		if err := ValidateGenesisDocChecksum(stateDB, genesisDoc); err != nil {
			return err
		}

		// Initialize a replicable sm.Store
		dbKeyLayoutVersion := globalConfig.Storage.ExperimentalKeyLayout
		stateStore := sm.NewStore(stateDB, sm.StoreOptions{
			DBKeyLayout: dbKeyLayoutVersion,
		})

		// .. and load the state machine
		stateMachine, err := stateStore.Load()
		if err != nil {
			return fmt.Errorf("error loading state machine for ChainID %s: %w", chainID, err)
		}

		// .. or fill it from db/genesis doc
		if stateMachine.IsEmpty() {
			// Load the state from database or GenesisDoc
			stateMachine, err = stateStore.LoadFromDBOrGenesisDoc(genesisDoc)
			if err != nil {
				return err
			}

			// State machine was empty, update now
			if err := stateStore.Save(stateMachine); err != nil {
				return fmt.Errorf("could not save newly initialized state machine: %w", err)
			}
		}

		// Prepare registerable instance mapped to ChainID
		reactor.RegisterInstance(InstanceKeyState, chainID, stateMachine)
		reactor.RegisterInstance(InstanceKeyStateStore, chainID, stateStore)
	}

	return nil
}

// InitMultiplexBlockStores loads a [bs.BlockStore] multiplex using
// the reactor's instance provider to retrieve a blockstore database instance
// by ChainID.
//
// First, a blockstore database instance is retrieved, then a non-snapshottable
// [bs.BlockStore] instance is created around the blockstore database instance
// for the respective replicated chain.
//
// This method also registers instances in the multiplexRegistry:
// - `blockStore`: the [bs.BlockStore] instance attached to the database.
func (reactor *Reactor) InitMultiplexBlockStores(chainIds []string) error {
	if len(chainIds) == 0 {
		// Nothing to do for now
		return nil
	}

	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving blockstore database instance by chain
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseBlock)

	for _, chainID := range chainIds {
		// Retrieve this chain's state database instance
		blockstoreDB := blockStoreProvider(chainID).(*ChainDB)

		// Initialize a [bs.BlockStore] (not snapshottable)
		blockStore := bs.NewBlockStore(
			blockstoreDB,
			bs.WithCompaction(globalConfig.Storage.Compact, globalConfig.Storage.CompactionInterval),
			bs.WithDBKeyLayout(globalConfig.Storage.ExperimentalKeyLayout),
		)

		// Prepare registerable instance mapped to ChainID
		reactor.RegisterInstance(InstanceKeyBlockStore, chainID, blockStore)
	}

	return nil
}
