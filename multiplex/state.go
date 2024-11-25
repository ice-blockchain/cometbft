package multiplex

import (
	"fmt"

	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
)

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
func (reactor *Reactor) InitMultiplexStates() error {
	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving GenesisDoc instance by chain
	genesisDocProvider := reactor.GetGenesisProvider()

	// Used for retrieving state database instance by chain
	stateMachineProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseState)

	// Validate genesis configuration
	// Then get genesis doc set hashes from dbs or update
	for _, chainID := range reactor.GetNetworks() {
		// Validate per-chain genesis doc
		genesisDoc := genesisDocProvider(chainID)
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
func (reactor *Reactor) InitMultiplexBlockStores() error {
	// Used for database key layouts
	globalConfig := reactor.GetNodeConfig()

	// Used for retrieving blockstore database instance by chain
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseBlock)

	for _, chainID := range reactor.GetNetworks() {
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
