package types

const (
	// Resources
	InstanceKeyPathData      = "fs/data"
	InstanceKeyPathConf      = "fs/config"
	InstanceKeyConfig        = "resources/config"
	InstanceKeyGenesisDoc    = "resources/genesis"
	InstanceKeyPrivValidator = "resources/privValidator"
	InstanceKeyStateMachine  = "resources/stateMachine"
	InstanceKeyStateStore    = "resources/stateStore"
	InstanceKeyBlockStore    = "resources/blockStore"
	InstanceKeyBlockExecutor = "resources/blockExec"
	// Databases
	ServiceKeyDatabaseBlock    = "database/blockstore"
	ServiceKeyDatabaseState    = "database/state"
	ServiceKeyDatabaseIndex    = "database/txindex"
	ServiceKeyDatabaseEvidence = "database/evidence"
	// Services
	ServiceKeyEventBus         = "chain/eventBus"
	ServiceKeyIndexers         = "chain/indexer"
	ServiceKeyPruner           = "chain/pruner"
	ServiceKeyMempoolReactor   = "chain/mempoolReactor"
	ServiceKeyBlockSyncReactor = "chain/blocksyncReactor"
	ServiceKeyConsensusReactor = "chain/consensusReactor"
	ServiceKeyEvidenceReactor  = "chain/evidenceReactor"
	ServiceKeyAddressesReactor = "chain/addressesReactor" // PEX

	// Multiplex
	ServiceKeyNodeRuntime      = "chain/nodeRuntime"
	ServiceKeyMultiplexReactor = "shared/multiplexReactor"
)
