package types

const (
	// Instance types.
	InstanceKeyPathData      = "fs/data"
	InstanceKeyPathConf      = "fs/config"
	InstanceKeyConfig        = "resources/config"
	InstanceKeyGenesisDoc    = "resources/genesis"
	InstanceKeyPrivValidator = "resources/privValidator"
	InstanceKeyStateMachine  = "resources/stateMachine"
	InstanceKeyStateStore    = "resources/stateStore"
	InstanceKeyBlockStore    = "resources/blockStore"
	InstanceKeyBlockExecutor = "resources/blockExec"

	ServiceKeyDatabaseBlock    = "database/blockstore"
	ServiceKeyDatabaseState    = "database/state"
	ServiceKeyDatabaseIndex    = "database/txindex"
	ServiceKeyDatabaseEvidence = "database/evidence"

	ServiceKeyEventBus = "chain/eventBus"
	ServiceKeyIndexers = "chain/indexer"
	ServiceKeyPruner   = "chain/pruner"

	ServiceKeyMempoolReactor   = "chain/mempoolReactor"
	ServiceKeyBlockSyncReactor = "chain/blocksyncReactor"
	ServiceKeyConsensusReactor = "chain/consensusReactor"
	ServiceKeyEvidenceReactor  = "chain/evidenceReactor"
	ServiceKeyAddressesReactor = "chain/addressesReactor" // PEX

	ServiceKeyNodeRuntime = "chain/nodeRuntime"
)
