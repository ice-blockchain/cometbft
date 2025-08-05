package runtime

import (
	"path/filepath"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// NewConfig updates a node configuration in-place to overwrite
// the seed nodes and WAL filepaths so that each replicated chain writes
// to a separate WAL-file.
//
// It returns the newly created *deep-copy* of the node configuration.
func NewConfig(
	baseConfig *config.Config,
	withChainID string,
	seedNodes string,
	syncConfig *config.StateSyncConfig,
	discoveryPort int,
) *config.Config {
	// Validate the provided ChainID
	extChainID := helpers.NewExtendedChainIDFromString(withChainID)
	address := extChainID.GetUserAddress()

	// Deep-copy the config object to create multiple nodes
	mxConfig := deepCopyConfig(baseConfig)

	// ----------------------------
	// P2P Configuration Overwrite
	mxConfig.P2P.Seeds = seedNodes
	mxConfig.P2P.ListenAddress = overwriteListenPort(
		baseConfig.P2P.ListenAddress,
		discoveryPort+1, // defaults to 30002
	)

	// ----------------------------
	// RPC Configuration Overwrite
	mxConfig.RPC.ListenAddress = overwriteListenPort(
		baseConfig.RPC.ListenAddress,
		discoveryPort+2, // defaults to 30003
	)

	// ----------------------------
	// WAL Configuration Overwrite
	// i.e.: data/%address%/%ChainID%/wal
	dataDir := filepath.Join(baseConfig.RootDir, config.DefaultDataDir)
	walFile := filepath.Join(dataDir, address, withChainID, "wal")
	walPath := filepath.Join(config.DefaultDataDir, address, withChainID, "wal")

	// We overwrite the wal file to allow parallel I/O for multiple nodes
	mxConfig.Consensus.SetWalFile(walFile)
	mxConfig.Consensus.WalPath = walPath

	// ----------------------------
	// Sync Configuration Overwrite
	// We enable state-sync here if the config requires it (default false)
	mxConfig.StateSync.Enable = syncConfig.Enable
	mxConfig.StateSync.TrustPeriod = syncConfig.TrustPeriod
	mxConfig.StateSync.TrustHeight = syncConfig.TrustHeight
	mxConfig.StateSync.TrustHash = syncConfig.TrustHash

	// Note: at least 2 witnesses are required for state-sync
	mxConfig.StateSync.RPCServers = make([]string, len(syncConfig.RPCServers))
	copy(mxConfig.StateSync.RPCServers, syncConfig.RPCServers)

	return mxConfig
}

// -----------------------------------------------------------------------------

// deepCopyConfig deep-copies a config pointer to create a new config object.
func deepCopyConfig(cfg *config.Config) *config.Config {
	// Re-allocate new config
	next := &config.Config{
		BaseConfig:      config.BaseConfig{},
		RPC:             &config.RPCConfig{},
		GRPC:            &config.GRPCConfig{},
		P2P:             &config.P2PConfig{},
		Mempool:         &config.MempoolConfig{},
		StateSync:       &config.StateSyncConfig{},
		BlockSync:       &config.BlockSyncConfig{},
		Consensus:       &config.ConsensusConfig{},
		Storage:         &config.StorageConfig{},
		TxIndex:         &config.TxIndexConfig{},
		Instrumentation: &config.InstrumentationConfig{},
	}

	// Copy values from base
	next.BaseConfig = cfg.BaseConfig
	*next.RPC = *cfg.RPC
	*next.GRPC = *cfg.GRPC
	*next.P2P = *cfg.P2P
	*next.Mempool = *cfg.Mempool
	*next.StateSync = *cfg.StateSync
	*next.BlockSync = *cfg.BlockSync
	*next.Consensus = *cfg.Consensus
	*next.Storage = *cfg.Storage
	*next.TxIndex = *cfg.TxIndex
	*next.Instrumentation = *cfg.Instrumentation

	return next
}
