package multiplex

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/ice-blockchain/cometbft/config"
)

// -----------------------------------------------------------------------------
// ReplicationStrategy

// HistoryReplicationStrategy() returns the historical node type which uses a mode of
// "History", i.e. it does not synchronize with replicated chains.
func HistoryReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("History")
}

// NetworkReplicationStrategy() returns the replicator node type which uses a mode of
// "Network", i.e. it does synchronize with replicated chains.
func NetworkReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("Network")
}

// DisableReplicationStrategy() returns the legacy node type which uses a mode,
// i.e. it disables multiplex features and uses the legacy node implementation.
func DisableReplicationStrategy() config.ReplicationStrategy {
	return config.NewReplicationStrategy("Disable")
}

// -----------------------------------------------------------------------------
// Options helper implementations for [config.MultiplexConfig].

// WithStrategy is an option helper that allows you to overwrite the
// default Strategy in [MultiplexConfig].
// By default, this option is set to "Disable".
func WithStrategy(strategy config.ReplicationStrategy) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.Strategy = strategy
	}
}

// WithSyncConfig is an option helper that allows you to overwrite the
// default SyncConfig in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithSyncConfig(syncConfigs map[string]*config.StateSyncConfig) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.SyncConfig = make(map[string]*config.StateSyncConfig, len(syncConfigs))
		for chainID, syncConfig := range syncConfigs {
			conf.SyncConfig[chainID] = syncConfig
		}
	}
}

// WithChainSeeds is an option helper that allows you to overwrite the
// default (empty) ChainSeeds in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithChainSeeds(chainSeeds map[string]string) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.ChainSeeds = make(map[string]string, len(chainSeeds))
		for chainID, seedNodes := range chainSeeds {
			conf.ChainSeeds[chainID] = seedNodes
		}
	}
}

// WithUserChains is an option helper that allows you to overwrite the
// default (empty) UserChains in [MultiplexConfig].
// By default, this option is set to an empty map.
func WithUserChains(userChains map[string][]string) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.UserChains = map[string][]string{}
		for address, fingerprints := range userChains {
			conf.UserChains[address] = make([]string, len(fingerprints))
			conf.UserChains[address] = append(conf.UserChains[address], fingerprints...)
		}
	}
}

// WithP2PStartPort is an option helper that allows you to overwrite the
// default P2PStartPort in [MultiplexConfig].
// By default, this option is set to 30001.
func WithP2PStartPort(p2pStartPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.P2PStartPort = p2pStartPort
	}
}

// WithRPCStartPort is an option helper that allows you to overwrite the
// default RPCStartPort in [MultiplexConfig].
// By default, this option is set to 40001.
func WithRPCStartPort(rpcStartPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.RPCStartPort = rpcStartPort
	}
}

// WithBroadcastPort is an option helper that allows you to overwrite the
// default BroadcastPort in [MultiplexConfig].
// By default, this option is set to 50001.
func WithBroadcastPort(broadcastPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.BroadcastPort = broadcastPort
	}
}

// NewConfigOverwrite updates a node configuration in-place to overwrite the
// services listen addresses and uses the chainRegistry instance to retrieve
// seed nodes configuration and state-sync configuration.
//
// This method uses [NewConfigOverwriteWithParameters] after having read the
// parameters from the chainRegistry.
func NewConfigOverwrite(
	baseConfig *config.Config,
	chainRegistry ChainRegistry,
	withChainID string,
) *config.Config {
	// Multiplex can be configured to start at different port
	p2pStartPort := int(baseConfig.P2PStartPort) // defaults to 30001
	rpcStartPort := int(baseConfig.RPCStartPort) // defaults to 40001

	// Find index of ChainID (deterministic due to sorting)
	nodeIdx, err := chainRegistry.FindChain(withChainID)
	if err != nil {
		panic(fmt.Errorf(
			"could not find ChainID %s: %w", withChainID, err))
	}

	// Seed nodes *may* be empty, error ignored here.
	seedNodes, _ := chainRegistry.GetSeeds(withChainID)

	// Sync configuration contains disabled state-sync configuration.
	syncConfig := config.DefaultStateSyncConfig()
	syncConfig.Enable = false

	// Deep-copy the config.Config object and overwrite ports.
	mxConfig, err := NewConfigOverwriteWithParameters(
		baseConfig,
		withChainID,
		seedNodes,
		syncConfig,
		p2pStartPort+nodeIdx,
		rpcStartPort+nodeIdx,
	)
	if err != nil {
		panic(fmt.Errorf(
			"could create a config overwrite for ChainID %s: %w", withChainID, err))
	}

	return mxConfig
}

// NewConfigOverwriteWithParameters updates a node configuration in-place to
// overwrite the services listen addresses such that there is one P2P- and
// one RPC port per replicated chain. Following ports overwrite apply:
//
// - P2P: legacy `26656`, multiplex `30001`...`3000x` with x the index of nodes
// - RPC: legacy `26657`, multiplex `40001`...`4000x` with x the index of nodes
//
// This method also overwrites the `P2P.Seeds` configuration option such that
// each replicated chain uses its own seed nodes, and the `WAL` file is changed
// so that each replicated chain writes to a separate WAL-file.
//
// It returns the newly created *deep-copy* of the node configuration.
func NewConfigOverwriteWithParameters(
	baseConfig *config.Config,
	withChainID string,
	seedNodes string,
	syncConfig *config.StateSyncConfig,
	p2pPortOverwrite int,
	rpcPortOverwrite int,
) (*config.Config, error) {
	// Validate the provided ChainID
	extChainID, err := NewExtendedChainIDFromLegacy(withChainID)
	if err != nil {
		return nil, fmt.Errorf(
			"found incompatible ChainID %s: %w", withChainID, err)
	}

	// Errors would have been handled in above statement
	address := extChainID.GetUserAddress()

	// Deep-copy the config object to create multiple nodes
	mxConfig := deepCopyConfig(baseConfig)

	// ----------------------------
	// P2P Configuration Overwrite
	mxConfig.P2P.Seeds = seedNodes // CAUTION: always uses seeds!
	mxConfig.P2P.ListenAddress = overwriteListenPort(
		baseConfig.P2P.ListenAddress,
		p2pPortOverwrite,
	)

	// ----------------------------
	// RPC Configuration Overwrite
	mxConfig.RPC.ListenAddress = overwriteListenPort(
		baseConfig.RPC.ListenAddress,
		rpcPortOverwrite,
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
	// We enable state-sync here if the config requires it
	mxConfig.StateSync.Enable = syncConfig.Enable
	mxConfig.StateSync.TrustPeriod = syncConfig.TrustPeriod
	mxConfig.StateSync.TrustHeight = syncConfig.TrustHeight
	mxConfig.StateSync.TrustHash = syncConfig.TrustHash

	// At least 2 witnesses are required for state-sync
	mxConfig.StateSync.RPCServers = make([]string, len(syncConfig.RPCServers))
	copy(mxConfig.StateSync.RPCServers, syncConfig.RPCServers)

	return mxConfig, nil
}

// -----------------------------------------------------------------------------
// Private helpers implementation.

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

// overwriteListenPort replaces the port in a service listen address.
func overwriteListenPort(laddr string, port int) string {
	re := regexp.MustCompile(`(.*)(\:\d+)(.*)`)
	newPort := ":" + strconv.Itoa(port)
	return re.ReplaceAllString(laddr, `$1`+newPort+`$3`)
}
