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

// WithDiscoveryPort is an option helper that allows you to overwrite the
// default DiscoveryPort in [MultiplexConfig].
// By default, this option is set to 50001.
func WithDiscoveryPort(discoveryPort uint16) func(*config.MultiplexConfig) {
	return func(conf *config.MultiplexConfig) {
		conf.DiscoveryPort = discoveryPort
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
) (*config.Config, error) {
	// Multiplex can be configured to start at different port
	discoveryPort := int(baseConfig.DiscoveryPort) // defaults to 30001

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
		discoveryPort,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"could not create a config overwrite for ChainID %s: %w", withChainID, err)
	}

	return mxConfig, nil
}

// NewConfigOverwriteWithParameters updates a node configuration in-place to
// overwrite the seed nodes and WAL filepaths so that each replicated chain
// writes to a separate WAL-file.
// This method overwrites the `P2P.Seeds` configuration option such that
// each replicated chain uses its own seed nodes.
//
// It returns the newly created *deep-copy* of the node configuration.
func NewConfigOverwriteWithParameters(
	baseConfig *config.Config,
	withChainID string,
	seedNodes string,
	syncConfig *config.StateSyncConfig,
	discoveryPort int,
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
	mxConfig.P2P.Seeds = seedNodes // CAUTION: always connect to seeds
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
