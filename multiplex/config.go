package multiplex

import (
	"time"

	"github.com/ice-blockchain/cometbft/config"
)

const (
	// Collecting metrics every 10 seconds, this may need to be adapted
	// to equal the Prometheus scrape interval (1s) for better granularity.
	DefaultMetricsTickerDuration = 10 * time.Second

	// Prometheus timeout configuration
	DefaultReadHeaderTimeout = 10 * time.Second

	// Transaction events timeout configuration. This duration defines the
	// maximum waiting time for transactions to appear in the tx indexer.
	// Used as a failsafe to stop [WaitForTransactionEvents] from waiting
	// for transactions forever upon completion of broadcast operations.
	DefaultTransactionTimeout = 60 * time.Second

	// Maximum number of indexer read operrations when expecting transaction
	// indexing events during or after a broadcast operation.
	// Used as a failsafe to stop [MultiplexBackend#OnTransactionIndexed] from
	// waiting forever.
	DefaultMaxIndexerReadAttempts = 20

	// Remote replication timeout configuration. This duration defines the
	// maximum waiting time for remote replication to complete.
	// Used as a failsafe to stop [WaitForRelaysReplicationCompleted] from
	// waiting for runtime updates forever.
	//
	// Using a timeout of 2 hours permits to cover for networks that grow
	// above of 2 million blocks with a blocksync range of 200-400 blocks.
	//
	// NOTE(midas): For a production environment, it is recommended to set
	// this timeout to 0 using `WithReplicationTimeout(0)`.
	DefaultReplicationTimeout = 2 * time.Hour

	// Network requests timeout configuration, e.g. [GetRemoteRelayInfo].
	DefaultRequestTimeout = 5 * time.Second
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
