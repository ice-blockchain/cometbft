package multiplex

import (
	_ "net/http/pprof" //nolint: gosec,gci // securely exposed on separate, optional port

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/store"
)

// SharedMetrics contains pointers to prometheus metrics instances
// configured with a node ID ("globally"/"shared"). These metrics
// are shared across one node instance and not chain-specific.
type SharedMetrics struct {
	StateMetrics *sm.Metrics
	StoreMetrics *store.Metrics
	P2PMetrics   *p2p.Metrics
}

// ReplicatedMetrics contains pointers to prometheus metrics instances
// configured with a chain ID and scope hash. These metrics are specific
// to one replicated chain.
type ReplicatedMetrics struct {
	StateMetrics     *sm.Metrics
	StoreMetrics     *store.Metrics
	ConsensusMetrics *cs.Metrics
	MempoolMetrics   *mempl.Metrics
	ProxyMetrics     *proxy.Metrics
	BlockSyncMetrics *blocksync.Metrics
	StateSyncMetrics *statesync.Metrics
}

// SharedMetricsProvider returns a [SharedMetrics] instance by scope hash.
type SharedMetricsProvider func(nodeID string) SharedMetrics

// ReplicatedMetricsProvider returns a [ReplicatedMetrics] instance by scope hash.
type ReplicatedMetricsProvider func(chainID string, userScopeHash string) ReplicatedMetrics

// GlobalMetricsProvider returns Metrics built using Prometheus client library
// if Prometheus is enabled. Otherwise, it returns no-op Metrics.
func GlobalMetricsProvider(config *cfg.InstrumentationConfig) SharedMetricsProvider {
	return func(nodeID string) SharedMetrics {
		if config.Prometheus {
			return SharedMetrics{
				StateMetrics: sm.PrometheusMetrics(config.Namespace, "node_id", nodeID),
				StoreMetrics: store.PrometheusMetrics(config.Namespace, "node_id", nodeID),
				P2PMetrics:   p2p.PrometheusMetrics(config.Namespace, "node_id", nodeID),
			}
		}
		return SharedMetrics{
			StateMetrics: sm.NopMetrics(),
			StoreMetrics: store.NopMetrics(),
			P2PMetrics:   p2p.NopMetrics(),
		}
	}
}

// MultiplexMetricsProvider returns Metrics built using Prometheus client library
// if Prometheus is enabled and adds the scope hash in context. Otherwise, it returns no-op Metrics.
func MultiplexMetricsProvider(config *cfg.InstrumentationConfig) ReplicatedMetricsProvider {
	return func(chainID string, userScopeHash string) ReplicatedMetrics {
		if config.Prometheus {
			return ReplicatedMetrics{
				StateMetrics:     sm.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				StoreMetrics:     store.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				ConsensusMetrics: cs.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				MempoolMetrics:   mempl.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				ProxyMetrics:     proxy.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				BlockSyncMetrics: blocksync.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				StateSyncMetrics: statesync.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
			}
		}
		return ReplicatedMetrics{
			StateMetrics:     sm.NopMetrics(),
			StoreMetrics:     store.NopMetrics(),
			ConsensusMetrics: cs.NopMetrics(),
			MempoolMetrics:   mempl.NopMetrics(),
			ProxyMetrics:     proxy.NopMetrics(),
			BlockSyncMetrics: blocksync.NopMetrics(),
			StateSyncMetrics: statesync.NopMetrics(),
		}
	}
}
