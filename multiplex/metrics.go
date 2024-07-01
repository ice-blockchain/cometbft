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

// SharedMetricsProvider returns a consensus, p2p and mempool Metrics.
type SharedMetricsProvider func(chainID string) (
	*sm.Metrics,
	*store.Metrics,
	*p2p.Metrics,
)

// ReplicatedMetricsProvider returns a consensus, p2p and mempool Metrics.
type ReplicatedMetricsProvider func(chainID string, userScopeHash string) (
	*cs.Metrics,
	*mempl.Metrics,
	*sm.Metrics,
	*proxy.Metrics,
	*blocksync.Metrics,
	*statesync.Metrics,
)

// ----------------------------------------------------------------------------
// Multiplex providers

// GlobalMetricsProvider returns Metrics built using Prometheus client library
// if Prometheus is enabled. Otherwise, it returns no-op Metrics.
func GlobalMetricsProvider(config *cfg.InstrumentationConfig) SharedMetricsProvider {
	return func(chainID string) (*sm.Metrics, *store.Metrics, *p2p.Metrics) {
		if config.Prometheus {
			return sm.PrometheusMetrics(config.Namespace, "chain_id", chainID),
				store.PrometheusMetrics(config.Namespace, "chain_id", chainID),
				p2p.PrometheusMetrics(config.Namespace, "chain_id", chainID)
		}
		return sm.NopMetrics(), store.NopMetrics(), p2p.NopMetrics()
	}
}

// MultiplexMetricsProvider returns Metrics built using Prometheus client library
// if Prometheus is enabled and adds the scope hash in context. Otherwise, it returns no-op Metrics.
func MultiplexMetricsProvider(config *cfg.InstrumentationConfig) ReplicatedMetricsProvider {
	return func(chainID string, userScopeHash string) (*cs.Metrics, *mempl.Metrics, *sm.Metrics, *proxy.Metrics, *blocksync.Metrics, *statesync.Metrics) {
		if config.Prometheus {
			return cs.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				mempl.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				sm.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				proxy.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				blocksync.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash),
				statesync.PrometheusMetrics(config.Namespace, "chain_id", chainID, "scope", userScopeHash)
		}
		return cs.NopMetrics(), mempl.NopMetrics(), sm.NopMetrics(), proxy.NopMetrics(), blocksync.NopMetrics(), statesync.NopMetrics()
	}
}
