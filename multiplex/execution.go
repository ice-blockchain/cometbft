package multiplex

import (
	"github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
)

// ----------------------------------------------------------------------------
// Builders

// NewMultiplexBlockExecutor returns a new BlockExecutor with a NopEventBus
// using ScopedStateStore and ScopedBlockStore instances.
//
// Call SetEventBus to provide an event bus.
func NewMultiplexBlockExecutor(
	stateStore *ScopedStateStore,
	logger log.Logger,
	proxyApp proxy.AppConnConsensus,
	mempool mempool.Mempool,
	evpool sm.EvidencePool,
	blockStore *ScopedBlockStore,
	options ...sm.BlockExecutorOption,
) *sm.BlockExecutor {
	res := sm.NewBlockExecutor(
		stateStore,
		logger,
		proxyApp,
		mempool,
		evpool,
		blockStore,
		options...,
	)

	for _, option := range options {
		option(res)
	}

	return res
}
