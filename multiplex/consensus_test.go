package multiplex_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/proxy"
)

func TestMultiplexReactorPrepareConsensusInstanceWithReactor(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		assert.NoError(t, err)
	}()

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	assert.NoError(t, err, "should not error while waiting for networks")

	// Start an ABCI client
	chainIds := reactor.GetNetworks()
	abciClient := proxy.NewMultiplexAppConn(
		chainIds,
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.PrometheusMetrics(globalCfg.Instrumentation.Namespace+"_"+string(reactor.GetNodeKey().ID())),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range chainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		assert.NoError(t, err, "should not error for consensus handshake")
	}
}

func TestMultiplexReactorCreateConsensusInstanceReactors(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5

	rootDir, globalCfg, reactor := ResetTestMultiplexConsensus(t,
		numChains,
	)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		assert.NoError(t, err)
	}()

	// Start the reactor
	err := reactor.Start()
	require.NoError(t, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	assert.NoError(t, err, "should not error while waiting for networks")

	// Start an ABCI client
	chainIds := reactor.GetNetworks()
	abciClient := proxy.NewMultiplexAppConn(
		chainIds,
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(t, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Uses to retrieve reactors per network
	servicesProvider := reactor.GetServicesProvider()
	require.NotNil(t, servicesProvider, "services provider must not be nil")

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range chainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		assert.NoError(t, err, "should not error for consensus handshake")

		// Test with blockSync=true
		blockSync := true
		err = reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainID,
			blockSync,
			false, // waitSync
		)
		assert.NoError(t, err, "should not error creating consensus reactors")

		// Type-assertions make sure we have correct reactors set.
		testMempoolReactor := servicesProvider(mx.ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		testBlockSyncReactor := servicesProvider(mx.ServiceKeyBlockSyncReactor, chainID).(*blocksync.Reactor)
		testConsensusReactor := servicesProvider(mx.ServiceKeyConsensusReactor, chainID).(*cs.Reactor)
		testEvidenceReactor := servicesProvider(mx.ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor)

		// Also make sure we have actual instances, not nil
		assert.NotNil(t, testMempoolReactor, "mempool reactor must not be nil")
		assert.NotNil(t, testBlockSyncReactor, "blockSync reactor must not be nil")
		assert.NotNil(t, testConsensusReactor, "consensus reactor must not be nil")
		assert.NotNil(t, testEvidenceReactor, "evidence reactor must not be nil")
	}
}

// CAUTION: the GenesisDocProvider is maleated to contain correct ChainIDs
// CAUTION: the MultiplexConfig is entirely random and *not synchronized* with genesis docs.
func ResetTestMultiplexConsensus(
	tb testing.TB,
	numChains int,
) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	nodeCfg := config.TestConfig()
	nodeCfg.SetRoot(rootDir)
	nodeCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&nodeCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, nodeCfg, mockGenesisProvider)

	return rootDir, nodeCfg, reactor
}
