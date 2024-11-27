package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	cmtnode "github.com/ice-blockchain/cometbft/node"
)

// IMPORTANT: We use goleak in this unit test to make sure that using a proper
// shutdown process, the process won't produce any goroutine leaks.
func TestMultiplexClientContextNoShutdownLeak(t *testing.T) {
	numNetworks := 1

	// IMPORTANT: Makes sure that we don't have goroutine leaks.
	defer goleak.VerifyNone(t)

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	globalCfg,
		testMultiplex,
		testReactor, shutdownFn := requireStartNodesMultiplex(
		t,
		context.Background(),
		numNetworks, // 1 NETWORK!
		cmtlog.NewNopLogger(),
	)

	// Shutdown routine
	defer func() {
		defer os.RemoveAll(globalCfg.RootDir)

		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()

	// And also test some of the configuration/resources
	assert.NotNil(t, globalCfg, "should create global configuration")
	assert.NotNil(t, testMultiplex, "should create nodes multiplex")
	assert.NotNil(t, testReactor, "should create multiplex reactor")

	// Test some base configuration data
	actualNumNetworks := len(testReactor.GetNetworks())
	assert.Equal(t, numNetworks, actualNumNetworks)

	// Test that we have all the required networks
	for _, testChainID := range testReactor.GetNetworks() {
		assert.Contains(t, testMultiplex, testChainID)
		assert.NotNil(t, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
		genesisDoc := nodeInstance.GenesisDoc()

		// Verify that we are on the correct ChainID
		assert.Equal(t, testChainID, genesisDoc.ChainID)
	}
}

// CAUTION: This implementation intentionally produces a leak because
// the shutdownFn is being called too fast from another goroutine.
func TestMultiplexClientContextWithShutdownLeak(t *testing.T) {
	numNetworks := 1

	// Initialize and START the nodes multiplex
	// For debug, change the logger to cmtlog.TestingLogger()
	//
	// CAUTION: This implementation intentionally produces a leak because
	// the shutdownFn is being called too fast from another goroutine.
	globalCfg,
		testMultiplex,
		testReactor, shutdownFn := unsafeStartNodesMultiplexWithGoRoutineLeak(
		t,
		context.Background(),
		numNetworks, // 1 NETWORK!
		cmtlog.NewNopLogger(),
	)

	// Shutdown routine
	defer func() {
		defer os.RemoveAll(globalCfg.RootDir)

		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(t, err)
	}()

	// Still should test some of the configuration/resources
	assert.NotNil(t, globalCfg, "should create global configuration")
	assert.NotNil(t, testMultiplex, "should create nodes multiplex")
	assert.NotNil(t, testReactor, "should create multiplex reactor")

	// We are interested only in finding the goroutine leaks when a shutdown
	// routine is being called too fast and doesn't wait for nodes to be up.
	err := goleak.Find()
	assert.Error(t, err, "should find leaks given improper shutdown process")
}

// ----------------------------------------------------------------------------

// fixConfigOverwrite fixes the configuration overwrite to permit multiple
// tests to run simultaneous node instances (in parallel).
func fixConfigOverwrite(tb testing.TB, globalCfg *config.Config) *config.Config {
	tb.Helper()

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + tb.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfg.ChainSeeds {
		globalCfg.ChainSeeds[chainID] = ""
	}

	return globalCfg
}

// requireStartNodesMultiplex configures a nodes multiplex *randomly* and starts
// individual nodes in a separate goroutine per network.
func requireStartNodesMultiplex(
	tb testing.TB,
	ctx context.Context,
	numChains int,
	customLogger cmtlog.Logger,
) (
	*config.Config,
	mx.MultiplexMap[*cmtnode.Node],
	*mx.Reactor,
	mx.NodesMultiplexShutdownFn,
) {
	tb.Helper()

	_, globalCfg := ResetTestMultiplexNode(tb, numChains)

	// Forces multi-test compat, disables GRPC, chain seeds
	globalCfg = fixConfigOverwrite(tb, globalCfg)

	if customLogger == nil {
		customLogger = cmtlog.NewNopLogger()
	}

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, shutdownFn, err := mx.NewNodesMultiplex(
		ctx,
		globalCfg,
		customLogger,
		&client.DefaultClient{},
	)
	require.NoError(tb, err, "should create node instance")
	require.NotNil(tb, testMultiplex, "should return a multiplex map with a node")
	require.Len(tb, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testReactor.GetNetworks()))

	// Test that we have all the required networks
	for _, testChainID := range testReactor.GetNetworks() {
		require.Contains(tb, testMultiplex, testChainID)
		require.NotNil(tb, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
		userAddress, err := testReactor.GetChainRegistry().GetAddress(testChainID)
		require.NoError(tb, err, "should find user address by ChainID")

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(tb, nodeInstance, globalCfg, userAddress, testChainID)

		// Verify that we can start the node correctly
		go func(cn *cmtnode.Node) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(tb, err)
		}(nodeInstance)
	}

	// Wait for all nodes to be up and running
	// t.Logf("Waiting for %d nodes to be up and running.", len(testReactor.GetNetworks()))
	wg.Wait()

	return globalCfg, testMultiplex, testReactor, shutdownFn
}

// unsafeStartNodesMultiplexWithGoRoutineLeak configures a nodes multiplex
// randomly and starts individual nodes in a separate goroutine per network.
//
// IMPORTANT:
// You should not use this method directly, use requireStartNodesMultiplex.
//
// CAUTION: This implementation intentionally produces a leak because
// the shutdownFn is being called too fast from another goroutine.
func unsafeStartNodesMultiplexWithGoRoutineLeak(
	tb testing.TB,
	ctx context.Context,
	numChains int,
	customLogger cmtlog.Logger,
) (
	*config.Config,
	mx.MultiplexMap[*cmtnode.Node],
	*mx.Reactor,
	mx.NodesMultiplexShutdownFn,
) {
	tb.Helper()

	_, globalCfg := ResetTestMultiplexNode(tb, numChains)

	// Forces multi-test compat, disables GRPC, chain seeds
	globalCfg = fixConfigOverwrite(tb, globalCfg)

	if customLogger == nil {
		customLogger = cmtlog.NewNopLogger()
	}

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	testMultiplex, testReactor, shutdownFn, err := mx.NewNodesMultiplex(
		ctx,
		globalCfg,
		customLogger,
		&client.DefaultClient{},
	)
	require.NoError(tb, err, "should create node instance")
	require.NotNil(tb, testMultiplex, "should return a multiplex map with a node")
	require.Len(tb, testMultiplex, numChains, fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	// Test that we have all the required networks
	for _, testChainID := range testReactor.GetNetworks() {
		require.Contains(tb, testMultiplex, testChainID)
		require.NotNil(tb, testMultiplex[testChainID])

		// Type-assertion to verify that we have a correct instance
		nodeInstance := testMultiplex[testChainID].GetInstance().(*cmtnode.Node)
		userAddress, err := testReactor.GetChainRegistry().GetAddress(testChainID)
		require.NoError(tb, err, "should find user address by ChainID")

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(tb, nodeInstance, globalCfg, userAddress, testChainID)

		// Verify that we can start the node correctly
		go func(cn *cmtnode.Node) {
			err := cn.Start()
			require.NoError(tb, err)
		}(nodeInstance)
	}

	// CAUTION:
	// The following block causes the application to shutdown too fast and leave
	// some goroutines hanging - notably AutoFile and RPC server are concerned.
	go func() {
		<-ctx.Done()
		err := shutdownFn(testMultiplex, testReactor)
		require.NoError(tb, err)
	}()

	return globalCfg, testMultiplex, testReactor, shutdownFn
}
