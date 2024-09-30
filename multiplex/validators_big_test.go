package multiplex

import (
	"context"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	types "github.com/cometbft/cometbft/types"
)

func TestBigMultiplexValidatorSetStartStopProduceBlocksOneChainEightValidators(t *testing.T) {

	testName := "mx_test_validator_set_start_stop_produce_blocks_one_chain_eight_validators"
	numValidators := 8
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	// -----------------------------------------------------------------------
	// NODES CONFIGURATION

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)
	require.Contains(t, privValidators, mxtest.TestScopeHash)
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	// Configures eight independent validator nodes with same userScopes
	var p2pNodeKey1stNode string
	var validatorNodes []*ScopedNode
	var validatorConfs []*cfg.Config
	startListenPort := 30001
	fstValListenPort := startListenPort
	for i := 0; i < numValidators; i++ {
		nodePrivValidator := privValidators[mxtest.TestScopeHash][i]

		persistentPeers := ""
		if i > 0 {
			persistentPeers = p2pNodeKey1stNode + "@127.0.0.1:" + strconv.Itoa(fstValListenPort)
			startListenPort = startListenPort + 4000 // careful if more than 8 validators (port invalid)
		}

		valNodeConfig, valNodeRegistry, _ := assertConfigureMultiplexNodeRegistry(t,
			testName+"-0",
			userScopes,
			validatorsPubs,    // non-empty => ResetTestRootMultiplexWithValidators
			nodePrivValidator, // sets custom privValidator
			startListenPort,   // overwrite the listen port for each validator
			persistentPeers,   // empty persistent peers for first validator
		)

		if i == 0 {
			firstNode := valNodeRegistry.Nodes[mxtest.TestScopeHash]
			p2pNodeKey1stNode = string(firstNode.NodeKey().ID())
		}

		validatorNodes = append(validatorNodes, valNodeRegistry.Nodes[mxtest.TestScopeHash])
		validatorConfs = append(validatorConfs, valNodeConfig)
	}

	require.NotEmpty(t, p2pNodeKey1stNode)
	require.Equal(t, numValidators, len(validatorNodes), "number of nodes should equal number of validators")
	require.Equal(t, numValidators, len(validatorConfs), "number of node configs should equal number of validators")

	// -----------------------------------------------------------------------
	// SHUTDOWN PROCESS

	// Shutdown routine for all configs / nodes
	defer func(configs []*cfg.Config, nodes []*ScopedNode) {
		// after node stop, remove all test files
		defer func() {
			for _, conf := range configs {
				os.RemoveAll(conf.RootDir)
			}
		}()

		// shutdown nodes gracefully
		for _, n := range nodes {
			_ = n.Stop()
		}
	}(validatorConfs, validatorNodes)

	// Uses a singleton scope registry to create SHA256 once per iteration
	scopeRegistry, err := DefaultScopeHashProvider(&validatorConfs[0].UserConfig)
	require.NoError(t, err)

	// -----------------------------------------------------------------------
	// NODES START ROUTINE

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(numValidators)

	for _, n := range validatorNodes {
		// Calls wg.Done() after executing node.Start()
		assertStartValidatorNodeParallel(t, n.Config(), n, &wg, scopeRegistry)
	}

	// Wait for both nodes to be up and running
	t.Logf("Waiting for %d nodes to be up and running.", numValidators)
	wg.Wait()

	// -----------------------------------------------------------------------
	// BLOCKS PRODUCTIONS
	// This block waits for nodes to relay 2 blocks, note that not all validator
	// nodes (8) produce at least one block. A longer test with a blockchain of
	// at least 100 blocks may be better to assess this metric (needs bigger timeout).

	blocksWg := sync.WaitGroup{}
	blocksWg.Add(numValidators)

	expectedBlocks := 2
	actualNumBlocks := make(map[string]int, numValidators)

	for _, n := range validatorNodes {
		go func(sn *ScopedNode, maxBlocks int) {
			// wait for the node to produce blocks
			blocksSub, err := sn.EventBus().Subscribe(context.Background(), "node_test", types.EventQueryNewBlock)
			assert.NoError(t, err)

			numBlocks := 0

		NODE_BLOCKS_LOOP:
			for {
				select {
				case <-blocksSub.Out():
					numBlocks++
					if numBlocks == maxBlocks {
						actualNumBlocks[sn.Config().P2P.ListenAddress] = numBlocks
						blocksWg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					blocksWg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(30 * time.Second):
					blocksWg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(n, expectedBlocks)
	}

	// Wait for all nodes to have relayed 2 blocks in parallel
	blocksWg.Wait()

	// We assert that all validator nodes relayed 2 blocks, if an error
	// occurred on one of the validators, the map entry won't exist.
	// Note that this does not check whether all validators do participate
	// in the block generation process as they may also just *relay* blocks.
	for _, n := range validatorNodes {
		assert.Contains(t, actualNumBlocks, n.Config().P2P.ListenAddress)
		assert.Equal(t, actualNumBlocks[n.Config().P2P.ListenAddress], expectedBlocks)
	}
}

func TestBigMultiplexValidatorSetStartStopProduceBlocksManyChainsEightValidators(t *testing.T) {
	testName := "mx_test_validator_set_start_stop_produce_blocks_many_chains_eight_validators"
	numValidators := 8
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here (1st scope).
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3x Scope, by Midas!"},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Likes", "Media", "Reviews", "Links"},
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ICE", "Osmosis"},
		"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Posts", "Likes"},
	}

	// -----------------------------------------------------------------------
	// NODES CONFIGURATION

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)
	require.Contains(t, privValidators, mxtest.TestScopeHash)
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	var validatorNodes []*ScopedNode
	var validatorConfs []*cfg.Config

	// ni for "network index"
	ni := 0
	for scopeHash := range privValidators {
		// Configures numValidators validator nodes with same scopeHash
		var p2pNodeKey1stNode string
		startListenPort := 30001 + ni
		for i := 0; i < numValidators; i++ {
			nodePrivValidator := privValidators[scopeHash][i]

			persistentPeers := ""
			if i > 0 {
				persistentPeers = p2pNodeKey1stNode + "@127.0.0.1:" + strconv.Itoa(startListenPort)
				startListenPort = startListenPort + 4000 // careful if more than 8 validators (port invalid)
			}

			valNodeConfig, valNodeRegistry, _ := assertConfigureMultiplexNodeRegistry(t,
				testName+"-"+strconv.Itoa(ni), // ni for "network index"
				userScopes,
				validatorsPubs,    // non-empty => ResetTestRootMultiplexWithValidators
				nodePrivValidator, // sets custom privValidator
				startListenPort,   // overwrite the listen port for each validator
				persistentPeers,   // empty persistent peers for first validator
			)

			if i == 0 {
				firstNode := valNodeRegistry.Nodes[scopeHash]
				p2pNodeKey1stNode = string(firstNode.NodeKey().ID())
			}

			validatorNodes = append(validatorNodes, valNodeRegistry.Nodes[scopeHash])
			validatorConfs = append(validatorConfs, valNodeConfig)
		}

		require.NotEmpty(t, p2pNodeKey1stNode)
		assert.Equal(t, numValidators*(ni+1), len(validatorNodes), "number of nodes should equal number of validators")
		assert.Equal(t, numValidators*(ni+1), len(validatorConfs), "number of node configs should equal number of validators")

		// Next network init
		ni++
	}
}
