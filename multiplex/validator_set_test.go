package multiplex

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	"github.com/cometbft/cometbft/privval"
	types "github.com/cometbft/cometbft/types"
)

func TestMultiplexValidatorSetGenesisUsesCorrectValidators(t *testing.T) {

	testName := "mx_test_validator_set_genesis_uses_correct_validators"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0,  // 0-startPortOverwrite (defaults to 30001)
		"", // empty persistent peers
	)

	expectedNumChains := 1
	expectedValidators := numValidators + 1 // priv validator

	require.Equal(t, expectedNumChains, len(r.Nodes))

	actualScopedNode := r.Nodes[mxtest.TestScopeHash]
	actualGenesisDoc := actualScopedNode.GenesisDoc()
	actualValidators := actualGenesisDoc.Validators

	actualPrivValPub, err := actualScopedNode.PrivValidator().GetPubKey()
	require.NoError(t, err, "scoped node priv validator should be set")

	assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

	findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
		return v.PubKey.Address().String() == actualPrivValPub.Address().String()
	})

	assert.NotEqual(t, -1, findPrivValidatorAddress)
}

func TestMultiplexValidatorSetGenesisUsesCorrectValidatorsTwoChains(t *testing.T) {

	testName := "mx_test_validator_set_genesis_uses_correct_validators_two_chains"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash)

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0,  // 0-startPortOverwrite (defaults to 30001)
		"", // empty persistent peers
	)

	expectedNumChains := 2
	expectedValidators := numValidators + 1 // priv validator
	require.Equal(t, expectedNumChains, len(r.Nodes))
	require.Contains(t, r.Nodes, mxtest.TestScopeHash, "nodes registry should contain TestScopeHash")

	// Asserts the number of validators for each chain and
	// asserts that the priv validators are present in validators
	for _, node := range r.Nodes {
		actualGenesisDoc := node.GenesisDoc()
		actualValidators := actualGenesisDoc.Validators

		actualPrivValPub, err := node.PrivValidator().GetPubKey()
		require.NoError(t, err, "scoped node priv validator should be set")

		assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

		findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
			return v.PubKey.Address().String() == actualPrivValPub.Address().String()
		})

		assert.NotEqual(t, -1, findPrivValidatorAddress, "priv validator should be present in genesis validators")
	}
}

func TestMultiplexValidatorSetManyChainsHaveCorrectValidators(t *testing.T) {

	testName := "mx_test_validator_set_many_chains_have_correct_validators"
	numValidators := 3
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here (1st scope).
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3x Scope, by Midas!"},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Likes", "Media", "Reviews", "Links"},
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ICE", "Osmosis"},
		"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Posts", "Likes"},
	}

	validatorsPubs, _ := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")

	_, r, _ := assertConfigureMultiplexNodeRegistry(t,
		testName,
		userScopes,
		validatorsPubs, // non-empty validators => ResetTestRootMultiplexWithValidators
		nil,
		0,  // 0-startPortOverwrite (defaults to 30001)
		"", // empty persistent peers
	)

	expectedNumChains := 12
	expectedValidators := numValidators + 1 // priv validator
	require.Equal(t, expectedNumChains, len(r.Nodes))
	require.Contains(t, r.Nodes, mxtest.TestScopeHash, "nodes registry should contain TestScopeHash")

	// Asserts the number of validators for each chain and
	// asserts that the priv validators are present in validators
	for _, node := range r.Nodes {
		actualGenesisDoc := node.GenesisDoc()
		actualValidators := actualGenesisDoc.Validators
		actualPrivValPub, err := node.PrivValidator().GetPubKey()
		require.NoError(t, err, "scoped node priv validator should be set")

		assert.Equal(t, expectedValidators, len(actualGenesisDoc.Validators), "validator set should contain validators and priv validator")

		findPrivValidatorAddress := slices.IndexFunc(actualValidators, func(v types.GenesisValidator) bool {
			return v.PubKey.Address().String() == actualPrivValPub.Address().String()
		})

		assert.NotEqual(t, -1, findPrivValidatorAddress, "priv validator should be present in genesis validators")
	}
}

func TestMultiplexValidatorSetStartStopSingleValidatorChain(t *testing.T) {

	testName := "mx_test_validator_set_start_stop_single_validator_chain"
	numValidators := 1
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Contains(t, privValidators, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	privValidator_n1 := privValidators[mxtest.TestScopeHash][0]

	// Configures one independent validator node
	c1, r1, s1 := assertConfigureMultiplexNodeRegistry(t,
		testName+"-0",
		userScopes,
		validatorsPubs,   // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n1, // sets custom privValidator
		0,                // 0-startPortOverwrite (defaults to 30001)
		"",               // empty persistent peers
	)

	var wg sync.WaitGroup

	// Starts, produces block and stops the first validator node
	// Calls wg.Add() before start and wg.Done() after stop
	assertStartValidatorNode(t, c1, r1.Nodes[mxtest.TestScopeHash], &wg, s1)

	// Waits until the node produces a block and stops
	wg.Wait()
}

func TestMultiplexValidatorSetStartStopTwoNodesWithPrivValidator(t *testing.T) {

	testName := "mx_test_validator_set_start_stop_two_nodes_with_priv_validator"
	numValidators := 2
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Contains(t, privValidators, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	privValidator_n1 := privValidators[mxtest.TestScopeHash][0]
	privValidator_n2 := privValidators[mxtest.TestScopeHash][1]

	// Configures two independent validator nodes with same userScopes
	c1, r1, scopeRegistry := assertConfigureMultiplexNodeRegistry(t,
		testName+"-0",
		userScopes,
		validatorsPubs,   // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n1, // sets custom privValidator
		0,                // 0-startPortOverwrite (defaults to 30001)
		"",               // empty persistent peers
	)

	p2pKey := r1.Nodes[mxtest.TestScopeHash].NodeKey().ID()
	require.NotEmpty(t, p2pKey)

	c2, r2, _ := assertConfigureMultiplexNodeRegistry(t,
		testName+"-1",
		userScopes,
		validatorsPubs,   // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n2, // sets custom privValidator
		40001,            // second validator uses 40001, 41001, 42001, 43001, etc.
		"",               // empty persistent peers
	)

	// Shutdown routines
	defer func(conf *cfg.Config, reg *NodeRegistry) {
		defer os.RemoveAll(conf.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(c1, r1)
	defer func(conf *cfg.Config, reg *NodeRegistry) {
		defer os.RemoveAll(conf.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(c2, r2)

	configs := []*cfg.Config{c1, c2}
	nodes := []*ScopedNode{
		r1.Nodes[mxtest.TestScopeHash],
		r2.Nodes[mxtest.TestScopeHash],
	}
	require.Equal(t, numValidators, len(configs), "number of node configs should equal number of validators")
	require.Equal(t, numValidators, len(nodes), "number of nodes should equal number of validators")

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(numValidators)

	for i, n := range nodes {
		config := configs[i]

		// Calls wg.Done() after executing node.Start()
		assertStartValidatorNodeParallel(t, config, n, &wg, scopeRegistry)
	}

	// Wait for both nodes to have produced a block
	t.Logf("Waiting for %d nodes to be up and running.", numValidators)
	wg.Wait()
}

func TestBigMultiplexValidatorSetStartStopTwoNodesBothProduceBlocks(t *testing.T) {

	testName := "mx_test_validator_set_start_stop_two_nodes_both_produce_blocks"
	numValidators := 2
	userScopes := map[string][]string{
		// mind changing mxtest.TestScopeHash if you make a change here.
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
	}

	validatorsPubs, privValidators := createTestValidatorSet(t, testName, numValidators, userScopes)
	require.NotEmpty(t, validatorsPubs, "should create a valid validator set")
	require.Contains(t, validatorsPubs, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Contains(t, privValidators, mxtest.TestScopeHash, "should contain mxtest.TestScopeHash")
	require.Equal(t, len(privValidators[mxtest.TestScopeHash]), numValidators)

	privValidator_n1 := privValidators[mxtest.TestScopeHash][0]
	privValidator_n2 := privValidators[mxtest.TestScopeHash][1]

	// Configures two independent validator nodes with same userScopes
	c1, r1, scopeRegistry := assertConfigureMultiplexNodeRegistry(t,
		testName+"-0",
		userScopes,
		validatorsPubs,   // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n1, // sets custom privValidator
		0,                // 0-startPortOverwrite (defaults to 30001)
		"",               // empty persistent peers for first validator
	)

	p2pKey := r1.Nodes[mxtest.TestScopeHash].NodeKey().ID()
	require.NotEmpty(t, p2pKey)

	c2, r2, _ := assertConfigureMultiplexNodeRegistry(t,
		testName+"-1",
		userScopes,
		validatorsPubs,                    // non-empty => ResetTestRootMultiplexWithValidators
		privValidator_n2,                  // sets custom privValidator
		40001,                             // second validator uses 40001, 41001, 42001, 43001, etc.
		string(p2pKey)+"@127.0.0.1:30001", // persist first validator connection
	)

	// Shutdown first validator node
	defer func(conf *cfg.Config, reg *NodeRegistry) {
		defer os.RemoveAll(conf.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(c1, r1)

	// Shutdown second validator node
	defer func(conf *cfg.Config, reg *NodeRegistry) {
		defer os.RemoveAll(conf.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(c2, r2)

	configs := []*cfg.Config{c1, c2}
	nodes := []*ScopedNode{
		r1.Nodes[mxtest.TestScopeHash],
		r2.Nodes[mxtest.TestScopeHash],
	}
	require.Equal(t, numValidators, len(configs), "number of node configs should equal number of validators")
	require.Equal(t, numValidators, len(nodes), "number of nodes should equal number of validators")

	// -----------------------------------------------------------------------
	// NODES START ROUTINE

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(numValidators)

	for i, n := range nodes {
		config := configs[i]

		// Calls wg.Done() after executing node.Start()
		assertStartValidatorNodeParallel(t, config, n, &wg, scopeRegistry)
	}

	// Wait for both nodes to be up and running
	t.Logf("Waiting for %d nodes to be up and running.", numValidators)
	wg.Wait()

	// -----------------------------------------------------------------------
	// BLOCKS PRODUCTIONS

	blocksWg := sync.WaitGroup{}
	blocksWg.Add(numValidators)

	expectedBlocks := 2
	actualNumBlocks := make(map[string]int, numValidators)

	for _, n := range nodes {
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
				case <-time.After(15 * time.Second):
					blocksWg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(n, expectedBlocks)
	}

	// Wait for both nodes to produce 2 blocks in parallel
	blocksWg.Wait()

	// We assert that both validators produced 2 blocks, if an error
	// occurred on one of the validators, the map entry won't exist.
	for _, n := range nodes {
		assert.Contains(t, actualNumBlocks, n.Config().P2P.ListenAddress)
		assert.Equal(t, actualNumBlocks[n.Config().P2P.ListenAddress], expectedBlocks)
	}
}

// ----------------------------------------------------------------------------

func createTestValidatorSet(
	t testing.TB,
	testName string,
	numValidators int,
	userScopes map[string][]string, // user_address:[scope1,scope2]
) (map[string][]string, map[string][]*privval.FilePV) {
	t.Helper()

	validatorSet := map[string][]string{}
	privValidatorSet := map[string][]*privval.FilePV{}

	for userAddress, scopes := range userScopes {
		for _, scope := range scopes {
			scopeHash := mxtest.CreateScopeHash(userAddress + ":" + scope)

			// Generates numValidators random priv validators
			privValidatorSet[scopeHash] = make([]*privval.FilePV, numValidators)
			privValidatorSet[scopeHash] = mxtest.GeneratePrivValidators(testName, numValidators)

			validatorSet[scopeHash] = make([]string, len(privValidatorSet[scopeHash]))

			for i, pv := range privValidatorSet[scopeHash] {
				pvPubKey := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
				validatorSet[scopeHash][i] = pvPubKey
			}
		}
	}

	return validatorSet, privValidatorSet
}

func assertStartValidatorNode(
	t testing.TB,
	config *cfg.Config,
	node *ScopedNode,
	wg *sync.WaitGroup,
	scopeRegistry *ScopeRegistry,
) {
	nodeDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	require.DirExists(t, nodeDataDir, "scoped node data dir should exist")

	wg.Add(1)
	t.Logf("Starting new node: %s - %s", node.ScopeHash, node.GenesisDoc().ChainID)

	walFolder := node.ScopeHash[:16] // uses hex

	// Overwrite wal file on a per-node basis
	walFile := filepath.Join(nodeDataDir, "cs.wal", walFolder, "wal")

	// We overwrite the wal file to allow parallel I/O for multiple nodes
	t.Logf("Using walFile: %s", walFile)
	node.Config().Consensus.SetWalFile(walFile)

	userAddress, err := scopeRegistry.GetAddress(node.ScopeHash)
	require.NoError(t, err)

	// We set the PrivValidator for every node and consensus reactors
	usePrivValidatorFromFiles(t, node, config, userAddress)

	// Calls wg.Done() after producing a block and executing node.Stop()
	assertStartStopScopedNode(t, wg, node)
}

func assertStartValidatorNodeParallel(
	t testing.TB,
	config *cfg.Config,
	node *ScopedNode,
	wg *sync.WaitGroup,
	scopeRegistry *ScopeRegistry,
) {
	nodeDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)

	userAddress, err := scopeRegistry.GetAddress(node.ScopeHash)
	require.NoError(t, err)

	// Overwrite wal file on a per-node basis
	walFolder := node.ScopeHash[:16] // uses hex
	walFile := filepath.Join(nodeDataDir, "cs.wal", walFolder, "wal")

	// We overwrite the wal file to allow parallel I/O for multiple nodes
	node.Config().Consensus.SetWalFile(walFile)

	// We reset the PrivValidator for every node and consensus reactors
	usePrivValidatorFromFiles(t, node, config, userAddress)

	go func(sn *ScopedNode) {
		defer wg.Done()
		t.Logf("Starting new node: %s - %s", sn.ScopeHash, sn.GenesisDoc().ChainID)
		t.Logf("Using listen addr: p2p:%s - rpc:%s", sn.Config().P2P.ListenAddress, sn.Config().RPC.ListenAddress)
		err := sn.Start()
		require.NoError(t, err, "node startup should not produce an error")
	}(node)
}
