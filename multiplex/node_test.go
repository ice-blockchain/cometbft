package multiplex

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	dbm "github.com/cometbft/cometbft-db"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/crypto/tmhash"
	cs "github.com/cometbft/cometbft/internal/consensus"
	cmttest "github.com/cometbft/cometbft/internal/test"
	"github.com/cometbft/cometbft/libs/log"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
	cmtnode "github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	types "github.com/cometbft/cometbft/types"
)

// It is primordial that this test always uses `cmtnode` to be able to evaluate
// the *default node behaviour* without multiplex ; and it should also be first.
func TestMultiplexNodeLegacyGenesis(t *testing.T) {
	// cmtnode.setup defines private constants that are overwritten
	// in cmtmx.setup and therefor this test must use the legacy key
	legacyGenesisDocHashKey := []byte("genesisDocHash")

	config := cmttest.ResetTestRoot("mx_node_legacy_genesis")
	defer os.RemoveAll(config.RootDir)

	// Use goleveldb so we can reuse the same db for the second NewNode()
	config.DBBackend = string(dbm.GoLevelDBBackend)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	privValidator, err := privval.LoadOrGenFilePV(
		config.PrivValidatorKeyFile(),
		config.PrivValidatorStateFile(),
		useDefaultKeyGenFunc(),
	)
	require.NoError(t, err)

	n, err := cmtnode.NewNode(
		context.Background(),
		config,
		privValidator,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)

	// Start and stop to close the db for later reading
	err = n.Start()
	require.NoError(t, err)

	err = n.Stop()
	require.NoError(t, err)

	// Ensure the genesis doc hash is saved to db
	stateDB, err := cfg.DefaultDBProvider(&cfg.DBContext{ID: "state", Config: config})
	require.NoError(t, err)

	genDocHash, err := stateDB.Get(legacyGenesisDocHashKey)
	require.NoError(t, err)
	assert.NotNil(t, genDocHash, "genesis doc hash should be saved in db under 'genesisDoc'")
	assert.Len(t, genDocHash, tmhash.Size)

	err = stateDB.Close()
	require.NoError(t, err)
}

func TestMultiplexLegacyNodeStartAndProduceBlocks(t *testing.T) {
	config, n := assertStartLegacyNode(t, "mx_node_legacy_start_and_produce")

	// Shutdown routine
	defer os.RemoveAll(config.RootDir)
	defer func(cn *cmtnode.Node) {
		_ = cn.Stop()
	}(n)

	maxBlocks := 5

	// wait for the node to produce a block
	blocksSub, err := n.EventBus().Subscribe(context.Background(), "node_test", types.EventQueryNewBlock)
	require.NoError(t, err)

	numBlocks := 0
BLOCKS_LOOP:
	for {
		select {
		case <-blocksSub.Out():
			numBlocks++
			if numBlocks == maxBlocks {
				break BLOCKS_LOOP
			}
		case <-blocksSub.Canceled():
			t.Fatal("blocksSub was canceled")
			break BLOCKS_LOOP
		case <-time.After(15 * time.Second):
			t.Fatal("timed out waiting for the node to produce a block")
			break BLOCKS_LOOP
		}
	}
}

func TestMultiplexNodeSingularReplicationFallbackWithEmptyScopes(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_fallback_empty_scopes",
		"",
		map[string][]string{},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.Contains(t, r.Nodes, "") // empty key for singular mode
}

func TestMultiplexNodePluralReplicationConfig(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_plural_replication_config",
		"",
		map[string][]string{
			// mind changing mxtest.TestScopeHash if you make a change here.
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
	assert.Contains(t, r.Nodes, mxtest.TestScopeHash)
}

func TestMultiplexNodePluralReplicationConfigWithManyNodes(t *testing.T) {
	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_new_node_plural_replication_config_with_many_nodes",
		"",
		map[string][]string{
			// mind changing mxtest.TestScopeHash if you make a change here.
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF190888BE0F48DE88927C3F49215B96548273AF": {"Scoping"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Private"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Globals"},
			"EB6E5F1BFF0B9F74C2731090ADE7B5C78EEEA74F": {"Default"},
			"5DD7B1B86EDABBC1C077CB9D846805F7FA10B0DD": {"Spaced scope"},
			"08A6F3BFF2C4513A3D5BD057A5DF3B274676750F": {"Default"},
			"76D0E67F5CD8EEFFA238E5CF27969AFB6C1AC73B": {"Default"},
			"7A435E5D913D8A4960658F51B7E260BA512F6CAD": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	numReplicatedChains := 9

	r, err := NewMultiplexNode(
		context.Background(),
		config,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, numReplicatedChains, len(r.Nodes), "the registry should contain correct number of nodes")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
	assert.Contains(t, r.Nodes, mxtest.TestScopeHash)
}

func TestMultiplexNodeDefaultMultiplexNode(t *testing.T) {

	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_default_multiplex_node",
		"",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	// create & start node
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	assert.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode
}

func TestMultiplexNodeSingleChainStartStop(t *testing.T) {

	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_default_multiplex_node_single_start_stop",
		"",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	// Uses a singleton scope registry to create SHA256 once
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	require.NoError(t, err)

	// create node registry
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	require.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, 1, len(r.Nodes), "the registry should contain exactly one node")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode

	var wg sync.WaitGroup

	// Tries to start/stop nodes SEQUENTIALLY
	baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	for _, n := range r.Nodes {
		userAddress, err := scopeRegistry.GetAddress(n.ScopeHash)
		require.NoError(t, err)

		wg.Add(1)
		t.Logf("Starting new node: %s - %s", n.ScopeHash, n.GenesisDoc().ChainID)

		walFolder := n.ScopeHash[:16] // uses hex

		// Overwrite wal file on a per-node basis
		walFile := filepath.Join(baseDataDir, "cs.wal", walFolder, "wal")

		t.Logf("Using walFile: %s", walFile)
		n.Config().Consensus.SetWalFile(walFile)

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(t, n, config, userAddress)

		// Calls wg.Done() after executing node.Stop()
		assertStartStopScopedNode(t, &wg, n)
	}

	wg.Wait()
}

func TestMultiplexNodeMultipleChainsStartStopSequential(t *testing.T) {

	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_default_multiplex_node_multiple_start_stop",
		"",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF190888BE0F48DE88927C3F49215B96548273AF": {"Scoping", "multiple scopes"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Private"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"Globals"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	expectedNumChains := 5

	// Uses a singleton scope registry to create SHA256 once
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	require.NoError(t, err)

	// create node registry
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	require.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, expectedNumChains, len(r.Nodes), "the registry should contain correct number of nodes")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode

	var wg sync.WaitGroup

	// Tries to start/stop nodes SEQUENTIALLY
	baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	for _, n := range r.Nodes {
		userAddress, err := scopeRegistry.GetAddress(n.ScopeHash)
		require.NoError(t, err)

		wg.Add(1)
		t.Logf("Starting new node: %s - %s", n.ScopeHash, n.GenesisDoc().ChainID)

		walFolder := n.ScopeHash[:16] // uses hex

		// Overwrite wal file on a per-node basis
		walFile := filepath.Join(baseDataDir, "cs.wal", walFolder, "wal")

		// We overwrite the wal file to allow parallel I/O for multiple nodes
		t.Logf("Using walFile: %s", walFile)
		n.Config().Consensus.SetWalFile(walFile)

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(t, n, config, userAddress)

		// Calls wg.Done() after executing node.Stop()
		assertStartStopScopedNode(t, &wg, n)
	}

	wg.Wait()
}

func TestMultiplexNodeComplexConfigStartStopSequential(t *testing.T) {

	config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
		"mx_node_default_multiplex_node_complex_start_stop",
		"",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default", "ReplChain", "Third", "Fourth", "Fifth"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ReplChain", "Default"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"First", "Second"},
		},
	)
	defer os.RemoveAll(config.RootDir)

	expectedNumChains := 12

	// Uses a singleton scope registry to create SHA256 once
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	require.NoError(t, err)

	// create node registry
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	require.NotEmpty(t, r.Nodes, "the registry should not be empty")
	assert.Equal(t, expectedNumChains, len(r.Nodes), "the registry should contain correct number of nodes")
	assert.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode

	var wg sync.WaitGroup

	// Tries to start/stop nodes SEQUENTIALLY
	baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)
	for _, n := range r.Nodes {
		userAddress, err := scopeRegistry.GetAddress(n.ScopeHash)
		require.NoError(t, err)

		wg.Add(1)
		t.Logf("Starting new node: %s - %s", n.ScopeHash, n.GenesisDoc().ChainID)

		walFolder := n.ScopeHash[:16] // uses hex

		// Overwrite wal file on a per-node basis
		walFile := filepath.Join(baseDataDir, "cs.wal", walFolder, "wal")

		// We overwrite the wal file to allow parallel I/O for multiple nodes
		t.Logf("Using walFile: %s", walFile)
		n.Config().Consensus.SetWalFile(walFile)

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(t, n, config, userAddress)

		// Calls wg.Done() after executing node.Stop()
		assertStartStopScopedNode(t, &wg, n)
	}

	wg.Wait()
}

func TestMultiplexNodeTwoChainsBothProduceBlocks(t *testing.T) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	wg := sync.WaitGroup{}
	wg.Add(len(r.Nodes))

	expectedBlocks := 10
	actualNumBlocks := make(map[string]int, len(r.Nodes))

	for _, n := range r.Nodes {
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
						actualNumBlocks[sn.ScopeHash] = numBlocks
						wg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					wg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(15 * time.Second):
					wg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(n, expectedBlocks)
	}

	// Wait for both nodes to produce 10 blocks in parallel
	wg.Wait()

	// We assert that both networks produced 10 blocks, if an error
	// occurred on one of the networks, the map entry won't exist.
	for _, scopeHash := range r.Scopes {
		assert.Contains(t, actualNumBlocks, scopeHash)
		assert.Equal(t, actualNumBlocks[scopeHash], expectedBlocks)
	}
}

func TestBigMultiplexNodeTwoChainsBothProduceLongChain(t *testing.T) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	wg := sync.WaitGroup{}
	wg.Add(len(r.Nodes))

	expectedBlocks := 100
	actualNumBlocks := make(map[string]int, len(r.Nodes))

	for _, n := range r.Nodes {
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
						actualNumBlocks[sn.ScopeHash] = numBlocks
						wg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					wg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(15 * time.Second):
					wg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(n, expectedBlocks)
	}

	// Wait for both nodes to produce 100 blocks in parallel
	// Note: sometimes one of the networks may progress faster and reach 120 blocks.
	wg.Wait()

	// We assert that both networks produced 100 blocks, if an error
	// occurred on one of the networks, the map entry won't exist.
	for _, scopeHash := range r.Scopes {
		assert.Contains(t, actualNumBlocks, scopeHash)
		assert.Equal(t, actualNumBlocks[scopeHash], expectedBlocks)
	}
}

// ----------------------------------------------------------------------------
// Benchmarks

func BenchmarkMultiplexLegacyNodeConsensus(t *testing.B) {

	config, n := assertStartLegacyNode(t, "mx_bench_node_legacy_node_consensus")

	// Shutdown routine
	defer func(cn *cmtnode.Node) {
		defer os.RemoveAll(config.RootDir)
		_ = cn.Stop()
	}(n)

	// We shall create random ids to push many txes
	src := rand.NewSource(time.Now().Unix())
	randomizer := rand.New(src)
	mtx := sync.Mutex{}

	// Every iteration should create a transaction with a random value
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		mtx.Lock()
		randomizeData := randomizer.Intn(999999)
		mtx.Unlock()

		randomId := strconv.Itoa(randomizeData)

		t.Logf("Now broadcasting transaction: test=value%s", randomId)
		http.Get("http://127.0.0.1:36657/broadcast_tx_commit?tx=\"test=value" + randomId + "\"")
	}
}

func BenchmarkMultiplexNodeSequentialStartStopTwoChains(t *testing.B) {
	t.ResetTimer()
	for i := 0; i < t.N; i++ {
		// Stop timer until nodes initialized:
		// - Node config not timed
		// - SHA256 generation not timed
		// - Node service instances not timed
		t.StopTimer()

		// Reset the node configuration  files
		config := mxtest.ResetTestRootMultiplexWithChainIDAndScopes(
			"mx_bench_node_default_multiplex_node_sequential_start_stop",
			"",
			map[string][]string{
				"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
			},
		)
		defer os.RemoveAll(config.RootDir)

		requiredNumChains := 2

		// Uses a singleton scope registry to create SHA256 once per iteration
		scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
		require.NoError(t, err)

		baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)

		// Create node registry
		r, err := DefaultMultiplexNode(config, log.TestingLogger())
		require.NoError(t, err)
		require.NotEmpty(t, r.Nodes, "the registry should not be empty")
		require.Equal(t, requiredNumChains, len(r.Nodes), "the registry should contain correct number of nodes")
		require.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode

		// Start timer to benchmark node's start/stop
		t.StartTimer()

		// Reset wait group for every iteration
		var wg sync.WaitGroup

		// Benchmarks the start/stop of multiple nodes every iteration
		for _, n := range r.Nodes {
			userAddress, err := scopeRegistry.GetAddress(n.ScopeHash)
			require.NoError(t, err)

			wg.Add(1)
			t.Logf("Starting new node: %s - %s", n.ScopeHash, n.GenesisDoc().ChainID)

			// Overwrite wal file on a per-node basis
			walFolder := n.ScopeHash[:16] // uses hex
			walFile := filepath.Join(baseDataDir, "cs.wal", walFolder, "wal")

			// We overwrite the wal file to allow parallel I/O for multiple nodes
			n.Config().Consensus.SetWalFile(walFile)

			// We reset the PrivValidator for every node and consensus reactors
			usePrivValidatorFromFiles(t, n, config, userAddress)

			// Calls wg.Done() after executing node.Stop()
			assertStartStopScopedNode(t, &wg, n)
		}

		// Wait for the multiple nodes to execute their `wg.Done()` call
		wg.Wait()
	}
}

func BenchmarkMultiplexNodeTriggerConsensusSingleChain(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_single_chain",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall create random ids to push many txes
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		for pb.Next() {
			nodeRPC := "http://127.0.0.1:40001"

			mtx.Lock()
			randomizeData := randomizer.Intn(999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)

			txData := "test=value" + randomVal

			t.Logf("Now broadcasting transaction: %s", txData)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexNodeTriggerConsensusTwoChains(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_chains",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexNodeTriggerConsensusTwoUserChains(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexNodeTriggerConsensusFourChains(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3X Sc0p3#!"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Transactions"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexNodeTriggerConsensusEightChains(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3X Sc0p3#!"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Likes"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ICE"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"1", "2"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

func BenchmarkMultiplexNodeTriggerConsensusTwelveChains(t *testing.B) {

	config, r, _ := assertStartMultiplexNodeRegistry(t,
		"mx_bench_node_multiplex_node_trigger_consensus_two_users",
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "C0mpl3X Sc0p3#!"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Posts", "Likes", "Media", "Events", "Assets"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ICE", "Osmosis"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"1", "2"},
		},
		map[string][]string{}, // empty validator set = default priv validator
	)

	// Shutdown routine
	defer func(reg *NodeRegistry) {
		defer os.RemoveAll(config.RootDir)
		for _, n := range reg.Nodes {
			_ = n.Stop()
		}
	}(r)

	// We shall randomly pick a node index
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	t.SetParallelism(1000)
	t.ResetTimer()
	t.RunParallel(func(pb *testing.PB) {
		// Every iteration should create a transaction with a random value
		// and broadcast it using a random node index from the list of nodes
		for pb.Next() {
			mtx.Lock()
			randomizeData := randomizer.Intn(999999999)
			mtx.Unlock()

			randomVal := strconv.Itoa(randomizeData)
			txData := "test=value" + randomVal

			mtx.Lock()
			rpcPort := randomizer.Intn(len(r.Nodes)) + 40001
			mtx.Unlock()

			nodeRPC := "http://127.0.0.1:" + strconv.Itoa(rpcPort)

			t.Logf("Now broadcasting transaction: %s to %s", txData, nodeRPC)
			http.Get(nodeRPC + "/broadcast_tx_commit?tx=\"" + txData + "\"")
		}
	})
}

// ----------------------------------------------------------------------------

func useDefaultKeyGenFunc() func() (crypto.PrivKey, error) {
	return func() (crypto.PrivKey, error) {
		return ed25519.GenPrivKey(), nil
	}
}

func usePrivValidatorFromFiles(t testing.TB, n *ScopedNode, config *cfg.Config, userAddress string) {
	t.Helper()

	scopeId := NewScopeIDFromHash(n.ScopeHash)
	userConfDir := filepath.Join(config.RootDir, cfg.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir, userAddress)

	folderName := scopeId.Fingerprint()
	privValKeyDir := filepath.Join(userConfDir, folderName)
	privValStateDir := filepath.Join(userDataDir, folderName)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(config.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(config.PrivValidatorStateFile()))

	// Reload the priv validator from files. This overwrites the PrivValidator
	// so that it uses the default privval or a generated privval.
	newPV, err := privval.LoadOrGenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
	require.NoError(t, err)
	t.Logf("Using priv validator from files: %s\n", newPV.GetAddress())

	n.SetPrivValidator(newPV)

	consensusReactor := n.Switch().Reactor("CONSENSUS").(*cs.Reactor)
	consensusReactor.SetPrivValidator(newPV)
}

func assertStartStopScopedNode(t testing.TB, wg *sync.WaitGroup, n *ScopedNode) {
	t.Helper()

	err := n.Start()
	require.NoError(t, err)

	// wait for the node to produce a block
	blocksSub, err := n.EventBus().Subscribe(context.Background(), "node_test", types.EventQueryNewBlock)
	require.NoError(t, err)
	select {
	case <-blocksSub.Out():
	case <-blocksSub.Canceled():
		t.Fatal("blocksSub was canceled")
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for the node to produce a block")
	}

	// stop the node
	go func() {
		defer wg.Done()
		err = n.Stop()
		require.NoError(t, err)
	}()

	select {
	case <-n.Quit():
	case <-time.After(5 * time.Second):
		pid := os.Getpid()
		p, err := os.FindProcess(pid)
		if err != nil {
			panic(err)
		}
		err = p.Signal(syscall.SIGABRT)
		fmt.Println(err)
		t.Fatal("timed out waiting for shutdown")
	}
}

func assertConfigureMultiplexNodeRegistry(
	t testing.TB,
	testName string,
	userScopes map[string][]string, // user_address:[scope1,scope2]
	validators map[string][]string, // scope_hash:[validator1,validator2]
	privValidator *privval.FilePV,
	startPortOverwrite int,
	persistentPeers string,
) (*cfg.Config, *NodeRegistry, *ScopeRegistry) {
	t.Helper()

	// Reset the node configuration files
	config := &cfg.Config{}
	if len(validators) == 0 {
		// Uses DEFAULT priv validator key
		config = mxtest.ResetTestRootMultiplexWithChainIDAndScopes(testName, "", userScopes)
	} else if privValidator == nil {
		// Uses RANDOM priv validator key
		config = mxtest.ResetTestRootMultiplexWithValidators(testName, "", userScopes, validators, nil, persistentPeers)
	} else /* privValidator != nil */ {
		// Uses SPECIFIC priv validator key
		config = mxtest.ResetTestRootMultiplexWithValidators(testName, "", userScopes, validators, privValidator, persistentPeers)
	}

	if startPortOverwrite > 0 {
		t.Logf("Using start port overwrite: %d", startPortOverwrite)
		config.UserConfig.ListenPort = startPortOverwrite
	}

	// Uses a singleton scope registry to create SHA256 once per iteration
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	require.NoError(t, err)

	// Create node registry
	r, err := DefaultMultiplexNode(config, log.TestingLogger())
	require.NoError(t, err)
	require.NotEmpty(t, r.Nodes, "the registry should not be empty")
	require.NotContains(t, r.Nodes, "") // empty key should not exist in plural mode

	return config, r, scopeRegistry
}

// When using this function, do not forget to call Stop()
// for every node that is started and created in the NodeRegistry
func assertStartMultiplexNodeRegistry(
	t testing.TB,
	testName string,
	userScopes map[string][]string, // user_address:[scope1,scope2]
	validators map[string][]string, // scope_hash:[validator1,validator2]
) (*cfg.Config, *NodeRegistry, *ScopeRegistry) {
	t.Helper()

	// Uses ResetTestRootMultiplex* and DefaultMultiplexNode
	// The node registry and scope registry are fully setup.
	config, r, scopeRegistry := assertConfigureMultiplexNodeRegistry(t, testName, userScopes, validators, nil, 0, "") // nil default priv validator
	baseDataDir := filepath.Join(config.RootDir, cfg.DefaultDataDir)

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(r.Nodes))

	for _, n := range r.Nodes {
		userAddress, err := scopeRegistry.GetAddress(n.ScopeHash)
		require.NoError(t, err)

		// Overwrite wal file on a per-node basis
		walFolder := n.ScopeHash[:16] // uses hex
		walFile := filepath.Join(baseDataDir, "cs.wal", walFolder, "wal")

		// We overwrite the wal file to allow parallel I/O for multiple nodes
		n.Config().Consensus.SetWalFile(walFile)

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(t, n, config, userAddress)

		go func(sn *ScopedNode) {
			defer wg.Done()
			t.Logf("Starting new node: %s - %s", sn.ScopeHash, sn.GenesisDoc().ChainID)
			t.Logf("Using listen addr: p2p:%s - rpc:%s", sn.Config().P2P.ListenAddress, sn.Config().RPC.ListenAddress)
			err := sn.Start()
			require.NoError(t, err)
		}(n)
	}

	// Wait for both nodes to have produced a block
	t.Logf("Waiting for %d nodes to be up and running.", len(r.Nodes))
	wg.Wait()

	return config, r, scopeRegistry
}

func assertStartLegacyNode(t testing.TB, testName string) (*cfg.Config, *cmtnode.Node) {
	config := cmttest.ResetTestRoot(testName)

	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	require.NoError(t, err)

	privValidator, err := privval.LoadOrGenFilePV(
		config.PrivValidatorKeyFile(),
		config.PrivValidatorStateFile(),
		useDefaultKeyGenFunc(),
	)
	require.NoError(t, err)

	n, err := cmtnode.NewNode(
		context.Background(),
		config,
		privValidator,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		cmtnode.DefaultGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		cmtnode.DefaultMetricsProvider(config.Instrumentation),
		log.TestingLogger(),
	)
	require.NoError(t, err)

	// Start and stop to close the db for later reading
	err = n.Start()
	require.NoError(t, err)
	return config, n
}
