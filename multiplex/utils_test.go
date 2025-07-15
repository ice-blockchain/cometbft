package multiplex_test

import (
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	cmtnet "github.com/ice-blockchain/cometbft/internal/net"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/node"
	cmtnode "github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	mx "github.com/ice-blockchain/cometbft/multiplex"
)

const (
	defaultNodeName = "host_peer"
	testCh          = 0x01
	testAddress     = "CC8E6555A3F401FF61DA098F94D325E7041BC43A"
	testFinHash     = "1A63C0E60122F9BB"
	testChainID     = "mx-chain-" + testAddress + "-" + testFinHash
)

var (
	genesisDocHashKey = []byte("mxGenesisDocHash")
	stateKey          = []byte("stateKey")
)

// CAUTION: this is not a GenesisDocSet, but just a GenesisDoc and is used
// to test the multiplex fallback to a legacy node implementation.
var testLegacyGenesisDocFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
	  ` + testDefaultGenesisValidator + `
	],
	"app_hash": ""
}`

var (
	testGenesisValidatorPubKey  = "AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
	testDefaultGenesisValidator = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"` + testGenesisValidatorPubKey + `"
	},
	"power": "10",
	"name": ""
}`
)

// This produces a GenesisDocSet instance with exactly one chain.
var testGenesisDocWithValidatorsFmt = `{
	"genesis_time": "2018-10-10T08:20:13.695936996Z",
	"chain_id": "%s",
	"initial_height": "1",
	"consensus_params": {
		"block": {
			"max_bytes": "22020096",
			"max_gas": "-1",
			"time_iota_ms": "10"
		},
		"synchrony": {
			"message_delay": "500000000",
			"precision": "10000000"
		},
		"evidence": {
			"max_age_num_blocks": "100000",
			"max_age_duration": "172800000000000",
			"max_bytes": "1048576"
		},
		"validator": {
			"pub_key_types": [
				"ed25519"
			]
		},
		"abci": {
			"vote_extensions_enable_height": "0"
		},
		"version": {},
		"feature": {
			"vote_extensions_enable_height": "0",
			"pbts_enable_height": "1"
		}
	},
	"validators": [
		%s
	],
	"app_hash": ""
}`

var testPrivValidatorState = `{
  "height": "0",
  "round": 0,
  "step": 0
}`

func makeChainRegistryFromConfig(tb testing.TB, conf config.MultiplexConfig) mx.ChainRegistry {
	tb.Helper()

	chainRegistry, err := mx.NewChainRegistry(&conf, "")
	require.NoError(tb, err, "should create chain registry from config")

	return chainRegistry
}

func makeRandomMultiplexConfig(tb testing.TB, numChains int, discoveryPort int) config.MultiplexConfig {
	tb.Helper()

	if numChains == 0 {
		return config.MultiplexBaseConfig(
			map[string]string{},
			map[string][]string{},
		).MultiplexConfig
	}

	randomChainIDs := make([]string, numChains)
	randChainSeeds := make(map[string]string, numChains)
	randUserChains := make(map[string][]string, numChains)

	for i := 0; i < numChains; i++ {
		userPubKey := ed25519.GenPrivKey().PubKey()
		userAddress := userPubKey.Address().String()
		fingerprint := helpers.MakeFingerprint("Posts") // This is the "scope"

		chainID := helpers.NewExtendedChainID(userAddress, fingerprint)
		require.NotNil(tb, chainID, "should create random ChainID")

		randUserChains[userAddress] = make([]string, 1)
		randUserChains[userAddress][0] = chainID.String()

		// Uses a fake (random) seed node ID
		testSeedNodeKey := &p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
		testSeedNodeID := testSeedNodeKey.ID()

		randomChainIDs[i] = chainID.String()
		randChainSeeds[chainID.String()] = string(testSeedNodeID) + "@127.0.0.1:30001"
	}

	return config.MultiplexConfig{
		Strategy:      mx.NetworkReplicationStrategy(),
		ChainSeeds:    randChainSeeds,
		UserChains:    randUserChains,
		DiscoveryPort: uint16(discoveryPort),
	}
}

func makeWalPath(rootDir, address, chainID string) string {
	return filepath.Join(rootDir, config.DefaultDataDir, address, chainID, "wal")
}

func createTempMemDB(
	tb testing.TB,
	rootDir string,
	numDatabases int,
) ([]dbm.DB, error) {
	tb.Helper()

	dbPtrs := make([]dbm.DB, numDatabases)
	dbType := dbm.BackendType("memdb")

	for i := 0; i < numDatabases; i++ {
		dbDir, err := os.MkdirTemp(rootDir, "cometbft-memdb-*")
		if err != nil {
			return dbPtrs, err
		}

		dbID := "db-" + strconv.Itoa(i)
		db, err := dbm.NewDB(dbID, dbType, dbDir)
		require.NoError(tb, err, "should create new DB instance")

		dbPtrs[i] = db
		db.Close() // noop for memdb
	}

	return dbPtrs, nil
}

func randomGenesisDocSet(opt ...int) mx.GenesisDocSet {
	cnt := 2
	if len(opt) > 0 {
		cnt = opt[0]
	}

	userGenDocs := make(mx.GenesisDocSet, cnt)
	for i := 0; i < cnt; i++ {
		valPubKey := ed25519.GenPrivKey().PubKey()

		// address and fingerprint added to ChainID
		valAddress := valPubKey.Address().String()
		fingerprint := strings.ToUpper(hex.EncodeToString(
			tmhash.Sum([]byte("Posts"))[:8], // 8 bytes only
		))

		mxChainID := "test-chain-" + valAddress + "-" + fingerprint
		userGenDocs[i] = types.GenesisDoc{
			GenesisTime:   cmttime.Now(),
			ChainID:       mxChainID,
			InitialHeight: 1000,
			Validators: []types.GenesisValidator{{
				Address: valPubKey.Address(),
				PubKey:  valPubKey,
				Power:   10,
				Name:    "myval",
			}},
			ConsensusParams: types.DefaultConsensusParams(),
			AppHash:         []byte{1, 2, 3},
			AppState:        []byte(`{"account_owner":"Bob"}`),
		}
	}
	return userGenDocs
}

func makeRandomNodeKey() *p2p.NodeKey {
	priv := ed25519.GenPrivKey()
	return &p2p.NodeKey{PrivKey: priv}
}

// mockGenesisDocSetProviderFunc mocks a GenesisDocSet provider helper.
func mockGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    randomGenesisDocSet(3),
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

// mockErrorGenesisDocSetProviderFunc mocks a provider helper that errors.
func mockErrorGenesisDocSetProviderFunc() node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		return nil, errors.New("testError")
	}
}

// Do not use this in TestMultiplexReactorNewReactor.
func makeTestReactor(tb testing.TB, nodeCfg *config.Config) *mx.Reactor {
	tb.Helper()

	return makeTestReactorWithGenesisDocProvider(tb, nodeCfg, mockGenesisDocSetProviderFunc())
}

func makeTestReactorWithGenesisDocProvider(
	tb testing.TB,
	nodeCfg *config.Config,
	genDocProvider node.GenesisDocProvider,
) *mx.Reactor {
	tb.Helper()

	nodeKey := makeRandomNodeKey()

	chainRegistry, err := mx.NewChainRegistry(&nodeCfg.MultiplexConfig, "")
	require.NoError(tb, err, "should create chain registry instance")

	return mx.NewReactor(
		tb.Context(),
		nodeKey,
		nodeCfg,
		cmtlog.NewNopLogger(),
		chainRegistry,
		genDocProvider,
	)
}

func setReactorNodesStopsServers(reactor *mx.Reactor) {
	testChainIds := reactor.GetNetworks()
	serviceProvider := reactor.GetServicesProvider()
	for _, chainID := range testChainIds {
		nodeForChain := serviceProvider(mx.ServiceKeyNodeRuntime, chainID)
		if nodeForChain == nil {
			continue
		}

		runningNode := nodeForChain.(*cmtnode.Node)

		node.NodeWithStartRPC(true)(runningNode)
		node.NodeWithStartP2P(true)(runningNode)
		node.NodeWithStartMonitor(true)(runningNode)
	}
}

// multiplexGenesisDocProviderFunc mocks a GenesisDocSet provider helper
// by injecting ChainID values in genesis docs.
// CAUTION this should only be done for testing.
func mockMultiplexGenesisDocProviderFunc(
	conf *config.MultiplexConfig,
	numChains int,
) node.GenesisDocProvider {
	genesisDocSet := randomGenesisDocSet(numChains)

	// Requires synchrony between passed MultiplexConfig and numChains
	index := 0
	for _, chainIds := range conf.UserChains {
		for _, chainID := range chainIds {
			// CAUTION intentionally malleating GenesisDoc
			genesisDocSet[index].ChainID = chainID
			index++
		}
	}

	return func() (node.IChecksummedGenesisDoc, error) {
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs:    genesisDocSet,
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

func useDefaultKeyGenFunc() func() (crypto.PrivKey, error) {
	return func() (crypto.PrivKey, error) {
		return ed25519.GenPrivKey(), nil
	}
}

// assertStartNodesMultiplex configures a nodes multiplex *randomly* and starts
// individual nodes in a separate goroutine per network.
func assertStartNodesMultiplex(tb testing.TB, numChains int, customLogger cmtlog.Logger, startServers bool) (
	*config.Config,
	*mx.Reactor,
	func(*mx.Reactor),
) {
	tb.Helper()

	_, globalCfg := ResetTestMultiplexNode(tb, numChains)

	// Forces multi-test allowance, disables GRPC
	globalCfg.Instrumentation.Namespace = "cometbft:" + tb.Name()
	globalCfg.GRPC.ListenAddress = ""            // disabled GRPC
	globalCfg.GRPC.Privileged.ListenAddress = "" // disabled GRPC
	globalCfg.Consensus.CreateEmptyBlocks = true // when using *Node

	// Seeds must be valid (or empty), otherwise dialing will fail
	for chainID := range globalCfg.ChainSeeds {
		globalCfg.ChainSeeds[chainID] = ""
	}

	if customLogger == nil {
		customLogger = cmtlog.NewNopLogger()
	}

	// The multiplex configuration will be ENABLED.
	// Should create the [node.Node] instance using [mx.NewNodesMultiplex]
	_, testReactor, err := mx.NewNodesMultiplex(
		tb.Context(),
		&client.DefaultAcceptor{},
		globalCfg,
		customLogger,
		node.NodeWithStartRPC(false),
		node.NodeWithStartP2P(false),
		node.NodeWithStartMonitor(false),
	)
	require.NoError(tb, err, "should create node instance")
	require.Equal(tb, numChains, testReactor.Size(), fmt.Sprintf(
		"should contain exactly %d networks", numChains))

	require.NotNil(tb, testReactor)

	testChainIds := testReactor.GetNetworks()
	require.Len(tb, testChainIds, numChains)

	// CAUTION: This activates runtimes for pre-configured networks.
	mx.ReactorWithActiveRuntimes(tb.Context(), testChainIds, map[string][]string{})(testReactor)
	servicesProvider := testReactor.GetServicesProvider()

	if startServers && numChains > 0 {
		firstChainID := testChainIds[0]

		nodeRuntime := servicesProvider(mx.ServiceKeyNodeRuntime, firstChainID)
		require.NotNil(tb, nodeRuntime, "should create node.Node instance")
		fstRunningNode := nodeRuntime.(*cmtnode.Node)

		var rpcErr error
		_, rpcErr = fstRunningNode.StartRPC()
		require.NoError(tb, rpcErr, "should start CometBFT RPC server using Node impl")

		_, p2pErr := fstRunningNode.StartP2P()
		require.NoError(tb, p2pErr, "should start CometBFT P2P server using Node impl")
	}

	// Reset wait group for every iteration
	wg := sync.WaitGroup{}
	wg.Add(len(testChainIds))

	// Test that we have all the required networks
	for _, withChainID := range testChainIds {
		nodeRuntime := servicesProvider(mx.ServiceKeyNodeRuntime, withChainID)
		require.NotNil(tb, nodeRuntime, "should create node.Node instance")
		nodeInstance, ok := nodeRuntime.(*cmtnode.Node)
		require.Equal(tb, true, ok, "should register node.Node instance")

		userAddress, err := testReactor.GetChainRegistry().GetAddress(withChainID)
		require.NoError(tb, err, "should find user address by ChainID")

		// We reset the PrivValidator for every node and consensus reactors
		usePrivValidatorFromFiles(tb, nodeInstance, globalCfg, userAddress, withChainID)

		// Verify that we can start the node correctly
		go func(cn *cmtnode.Node, chainID string) {
			defer wg.Done()
			// t.Logf("Starting new node: %s", cn.GenesisDoc().ChainID)
			// t.Logf("Using listen addr: p2p:%s - rpc:%s", cn.Config().P2P.ListenAddress, cn.Config().RPC.ListenAddress)
			err := cn.Start()
			require.NoError(tb, err)

			// Must start CONSENSUS, MEMPOOL, etc.
			consensusErr := testReactor.StartConsensusInstanceReactors(
				tb.Context(),
				chainID,
				false, // sendStatusToPeers
			)
			require.NoError(tb, consensusErr)
		}(nodeInstance, withChainID)
	}

	// Wait for all nodes to be up and running
	// t.Logf("Waiting for %d nodes to be up and running.", len(testChainIds))
	wg.Wait()

	shutdownFn := func(withReactor *mx.Reactor) {
		defer os.RemoveAll(globalCfg.RootDir)

		nodesProvider := withReactor.GetServicesProvider()
		testChainIds := withReactor.GetNetworks()
		stoppingServers := false
		for i, withChainID := range testChainIds {
			withReactor.StopConsensusInstanceReactors(
				tb.Context(),
				withChainID,
			)

			// Since we are not using MultiplexBackend, we must instruct
			// a node to shutdown servers, i.e. replace MultiplexBackend.Close.
			if startServers && (i == 0 || !stoppingServers) {
				nodeRuntime := nodesProvider(mx.ServiceKeyNodeRuntime, withChainID)
				if nodeRuntime != nil {
					if rn, isNode := nodeRuntime.(*cmtnode.Node); isNode {
						node.NodeWithStartRPC(true)(rn)
						node.NodeWithStartP2P(true)(rn)
						node.NodeWithStartMonitor(true)(rn)
						stoppingServers = true
					}
				}
			}
		}

		// assertStartNodesMultiplex started the *Node(s).
		withReactor.StopAllNodeInstances()

		if withReactor.IsRunning() {
			err := withReactor.Stop()
			require.NoError(tb, err)
		}
	}

	return globalCfg, testReactor, shutdownFn
}

func assertWaitForNodesMultiplexToProduceBlocks(
	tb testing.TB,
	testReactor *mx.Reactor,
	expectedBlocks int,
	maximumDuration time.Duration,
	subscriberName string,
	broadcastTxFn func(testing.TB, string, int),
) (actualNumBlocks map[string]int, actualNumTxes map[string]int) {
	tb.Helper()

	testChainIds := testReactor.GetNetworks()

	wg := sync.WaitGroup{}
	wg.Add(len(testChainIds))

	mtx := sync.RWMutex{}
	actualNumBlocks = make(map[string]int, len(testChainIds))
	actualNumTxes = make(map[string]int, len(testChainIds))
	servicesProvider := testReactor.GetServicesProvider()
	for _, testChainID := range testChainIds {
		nodeRuntime := servicesProvider(mx.ServiceKeyNodeRuntime, testChainID)
		require.NotNil(tb, nodeRuntime, "should find node.Node instance")
		nodeInstance, ok := nodeRuntime.(*cmtnode.Node)
		require.Equal(tb, true, ok, "should register node.Node instance")

		if !nodeInstance.Config().Consensus.CreateEmptyBlocks {
			// Must broadcast transactions
			go broadcastTxFn(tb, testChainID, 1) // 1 transaction
		}

		// Parallel goroutines with internal blocks loops
		go func(the_chain string, the_node *cmtnode.Node, maxBlocks int) {
			// Wait for the node to produce blocks
			blocksSub, err := the_node.EventBus().Subscribe(
				tb.Context(),
				subscriberName,
				types.EventQueryNewBlock,
			)
			assert.NoError(tb, err)

			defer the_node.EventBus().Unsubscribe(
				tb.Context(),
				subscriberName,
				types.EventQueryNewBlock,
			)

			numBlocks := 0

			mtx.Lock()
			actualNumTxes[the_chain] = 0
			mtx.Unlock()

		NODE_BLOCKS_LOOP:
			for tb.Context().Err() == nil {
				select {
				case msg := <-blocksSub.Out():
					numBlocks++
					eventNewBlock := msg.Data().(types.EventDataNewBlock)
					actualNumTxes[the_chain] += len(eventNewBlock.Block.Data.Txs)

					if maxBlocks > 0 && numBlocks == maxBlocks {
						mtx.Lock()
						actualNumBlocks[the_chain] = numBlocks
						mtx.Unlock()
						wg.Done()
						break NODE_BLOCKS_LOOP
					}
				case <-blocksSub.Canceled():
					mtx.Lock()
					actualNumBlocks[the_chain] = numBlocks
					mtx.Unlock()
					wg.Done()
					break NODE_BLOCKS_LOOP
				case <-time.After(maximumDuration):
					mtx.Lock()
					actualNumBlocks[the_chain] = numBlocks
					mtx.Unlock()
					wg.Done()
					break NODE_BLOCKS_LOOP
				}
			}
		}(testChainID, nodeInstance, expectedBlocks)
	}

	// Wait for all nodes to produce X blocks in parallel
	wg.Wait()

	// We assert that all networks produced at least X blocks, if an error
	// occurred on one of the networks, the map entry won't exist.
	for _, chainID := range testChainIds {
		assert.Contains(tb, actualNumBlocks, chainID)

		if expectedBlocks > 0 {
			assert.Equal(tb, expectedBlocks, actualNumBlocks[chainID])
		}
	}

	mtx.RLock()
	defer mtx.RUnlock()
	return actualNumBlocks, actualNumTxes
}

func broadcastRawTx(tb testing.TB, chainID string, numTxes int) {
	tb.Helper()

	// We shall randomly pick values
	randomizer := rand.New(rand.NewSource(time.Now().Unix()))
	mtx := sync.Mutex{}

	for i := 0; i < numTxes; i++ {
		mtx.Lock()
		randomizeData := randomizer.Intn(999999999)
		mtx.Unlock()

		randomVal := strconv.Itoa(randomizeData)
		txData := "test=value" + randomVal

		go func(rpcHost, network, txData string) {
			rpcPath := "/broadcast_tx_commit/" + network

			// For debug, uncomment the following line
			tb.Logf("Now broadcasting transaction: %s to %s", txData, rpcHost)
			http.Get(rpcHost + rpcPath + "?tx=\"" + txData + "\"")
		}("http://127.0.0.1:30002", chainID, txData)
	}
}

func usePrivValidatorFromFiles(
	tb testing.TB,
	n *cmtnode.Node,
	conf *config.Config,
	userAddress string,
	chainID string,
) {
	tb.Helper()

	userConfDir := filepath.Join(conf.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(conf.RootDir, config.DefaultDataDir, userAddress)

	privValKeyDir := filepath.Join(userConfDir, chainID)
	privValStateDir := filepath.Join(userDataDir, chainID)

	privValKeyFile := filepath.Join(privValKeyDir, filepath.Base(conf.PrivValidatorKeyFile()))
	privValStateFile := filepath.Join(privValStateDir, filepath.Base(conf.PrivValidatorStateFile()))

	// Reload the priv validator from files. This overwrites the PrivValidator
	// so that it uses the default privval or a generated privval.
	newPV, err := privval.LoadOrGenFilePV(privValKeyFile, privValStateFile, useDefaultKeyGenFunc())
	require.NoError(tb, err)
	// t.Logf("Using priv validator from files: %s\n", newPV.GetAddress())

	n.SetPrivValidator(newPV)

	consensusReactor := n.Switch().Reactor(chainID, "CONSENSUS").(*cs.Reactor)
	consensusReactor.SetPrivValidator(newPV)
}

func malleateTxIndex(ni *mx.MultiNetworkNodeInfo, v string) {
	o := ni.GetOther()
	o.TxIndex = v
	ni.SetOther(o)
}

func malleateRPCAddress(ni *mx.MultiNetworkNodeInfo, v string) {
	o := ni.GetOther()
	o.RPCAddress = v
	ni.SetOther(o)
}

func emptyNodeInfo() p2p.NodeInfo {
	return mx.NewMultiNetworkNodeInfo()
}

func testNodeInfo(id p2p.ID, name string) p2p.NodeInfo {
	return testNodeInfoWithNetwork(id, name, "testing")
}

func testNodeInfoWithNetwork(id p2p.ID, name, network string) p2p.NodeInfo {
	p2pListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())
	rpcListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())

	mnni := &mx.MultiNetworkNodeInfo{}
	mnni.SetNetworks([]string{network})
	mnni.SetProtocolVersions([]mx.ChainProtocolVersion{
		mx.NewChainProtocolVersion(network, mx.DefaultProtocolVersion),
	})
	mnni.SetID(id)
	mnni.SetListenAddr(p2pListenAddr)
	mnni.SetChannels([]byte{testCh})
	mnni.SetVersion("1.2.3-rc0-deadbeef")
	mnni.SetMoniker(name)
	mnni.SetOther(p2p.DefaultNodeInfoOther{
		TxIndex:    "on",
		RPCAddress: rpcListenAddr,
	})

	return mnni
}

func getFreePort() int {
	port, err := cmtnet.GetFreePort()
	if err != nil {
		panic(err)
	}
	return port
}
