package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
)

// mockNodeInfoWithNetworks creates a [mx.MultiNetworkNodeInfo] instance that
// is adapted to the list of networks, moniker name and p2p.NodeKey ID.
func mockNodeInfoWithNetworks(
	id p2p.ID,
	name string,
	networks []string,
) mx.MultiNetworkNodeInfo {
	numNetworks := len(networks)
	protocolVersions := make([]mx.ChainProtocolVersion, numNetworks)
	listenAddresses := make([]mx.ChainListenAddr, numNetworks)
	rpcNodeAddresses := make([]mx.ChainListenAddr, numNetworks)

	// Make sure ChainIDs are sorted
	slices.Sort(networks)

	// create versions and listen addresses per network
	for i, chainID := range networks {
		protocolVersion := mx.NewChainProtocolVersion(chainID, mx.DefaultProtocolVersion)
		p2pListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())
		rpcListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())

		protocolVersions[i] = protocolVersion
		listenAddresses[i] = mx.NewChainListenAddr(chainID, p2pListenAddr)
		rpcNodeAddresses[i] = mx.NewChainListenAddr(chainID, rpcListenAddr)
	}

	return mx.MultiNetworkNodeInfo{
		Networks:         networks,
		ProtocolVersions: protocolVersions,
		ListenAddrs:      listenAddresses,
		RPCAddresses:     rpcNodeAddresses,

		DefaultNodeID: id,
		ListenAddr:    listenAddresses[0].ListenAddr,
		Version:       "1.2.3-rc0-deadbeef",
		Channels:      []byte{testCh}, // define in handshaker_test
		Moniker:       name,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: rpcNodeAddresses[0].ListenAddr,
		},
	}
}

func TestMultiplexReactorCreateTransportSwitches(t *testing.T) {
	numChains := 5
	rootDir, globalCfg, reactor := ResetTestMultiplexP2P(t, numChains)
	defer os.RemoveAll(rootDir)

	// Requires correct MultiNetworkNodeInfo
	testNodeInfo := mockNodeInfoWithNetworks(
		reactor.GetNodeKey().ID(),
		globalCfg.Moniker,
		reactor.GetNetworks(),
	)
	reactor.SetNodeInfo(testNodeInfo)

	// Should create [p2p.MultiplexTransport] instances
	err := reactor.CreateTransportSwitches(context.TODO())
	assert.NoError(t, err, "should not error creating transports and switches")

	transportsProvider := reactor.GetInstanceProvider(mx.InstanceKeyP2PTransport)
	assert.NotNil(t, transportsProvider, "transport provider must not be nil")

	switchesProvider := reactor.GetInstanceProvider(mx.InstanceKeyP2PSwitch)
	assert.NotNil(t, switchesProvider, "event switch provider must not be nil")

	for _, chainID := range reactor.GetNetworks() {
		testTransport := transportsProvider(chainID).(*p2p.MultiplexTransport)
		assert.NotNil(t, testTransport)
		assert.NotNil(t, testTransport.NetAddress())

		testSwitch := switchesProvider(chainID).(*p2p.Switch)
		assert.NotNil(t, testSwitch)

		testReactors := testSwitch.Reactors()
		assert.Len(t, testReactors, 4) // mempool, blocksync, consensus, evidence
		assert.Contains(t, testReactors, "MEMPOOL")
		assert.Contains(t, testReactors, "BLOCKSYNC")
		assert.Contains(t, testReactors, "CONSENSUS")
		assert.Contains(t, testReactors, "EVIDENCE")

		assert.NotNil(t, testSwitch.Reactor("MEMPOOL"))
		assert.NotNil(t, testSwitch.Reactor("BLOCKSYNC"))
		assert.NotNil(t, testSwitch.Reactor("CONSENSUS"))
		assert.NotNil(t, testSwitch.Reactor("EVIDENCE"))
	}
}

func TestMultiplexReactorCreateAddressBooks(t *testing.T) {
	numChains := 5
	rootDir, globalCfg, reactor := ResetTestMultiplexP2P(t, numChains)
	defer os.RemoveAll(rootDir)

	// Requires correct MultiNetworkNodeInfo
	testNodeInfo := mockNodeInfoWithNetworks(
		reactor.GetNodeKey().ID(),
		globalCfg.Moniker,
		reactor.GetNetworks(),
	)
	reactor.SetNodeInfo(testNodeInfo)

	err := reactor.CreateTransportSwitches(context.TODO())
	require.NoError(t, err, "should not error creating transports and switches")

	// Should create [p2p.pex.AddrBook] instances
	err = reactor.CreateAddressBooks(context.TODO())
	assert.NoError(t, err, "should not error creating pex address books")

	// Should set the AddrBook on [p2p.Switch]
	switchesProvider := reactor.GetInstanceProvider(mx.InstanceKeyP2PSwitch)
	assert.NotNil(t, switchesProvider, "event switch provider must not be nil")

	for _, chainID := range reactor.GetNetworks() {
		// Must be tested in chain_registry_test.go
		userAddress, err := reactor.GetChainRegistry().GetAddress(chainID)
		require.NoError(t, err, "should not error given valid ChainID")
		require.NotEmpty(t, userAddress)

		eventSwitch := switchesProvider(chainID).(*p2p.Switch)
		assert.NotNil(t, eventSwitch)

		// Do we have the PEX and AddrBook?
		testReactors := eventSwitch.Reactors()
		assert.Contains(t, testReactors, "PEX")
		assert.NotNil(t, eventSwitch.GetAddrBook())
	}
}

// CAUTION: this test method sets up a full consensus multiplex with reactors.
// CAUTION: this method *waits* for all networks to be configured with [Reactor#WaitForNetworks].
func ResetTestMultiplexP2P(tb testing.TB, numChains int) (string, *config.Config, *mx.Reactor) {
	tb.Helper()

	rootDir, err := os.MkdirTemp("", tb.Name())
	require.NoError(tb, err)

	globalCfg := config.TestConfig()
	globalCfg.SetRoot(rootDir)
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&globalCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, globalCfg, mockGenesisProvider)

	// Start the reactor
	err = reactor.Start()
	require.NoError(tb, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	require.NoError(tb, err, "should not error while waiting for networks")

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		reactor.GetNetworks(),
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(tb, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range reactor.GetNetworks() {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		require.NoError(tb, err, "should not error for consensus handshake")

		blockSync := false
		err := reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainID,
			blockSync,
		)
		require.NoError(tb, err, "should not error creating consensus reactors")
	}

	return rootDir, globalCfg, reactor
}
