package multiplex_test

import (
	"context"
	"fmt"
	"os"
	"slices"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

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
) *mx.MultiNetworkNodeInfo {
	numNetworks := len(networks)
	protocolVersions := make([]mx.ChainProtocolVersion, numNetworks)

	// Make sure ChainIDs are sorted
	slices.Sort(networks)

	p2pListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())
	rpcListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())

	// create versions and listen addresses per network
	for i, chainID := range networks {
		protocolVersion := mx.NewChainProtocolVersion(chainID, mx.DefaultProtocolVersion)
		protocolVersions[i] = protocolVersion
	}

	return &mx.MultiNetworkNodeInfo{
		Networks:         networks,
		ProtocolVersions: protocolVersions,

		DefaultNodeID: id,
		ListenAddr:    p2pListenAddr,
		Version:       "1.2.3-rc0-deadbeef",
		Channels:      []byte{testCh}, // defined in handshaker_test
		Moniker:       name,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: rpcListenAddr,
		},
	}
}

func TestMultiplexReactorP2PCreateTransportSwitchesWithReactors(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, globalCfg, reactor := ResetTestMultiplexP2P(t, numChains)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		require.NoError(t, err)
	}()

	testChainIds := reactor.GetNetworks()

	// Requires correct MultiNetworkNodeInfo
	testNodeInfo := mockNodeInfoWithNetworks(
		reactor.GetNodeKey().ID(),
		globalCfg.Moniker,
		testChainIds,
	)
	reactor.SetNodeInfo(testNodeInfo)

	// Should create [p2p.MultiplexTransport] instances
	err := reactor.CreateTransportSwitchesWithReactors(context.TODO(), testChainIds)
	assert.NoError(t, err, "should not error creating transports and switches")

	assert.NotNil(t, reactor.GetEventSwitchForCometBFT())
	assert.NotNil(t, reactor.GetTransportForCometBFT())

	testSwitch := reactor.GetEventSwitchForCometBFT()
	testTransport := reactor.GetTransportForCometBFT()
	assert.NotNil(t, testTransport.NetAddress())

	for _, chainID := range testChainIds {
		testReactors := testSwitch.Reactors(chainID)
		assert.Len(t, testReactors, 4) // mempool, blocksync, consensus, evidence
		assert.Contains(t, testReactors, "MEMPOOL")
		assert.Contains(t, testReactors, "BLOCKSYNC")
		assert.Contains(t, testReactors, "CONSENSUS")
		assert.Contains(t, testReactors, "EVIDENCE")

		assert.NotNil(t, testSwitch.Reactor(chainID, "MEMPOOL"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "BLOCKSYNC"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "CONSENSUS"))
		assert.NotNil(t, testSwitch.Reactor(chainID, "EVIDENCE"))
	}
}

func TestMultiplexReactorP2PCreateAddressBooks(t *testing.T) {
	defer goleak.VerifyNone(t)

	numChains := 5
	rootDir, globalCfg, reactor := ResetTestMultiplexP2P(t, numChains)
	defer func() {
		defer os.RemoveAll(rootDir)
		err := reactor.Stop()
		require.NoError(t, err)
	}()

	testChainIds := reactor.GetNetworks()

	// Requires correct MultiNetworkNodeInfo
	testNodeInfo := mockNodeInfoWithNetworks(
		reactor.GetNodeKey().ID(),
		globalCfg.Moniker,
		testChainIds,
	)
	reactor.SetNodeInfo(testNodeInfo)

	err := reactor.CreateTransportSwitchesWithReactors(context.TODO(), testChainIds)
	require.NoError(t, err, "should not error creating transports and switches")

	// Should create [p2p.pex.AddrBook] instances
	err = reactor.CreateAddressBooks(context.TODO(), testChainIds)
	assert.NoError(t, err, "should not error creating pex address books")

	// Should set the AddrBook on [p2p.Switch]
	// switchesProvider := reactor.GetInstanceProvider(mx.InstanceKeyP2PSwitch)
	// assert.NotNil(t, switchesProvider, "event switch provider must not be nil")

	assert.NotNil(t, reactor.GetEventSwitchForCometBFT())
	testSwitch := reactor.GetEventSwitchForCometBFT()

	for _, chainID := range testChainIds {
		// eventSwitch := switchesProvider(chainID).(*p2p.Switch)
		// assert.NotNil(t, eventSwitch)

		// Do we have the PEX and AddrBook?
		testReactors := testSwitch.Reactors(chainID)
		assert.Contains(t, testReactors, "PEX")
		assert.NotNil(t, testSwitch.GetAddrBook())
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
	globalCfg.MultiplexConfig = makeRandomMultiplexConfig(tb, numChains, 30001)
	mockGenesisProvider := mockMultiplexGenesisDocProviderFunc(&globalCfg.MultiplexConfig, numChains)

	// Create a test reactor
	reactor := makeTestReactorWithGenesisDocProvider(tb, globalCfg, mockGenesisProvider)

	// Start the reactor
	err = reactor.Start()
	require.NoError(tb, err, "should start the multiplex reactor")

	err = reactor.WaitForNetworks()
	require.NoError(tb, err, "should not error while waiting for networks")

	testChainIds := reactor.GetNetworks()

	// Start an ABCI client
	abciClient := proxy.NewMultiplexAppConn(
		testChainIds,
		proxy.DefaultClientCreator(globalCfg.ProxyApp, globalCfg.ABCI, globalCfg.DBDir()),
		proxy.NopMetrics(),
	)
	abciClient.SetLogger(cmtlog.NewNopLogger())
	err = abciClient.Start()
	require.NoError(tb, err, "should start ABCI client with ChainConns interface")

	// Reactor: ABCI; ABCI: Reactor.
	reactor.SetABCIClient(abciClient)

	// Should now be able to do consensus handshake and load state machines
	for _, chainID := range testChainIds {
		err = reactor.PrepareConsensusInstanceWithReactor(context.TODO(), chainID)
		require.NoError(tb, err, "should not error for consensus handshake")

		blockSync := false
		err := reactor.CreateConsensusInstanceReactors(
			context.TODO(),
			chainID,
			blockSync,
			false, // waitSync
		)
		require.NoError(tb, err, "should not error creating consensus reactors")
	}

	return rootDir, globalCfg, reactor
}
