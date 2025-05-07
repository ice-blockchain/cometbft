package multiplex_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtnet "github.com/ice-blockchain/cometbft/internal/net"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
)

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

func TestMultiplexMultiNetworkNodeInfoValidate(t *testing.T) {
	// empty fails
	ni := &mx.MultiNetworkNodeInfo{}
	require.Error(t, ni.Validate())

	maxNumChannels := p2p.MaxNumChannels()

	channels := make([]byte, maxNumChannels)
	for i := 0; i < maxNumChannels; i++ {
		channels[i] = byte(i)
	}
	dupChannels := make([]byte, 5)
	copy(dupChannels, channels[:5])
	dupChannels = append(dupChannels, testCh) //nolint:makezero // huge errors when we don't do it the "wrong" way

	nonASCII := "¢§µ"
	emptyTab := "\t"
	emptySpace := "  "

	testCases := []struct {
		testName         string
		malleateNodeInfo func(*mx.MultiNetworkNodeInfo)
		expectErr        bool
	}{
		{
			"Too Many Channels",
			func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(append(channels, byte(maxNumChannels))) }, //nolint: makezero
			true,
		},
		{"Duplicate Channel", func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(dupChannels) }, true},
		{"Good Channels", func(ni *mx.MultiNetworkNodeInfo) { ni.SetChannels(ni.Channels[:5]) }, false},

		{"Invalid NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.SetListenAddr("not-an-address") }, true},
		{"Good NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.SetListenAddr("0.0.0.0:26656") }, false},

		{"Non-ASCII Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(nonASCII) }, true},
		{"Empty tab Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(emptyTab) }, true},
		{"Empty space Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion(emptySpace) }, true},
		{"Empty Version", func(ni *mx.MultiNetworkNodeInfo) { ni.SetVersion("") }, false},

		{"Non-ASCII Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(nonASCII) }, true},
		{"Empty tab Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(emptyTab) }, true},
		{"Empty space Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker(emptySpace) }, true},
		{"Empty Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker("") }, true},
		{"Good Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.SetMoniker("hey its me") }, false},

		{"Non-ASCII TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, nonASCII) }, true},
		{"Empty tab TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, emptyTab) }, true},
		{"Empty space TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, emptySpace) }, true},
		{"Empty TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, "") }, false},
		{"Off TxIndex", func(ni *mx.MultiNetworkNodeInfo) { malleateTxIndex(ni, "off") }, false},

		{"Non-ASCII RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, nonASCII) }, true},
		{"Empty tab RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, emptyTab) }, true},
		{"Empty space RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, emptySpace) }, true},
		{"Empty RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, "") }, false},
		{"Good RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { malleateRPCAddress(ni, "0.0.0.0:26657") }, false},
	}

	nodeKey := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	// test case passes
	ni = testNodeInfo(nodeKey.ID(), name).(*mx.MultiNetworkNodeInfo)
	ni.SetChannels(channels)
	require.NoError(t, ni.Validate())

	for i, tc := range testCases {
		ni := testNodeInfo(nodeKey.ID(), name).(*mx.MultiNetworkNodeInfo)
		ni.SetChannels(channels)
		tc.malleateNodeInfo(ni)
		err := ni.Validate()
		if tc.expectErr {
			require.Error(t, err, fmt.Sprintf(tc.testName+" should error at %d", i))
		} else {
			require.NoError(t, err, tc.testName)
		}
	}
}

func TestMultiplexMultiNetworkNodeInfoCompatible(t *testing.T) {
	defer goleak.VerifyNone(t)

	nodeKey1 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeKey2 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	var newTestChannel byte = 0x2

	// test NodeInfo is compatible
	ni1 := testNodeInfo(nodeKey1.ID(), name).(*mx.MultiNetworkNodeInfo)
	ni2 := testNodeInfo(nodeKey2.ID(), name).(*mx.MultiNetworkNodeInfo)
	require.NoError(t, ni1.CompatibleWith(ni2))

	// add another channel; still compatible
	ni2.Channels = append(ni2.Channels, newTestChannel)
	assert.True(t, ni2.HasChannel(newTestChannel))
	require.NoError(t, ni1.CompatibleWith(ni2))

	// wrong NodeInfo type is not compatible
	_, netAddr := p2p.CreateRoutableAddr()
	ni3 := p2p.NewMockNodeInfo(netAddr)
	require.Error(t, ni1.CompatibleWith(ni3))

	testCases := []struct {
		testName         string
		malleateNodeInfo func(*mx.MultiNetworkNodeInfo)
	}{
		{"Wrong block version", func(ni *mx.MultiNetworkNodeInfo) { ni.ProtocolVersions[0].Block++ }},
		{"No common channels", func(ni *mx.MultiNetworkNodeInfo) { ni.Channels = []byte{newTestChannel} }},
	}

	for i, tc := range testCases {
		ni := testNodeInfo(nodeKey2.ID(), name).(*mx.MultiNetworkNodeInfo)
		tc.malleateNodeInfo(ni)
		require.Error(t, ni1.CompatibleWith(ni), fmt.Sprintf("should error at %d", i))
	}
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
