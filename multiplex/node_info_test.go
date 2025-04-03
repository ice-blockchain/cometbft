package multiplex_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtnet "github.com/ice-blockchain/cometbft/internal/net"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
)

func TestMultiplexMultiNetworkNodeInfoValidate(t *testing.T) {
	// empty fails
	ni := mx.MultiNetworkNodeInfo{}
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
			func(ni *mx.MultiNetworkNodeInfo) { ni.Channels = append(channels, byte(maxNumChannels)) }, //nolint: makezero
			true,
		},
		{"Duplicate Channel", func(ni *mx.MultiNetworkNodeInfo) { ni.Channels = dupChannels }, true},
		{"Good Channels", func(ni *mx.MultiNetworkNodeInfo) { ni.Channels = ni.Channels[:5] }, false},

		{"Invalid NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.ListenAddr = "not-an-address" }, true},
		{"Good NetAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.ListenAddr = "0.0.0.0:26656" }, false},

		{"Non-ASCII Version", func(ni *mx.MultiNetworkNodeInfo) { ni.Version = nonASCII }, true},
		{"Empty tab Version", func(ni *mx.MultiNetworkNodeInfo) { ni.Version = emptyTab }, true},
		{"Empty space Version", func(ni *mx.MultiNetworkNodeInfo) { ni.Version = emptySpace }, true},
		{"Empty Version", func(ni *mx.MultiNetworkNodeInfo) { ni.Version = "" }, false},

		{"Non-ASCII Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.Moniker = nonASCII }, true},
		{"Empty tab Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.Moniker = emptyTab }, true},
		{"Empty space Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.Moniker = emptySpace }, true},
		{"Empty Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.Moniker = "" }, true},
		{"Good Moniker", func(ni *mx.MultiNetworkNodeInfo) { ni.Moniker = "hey its me" }, false},

		{"Non-ASCII TxIndex", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.TxIndex = nonASCII }, true},
		{"Empty tab TxIndex", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.TxIndex = emptyTab }, true},
		{"Empty space TxIndex", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.TxIndex = emptySpace }, true},
		{"Empty TxIndex", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.TxIndex = "" }, false},
		{"Off TxIndex", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.TxIndex = "off" }, false},

		{"Non-ASCII RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.RPCAddress = nonASCII }, true},
		{"Empty tab RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.RPCAddress = emptyTab }, true},
		{"Empty space RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.RPCAddress = emptySpace }, true},
		{"Empty RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.RPCAddress = "" }, false},
		{"Good RPCAddress", func(ni *mx.MultiNetworkNodeInfo) { ni.Other.RPCAddress = "0.0.0.0:26657" }, false},
	}

	nodeKey := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	// test case passes
	ni = testNodeInfo(nodeKey.ID(), name).(mx.MultiNetworkNodeInfo)
	ni.Channels = channels
	require.NoError(t, ni.Validate())

	for i, tc := range testCases {
		ni := testNodeInfo(nodeKey.ID(), name).(mx.MultiNetworkNodeInfo)
		ni.Channels = channels
		tc.malleateNodeInfo(&ni)
		err := ni.Validate()
		if tc.expectErr {
			require.Error(t, err, fmt.Sprintf(tc.testName+" should error at %d", i))
		} else {
			require.NoError(t, err, tc.testName)
		}
	}
}

func TestMultiplexMultiNetworkNodeInfoCompatible(t *testing.T) {
	nodeKey1 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	nodeKey2 := p2p.NodeKey{PrivKey: ed25519.GenPrivKey()}
	name := "testing"

	var newTestChannel byte = 0x2

	// test NodeInfo is compatible
	ni1 := testNodeInfo(nodeKey1.ID(), name).(mx.MultiNetworkNodeInfo)
	ni2 := testNodeInfo(nodeKey2.ID(), name).(mx.MultiNetworkNodeInfo)
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
		ni := testNodeInfo(nodeKey2.ID(), name).(mx.MultiNetworkNodeInfo)
		tc.malleateNodeInfo(&ni)
		require.Error(t, ni1.CompatibleWith(ni), fmt.Sprintf("should error at %d", i))
	}
}

func emptyNodeInfo() p2p.NodeInfo {
	return mx.MultiNetworkNodeInfo{}
}

func testNodeInfo(id p2p.ID, name string) p2p.NodeInfo {
	return testNodeInfoWithNetwork(id, name, "testing")
}

func testNodeInfoWithNetwork(id p2p.ID, name, network string) p2p.NodeInfo {
	p2pListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())
	rpcListenAddr := fmt.Sprintf("127.0.0.1:%d", getFreePort())

	return mx.MultiNetworkNodeInfo{
		Networks: []string{network},
		ProtocolVersions: []mx.ChainProtocolVersion{
			mx.NewChainProtocolVersion(network, mx.DefaultProtocolVersion),
		},
		DefaultNodeID: id,
		ListenAddr:    p2pListenAddr,
		Version:       "1.2.3-rc0-deadbeef",
		Channels:      []byte{testCh},
		Moniker:       name,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: rpcListenAddr,
		},
	}
}

func getFreePort() int {
	port, err := cmtnet.GetFreePort()
	if err != nil {
		panic(err)
	}
	return port
}
