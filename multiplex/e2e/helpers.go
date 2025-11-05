package e2e

import (
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
)

// -----------------------------------------------------------------------------
// Test helpers

// MakeConfig creates a test configuration with custom rootDir.
func MakeConfig(tb testing.TB, rootDir string) *config.Config {
	tb.Helper()

	conf := config.TestConfig()
	conf.SetRoot(rootDir)

	return conf
}

// MakeRelayAddresses creates one or more
func MakeRelayAddresses(
	tb testing.TB,
	baseHost string,
	startPort uint16,
	num ...int,
) (out []*helpers.RelayAddress) {
	tb.Helper()

	if len(num) == 0 {
		num[0] = 1
	}

	out = make([]*helpers.RelayAddress, 0, num[0])
	for i := 0; i < num[0]; i++ {
		addr, err := helpers.NewRelayAddress("http://" + baseHost + ":" + strconv.Itoa(
			int(startPort)+i,
		))
		require.NoError(tb, err)

		out = append(out, addr)
	}
	return
}

// CAUTION: This helper uses an empty multiplex config on multiple relays.
func ResetTestMultiplexRelays(
	tb testing.TB,
	numRelays int,
	customLogger cmtlog.Logger,
	withAcceptors []client.Acceptor,
	withOptions ...mx.MultiplexBackendOption,
) []*mx.MultiplexBackend {
	tb.Helper()

	backends := make([]*mx.MultiplexBackend, numRelays)

	tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-1")
	require.NoError(tb, err)

	relayConf1 := MakeConfig(tb, tmpRootDir)
	relayConf1.DiscoveryPort = 30001
	relayConf1.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", 30002)
	relayConf1.P2P.ExternalAddress = fmt.Sprintf("tcp://127.0.0.1:%v", 30002)
	relayConf1.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%v", 30000)
	relayConf1.Instrumentation.Namespace += "_1"

	var acceptorRelay1 client.Acceptor
	if len(withAcceptors) > 0 {
		acceptorRelay1 = withAcceptors[0]
	} else {
		acceptorRelay1 = &client.DefaultAcceptor{}
	}

	serverRelay1, err := mx.NewServer(
		tb.Context(),
		acceptorRelay1,
		relayConf1,
		customLogger.With("process", "relay-1"),
		withOptions...,
	)
	require.NoError(tb, err, "should create first server instance")

	backends[0] = serverRelay1

	for r := 1; r < numRelays; r++ {
		tmpRootDir, err := os.MkdirTemp("", tb.Name()+"-"+strconv.Itoa(r+1))
		require.NoError(tb, err)

		discoveryPort := relayConf1.DiscoveryPort + uint16(r*100)

		relayConfX := MakeConfig(tb, tmpRootDir)
		relayConfX.DiscoveryPort = discoveryPort
		relayConfX.P2P.ListenAddress = fmt.Sprintf("tcp://0.0.0.0:%v", discoveryPort+1)
		relayConfX.P2P.ExternalAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort+1)
		relayConfX.RPC.ListenAddress = fmt.Sprintf("tcp://127.0.0.1:%v", discoveryPort-1)
		relayConfX.Instrumentation.Namespace += "_" + strconv.Itoa(r+1)

		var acceptorRelayX client.Acceptor
		if len(withAcceptors) > r {
			acceptorRelayX = withAcceptors[r] // 1 and up
		} else {
			acceptorRelayX = &client.DefaultAcceptor{}
		}

		serverRelayX, err := mx.NewServer(
			tb.Context(),
			acceptorRelayX,
			relayConfX,
			customLogger.With("process", "relay-"+strconv.Itoa(r+1)),
			withOptions...,
		)
		require.NoError(tb, err, "should create another server instance with cursor at "+strconv.Itoa(r))

		backends[r] = serverRelayX
	}

	return backends
}
