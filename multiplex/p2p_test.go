package multiplex

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/cosmos/gogoproto/proto"

	p2pproto "github.com/cometbft/cometbft/api/cometbft/p2p/v1"
	"github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cmtnet "github.com/cometbft/cometbft/internal/net"
	cmtrand "github.com/cometbft/cometbft/internal/rand"
	"github.com/cometbft/cometbft/libs/log"
	cmtsync "github.com/cometbft/cometbft/libs/sync"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/conn"
	"github.com/cometbft/cometbft/version"
)

func benchmarkSwitchBroadcast(b *testing.B, switches []*p2p.Switch, numPairs int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()
	chMsg := &p2pproto.PexAddrs{
		Addrs: []p2pproto.NetAddress{
			{
				ID: "1",
			},
		},
	}

	b.SetParallelism(numPairs * 2) // 1 proc per switch
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should transport a message from switch 1 to switch 2
		for pb.Next() {
			switchIdx := randomizer.Intn(numPairs * 2)
			sw := switches[switchIdx]

			// Transport a message
			chID := byte(switchIdx % 4)
			sw.Broadcast(p2p.Envelope{ChannelID: chID, Message: chMsg})
		}
	})
}

func benchmarkTransportValidate(b *testing.B, transports []*p2p.MultiplexTransport, keys []p2p.NodeKey, numNodes int) {
	b.Helper()

	// Creates a thread-safe rand instance
	randomizer := cmtrand.NewRand()

	b.SetParallelism(numNodes) // 1 proc per transport
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		// Every iteration should test the dialer of a multiplex transport instance
		// and accept an incoming connection.
		for pb.Next() {
			mtIdx := randomizer.Intn(numNodes)
			mt := transports[mtIdx]
			nk := keys[mtIdx]
			id := nk.ID()

			laddr := p2p.NewNetAddress(id, mt.GetListener().Addr())
			mt.Dial(*laddr, p2p.PublicPeerConfig{}.GetConfig())
		}
	})
}

// ----------------------------------------------------------------------------
// cometbft Switch implementation benchmarks

func BenchmarkP2PSwitch1K(b *testing.B) {
	numPairs := 1000

	// Reset benchmark fs
	rootDir, switchPtrs := ResetP2PSwitchTestRoot(b, "test-p2p-transport-1k", numPairs)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numPairs*2
	benchmarkSwitchBroadcast(b, switchPtrs, numPairs)
}

// ----------------------------------------------------------------------------
// cometbft MultiplexTransport implementation benchmarks

func BenchmarkP2PTransport1K(b *testing.B) {
	numNodes := 1000

	// Reset benchmark fs
	rootDir, mtPtrs, keyPtrs := ResetP2PTransportTestRoot(b, "test-p2p-transport-1k", numNodes)
	defer os.RemoveAll(rootDir)

	// Runs b.RunParallel() with GOMAXPROCS=numNodes
	benchmarkTransportValidate(b, mtPtrs, keyPtrs, numNodes)
}

// ----------------------------------------------------------------------------
// Mocks and Test Helpers

var cfg *config.P2PConfig

func init() {
	cfg = config.DefaultP2PConfig()
	cfg.PexReactor = true
	cfg.AllowDuplicateIP = true
}

type PeerMessage struct {
	Contents proto.Message
	Counter  int
}

type TestReactor struct {
	p2p.BaseReactor

	mtx          cmtsync.Mutex
	channels     []*conn.ChannelDescriptor
	logMessages  bool
	msgsCounter  int
	msgsReceived map[byte][]PeerMessage
}

var _ p2p.Reactor = (*TestReactor)(nil)

func NewTestReactor(channels []*conn.ChannelDescriptor, logMessages bool) *TestReactor {
	tr := &TestReactor{
		channels:     channels,
		logMessages:  logMessages,
		msgsReceived: make(map[byte][]PeerMessage),
	}
	tr.BaseReactor = *p2p.NewBaseReactor("TestReactor", tr)
	tr.SetLogger(log.TestingLogger())
	return tr
}

// GetChannels implements Reactor
func (tr *TestReactor) GetChannels() []*conn.ChannelDescriptor {
	return tr.channels
}

// AddPeer implements Reactor
func (*TestReactor) AddPeer(p2p.Peer) {}

// RemovePeer implements Reactor
func (*TestReactor) RemovePeer(p2p.Peer, any) {}

// Receive implements Reactor
func (tr *TestReactor) Receive(e p2p.Envelope) {
	if tr.logMessages {
		tr.mtx.Lock()
		defer tr.mtx.Unlock()
		//fmt.Printf("Received: %X, %X\n", e.ChannelID, e.Message)
		tr.msgsReceived[e.ChannelID] = append(tr.msgsReceived[e.ChannelID], PeerMessage{Contents: e.Message, Counter: tr.msgsCounter})
		tr.msgsCounter++
	}
}

// ----------------------------------------------------------------------------
// Exported helpers

func ResetP2PSwitchTestRoot(t testing.TB, testName string, numPairs int) (string, []*p2p.Switch) {
	t.Helper()

	// Creates many *prefixed* database instances using global db
	rootDir, _ := ResetPrefixDBTestRoot(t, testName, numPairs)

	// Prepares slice of P2P switches
	switchPtrs := make([]*p2p.Switch, numPairs*2)

	// Creates many connected pairs of *P2P* switches
	for i := 0; i < numPairs; i++ {
		sw1, sw2 := MakeSwitchPair(initSwitchFn)

		t.Cleanup(func() {
			if err := sw2.Stop(); err != nil {
				t.Error(err)
			}
			if err := sw1.Stop(); err != nil {
				t.Error(err)
			}
		})

		idx := i * 2
		switchPtrs[idx] = sw1
		switchPtrs[idx+1] = sw2
	}

	return rootDir, switchPtrs
}

func ResetP2PTransportTestRoot(t testing.TB, testName string, numNodes int) (string, []*p2p.MultiplexTransport, []p2p.NodeKey) {
	t.Helper()

	// Creates many *prefixed* database instances using global db
	rootDir, _ := ResetPrefixDBTestRoot(t, testName, numNodes)

	// Prepares slice of P2P transport multiplexes
	mtPtrs := make([]*p2p.MultiplexTransport, numNodes)
	keyPtrs := make([]p2p.NodeKey, numNodes)

	// Creates many multiplex transport instances
	for i := 0; i < numNodes; i++ {
		pv := ed25519.GenPrivKey()
		id := p2p.PubKeyToID(pv.PubKey())
		nk := p2p.NodeKey{
			PrivKey: pv,
		}

		mt := newMultiplexTransport(
			testNodeInfo(
				id, "transport",
			),
			nk,
		)

		// Unlimited incoming connections
		p2p.MultiplexTransportMaxIncomingConnections(0)(mt)

		addr, err := p2p.NewNetAddressString(p2p.IDAddressString(id, "127.0.0.1:0"))
		if err != nil {
			t.Fatal(err)
		}

		if err := mt.Listen(*addr); err != nil {
			t.Fatal(err)
		}

		// give the listener some time to get ready
		time.Sleep(20 * time.Millisecond)

		mtPtrs[i] = mt
		keyPtrs[i] = nk
	}

	return rootDir, mtPtrs, keyPtrs
}

// Convenience method for creating two switches connected to each other.
// XXX: note this uses net.Pipe and not a proper TCP conn.
func MakeSwitchPair(initSwitch func(int, *p2p.Switch) *p2p.Switch) (*p2p.Switch, *p2p.Switch) {
	// Create two switches that will be interconnected.
	switches := p2p.MakeConnectedSwitches(cfg, 2, initSwitch, p2p.Connect2Switches)
	return switches[0], switches[1]
}

// ----------------------------------------------------------------------------

func initSwitchFn(_ int, sw *p2p.Switch) *p2p.Switch {
	sw.SetAddrBook(&p2p.AddrBookMock{
		Addrs:    make(map[string]struct{}),
		OurAddrs: make(map[string]struct{}),
	})

	// Make two reactors of two channels each
	sw.AddReactor("foo", NewTestReactor([]*conn.ChannelDescriptor{
		{ID: byte(0x00), Priority: 10, MessageType: &p2pproto.Message{}},
		{ID: byte(0x01), Priority: 10, MessageType: &p2pproto.Message{}},
	}, true))
	sw.AddReactor("bar", NewTestReactor([]*conn.ChannelDescriptor{
		{ID: byte(0x02), Priority: 10, MessageType: &p2pproto.Message{}},
		{ID: byte(0x03), Priority: 10, MessageType: &p2pproto.Message{}},
	}, true))

	return sw
}

// newMultiplexTransport returns a tcp connected multiplexed peer
// using the default MConnConfig. It's a convenience function used
// for testing.
func newMultiplexTransport(
	nodeInfo p2p.NodeInfo,
	nodeKey p2p.NodeKey,
) *p2p.MultiplexTransport {
	return p2p.NewMultiplexTransport(
		nodeInfo, nodeKey, conn.DefaultMConnConfig(),
	)
}

func testNodeInfo(id p2p.ID, name string) p2p.NodeInfo {
	port := getFreePort()
	return p2p.DefaultNodeInfo{
		ProtocolVersion: p2p.NewProtocolVersion(
			version.P2PProtocol,
			version.BlockProtocol,
			0,
		),
		DefaultNodeID: id,
		ListenAddr:    fmt.Sprintf("127.0.0.1:%d", port),
		Network:       "testing",
		Version:       "1.2.3-rc0-deadbeef",
		Channels:      []byte{0x01},
		Moniker:       name,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    "on",
			RPCAddress: fmt.Sprintf("127.0.0.1:%d", port),
		},
	}
}

func testDialer(dialAddr p2p.NetAddress, errc chan error) {
	var (
		pv     = ed25519.GenPrivKey()
		dialer = newMultiplexTransport(
			testNodeInfo(p2p.PubKeyToID(pv.PubKey()), "host_peer"),
			p2p.NodeKey{
				PrivKey: pv,
			},
		)
	)

	_, err := dialer.Dial(dialAddr, p2p.PublicPeerConfig{}.GetConfig())
	if err != nil {
		errc <- err
		return
	}

	// Signal that the connection was established.
	errc <- nil
}

func getFreePort() int {
	port, err := cmtnet.GetFreePort()
	if err != nil {
		panic(err)
	}
	return port
}
