package runtime_test

import (
	"net"
	"reflect"
	"testing"
	"unsafe"

	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeRuntimeComposerStartStop(t *testing.T) {

}

func TestMultiplexRuntimeRuntimeComposerReset(t *testing.T) {

}

func TestMultiplexRuntimeRuntimeComposerCompose(t *testing.T) {

}

func TestMultiplexRuntimeRuntimeComposerInject(t *testing.T) {

}

func TestMultiplexRuntimeRuntimeComposerBuild(t *testing.T) {

}

func TestMultiplexRuntimeRuntimeComposerUnload(t *testing.T) {

}

// ----------------------------------------------------------------------------

func composerConnectionPool(tb testing.TB, cpr types.RuntimeComposer) *p2p.ConnectionPool {
	tb.Helper()

	field := reflect.ValueOf(cpr).Elem().FieldByName("connectionPool")
	if !field.IsValid() {
		tb.Fatal("connectionPool field not found")
	}
	if field.IsNil() {
		return nil
	}

	value := reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Interface()
	return value.(*p2p.ConnectionPool)
}

type mockConnectionPool struct {
	service.BaseService

	nodeInfo *p2p.MultiNetworkNodeInfo
	nodeKey  *cmtp2p.NodeKey
}

var _ types.ConnectionManager = (*mockConnectionPool)(nil)
var _ cmtp2p.Pool = (*mockConnectionPool)(nil)

func (m *mockConnectionPool) NodeKey() *cmtp2p.NodeKey  { return m.nodeKey }
func (m *mockConnectionPool) NodeInfo() cmtp2p.NodeInfo { return m.nodeInfo }
func (*mockConnectionPool) Transport() *cmtp2p.MultiplexTransport {
	return &cmtp2p.MultiplexTransport{}
}
func (*mockConnectionPool) Dispatcher() cmtp2p.Dispatcher { return &cmtp2p.MockDispatcherImpl{} }
func (*mockConnectionPool) Connector() cmtp2p.Connector   { return nil }
func (*mockConnectionPool) Handshaker() cmtp2p.Handshaker { return nil }

func (*mockConnectionPool) NumPeers(chainIds ...string) (inbound, outbound, dialing int) {
	return 0, 0, 0
}
func (*mockConnectionPool) Peers(chainIds ...string) *cmtp2p.PeerSet { return nil }
func (*mockConnectionPool) AddPeer(peer *cmtp2p.PeerImpl) error      { return nil }
func (*mockConnectionPool) RemovePeer(peerID cmtp2p.ID) error        { return nil }
func (*mockConnectionPool) HasPeer(peer *cmtp2p.PeerImpl) bool       { return false }
func (*mockConnectionPool) HasPeerID(id cmtp2p.ID) bool              { return false }
func (*mockConnectionPool) HasPeerIP(ip net.IP) bool                 { return false }

func (*mockConnectionPool) Broadcast(e cmtp2p.Envelope) error    { return nil }
func (*mockConnectionPool) TryBroadcast(e cmtp2p.Envelope) error { return nil }

func (*mockConnectionPool) HasConnection(peerID cmtp2p.ID) bool                     { return false }
func (*mockConnectionPool) Connection(peerID cmtp2p.ID) *conn.MConnection           { return nil }
func (*mockConnectionPool) HasPeerForChainID(peerID cmtp2p.ID, chainID string) bool { return false }
func (*mockConnectionPool) SetPeerForChainID(peerID cmtp2p.ID, chainID string) int  { return 0 }
func (*mockConnectionPool) InitPeerForChainID(peerID cmtp2p.ID, chainID string) *cmtp2p.PeerImpl {
	return nil
}
func (*mockConnectionPool) AddPeerForChainID(peerID cmtp2p.ID, chainID string) bool { return false }
