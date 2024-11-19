package multiplex_test

import (
	"net"
	"reflect"
	"testing"
	"time"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/libs/protoio"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/p2p"
)

const (
	defaultNodeName = "host_peer"
	testCh          = 0x01
)

func TestMultiplexTransportHandshake(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	var (
		peerPV       = ed25519.GenPrivKey()
		peerNodeInfo = testNodeInfo(p2p.PubKeyToID(peerPV.PubKey()), defaultNodeName)
	)

	go func() {
		c, err := net.Dial(ln.Addr().Network(), ln.Addr().String())
		if err != nil {
			t.Error(err)
			return
		}

		go func(c net.Conn) {
			_, err := protoio.NewDelimitedWriter(c).WriteMsg(peerNodeInfo.(mx.MultiNetworkNodeInfo).ToProto())
			if err != nil {
				t.Error(err)
			}
		}(c)
		go func(c net.Conn) {
			var pbni mxp2p.MultiNetworkNodeInfo

			protoReader := protoio.NewDelimitedReader(c, p2p.MaxNodeInfoSize())
			_, err := protoReader.ReadMsg(&pbni)
			if err != nil {
				t.Error(err)
			}

			_, err = mx.MultiNetworkNodeInfoFromProto(&pbni)
			if err != nil {
				t.Error(err)
			}
		}(c)
	}()

	c, err := ln.Accept()
	if err != nil {
		t.Fatal(err)
	}

	ni, err := mx.MultiplexTransportHandshake(c, 20*time.Millisecond, emptyNodeInfo())
	if err != nil {
		t.Fatal(err)
	}

	if have, want := ni, peerNodeInfo; !reflect.DeepEqual(have, want) {
		t.Errorf("have %v, want %v", have, want)
	}
}
