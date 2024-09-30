package multiplex

import (
	"net"
	"time"

	mxp2p "github.com/cometbft/cometbft/api/cometbft/multiplex/v1"
	"github.com/cometbft/cometbft/libs/protoio"
	"github.com/cometbft/cometbft/p2p"
)

// MultiplexTransportHandshake implements [p2p.TransportHandshakeFn] to permit using
// node multiplexes to exchange [mxp2p.MultiNetworkNodeInfo] messages between peers
// and execute a handshake between nodeInfo and the returned [p2p.NodeInfo] instance.
//
// Mimics the same behaviour as the default implementation in [p2p.MultiplexTransport].
func MultiplexTransportHandshake(
	c net.Conn,
	timeout time.Duration,
	nodeInfo p2p.NodeInfo,
) (p2p.NodeInfo, error) {

	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	var (
		errc = make(chan error, 2)

		pbpeerNodeInfo mxp2p.MultiNetworkNodeInfo
		peerNodeInfo   MultiNetworkNodeInfo
		ourNodeInfo    = nodeInfo.(MultiNetworkNodeInfo)
	)

	go func(errc chan<- error, c net.Conn) {
		_, err := protoio.NewDelimitedWriter(c).WriteMsg(ourNodeInfo.ToProto())
		errc <- err
	}(errc, c)
	go func(errc chan<- error, c net.Conn) {
		protoReader := protoio.NewDelimitedReader(c, p2p.MaxNodeInfoSize())
		_, err := protoReader.ReadMsg(&pbpeerNodeInfo)
		errc <- err
	}(errc, c)

	for i := 0; i < cap(errc); i++ {
		err := <-errc
		if err != nil {
			return nil, err
		}
	}

	peerNodeInfo, err := MultiNetworkNodeInfoFromToProto(&pbpeerNodeInfo)
	if err != nil {
		return nil, err
	}

	return peerNodeInfo, c.SetDeadline(time.Time{})
}
