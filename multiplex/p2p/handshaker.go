package p2p

import (
	"context"
	"net"
	"sync"
	"time"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/protoio"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
)

// connectionHandshaker defines a runtime composer.
type connectionHandshaker struct {
	mtx *sync.Mutex

	nodeInfo *MultiNetworkNodeInfo

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ cmtp2p.Handshaker = (*connectionHandshaker)(nil)

type HandshakerOption func(*connectionHandshaker)

// NewHandshaker creates a new database service.
func NewHandshaker(
	ctx context.Context,
	nodeInfo *MultiNetworkNodeInfo,
	logger cmtlog.Logger,
	options ...HandshakerOption,
) cmtp2p.Handshaker {
	h := &connectionHandshaker{
		mtx:      new(sync.Mutex),
		nodeInfo: nodeInfo,

		// Options
		logger: logger,
	}

	// Use option helpers
	h.SetOptions(options...)
	return h
}

// HandshakerWithLogger injects a custom logger instance.
func HandshakerWithLogger(logger cmtlog.Logger) HandshakerOption {
	return func(h *connectionHandshaker) {
		h.logger = logger
	}
}

// ----------------------------------------------------------------------------
// Handshaker API implementation

// NodeInfo returns the local node information.
func (h *connectionHandshaker) NodeInfo() cmtp2p.NodeInfo {
	h.mtx.Lock()
	defer h.mtx.Unlock()

	return h.nodeInfo
}

// Handshake executes a handshake and returns a remote node information.
func (h *connectionHandshaker) Handshake(
	c net.Conn,
	timeout time.Duration,
) (cmtp2p.NodeInfo, error) {
	if err := c.SetDeadline(time.Now().Add(timeout)); err != nil {
		return nil, err
	}

	var (
		errc = make(chan error, 2)

		pbpeerNodeInfo mxp2p.MultiNetworkNodeInfo
		peerNodeInfo   *MultiNetworkNodeInfo
		ourNodeInfo    = h.NodeInfo().(*MultiNetworkNodeInfo)
	)

	go func(errc chan<- error, c net.Conn) {
		niProto, err := ourNodeInfo.ToProto()
		if err != nil {
			errc <- err
			return
		}
		_, err = protoio.NewDelimitedWriter(c).WriteMsg(niProto)
		errc <- err
	}(errc, c)
	go func(errc chan<- error, c net.Conn) {
		protoReader := protoio.NewDelimitedReader(c, maxNodeInfoSize)
		_, err := protoReader.ReadMsg(&pbpeerNodeInfo)
		errc <- err
	}(errc, c)

	for i := 0; i < cap(errc); i++ {
		err := <-errc
		if err != nil {
			return nil, err
		}
	}

	peerNodeInfo, err := MultiNetworkNodeInfoFromProto(&pbpeerNodeInfo)
	if err != nil {
		return nil, err
	}

	return peerNodeInfo, c.SetDeadline(time.Time{})
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (h *connectionHandshaker) SetOptions(options ...HandshakerOption) {
	for _, option := range options {
		option(h)
	}
}

// Logger returns the logger instance.
func (h *connectionHandshaker) Logger() cmtlog.Logger {
	return h.logger
}
