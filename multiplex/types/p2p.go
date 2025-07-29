package types

import (
	"net"
	"time"

	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
)

// ConnectionManager defines the contract for a connection manager.
type ConnectionManager interface {
	service.Service

	// NodeKey returns the local relay node public key (ed25519), or node ID.
	NodeKey() *cmtp2p.NodeKey
	// NodeInfo returns the local relay node information.
	NodeInfo() cmtp2p.NodeInfo

	// Dispatcher returns the injected packet dispatcher.
	Dispatcher() cmtp2p.Dispatcher
	// Connector returns the injected connection dialer.
	Connector() cmtp2p.Connector
	// Handshaker returns the injected connection handshaker.
	Handshaker() Handshaker
}

// Handshaker defines the contract for a connection handshaker.
type Handshaker interface {
	// NodeInfo returns the local node information.
	NodeInfo() cmtp2p.NodeInfo

	// Handshake executes a handshake and returns a remote node information.
	Handshake(c net.Conn, timeout time.Duration) (cmtp2p.NodeInfo, error)
}
