package types

import (
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

	// Transport returns the packet transporter.
	Transport() cmtp2p.Transport
	// Dispatcher returns the injected packet dispatcher.
	Dispatcher() cmtp2p.Dispatcher
	// Connector returns the injected connection dialer.
	Connector() cmtp2p.Connector
	// Handshaker returns the injected connection handshaker.
	Handshaker() cmtp2p.Handshaker
}
