package server

import (
	"github.com/ice-blockchain/cometbft/p2p"
	rpctypes "github.com/ice-blockchain/cometbft/rpc/jsonrpc/types"
)

const (
	// ReplicationChannel is used to send chain replication messages.
	// This channel can only be used using `DiscoveryPort` - Discovery switch.
	ReplicationChannel = byte(0x90)

	// AckBroadcastChannel is used to send transaction broadcast receipts.
	// This channel can only be used using `DiscoveryPort+1` - CometBFT switch.
	AckBroadcastChannel = byte(0x91)

	// RuntimeChannel is used to send runtime state messages.
	// This channel can only be used using `DiscoveryPort+1` - CometBFT switch.
	RuntimeChannel = byte(0x92)
)

// RPCResultRelayInfo describes relays information.
type RPCResultRelayInfo struct {
	DefaultNodeID p2p.ID   `json:"id"` // authenticated identifier
	Networks      []string `json:"networks"`
	ListenAddress string   `json:"listen_address"`
	DiscoveryPort uint16   `json:"discovery_port"`
}

// RelayInfoServer defines a server that is responsible of enabling
// RPC discovery for a multiplex backend.
//
// i.e. RPC discovery may be used to determine a relay's ID.
type RelayInfoServer struct {
	backend Backend
}

// NewRelayInfoServer creates a new discovery server instance.
func NewRelayInfoServer(b Backend) *RelayInfoServer {
	return &RelayInfoServer{
		backend: b,
	}
}

// GetRelayInfo may be used as a [rpctypes.RPCFunc] and returns a particular
// relay's information, including its' relay ID that must be used when creating
// TLS secret connections with handshakes, e.g. transport of P2P requests.
func (s *RelayInfoServer) GetRelayInfo(*rpctypes.Context) (*RPCResultRelayInfo, error) {
	result := &RPCResultRelayInfo{
		DefaultNodeID: s.backend.GetRelayID(),
		Networks:      s.backend.GetNetworks(),
		ListenAddress: s.backend.GetListenAddress(),
		DiscoveryPort: s.backend.GetDiscoveryPort(),
	}

	return result, nil
}
