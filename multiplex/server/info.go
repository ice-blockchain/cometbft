package server

import (
	rpctypes "github.com/ice-blockchain/cometbft/rpc/jsonrpc/types"
)

// RelayInfo defines a server that is responsible of enabling
// RPC discovery for a multiplex backend.
//
// i.e. RPC discovery may be used to determine a relay's ID.
type RelayInfo struct {
	backend Backend
}

// NewRelayInfo creates a new discovery server instance.
func NewRelayInfo(b Backend) *RelayInfo {
	return &RelayInfo{
		backend: b,
	}
}

// GetRelayInfo may be used as a [rpctypes.RPCFunc] and returns a particular
// relay's information, including its' relay ID that must be used when creating
// TLS secret connections with handshakes, e.g. transport of P2P requests.
func (s *RelayInfo) GetRelayInfo(*rpctypes.Context) (*RPCResultRelayInfo, error) {
	result := &RPCResultRelayInfo{
		DefaultNodeID: s.backend.GetRelayID(),
	}

	return result, nil
}
