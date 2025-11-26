package rpc

import (
	rpctypes "github.com/ice-blockchain/cometbft/rpc/jsonrpc/types"
)

type Server interface {
	GetRelayInfo(ctx *rpctypes.Context) (*RPCResultRelayInfo, error)
	InitValidators(
		ctx *rpctypes.Context,
		networks []string,
	) (*RPCResultInitValidators, error)
}

type RPCServer struct {
	backend Backend
}

// Assert that our implementation satisfy the rpc.Server interface.
var _ Server = (*RPCServer)(nil)

// NewRPCServer creates a new discovery server instance.
func NewRPCServer(b Backend) *RPCServer {
	return &RPCServer{
		backend: b,
	}
}

// GetRelayInfo may be used as a [rpctypes.RPCFunc] and returns a particular
// relay's information, including its' relay ID that must be used when creating
// TLS secret connections with handshakes, e.g. transport of P2P requests.
func (s *RPCServer) GetRelayInfo(*rpctypes.Context) (*RPCResultRelayInfo, error) {
	result := &RPCResultRelayInfo{
		DefaultNodeID:    s.backend.GetRelayID(),
		Networks:         s.backend.GetNetworks(),
		ListenAddress:    s.backend.GetListenAddress(),
		DiscoveryPort:    s.backend.GetDiscoveryPort(),
		ValidatorPubs:    s.backend.GetValidatorPubs(),
		LastBlockHeights: s.backend.GetLastBlockHeights(),
	}

	return result, nil
}

// InitValidators should initialize validators for networks and
// should return a map of public keys per ChainID, or an error.
func (s *RPCServer) InitValidators(
	_ *rpctypes.Context,
	networks []string,
) (*RPCResultInitValidators, error) {
	// Start the backend initialization for networks,
	// i.e. should call reactor.AllocateNetwork().
	pubKeys, err := s.backend.GetLocalNetworkValidators(networks)
	if err != nil {
		return nil, err
	}

	result := &RPCResultInitValidators{
		Networks:      networks,
		ValidatorPubs: pubKeys,
	}

	return result, nil
}
