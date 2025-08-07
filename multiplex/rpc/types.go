package rpc

import (
	"context"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/p2p"
)

// RPCResultRelayInfo describes relays information.
type RPCResultRelayInfo struct {
	DefaultNodeID p2p.ID            `json:"id"` // authenticated identifier
	Networks      []string          `json:"networks"`
	ListenAddress string            `json:"listen_address"`
	DiscoveryPort uint16            `json:"discovery_port"`
	ValidatorPubs map[string]string `json:"validator_pubkeys"`
}

// RPCResultInitValidators describes the result of validators orchestration.
type RPCResultInitValidators struct {
	Networks      []string          `json:"networks"`
	ValidatorPubs map[string]string `json:"validator_pubkeys"`
}

// Backend defines a backend that is injected to [RPCServer].
type Backend interface {
	// GetRelayID should return a [p2p.ID] instance that identifies a relay.
	GetRelayID() p2p.ID

	// GetNetworks should return a slice of supported ChainID values.
	GetNetworks() []string

	// GetListenAddress should return the relay's listen address.
	GetListenAddress() string

	// GetDiscoveryPort should return the `DiscoveryPort` config value.
	GetDiscoveryPort() uint16

	// GetValidatorPubs should return the validator public keys by ChainID.
	// NOTE: For convenience, public keys should be hexadecimal format.
	GetValidatorPubs() map[string]string

	// GetLocalNetworkValidators should initialize validators for networks and
	// should return a map of public keys by ChainID.
	// NOTE: For convenience, public keys should be hexadecimal format.
	GetLocalNetworkValidators(networks []string) (map[string]string, error)
}

// Client defines a RPC client implementation.
type Client interface {
	// GetRemoteValidatorsInfo should request a RPCResultInitValidator object
	// which contains a map of validators public keys by ChainID.
	GetRemoteValidatorsInfo(
		ctx context.Context,
		relayAddress *helpers.RelayAddress,
		networks []string,
	) (*RPCResultInitValidators, error)

	// GetRemoteRelayInfo should request a relay information object which contains
	// a CometBFT Node ID, the supported networks and the node's listen address.
	GetRemoteRelayInfo(
		ctx context.Context,
		relayAddress *helpers.RelayAddress,
	) (*RPCResultRelayInfo, error)

	// GetRemoteDiscoveryAddress should request a relay information and determine
	// the discovery address using the fetched DiscoveryPort.
	GetRemoteDiscoveryAddress(
		ctx context.Context,
		relayAddress *helpers.RelayAddress,
	) (*helpers.RelayAddress, error)
}
