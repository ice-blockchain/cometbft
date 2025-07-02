package rpc

import "github.com/ice-blockchain/cometbft/p2p"

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

	// InitValidators should initialize validators for networks and
	// should return a map of public keys by ChainID.
	// NOTE: For convenience, public keys should be hexadecimal format.
	InitValidators(networks []string) (map[string]string, error)
}
