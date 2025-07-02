package rpc

import "github.com/ice-blockchain/cometbft/p2p"

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
