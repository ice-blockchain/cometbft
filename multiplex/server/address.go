package server

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/ice-blockchain/cometbft/p2p"
)

const (
	IndexDefaultPort    = 0
	IndexRelayInfoPort  = 1
	IndexMxP2PStartPort = 2
	IndexMxRPCStartPort = 3
)

// PortsConfig permits to change the number of networks ports between the P2P
// and RPC start ports. This is mostly used for testing purposes.
type PortsConfig struct {
	// Determines a number of ports to skip between P2P and RPC start ports.
	PortsBetweenServices uint16
}

// DefaultPortsConfig returns the default network ports configuration.
func DefaultPortsConfig() PortsConfig {
	return PortsConfig{
		PortsBetweenServices: 10000,
	}
}

// RelayAddress wraps a relay URL into a fully qualified relay address
// which may contain a scheme, relay ID, hostname and port.
type RelayAddress struct {
	// Contains an optional relay ID (CometBFT Node ID).
	id p2p.ID

	// Contains the hostname without scheme and port.
	host string

	// Contains the relay's default port. This port is used
	// to determine other ports of one relay.
	port uint16

	// Contains configuration for network ports between services.
	conf PortsConfig
}

// NewRelayAddress parses a relay URL and creates a [RelayAddress] instance.
func NewRelayAddress(
	relay string,
	options ...func(*RelayAddress),
) (*RelayAddress, error) {
	re := regexp.MustCompile(`(tcp\:\/\/)?((.*)@)?(.*)(\:(\d+))(.*)`)
	matches := re.FindStringSubmatch(relay)

	relayHost := matches[4]
	relayPort, err := strconv.ParseInt(matches[6], 10, 32) // int32
	if err != nil {
		return nil, err
	}

	addr := &RelayAddress{
		host: relayHost,
		port: uint16(relayPort),
		conf: DefaultPortsConfig(),
	}

	hasRelayID := len(matches[2]) > 0
	if hasRelayID {
		addr.id = p2p.ID(matches[3])
	}

	for _, option := range options {
		option(addr)
	}

	return addr, nil
}

// WithPortsConfig is an option helper to customize the ports configuration.
func WithPortsConfig(cfg PortsConfig) func(*RelayAddress) {
	return func(a *RelayAddress) {
		a.conf = cfg
	}
}

// SetID is a setter implementation for a RelayAddress ID.
func (a *RelayAddress) SetID(id p2p.ID) {
	a.id = id
}

// HasID returns true if the relay ID is not empty.
func (a *RelayAddress) HasID() bool {
	return len(a.id) > 0
}

// ID returns the optional relay ID.
func (a *RelayAddress) ID() p2p.ID {
	return a.id
}

// Static multi-port convention for multiplex backends.
//
// Ports returns an ordered slice of network ports. We use a convention
// by which all ports are set statically depending on the default port.
func (a *RelayAddress) Ports() []uint16 {
	offsetRPC := a.conf.PortsBetweenServices
	return []uint16{
		a.port,                 // P2P Discovery Port ("default port")
		a.port - 1,             // RPC Discovery Port
		a.port + 1,             // P2P Start Port (multiplex)
		a.port + offsetRPC + 1, // RPC Start Port (multiplex)
	}
}

// NetAddress returns a [p2p.NetAddress] instance or an error.
// Note that the NetAddress always uses the default port.
func (a *RelayAddress) NetAddress() (*p2p.NetAddress, error) {
	addr, err := p2p.NewNetAddressString(a.String())
	if err != nil {
		return nil, fmt.Errorf(
			"error with relay address %s: %w", a.String(), err)
	}

	return addr, nil
}

// String returns the fully qualified relay address including
// a scheme (tcp://) and its default relay port.
func (a *RelayAddress) String() string {
	return a.StringWithPortIndex(IndexDefaultPort)
}

// StringWithoutId returns the relay address without the ID
// but including a scheme and its default relay port.
func (a *RelayAddress) StringWithoutId() string {
	bkid := a.id
	a.id = p2p.ID("")
	defer func() {
		a.id = bkid
	}()

	return a.String()
}

// StringWithPortIndex returns the fully qualified relay address
// including a scheme (tcp://) and uses the relay port by index.
// See also: [RelayAddress#GetRelayPorts]
func (a *RelayAddress) StringWithPortIndex(index int) string {
	// fully qualified relay address
	fqra := "tcp://"

	if len(a.id) > 0 {
		fqra = fqra + string(a.id) + "@"
	}

	port := a.port
	if index != IndexDefaultPort {
		port = a.Ports()[index]
	}

	fqra = fqra + a.host + ":" + strconv.Itoa(int(port))
	return fqra
}

// AddressForDiscovery returns the relay address associated with
// the P2P discovery process which runs on the *default port*.
func (a *RelayAddress) AddressForDiscovery() string {
	return a.StringWithPortIndex(IndexDefaultPort)
}

// AddressForRelayInfo returns the relay address associated with
// the RPC RelayInfo procedure which runs on the *relay info port*.
func (a *RelayAddress) AddressForRelayInfo() string {
	return a.StringWithPortIndex(IndexRelayInfoPort)
}
