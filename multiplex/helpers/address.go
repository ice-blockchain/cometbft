package helpers

import (
	"fmt"
	"math"
	"regexp"
	"strconv"

	"github.com/ice-blockchain/cometbft/p2p"
)

// RelayAddress wraps a relay URL into a fully qualified relay address
// which may contain a scheme, relay ID, hostname and port.
type RelayAddress struct {
	// Contains an optional relay ID (CometBFT Node ID).
	id p2p.ID

	// Contains the scheme, e.g. "tcp://", "http://"
	scheme string

	// Contains the hostname without scheme and port.
	host string

	// Contains the relay's port. This port is used to determine
	// other ports of one relay.
	port uint16
}

// NewRelayAddress parses a relay URL and creates a [RelayAddress] instance.
// A hostname is required, other sections are optional. With an empty value
// for the scheme, the default scheme will be returned `tcp://`.
func NewRelayAddress(
	relay string,
	options ...func(*RelayAddress),
) (*RelayAddress, error) {
	re := regexp.MustCompile(`(.*\:\/\/)?((.*)@)?(.*)(\:(\d+))(.*)`)
	matches := re.FindStringSubmatch(relay)
	if len(matches) == 0 || len(matches[0]) == 0 {
		return nil, fmt.Errorf(
			"invalid format for relay address '%s'", relay)
	}

	scheme := matches[1]
	if len(scheme) == 0 {
		scheme = "tcp://"
	}

	relayHost := matches[4]
	if len(relayHost) == 0 {
		return nil, fmt.Errorf(
			"found empty hostname in relay address '%s'", relay)
	}

	relayPort, err := strconv.ParseInt(matches[6], 10, 32) // int32
	if err != nil {
		return nil, fmt.Errorf(
			"invalid port number in relay address '%s': %w", relay, err)
	}
	if relayPort < 0 || relayPort > math.MaxUint16 {
		return nil, fmt.Errorf(
			"invalid port number in relay address '%s'", relay)
	}

	addr := &RelayAddress{
		host:   relayHost,
		port:   uint16(relayPort),
		scheme: scheme,
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

// Port returns the port of a relay address.
func (a *RelayAddress) Port() uint16 {
	return a.port
}

// SetPort sets a custom port of a relay address.
func (a *RelayAddress) SetPort(p uint16) {
	a.port = p
}

// Scheme returns the scheme of a relay address.
func (a *RelayAddress) Scheme() string {
	return a.scheme
}

// Host returns the host of a relay address.
func (a *RelayAddress) Host() string {
	return a.host
}

// NetAddress returns a [p2p.NetAddress] instance or an error.
func (a *RelayAddress) NetAddress() *p2p.NetAddress {
	addr, err := p2p.NewNetAddressString(a.String())
	if err != nil {
		panic(fmt.Errorf(
			"error with relay address %s: %w", a.String(), err).Error())
	}

	return addr
}

// NetAddressForCometBFT returns a [p2p.NetAddress] instance or an error.
// Note that this method uses [RelayAddress#AddressForCometBFT].
func (a *RelayAddress) NetAddressForCometBFT() *p2p.NetAddress {
	addrForCometBFT := a.AddressForCometBFT()
	addr, err := p2p.NewNetAddressString(addrForCometBFT)
	if err != nil {
		panic(fmt.Errorf(
			"error with cometbft address %s: %w", a.String(), err).Error())
	}

	return addr
}

// String returns the fully qualified relay address including
// a scheme (tcp://) and a port if specified.
func (a *RelayAddress) String() string {
	// fully qualified relay address
	fqra := a.scheme

	if len(a.id) > 0 {
		fqra = fqra + string(a.id) + "@"
	}

	fqra = fqra + a.host + ":" + strconv.Itoa(int(a.port))
	return fqra
}

// StringWithoutScheme returns the relay address with the scheme.
func (a *RelayAddress) StringWithoutScheme() string {
	bksc := a.scheme
	a.scheme = ""
	defer func() {
		a.scheme = bksc
	}()

	return a.String()
}

// StringWithoutId returns the relay address without the ID.
func (a *RelayAddress) StringWithoutId() string {
	bkid := a.id
	a.id = p2p.ID("")
	defer func() {
		a.id = bkid
	}()

	return a.String()
}

// StringWithoutId returns the relay address without the ID.
func (a *RelayAddress) StringHostname() string {
	bkid := a.id
	bksc := a.scheme
	a.id = p2p.ID("")
	a.scheme = ""
	defer func() {
		a.id = bkid
		a.scheme = bksc
	}()

	return a.String()
}

// Addresses returns an ordered slice of relay addresses.
// Following ports are used:
// - `DiscoveryPort`: P2P Discovery Server
// - `DiscoveryPort - 1`: RPC RelayInfo Procedure
// - `DiscoveryPort + 1`: P2P CometBFT Server
// - `DiscoveryPort + 2`: RPC CometBFT Server
//
// The relay addresses are returned in that same order.
func (a *RelayAddress) Addresses() []string {
	return []string{
		a.AddressForDiscovery(),
		a.AddressForRelayInfo(),
		a.AddressForCometBFT(),
		a.AddressForLightRPC(),
	}
}

// AddressForDiscovery returns the relay address associated with
// the P2P discovery process which runs on the *discovery port*.
//
// The discovery port can be set using [MultiplexConfig#DiscoveryPort].
func (a *RelayAddress) AddressForDiscovery() string {
	return a.String() // Discovery Port, i.e. :30001
}

// AddressForRelayInfo returns the relay address associated with
// the RPC RelayInfo procedure which runs on the *relay info port*.
//
// The relay info port is always `discoveryPort - 1`.
func (a *RelayAddress) AddressForRelayInfo() string {
	str := a.String()
	cpa, _ := NewRelayAddress(str)
	cpa.port = cpa.port - 1 // Discovery Port - 1, i.e. :30000
	return cpa.String()
}

// AddressForCometBFT returns the relay address associated with
// the CometBFT P2P server which runs on the *CometBFT P2P port*.
//
// The CometBFT P2P port is always `discoveryPort + 1`.
func (a *RelayAddress) AddressForCometBFT() string {
	str := a.String()
	cpa, _ := NewRelayAddress(str)
	cpa.port = cpa.port + 1 // Discovery Port + 1, i.e. :30002
	return cpa.String()
}

// AddressForLightRPC returns the relay address associated with
// the CometBFT RPC server which runs on the *CometBFT RPC port*.
//
// The CometBFT RPC port is always `discoveryPort + 2`.
func (a *RelayAddress) AddressForLightRPC() string {
	str := a.String()
	cpa, _ := NewRelayAddress(str)
	cpa.port = cpa.port + 2 // Discovery Port + 2, i.e. :30003
	return cpa.String()
}

// AddressForMonitoring returns the relay address associated with
// the Prometheus HTTP exported which runs on the *monitoring port*.
//
// The Prometheus HTTP port is always `discoveryPort + 3`.
func (a *RelayAddress) AddressForMonitoring() string {
	str := a.String()
	cpa, _ := NewRelayAddress(str)
	cpa.port = cpa.port + 3 // Discovery Port + 3, i.e. :30004
	return cpa.String()
}
