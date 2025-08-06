package p2p

// PeerErrorFn is used when a peer connection errors.
type PeerErrorFn func(*PeerImpl, any)

// IsPersistentFn is used to inject a custom persistence verification callback.
type IsPersistentFn func(*NetAddress) bool

// PeerConfig is used to bundle data we need to fully setup a Peer with an
// MConn, provided by the caller of Accept and Dial (currently the Switch). This
// a temporary measure until reactor setup is less dynamic and we introduce the
// concept of PeerBehaviour to communicate about significant Peer lifecycle
// events.
type PeerConfig struct {
	dispatcher Dispatcher
	outbound   bool
	metrics    *Metrics

	// isPersistent allows you to set a function, which, given socket address
	// (for outbound peers) OR self-reported address (for inbound peers), tells
	// if the peer is persistent or not.
	isPersistent IsPersistentFn
	onPeerError  PeerErrorFn
}

// PeerConfigOption defines the interface for option helpers.
type PeerConfigOption func(*PeerConfig)

// NewPeerConfig creates a new peer configuration around a Dispatcher.
func NewPeerConfig(
	d Dispatcher,
	peerErrorFn PeerErrorFn,
	options ...PeerConfigOption,
) PeerConfig {
	c := &PeerConfig{
		dispatcher:  d,
		onPeerError: peerErrorFn,
		// multiplex disables persistent peers.
		isPersistent: func(na *NetAddress) bool { return false },
	}

	for _, option := range options {
		option(c)
	}

	return *c
}

// PeerConfigOutbound sets the outbound property of c.
func PeerConfigOutbound(o bool) PeerConfigOption {
	return func(c *PeerConfig) {
		c.outbound = o
	}
}
