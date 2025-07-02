package server

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
