package server

import (
	"context"
	"sync"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// ----------------------------------------------------------------------------
// Jobs defines a multiplex background jobs implementation
//
// Jobs provides routines implementation for the broadcast process.
// This structure encapsulates routines implementation for further extension
// and the adapter instance injects the default implementation if necessary.
type Jobs struct {
	// Routine extensions/overwrites may be provided here.
	DiscoveryDialer DiscoveryDialerFn
	NodeReplRequest NodeReplRequestFn
	NetworksCreator NetworksCreatorFn
	RelaysBroadcast RelaysBroadcastFn
	CancelBroadcast CancelBroadcastFn
}

// RelayDialError contains an error attached to a relay address.
type RelayDialError struct {
	Addr  *RelayAddress
	Error error
}

// NodeReplRequestFn describes a function that may be run on a separate
// goroutine and which should send [ChainReplicationRequest] to relays.
//
// A write-only [client.BroadcastStatus] channel is used to transmit errors.
type NodeReplRequestFn func(
	context.Context,
	[]*RelayAddress,
	string,
	chan<- client.BroadcastStatus,
	cmtlog.Logger,
)

// DiscoveryDialerFn describes a function that may be run on a separate
// goroutine and which should dial relays to enable [ReplicationChannel].
//
// A write-only [RelayAddress] channel instance is accepted as errorsCh.
type DiscoveryDialerFn func(
	context.Context,
	[]*RelayAddress,
	*sync.WaitGroup,
	chan<- RelayDialError, // errorsCh
	cmtlog.Logger,
)

// NetworksCreatorFn describes a function that may be run on a separate
// goroutine and which should communicate with relays about missing networks.
type NetworksCreatorFn func(
	context.Context,
	map[string][]*RelayAddress,
	[]string,
	*sync.WaitGroup,
	cmtlog.Logger,
) error

// RelaysBroadcastFn describes a function that may be run on a separate
// goroutine and which should broadcast all transactions to relays.
//
// A write-only [client.BroadcastStatus] channel is used to transmit errors.
type RelaysBroadcastFn func(
	context.Context,
	map[string][]*RelayAddress, // relaysByChain
	map[string][]*RelayAddress, // replReqRelays
	string,
	[]client.Transaction,
	chan<- client.BroadcastStatus,
	cmtlog.Logger,
)

// CancelBroadcastFn describes a function that may be run on a separate
// goroutine and which should broadcast a rollback message to healthy relays.
//
// The method ignores error from the mempool as transaction are not found.
type CancelBroadcastFn func(
	context.Context,
	string,
	[]client.Transaction,
	cmtlog.Logger,
)
