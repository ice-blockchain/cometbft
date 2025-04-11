package server

import (
	"context"

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
	NodeReplRequest NodeReplRequestFn
	NetworksCreator NetworksCreatorFn
	RelaysBroadcast RelaysBroadcastFn
	CancelBroadcast CancelBroadcastFn
}

// NodeReplRequestFn describes a function that may be run on a separate
// goroutine and which should open connections to relays if necessary.
//
// A write-only [client.BroadcastStatus] channel is used to transmit errors.
type NodeReplRequestFn func(
	context.Context,
	[]*RelayAddress,
	string,
	chan<- client.BroadcastStatus,
)

// NetworksCreatorFn describes a function that may be run on a separate
// goroutine and which should communicate with relays about missing networks.
//
// A write-only [client.BroadcastStatus] channel is used to transmit errors.
// Also a string channel instance is accepted as newChainReadyCh where ChainIDs
// are pushed when a new network is ready (or is now known through relay).
type NetworksCreatorFn func(
	context.Context,
	map[string][]*RelayAddress,
	[]string,
	chan<- client.BroadcastStatus,
	chan<- string, // newChainReadyCh
)

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
)

// CancelBroadcastFn describes a function that may be run on a separate
// goroutine and which should broadcast a rollback message to healthy relays.
//
// The method ignores error from the mempool as transaction are not found.
type CancelBroadcastFn func(
	context.Context,
	string,
	[]client.Transaction,
)
