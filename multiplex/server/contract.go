package server

import (
	"context"
	"io"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// Server defines the contract for replication backend implementations.
//
// A server instance must be started before replication can happen and
// before broadcast operations can be forwarded to the [client.Client].
type Server interface {
	io.Closer

	// GetAcceptor returns the injected [client.Acceptor] implementation.
	GetAcceptor() client.Acceptor

	// MustStart executes a replication backend.
	MustStart()
}

// ----------------------------------------------------------------------------
// Backend defines a multiplex backend adapter
//
// Backend is an interface that defines the rules for the implementation of
// a backend adapter as required by [MultiplexClient]. The backend adapter is
// notably responsible for communicating with relays and transporting data.
//
// Note that a [Jobs] implementation is required to perform background tasks.
// This interface also embeds a [Server] interface.
type Backend interface {
	// Embeds MustStart() and Close()
	Server

	// GetRelayID should return a [p2p.ID] instance that identifies a relay.
	GetRelayID() p2p.ID

	// GetLogger should return a [cmtlog.Logger] instance.
	GetLogger() cmtlog.Logger

	// GetRoutines should return an implementation of [Jobs] methods.
	GetRoutines() *Jobs

	// GetNewChainReadyCh should return a read-only string channel.
	GetNewChainReadyCh() chan<- string

	// GetRelayAcceptTxCh should return a read-only string channel.
	GetRelayAcceptTxCh() chan<- string

	// WaitForNextAvailableNetwork should wait for a chain replication and
	// it should return a ChainID.
	WaitForNextAvailableNetwork(
		ctx context.Context,
	) (string, error)

	// WaitForRelayReplResponse shoulde wait for a relay replication response
	// and it should return a relay ID.
	WaitForRelayReplResponse(
		ctx context.Context,
	) (string, error)

	// WaitForRelayTxAcceptance should wait for a relay transaction acceptance
	// and it should return a transaction hash.
	WaitForRelayTxAcceptance(
		ctx context.Context,
	) (string, error)

	// GetLocalNetworkHeights should query the last block height and determine
	// a list of required networks. Iff the last block height is 1, the network
	// is considered unknown and may need to be explicitely created.
	GetLocalNetworkHeights(
		userAddress string,
		transactions ...client.Transaction,
	) (map[string]int64, []string)

	// FetchRelayAddresses should find the supported networks which it should
	// map (ChainID) to their respective listen addresses, and it returns a
	// slice of relays that produced errors, e.g. network error.
	FetchRelayAddresses(
		relays []string,
	) (map[string][]string, []string)

	// DiscoverRelayNetworks should dials all other relays and perform
	// handshakes to retrieve a [MultiNetworkNodeInfo] from each of the relays.
	DiscoverRelayNetworks(
		localSwitch *p2p.Switch,
		relay string,
	) ([]string, []string, error)

	// ApplyFilterReplRequestRelays should filter relays and return a map of
	// relays by ChainID with only relays that need to catchup, i.e. it should
	// return relays that will receive a chain replication request.
	ApplyFilterReplRequestRelays(
		requiredNetworks []string,
		relays []string,
		chainRelays map[string][]string,
	) map[string][]string

	// AddTransactions should execute the CheckTx call to add individual
	// transactions to the mempool by ChainID.
	AddTransactions(
		userAddress string,
		transactions ...client.Transaction,
	) error

	// RemoveTransactions should remove transactions from the local mempool
	// if they were added already, e.g. using AddTransactions.
	RemoveTransactions(
		userAddress string,
		transactions ...client.Transaction,
	) error
}
