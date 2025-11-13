package types

import (
	"context"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/rpc"
)

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

// ----------------------------------------------------------------------------
// Server defines the contract for replication server orchestration.
//
// A server instance must be started before replication can happen and
// before broadcast operations can be forwarded to a [client.Client].
//
// A server instance manages an instance of [IdleManager],
// and one of [client.Acceptor], which are used by runtime processes.
type Server interface {
	service.Service // Start, Stop, Reset.
	GetLogger() cmtlog.Logger
	Context() context.Context

	// Discovery returns the switch listening on `DiscoveryPort`.
	Discovery() *p2p.Switch
	// CometBFT returns the switch listening on `DiscoveryPort+2`.
	CometBFT() *p2p.Switch

	// RuntimeManager returns a node runtime manager, i.e. StartRuntime.
	RuntimeManager() RuntimeManager
	// IdleManager returns a node idle manager, i.e. OnActivate, OnIdle.
	IdleManager() IdleManager
	// Acceptor should return a [client.Acceptor] instance.
	Acceptor() client.Acceptor
	// ChainConns returns the ABCI client as defined with [proxy.ChainConns].
	ChainConns() proxy.ChainConns
}

// ----------------------------------------------------------------------------
// Backend defines a replication backend contract.
//
// Backend is an interface that defines the rules for the implementation of
// a backend adapter as required by [MultiplexClient]. The backend adapter is
// notably responsible for communicating with relays and transporting data.
//
// Note that a [Jobs] implementation is required to perform background tasks.
//
// See also:
// - [Server]
// - [rpc.Backend]
// - [rpc.Client]
// - [RelayComposer]
// - [RelayHelpers]
// - [BroadcastHelpers]
type Backend interface {
	Server
	rpc.Backend
	rpc.Client
	RelayComposer
	RelayHelpers
	BroadcastHelpers

	// Routines should return an implementation of [Jobs] methods.
	Routines() *Jobs

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

	// CancelBroadcastOperation should execute the CancelBroadcast routine
	// and it should remove transactions from the local mempool.
	CancelBroadcastOperation(
		ctx context.Context,
		userAddress string,
		transactions ...client.Transaction,
	) error

	// OnBroadcastComplete should update a runtime completion status.
	// It accepts a batch of transactions and a list of remoteRelays
	// that it should wait for until they have completed replication.
	//
	// This method should call [server.Registry#OnComplete].
	OnBroadcastComplete(
		ctx context.Context,
		userAddress string,
		remoteRelays []*helpers.RelayAddress,
		transactions ...client.Transaction,
	) error

	// OnBroadcastError should update a runtime completion status
	// and attach and log an error about the broadcast completion.
	//
	// This method should call [server.Registry#OnComplete].
	OnBroadcastError(
		reason error,
		userAddress string,
		transactions ...client.Transaction,
	) error
}

// ----------------------------------------------------------------------------
// RelayComposer defines helper methods to initialize and compose relays.
//
// This helper instance may be used to initialize global servers running
// for every relay, such as the Discovery or CometBFT event switches.
type RelayComposer interface {
	// Init initializes shared global resources for the backend.
	Init() error

	// InitSnapsAppClient initializes the local SnapsApp application.
	InitSnapsAppClient() error
	// InitRuntimeManager initializes the internal [RuntimeManager].
	InitRuntimeManager() error
	// InitDiscoverySwitch initializes the Discovery event switch.
	InitDiscoverySwitch() error
	// InitCometBFTSwitch initializes the CometBFT event switch.
	InitCometBFTSwitch() error
	// InitLightRPCRoutes initializes the CometBFT RPC routes.
	InitLightRPCRoutes() error

	// StartSharedServices starts the global services shared amongst networks.
	StartSharedServices() error
	// StartP2PServerDiscovery starts a P2P server, listening on `DiscoveryPort`.
	StartP2PServerDiscovery() error
	// StartRPCServerDiscovery starts a RPC server, listening on `DiscoveryPort-1`.
	StartRPCServerDiscovery() error
	// StartP2PServerCometBFT starts a P2P server, listening on `DiscoveryPort+1`.
	StartP2PServerCometBFT() error
	// StartRPCServerCometBFT starts a RPC server, listening on `DiscoveryPort+2`.
	StartRPCServerCometBFT() error

	// StartPrometheusServer starts a HTTP server, listening on `DiscoveryPort+3`.
	StartPrometheusServer() error

	// StopSharedServices stops the global services shared amongst networks.
	StopSharedServices() error
	// ResetSharedServices starts the global services shared amongst networks.
	ResetSharedServices(ctx context.Context) error
}

// ----------------------------------------------------------------------------
// RelayHelpers defines helper methods to read from relays.
//
// A helpers instance may be used to read information from relays such as
// their validators public keys, their server information, or the local
// blockchain heights stored on the relay.
type RelayHelpers interface {
	// SaveRelayInfo stores the RPC call response.
	SaveRelayInfo(relayInfo *rpc.RPCResultRelayInfo)
	// GetRelayInfo returns a a RPC call response or nil.
	GetRelayInfo(relayID p2p.ID) *rpc.RPCResultRelayInfo

	// GetLocalNetworkHeights should query the available networks and determine
	// a list of required networks. Iff the network cannot be found, it will be
	// considered unknown and may need to be explicitely created.
	//
	// Return order: requiredNetworks, mustCreateNetworks.
	GetLocalNetworkHeights(
		userAddress string,
		transactions ...client.Transaction,
	) ([]string, []string)

	// GetValidatorsByNetwork should find the supported networks, then map each
	// of the ChainID to a slice of validator public keys.
	GetValidatorsByNetwork(
		ctx context.Context,
		relays []*helpers.RelayAddress,
		networks []string,
	) (map[string][]string, error)

	// GetRelaysByNetwork should find the supported networks, then map each
	// to a slice of relay addresses, and it also returns a slice of relays
	// that produced errors, e.g. network error.
	GetRelaysByNetwork(
		ctx context.Context,
		relays []*helpers.RelayAddress,
	) ([]*helpers.RelayAddress, map[string][]*helpers.RelayAddress, []string)

	// CheckDialCompatibleRelay should dial a relay, executing a P2P handshake
	// and thereby defining whether a relay is compatible for dialing.
	CheckDialCompatibleRelay(
		ctx context.Context,
		relayAddress *helpers.RelayAddress,
	) error
}

// ----------------------------------------------------------------------------
// BroadcastHelpers defines helper methods to communicate with relays.
//
// A helpers instance may be used to filter relays for specific operations,
// or to wait for certain events, e.g. transaction events or ACK messages.
type BroadcastHelpers interface {
	// ApplyFilterReplRequestRelays should filter relays and return a map of
	// relays by ChainID with only relays that need to catchup, i.e. it should
	// return relays that will receive a chain replication request.
	ApplyFilterReplRequestRelays(
		requiredNetworks []string,
		relays []*helpers.RelayAddress,
		chainRelays map[string][]*helpers.RelayAddress,
	) map[string][]*helpers.RelayAddress

	// ApplyFilterAckTransactionRelayIds should filter relay IDs and return a
	// slice of relays IDs with only relays that must be waited for during the
	// AckTransactionBroadcast process.
	ApplyFilterAckTransactionRelayIds(
		chainRelays map[string][]*helpers.RelayAddress,
		catchupRelays map[string][]*helpers.RelayAddress,
	) []string

	// WaitForRelaysAckChainReplications should wait for *remote* relays replication
	// acceptance and it should return a list of accepting relays per ChainID.
	// Use this method to wait for a chain replication to be accepted *remotely*.
	WaitForRelaysAckChainReplications(
		ctx context.Context,
		catchupRelays map[string][]*helpers.RelayAddress,
		transactions ...client.Transaction,
	) (
		numExpected int,
		numReceived int,
		err error,
	)

	// WaitForRelaysAckTransactionBatch should wait for *remote* relays transaction
	// acceptance and it should return a list of accepting relays per tx hash.
	// Use this method to wait for a transaction batch to be accepted *remotely*.
	WaitForRelaysAckTransactionBatch(
		ctx context.Context,
		userAddress string,
		chainRelays map[string][]*helpers.RelayAddress,
		catchupRelays map[string][]*helpers.RelayAddress,
		transactions ...client.Transaction,
	) (
		numExpected int,
		numReceived int,
		err error,
	)
}
