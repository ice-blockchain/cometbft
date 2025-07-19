package server

import (
	"context"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/rpc"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
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
// Server defines the contract for replication backend implementations.
//
// A server instance must be started before replication can happen and
// before broadcast operations can be forwarded to a [client.Client].
//
// A server instance manages an instance of [runtime.RuntimeManager],
// and one of [client.Acceptor], which are used in runtime processes.
type Server interface {
	service.Service

	// GetRuntimeRegistry should return the node runtime manager.
	GetRuntimeRegistry() runtime.Manager

	// GetAcceptor should return a [client.Acceptor] instance.
	GetAcceptor() client.Acceptor

	// GetLogger should return a [cmtlog.Logger] instance.
	GetLogger() cmtlog.Logger

	// Implements [service.Service]
	Start() error
	Stop() error
	Reset(context.Context) error
}

// ----------------------------------------------------------------------------
// Backend defines a multiplex backend adapter
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
// - [RelayHelpers]
// - [BroadcastHelpers]
// - [ConsensusHandler]
type Backend interface {
	Server
	rpc.Backend
	RelayHelpers
	BroadcastHelpers
	ConsensusHandler

	// GetRoutines should return an implementation of [Jobs] methods.
	GetRoutines() *Jobs

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
		remoteRelays []*RelayAddress,
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
// ConsensusHandler defines the contract for consensus handlers.
//
// A ConsensusHandler may be used to start an asynchronous consensus instance,
// e.g. start the CometBFT reactors for consensus, blocksync and mempool.
type ConsensusHandler interface {
	// StartConsensusInstance should start the consensus reactors,
	// including mempool, blocksync, consensus and evidence reactors.
	StartConsensusInstance(chainID string) error
}

// ----------------------------------------------------------------------------
// RelayHelpers defines helper methods to read from relays.
//
// A helpers instance may be used to read information from relays such as
// their validators public keys, their server information, or the local
// blockchain heights stored on the relay.
type RelayHelpers interface {
	// GetLocalNetworkHeights should query the available networks and determine
	// a list of required networks. Iff the network cannot be found, it will be
	// considered unknown and may need to be explicitely created.
	//
	// Return order: requiredNetworks, mustCreateNetworks.
	GetLocalNetworkHeights(
		userAddress string,
		transactions ...client.Transaction,
	) ([]string, []string)

	// GetRemoteValidatorsInfo should request a RPCResultInitValidator object
	// which contains a map of validators public keys by ChainID.
	GetRemoteValidatorsInfo(
		ctx context.Context,
		relayAddress *RelayAddress,
		networks []string,
	) (*rpc.RPCResultInitValidators, error)

	// GetRemoteRelayInfo should request a relay information object which contains
	// a CometBFT Node ID, the supported networks and the node's listen address.
	GetRemoteRelayInfo(
		ctx context.Context,
		relayAddress *RelayAddress,
	) (*rpc.RPCResultRelayInfo, error)

	// GetValidatorsByNetwork should find the supported networks, then map each
	// of the ChainID to a slice of validator public keys.
	GetValidatorsByNetwork(
		ctx context.Context,
		relays []*RelayAddress,
		networks []string,
	) (map[string][]string, error)

	// GetRelaysByNetwork should find the supported networks, then map each
	// to a slice of relay addresses, and it also returns a slice of relays
	// that produced errors, e.g. network error.
	GetRelaysByNetwork(
		ctx context.Context,
		relays []*RelayAddress,
	) ([]*RelayAddress, map[string][]*RelayAddress, []string)

	// CheckDialCompatibleRelay should dial a relay, executing a P2P handshake
	// and thereby defining whether a relay is compatible for dialing.
	CheckDialCompatibleRelay(
		ctx context.Context,
		dialWithSw *p2p.Switch,
		relayAddress *RelayAddress,
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
		relays []*RelayAddress,
		chainRelays map[string][]*RelayAddress,
	) map[string][]*RelayAddress

	// ApplyFilterAckTransactionRelayIds should filter relay IDs and return a
	// slice of relays IDs with only relays that must be waited for during the
	// AckTransactionBroadcast process.
	ApplyFilterAckTransactionRelayIds(
		chainRelays map[string][]*RelayAddress,
		catchupRelays map[string][]*RelayAddress,
	) []string

	// WaitForRelaysAckChainReplications should wait for *remote* relays replication
	// acceptance and it should return a list of accepting relays per ChainID.
	// Use this method to wait for a chain replication to be accepted *remotely*.
	WaitForRelaysAckChainReplications(
		ctx context.Context,
		catchupRelays map[string][]*RelayAddress,
		transactions ...client.Transaction,
	) (
		relaysPerChain map[string][]string,
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
		chainRelays map[string][]*RelayAddress,
		catchupRelays map[string][]*RelayAddress,
		transactions ...client.Transaction,
	) (
		expectedRelaysPerTx,
		relaysPerTx map[string][]string,
		numExpected int,
		numReceived int,
		err error,
	)

	// WaitForRelaysReplicationCompleted should wait for *remote* relays replication
	// finalization and it should return a number of completed replication requests.
	// Use this method to wait for a chain replication to be *finalized* remotely.
	//
	// CAUTION: A chain replication may take hours to complete given a higher
	// number of blocks to synchronize with the network. Use accordingly.
	WaitForRelaysReplicationCompleted(
		ctx context.Context,
		syncingChainIds []string,
		transactions ...client.Transaction,
	) (numCompleted int, err error)

	// WaitForTransactionsEvents should wait for a number of transaction events
	// to confirm that transactions got included.
	WaitForTransactionsEvents(
		ctx context.Context,
		userAddress string,
		transactions ...client.Transaction,
	) (numCompleted int, err error)
}
