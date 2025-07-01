package server

import (
	"context"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// Server defines the contract for replication backend implementations.
//
// A server instance must be started before replication can happen and
// before broadcast operations can be forwarded to the [client.Client].
type Server interface {
	//io.Closer

	// GetAcceptor returns the injected [client.Acceptor] implementation.
	GetAcceptor() client.Acceptor

	// MustStart executes a replication backend.
	Start() error
	Stop() error
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

	// GetLogger should return a [cmtlog.Logger] instance.
	GetLogger() cmtlog.Logger

	// GetRoutines should return an implementation of [Jobs] methods.
	GetRoutines() *Jobs

	// GetRuntimeRegistry should return the active node runtime manager.
	GetRuntimeRegistry() *RuntimeRegistry

	// GetRelayID should return a [p2p.ID] instance that identifies a relay.
	GetRelayID() p2p.ID

	// GetListenAddress should return the relay's listen address.
	GetListenAddress() string

	// GetNetworks should return a slice of supported ChainID values.
	GetNetworks() []string

	// GetDiscoveryPort should return the `DiscoveryPort` config value.
	GetDiscoveryPort() uint16

	// GetValidatorPubs should return the validator public keys per ChainID.
	GetValidatorPubs() map[string]string

	// InitValidators should initialize validators for networks and
	// should return a map of public keys per ChainID.
	InitValidators(networks []string) (map[string]string, error)

	// StartConsensusInstance should start the consensus reactors,
	// including mempool, blocksync, consensus and evidence reactors.
	StartConsensusInstance(chainID string) error

	// UpdateMultiNetworkNodeInfo should update the NodeInfo pointer and
	// to permit communication of messages related to new, i.e. unknown
	// for this relay, networks.
	UpdateMultiNetworkNodeInfo(
		requiredNetworks []string,
	) error

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

	// CancelBroadcastOperation should execute the CancelBroadcast routine
	// and it should remove transactions from the local mempool.
	CancelBroadcastOperation(
		ctx context.Context,
		userAddress string,
		transactions ...client.Transaction,
	) error

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
	) (*RPCResultInitValidators, error)

	// GetValidatorsByNetwork should find the supported networks, then map each
	// of the ChainID to a slice of validator public keys.
	GetValidatorsByNetwork(
		ctx context.Context,
		relays []*RelayAddress,
		networks []string,
	) (map[string][]string, error)

	// GetRemoteRelayInfo should request a relay information object which contains
	// a CometBFT Node ID, the supported networks and the node's listen address.
	GetRemoteRelayInfo(
		ctx context.Context,
		relayAddress *RelayAddress,
	) (*RPCResultRelayInfo, error)

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

	// OnBroadcastComplete should update a runtime completion status.
	// It accepts a batch of transactions and a list of remoteRelays
	// that it should wait for until they have completed replication.
	//
	// This method should call [server.RuntimeRegistry#OnComplete].
	OnBroadcastComplete(
		ctx context.Context,
		userAddress string,
		remoteRelays []*RelayAddress,
		transactions ...client.Transaction,
	) error

	// OnBroadcastError should update a runtime completion status
	// and attach and log an error about the broadcast completion.
	//
	// This method should call [server.RuntimeRegistry#OnComplete].
	OnBroadcastError(
		reason error,
		userAddress string,
		transactions ...client.Transaction,
	) error
}
