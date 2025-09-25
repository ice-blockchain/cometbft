package types

import (
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	cmttypes "github.com/ice-blockchain/cometbft/types"
)

// IdleManager defines the contract for the runtime idle manager.
type IdleManager interface {
	service.Service

	// NumRuntimes returns the total number of active runtimes.
	NumRuntimes() uint64
	// NumSleeping returns the total number of sleeping runtimes.
	NumSleeping() uint64
	// ActiveRuntimes returns a number of active runtimes mapped by ChainID.
	ActiveRuntimes() map[string]uint64
	// SleepingRuntimes returns a list of sleeping runtimes.
	SleepingRuntimes() []string

	// OnActivate activates a runtime for chainID.
	OnActivate(chainID string) error
	// OnComplete completes a runtime for chainID.
	OnComplete(chainID string) error
	// OnIdle idles a sleeping runtime for chainID.
	OnIdle(chainID string) error

	// WaitForIndexedTransactions creates goroutines that wait for indexing events
	// with relevantChainIds and all transactions for each ChainID.
	WaitForIndexedTransactions(
		relevantChainIds []string,
		transactionsByChain map[string][]client.Transaction,
	) (numCompleted int)

	// WaitForChainReplications creates goroutines that wait for replications
	// with relevantChainIds and all transactions for each ChainID.
	WaitForChainReplications(
		relevantChainIds []string,
		transactionsByChain map[string][]client.Transaction,
	) (numCompleted int)
}

// RuntimeManager defines the contract for the runtime manager.
type RuntimeManager interface {
	service.Service

	// Resources returns the [ResourceManager].
	Resources() ResourceManager
	// Validators returns a map of [cmttypes.PrivValidator] by ChainID.
	Validators() map[string]cmttypes.PrivValidator
	// Composer returns the runtime composer instance.
	Composer() RuntimeComposer
	// ConsensusPool returns the consensus pool.
	ConsensusPool() ConsensusHandler

	// AddRuntime should add a genesisDoc for chainID.
	AddRuntime(chainID string, genesisDoc cmttypes.GenesisDoc) error
	// InitRuntime should initialize all services and resources for chainID.
	InitRuntime(chainID string, otherValPubKeys []string) error
	// StartRuntime should start all services for chainID.
	StartRuntime(chainID string) error
	// StopRuntime should stop all services for chainID.
	StopRuntime(chainID string) error
}

// RuntimeComposer defines the contract for the runtime orchestrator.
type RuntimeComposer interface {
	service.Service

	// SetSwitch is used to set a cmtp2p.Switch for CometBFT.
	SetSwitch(sw *cmtp2p.Switch)
	// Switch returns the cmtp2p.Switch instance for CometBFT.
	Switch() *cmtp2p.Switch

	// Compose initializes a runtime for chainID.
	Compose(chainID string, remoteValidatorPubKeys []string) error
	// Inject injects a running state machine and block store.
	Inject(chainID string) error
	// Build packages a node runtime and injects a [node.Node].
	Build(chainID string, abciClient proxy.ChainConns) error
	// Unload decomposes resources and services for chainID.
	Unload(chainID string) error

	// DataPath returns the filesystem path to data for chainID.
	DataPath(chainID string) string
	// ConfPath returns the filesystem path to config for chainID.
	ConfPath(chainID string) string
	// Config returns the configuration instance for chainID.
	Config(chainID string) *config.Config
	// GenesisDoc returns the genesis configuration for chainID.
	GenesisDoc(chainID string) cmttypes.GenesisDoc
	// Database returns a database for chainID.
	// Uses dbServiceKey as registered service key.
	Database(chainID, dbServiceKey string) *helpers.DBService
	// Validator returns the priv validator for chainID.
	Validator(chainID string) cmttypes.PrivValidator
	// EventBus returns the event bus for chainID.
	EventBus(chainID string) *cmttypes.EventBus
}

// ConsensusHandler defines the contract for the consensus handler.
type ConsensusHandler interface {
	service.Service

	// SetSwitch is used to set a cmtp2p.Switch for CometBFT.
	SetSwitch(sw *cmtp2p.Switch)
	// Switch returns the cmtp2p.Switch instance for CometBFT.
	Switch() *cmtp2p.Switch

	// ABCI returns the "application-blockchain client interface".
	ABCI() proxy.ChainConns

	// Handshake executes the consensus/ABCI handshake to set the App version.
	Handshake(chainID string) error
	// Inject injects mempool, blocksync, consensus and evidence reactors.
	Inject(chainID string) error
	// Execute starts mempool, blocksync, consensus and evidence reactors.
	Execute(chainID string) error
	// Shutdown stops mempool, blocksync, consensus and evidence reactors.
	Shutdown(chainID string) error
}

// ResourceManager defines the contract for the resources manager.
type ResourceManager interface {
	// Has returns true if a resource or service with name exists for chainID.
	Has(chainID, name string) bool

	// Set adds a resource or service with name for chainID.
	Set(chainID, name string, resource any) error

	// Get returns a resource or service by name and chainID.
	Get(chainID, name string) any

	// Multiplex returns a resource map for name by ChainID.
	Multiplex(name string) helpers.MultiplexMap[any]
}

// MessageManager defines the contract for a message pool.
type MessageManager interface {
	// AddIncoming adds a received message to the pool.
	AddIncoming(e cmtp2p.Envelope) error
	// AddOutgoing adds a sent message to the pool.
	AddOutgoing(dest cmtp2p.ID, e cmtp2p.Envelope) error
}

// ReplicationManager defines the contract for the replications manager.
type ReplicationManager interface {
	service.Service

	// Init initializes a replication processor for chainID with relays.
	Init(chainID string, relays []*helpers.RelayAddress) error
	// Process processes a message e with the replication pool.
	Process(peerID cmtp2p.ID, e cmtp2p.Envelope) error

	// Partners returns a list of relay ID from replication partners.
	Partners(chainID string) []cmtp2p.ID
	// Status returns the status of a chain replication.
	Status(chainID string) *mxp2p.ChainReplicationStatus
	// Requests returns the stored replication requests for chainID.
	Requests(chainID string) []*mxp2p.ChainReplicationRequest
	// Responses returns the stored replication responses for chainID.
	Responses(chainID string) []*mxp2p.ChainReplicationResponse

	// Accepted returns a channel, which is closed when chainID has 2/3+1 responses.
	Accepted(chainID string) chan struct{}
	// Completed returns a channel, which is closed when chainID has 2/3+1 completions.
	Completed(chainID string) chan struct{}

	// WaitAccepted blocks a thread until chainID has 2/3+1 responses.
	WaitAccepted(chainID string) bool
	// WaitCompleted blocks a thread until chainID has 2/3+1 completions.
	WaitCompleted(chainID string) bool
}

// BroadcastManager defines the contract for the broadcast operations manager.
type BroadcastManager interface {
	service.Service

	// EventBus returns an event bus for chainID.
	EventBus(chainID string) *cmttypes.EventBus

	// Init initializes a broadcast processor for txHash with relays.
	Init(chainID string, txHash string, relays []*helpers.RelayAddress) error
	// Process processes a received message e with the broadcast pool.
	Process(peerID cmtp2p.ID, e cmtp2p.Envelope) error

	// Partners returns a list of relay ID from broadcast partners for txHash.
	Partners(txHash string) []cmtp2p.ID
	// Responses returns the stored ack transaction messages for txHash.
	Responses(txHash string) []*mxp2p.AckTransactionBroadcast

	// Accepted returns a channel, which is closed when txHash has 2/3+1 ACK messages.
	Accepted(txHash string) chan struct{}
	// Indexed returns a channel, which is closed when txHash got indexed locally.
	Indexed(txHash string) chan struct{}

	// WaitAccepted blocks the thread until txHash has 2/3+1 ACK messages.
	WaitAccepted(txHash string) bool
	// WaitIndexed blocks the thread until txHash got indexed locally.
	WaitIndexed(txHash string) bool
}
