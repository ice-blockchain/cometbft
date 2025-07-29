package types

import (
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"

	cmtmem "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
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
}

// RuntimeManager defines the contract for the runtime manager.
type RuntimeManager interface {
	service.Service

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
	service.Service

	// Has returns true if a resource or service with name exists for chainID.
	Has(chainID, name string) bool

	// Set adds a resource or service with name for chainID.
	Set(chainID, name string, resource any) error

	// Get returns a resource or service by name and chainID.
	Get(chainID, name string) any
}

// ReplicationManager defines the contract for the replications manager.
type ReplicationManager interface {
	service.Service

	// Partners returns a list of relay ID from replication partners.
	Partners(chainID string) []cmtp2p.ID

	// Status returns the status of a chain replication.
	Status(chainID string) *mxp2p.ChainReplicationStatus
	// Requests returns the outgoing replication requests mapped by relay ID.
	Requests(chainID string) map[cmtp2p.ID]*mxp2p.ChainReplicationRequest
	// Responses returns the incoming replication responses mapped by relay ID.
	Responses(chainID string) map[cmtp2p.ID]*mxp2p.ChainReplicationResponse

	// Wait blocks the thread until chainID has 2/3+1 replication responses.
	Wait(chainID string) error
}

// BroadcastManager defines the contract for the broadcast operations manager.
type BroadcastManager interface {
	service.Service

	// Peers returns a list of relay ID from broadcast partners for txHash.
	Peers(txHash string) []cmtp2p.ID

	// Transactions returns the outgoing transaction messages mapped by relay ID.
	Transactions() map[cmtp2p.ID]cmtmem.Txs
	// Messages returns the incoming transaction ACKs for txHash mapped by relay ID.
	Messages(txHash string) map[cmtp2p.ID]*mxp2p.AckTransactionBroadcast

	// Wait blocks the thread until txHash has 2/3+1 ACK messages.
	Wait(txHash string) error
}
