package multiplex

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtlibs "github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcclient "github.com/ice-blockchain/cometbft/rpc/jsonrpc/client"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/state/indexer"
	blockidxkv "github.com/ice-blockchain/cometbft/state/indexer/block/kv"
	blockidxnull "github.com/ice-blockchain/cometbft/state/indexer/block/null"
	"github.com/ice-blockchain/cometbft/state/txindex"
	txidxkv "github.com/ice-blockchain/cometbft/state/txindex/kv"
	txidxnull "github.com/ice-blockchain/cometbft/state/txindex/null"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
)

const (
	// Instance types.
	InstanceKeyConfig           = "config"
	InstanceKeyStorage          = "storage"
	InstanceKeyState            = "state"
	InstanceKeyStateStore       = "stateStore"
	InstanceKeyBlockStore       = "blockStore"
	InstanceKeyPrivValidator    = "privValidator"
	InstanceKeyDatabaseBlock    = "database/blockStore"
	InstanceKeyDatabaseState    = "database/state"
	InstanceKeyDatabaseIndex    = "database/txIndex"
	InstanceKeyDatabaseEvidence = "database/evidence"
	InstanceKeyP2PSwitch        = "p2p/switch"
	InstanceKeyP2PTransport     = "p2p/transport"
	InstanceKeyFlagBlockSync    = "flag/blockSync"

	// Services types.
	ServiceKeyEventBus         = "eventBus"
	ServiceKeyIndexers         = "indexers"
	ServiceKeyPruner           = "pruner"
	ServiceKeyMempoolReactor   = "reactor/mempool"
	ServiceKeyBlockSyncReactor = "reactor/blockSync"
	ServiceKeyConsensusReactor = "reactor/consensus"
	ServiceKeyEvidenceReactor  = "reactor/evidence"
	ServiceKeyNodeRuntime      = "runtime/node"

	// Network requests timeout configuration, e.g. [GetRemoteRelayInfo].
	DefaultRequestTimeout = 2 * time.Second
)

// serviceProviderFn provides a [cmtlibs.Service] instance by name and ChainID.
type serviceProviderFn func(string, string) cmtlibs.Service

// multiplexProviderFn provides a [MultiplexMap] instance by name.
type multiplexProviderFn func(string) MultiplexMap[any]

// instanceProviderFn provides [any] instance by ChainID.
type instanceProviderFn func(string) any

// genesisDocProviderFn provides a [types.GenesisDoc] by ChainID.
type genesisDocProviderFn func(string) (*types.GenesisDoc, error)

// -----------------------------------------------------------------------------
// Reactor

// The Reactor implementation takes care of configuring node instances for the
// correct replicated blockchain networks. The reactor starts multiple listeners
// in parallel and sends messages on a channel to report about successful launch.
//
// When a set of node listeners is ready, the multiplex reactor sends a message on
// its channel `chainReadyCh` which contains a ChainID of the chain that is
// being replicated. After this happened, the node is able to start syncing state
// and/or blocks, as well as starting indexers, mempool, and other services.
//
// The [Reactor] structure implements [snapsapp.Reactor].
type Reactor struct {
	p2p.BaseReactor // BaseService + p2p.Switch

	// Registries for node services and network
	servicesProvider  func(string, string) cmtlibs.Service // service by name, e.g. "eventBus", and ChainID
	multiplexProvider func(string) MultiplexMap[any]       // multiplex by name, e.g. "database", "state", etc.

	genesisDocsMutex   sync.RWMutex
	initialGenesisDocs *ChecksummedGenesisDocSet

	// Node configuration
	//
	// The envMutex must be locked to access or modify environment resources.
	envMutex     sync.RWMutex
	nodeKey      *p2p.NodeKey
	nodeConfig   *config.Config
	userConfig   *config.MultiplexConfig
	abciClient   proxy.ChainConns
	snapsApp     *snapsapp.SnapsApp
	acceptorImpl client.Acceptor

	// Runtime(s) configuration
	//
	// The runtimesMutex must be locked to access or modify runtime configs.
	runtimesMutex sync.RWMutex
	chainRegistry ChainRegistry
	storagePaths  MultiplexFS
	configsPaths  MultiplexFS
	// RuntimeRegistry defines a registry of active and inactive runtimes.
	runtimeRegistry *server.RuntimeRegistry
	onIdleCallback  server.OnIdleFn

	// Networking layer
	//
	// The envMutex must be locked to access or modify environment resources.
	networkMutex    sync.RWMutex
	networks        []string
	nodeInfo        *MultiNetworkNodeInfo
	discoverySwitch *p2p.Switch
	cometbftSwitch  *p2p.Switch
	transport       *p2p.MultiplexTransport
	rpcMultiplexer  *http.ServeMux
	rpcRoutes       map[string]bool

	// Defines the duration for network requests to timeout.
	// Used in [Reactor#GetRemoteRelayInfo].
	relayInfoTimeout time.Duration
	knownRelayInfo   map[string]*server.RPCResultRelayInfo

	// Mapping of mempool partners relay IDs by transaction hash.
	poolRequestsMtx  sync.RWMutex
	poolRequestsSent map[string][]string

	// Services registry is a multiplex map which is searchable by service name
	// and which contains other multiplex maps where keys are ChainID values.
	//
	// Services priority contains a slice of service names to keep track of the
	// order of execution of services as used during the shutdown routine.
	//
	// To access this property, the mutex must be locked.
	servicesMutex    sync.RWMutex
	servicesRegistry NamedMultiplexMap[cmtlibs.Service]
	servicesPriority map[string]uint32
	servicesSequence []string

	// Multiplex registry is a multiplex map which is searchable by service name
	// and which contains other multiplex maps where keys are ChainID values.
	//
	// To access this property, the mutex must be locked.
	multiplexMutex    sync.RWMutex
	multiplexRegistry NamedMultiplexMap[any]
	multiplexMetrics  NamedMultiplexMap[any]

	// chainReadyChs contains channels that are opened when node instance
	// is being started by ChainID. These channels are written on when node
	// instances have started, inside the [OnStart] method.
	// Channels are consumed by [WaitForNetworks], by [InjectNewRuntime]
	// or by [NewNodesMultiplex].
	// This channel is closed by the [OnStop] method.
	chainReadyMtx sync.RWMutex
	chainReadyChs map[string]chan bool

	// ackReplResChs contains channels that are opened on-demand, when the
	// application expects to receive [ChainReplicationResponse] messages from
	// relevant relays, mapped by ChainID. Messages forwarded on this channel
	// must contain a [mxp2p.ChainReplicationResponse].
	// Channels are consumed by [MultiplexBackend#WaitForRelaysAckChainReplications].
	// These channels are closed by the [OnStop] method in case they were not yet
	// closed by the deferral process in [MultiplexBackend#WaitForRelaysAckChainReplications].
	ackReplResMtx sync.RWMutex
	ackReplResChs map[string]chan *mxp2p.ChainReplicationResponse

	// ackAcceptTxChs contains channels that are opened on-demand, when the
	// application expects to receive [AckTransactionBroadcast] messages from
	// relevant relays. Keys are transaction hashes. Messages forwarded on this
	// channel must contain a [mxp2p.AckTransactionBroadcast].
	// Channels are consumed by [MultiplexBackend#WaitForRelaysAckTransactionBatch].
	// These channels are closed by the [OnStop] method in case they were not yet
	// closed by the deferral process in [MultiplexBackend#WaitForRelaysAckTransactionBatch].
	ackAcceptTxMtx sync.RWMutex
	ackAcceptTxChs map[string]chan *mxp2p.AckTransactionBroadcast

	runtimeUpdatesMtx sync.RWMutex
	runtimeUpdatesChs map[string]chan *mxp2p.ChainReplicationComplete

	// ReplayPool defines a pool for concurrent processing of transaction buckets,
	// which consist of one or many batches of transactions by user address.
	replayPoolMtx sync.RWMutex
	replayPool    *server.ReplayPool

	// Internal
	logger cmtlog.Logger
}

type ReactorOption func(*Reactor)

// Type assertion to make sure this structure is compatible with snapsapp.
var _ snapsapp.Reactor = (*Reactor)(nil)

// NewReactor creates a new multiplex reactor around a [p2p.NodeKey],
// a global node configuration with [config.Config] and a [ChainRegistry].
//
// Note that the genesisDocsProvider must be passed as well but is being
// used only at Start of the reactor, when the initial [sm.State] is loaded.
func NewReactor(
	nodeKey *p2p.NodeKey,
	nodeCfg *config.Config,
	logger cmtlog.Logger,
	chainRegistry ChainRegistry,
	genesisDocsProvider node.GenesisDocProvider,
	options ...ReactorOption,
) *Reactor {
	// CometBFT servers share ports amongst networks
	p2pListenAddr := overwriteListenPort(
		nodeCfg.P2P.ListenAddress,
		int(nodeCfg.DiscoveryPort+1), // defaults to 30002
	)
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.DiscoveryPort+2), // defaults to 30003
	)

	// Make sure we always refer to the correct listen addresses:
	// - P2P Discovery Port: discovery_port
	// - RPC Discovery Port: discovery_port-1
	// - P2P CometBFT Port:  discovery_port+1
	// - RPC CometBFT Port:  discovery_port+2
	// - Prometheus Port:    discovery_port+3
	nodeCfg.P2P.ListenAddress = p2pListenAddr
	nodeCfg.RPC.ListenAddress = rpcListenAddr

	reactor := &Reactor{
		// Provides the ChainRegistry interface
		chainRegistry: chainRegistry,

		// Provides node information and config
		nodeKey:    nodeKey,
		nodeConfig: nodeCfg,
		userConfig: &nodeCfg.MultiplexConfig,

		// Provides an *ordered* slice of ChainID
		networks: chainRegistry.GetChains(),

		// Provides a default acceptor implementation
		acceptorImpl: &client.DefaultAcceptor{},

		runtimeRegistry: server.NewRuntimeRegistry(
			logger.With("module", "idle-manager"),
		),

		// Allocations
		servicesRegistry:  NamedMultiplexMap[cmtlibs.Service]{},
		servicesPriority:  map[string]uint32{},
		servicesSequence:  []string{},
		multiplexRegistry: NamedMultiplexMap[any]{},
		multiplexMetrics:  NamedMultiplexMap[any]{},
		storagePaths:      MultiplexFS{},
		configsPaths:      MultiplexFS{},
		knownRelayInfo:    map[string]*server.RPCResultRelayInfo{},
		rpcRoutes:         map[string]bool{},

		// Internal channels
		chainReadyChs:     make(map[string]chan bool),
		ackReplResChs:     make(map[string]chan *mxp2p.ChainReplicationResponse),
		ackAcceptTxChs:    make(map[string]chan *mxp2p.AckTransactionBroadcast),
		runtimeUpdatesChs: make(map[string]chan *mxp2p.ChainReplicationComplete),

		// Internals
		logger: logger,
	}

	// Set default OnIdle callback in case none is set through options.
	reactor.SetRuntimeRegistryOptions(
		server.RuntimeRegistryOnIdle(DefaultOnIdleCallback(reactor)),
	)

	// Enable overwrite of some optional properties.
	for _, option := range options {
		option(reactor)
	}

	reactor.BaseReactor = *p2p.NewBaseReactor("Multiplex", reactor)

	// Note that this expects the `genesis.json` to contain a GenesisDocSet.
	// This call to the underlying provider Validates the GenesisDocSet or
	// creates an empty genesis doc set with no checksum to verify.
	icsGenesisDocSet, err := genesisDocsProvider()
	if err != nil {
		reactor.logger.Debug("CAUTION: Using empty GenesisDocSet (not an error)")
	}

	// Locks the genesisDocsMutex for writing
	reactor.SetChecksummedGenesisDocSet(icsGenesisDocSet.(*ChecksummedGenesisDocSet))

	// Initializes instance providers (services, multiplex)
	reactor.initMultiplexProviders(icsGenesisDocSet)

	// TODO(midas): Write some more tests to determine correct threshold.
	reactor.replayPoolMtx.Lock()
	reactor.replayPool = server.NewReplayPool(
		reactor.logger.With("module", "replay"),
		server.ReplayPoolThreshold(10),
		server.ReplayPoolAcceptor(reactor.acceptorImpl),
	)
	reactor.replayPoolMtx.Unlock()

	return reactor
}

// DefaultOnIdleCallback returns a default implementation for the OnIdle
// callback used by the [server.RuntimeRegistry] when node runtimes are
// sleeping for a given period of time.
func DefaultOnIdleCallback(reactor *Reactor) func(chainID string) error {
	return func(chainID string) error {
		reactor.logger.Debug("Now idling inactive node runtime", "chain_id", chainID)

		if err := reactor.StopNodeInstance(chainID); err != nil {
			reactor.logger.Error("failed to stop node instance (idle-manager)",
				"err", err)
		}

		return nil
	}
}

// WithAcceptor is an option helper to inject a custom acceptor implementation
// which accepts a user address and an acceptor.
func WithAcceptor(
	acceptor client.Acceptor,
) func(*Reactor) {
	return func(r *Reactor) {
		r.SetAcceptor(acceptor)
	}
}

// WithOnIdleCallback
func WithOnIdleCallback(
	onIdle server.OnIdleFn,
) func(*Reactor) {
	return func(r *Reactor) {
		r.onIdleCallback = onIdle
	}
}

func WithRelayInfoTimeout(t time.Duration) func(*Reactor) {
	return func(r *Reactor) {
		r.relayInfoTimeout = t
	}
}

// ----------------------------------------------------------------------------
// Reactor public implementation

func (reactor *Reactor) SetOptions(options ...ReactorOption) {
	for _, option := range options {
		option(reactor)
	}
}

func (reactor *Reactor) SetRuntimeRegistryOptions(options ...server.RuntimeRegistryOption) {
	for _, option := range options {
		option(reactor.runtimeRegistry)
	}
}

// GetLogger returns a [cmtlog.Logger] instance.
func (reactor *Reactor) GetLogger() cmtlog.Logger {
	return reactor.logger
}

// SetLogger sets a custom [cmtlog.Logger] instance.
func (reactor *Reactor) SetLogger(logger cmtlog.Logger) {
	reactor.logger = logger
}

// GetNodeKey returns the [p2p.NodeKey] instance.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) GetNodeKey() *p2p.NodeKey {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.nodeKey
}

// GetNodeConfig returns a [config.Config] instance.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) GetNodeConfig() *config.Config {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.nodeConfig
}

// GetMultiplexConfig returns a [config.MultiplexConfig] instance.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) GetMultiplexConfig() *config.MultiplexConfig {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.userConfig
}

// GetABCIClient returns a [proxy.ChainConns] ABCI client.
// The ABCI, or "application-blockchain client interface"
// creates blocks proposals locally and includes transactions.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) GetABCIClient() proxy.ChainConns {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.abciClient
}

// SetABCIClient sets a custom [proxy.ChainConns] ABCI client.
// Note that this method is only used in tests for now.
// Internal mutex envMutex is locked for write.
func (reactor *Reactor) SetABCIClient(abciClient proxy.ChainConns) {
	reactor.envMutex.Lock()
	defer reactor.envMutex.Unlock()
	reactor.abciClient = abciClient
}

// GetSnapsApp returns a [snapsapp.SnapsApp] instance.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) GetSnapsApp() *snapsapp.SnapsApp {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.snapsApp
}

// SetSnapsApp returns a [snapsapp.SnapsApp] instance.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) SetSnapsApp(app *snapsapp.SnapsApp) {
	reactor.envMutex.Lock()
	defer reactor.envMutex.Unlock()
	reactor.snapsApp = app
}

// DiscoveryPort returns the configured DiscoveryPort.
// Internal mutex envMutex is locked for read.
func (reactor *Reactor) DiscoveryPort() uint16 {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.nodeConfig.DiscoveryPort
}

// GetAcceptor returns a [client.Acceptor] instance.
// Internal mutex envMutex is locked for read.
// See also: [WithAcceptor], [SetAcceptor]
func (reactor *Reactor) GetAcceptor() client.Acceptor {
	reactor.envMutex.RLock()
	defer reactor.envMutex.RUnlock()
	return reactor.acceptorImpl
}

// SetAcceptor sets a custom [client.Acceptor] acceptor implementation
// Internal mutex envMutex is locked for write.
func (reactor *Reactor) SetAcceptor(acceptor client.Acceptor) {
	reactor.envMutex.Lock()
	defer reactor.envMutex.Unlock()
	reactor.acceptorImpl = acceptor

	if reactor.snapsApp != nil {
		reactor.snapsApp.SetAcceptor(acceptor)
	}

	serviceProvider := reactor.GetServicesProvider()
	if serviceProvider == nil {
		// Acceptor will be set when first mempool reactor is created
		// i.e. Nothing to do.
		return
	}

	relevantChainIds := reactor.GetNetworks()
	for _, chainID := range relevantChainIds {
		memplReactorForChain := serviceProvider(ServiceKeyMempoolReactor, chainID)
		if memplReactorForChain == nil {
			continue
		}

		if memplReactor, ok := memplReactorForChain.(*mempl.Reactor); ok {
			memplReactor.SetAcceptor(acceptor)
		}
	}
}

// GetStoragePaths returns a [MultiplexFS] instance.
// Internal mutex runtimesMutex is locked for read.
//
// GetStoragePaths implements [snapsapp.Reactor].
func (reactor *Reactor) GetStoragePaths() map[string]string {
	reactor.runtimesMutex.RLock()
	defer reactor.runtimesMutex.RUnlock()
	return reactor.storagePaths
}

// SetStoragePaths sets a custom [MultiplexFS] map of storage paths.
// Internal mutex runtimesMutex is locked for write.
func (reactor *Reactor) SetStoragePaths(fs MultiplexFS) {
	reactor.runtimesMutex.Lock()
	defer reactor.runtimesMutex.Unlock()
	reactor.storagePaths = fs
}

// GetConfigsPaths returns a [MultiplexFS] instance.
// Internal mutex runtimesMutex is locked for read.
func (reactor *Reactor) GetConfigsPaths() map[string]string {
	reactor.runtimesMutex.RLock()
	defer reactor.runtimesMutex.RUnlock()
	return reactor.configsPaths
}

// SetConfigsPaths sets a custom [MultiplexFS] map of configs paths.
// Internal mutex runtimesMutex is locked for write.
func (reactor *Reactor) SetConfigsPaths(fs MultiplexFS) {
	reactor.runtimesMutex.Lock()
	defer reactor.runtimesMutex.Unlock()
	reactor.configsPaths = fs
}

// GetChainRegistry returns a [ChainRegistry] instance.
// Internal mutex runtimesMutex is locked for read.
func (reactor *Reactor) GetChainRegistry() ChainRegistry {
	reactor.runtimesMutex.RLock()
	defer reactor.runtimesMutex.RUnlock()
	return reactor.chainRegistry
}

// GetRuntimeRegistry returns the active node runtime manager.
func (reactor *Reactor) GetRuntimeRegistry() *server.RuntimeRegistry {
	reactor.runtimesMutex.Lock()
	defer reactor.runtimesMutex.Unlock()

	return reactor.runtimeRegistry
}

// GetChecksummedGenesisDocSet returns a [ChecksummedGenesisDocSet] instance.
// Internal mutex genesisDocsMutex is locked for read.
func (reactor *Reactor) GetChecksummedGenesisDocSet() *ChecksummedGenesisDocSet {
	reactor.genesisDocsMutex.RLock()
	defer reactor.genesisDocsMutex.RUnlock()
	return reactor.initialGenesisDocs
}

// SetChecksummedGenesisDocSet sets a custom [*ChecksummedGenesisDocSet].
// Internal mutex genesisDocsMutex is locked for read.
func (reactor *Reactor) SetChecksummedGenesisDocSet(ds *ChecksummedGenesisDocSet) {
	reactor.genesisDocsMutex.Lock()
	defer reactor.genesisDocsMutex.Unlock()
	reactor.initialGenesisDocs = ds
}

// GetMultiNetworkNodeInfo returns a [MultiNetworkNodeInfo] pointer.
// Internal mutex networkMutex is locked for read.
func (reactor *Reactor) GetMultiNetworkNodeInfo() *MultiNetworkNodeInfo {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.nodeInfo
}

// SetNodeInfo sets a custom [MultiNetworkNodeInfo] instance.
// Note that this method is only used in tests for now.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetNodeInfo(nodeInfo *MultiNetworkNodeInfo) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.nodeInfo = nodeInfo
}

// GetEventSwitchForDiscovery returns the [p2p.Switch] instance
// used to communicate [ChainReplicationRequest] messages.
// This switch must be used to send messages on `DiscoveryPort`.
// Internal mutex networkMutex is locked for read.
func (reactor *Reactor) GetEventSwitchForDiscovery() *p2p.Switch {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.discoverySwitch
}

// SetEventSwitchForDiscovery sets a custom [p2p.Switch] instance.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetEventSwitchForDiscovery(sw *p2p.Switch) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.discoverySwitch = sw
}

// GetEventSwitchForCometBFT returns the [p2p.Switch] instance
// used to communicate CometBFT messages, including block-sync.
// This switch must be used to send messages on `DiscoveryPort+1`.
// Internal mutex networkMutex is locked for read.
func (reactor *Reactor) GetEventSwitchForCometBFT() *p2p.Switch {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.cometbftSwitch
}

// SetEventSwitchForCometBFT sets a custom [p2p.Switch] instance.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetEventSwitchForCometBFT(sw *p2p.Switch) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.cometbftSwitch = sw
}

// GetTransportForCometBFT returns the [p2p.MultiplexTransport] instance.
// Internal mutex networkMutex is locked for read.
func (reactor *Reactor) GetTransportForCometBFT() *p2p.MultiplexTransport {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.transport
}

// SetTransportForCometBFT sets a custom [p2p.MultiplexTransport] instance.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetTransportForCometBFT(t *p2p.MultiplexTransport) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.transport = t
}

// GetRPCMultiplexer returns a [http.ServeMux] instance.
// Internal mutex networkMutex is locked for read.
func (reactor *Reactor) GetRPCMultiplexer() *http.ServeMux {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.rpcMultiplexer
}

// SetRPCMultiplexer sets a custom [http.ServeMux] instance.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetRPCMultiplexer(mux *http.ServeMux) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.rpcMultiplexer = mux
}

// ChannelForAckReplication creates or returns an unbuffered channel that accepts
// ChainReplicationResponse messages for chainID.
//
// ackReplResChs contains channels that are opened on-demand, when the
// application expects to receive [ChainReplicationResponse] messages from
// relevant relays, about one chainID.
func (reactor *Reactor) ChannelForAckReplication(chainID string) chan *mxp2p.ChainReplicationResponse {
	reactor.ackReplResMtx.RLock()
	replResChForChainID, hasChannel := reactor.ackReplResChs[chainID]
	reactor.ackReplResMtx.RUnlock()

	if !hasChannel {
		replResChForChainID = make(chan *mxp2p.ChainReplicationResponse)

		reactor.ackReplResMtx.Lock()
		reactor.ackReplResChs[chainID] = replResChForChainID
		reactor.ackReplResMtx.Unlock()
	}

	return replResChForChainID
}

// CloseAckReplicationChannel closes the replication acceptance channel
// for chainID and deletes it gracefully from the registry.
func (reactor *Reactor) CloseAckReplicationChannel(chainID string) {
	reactor.ackReplResMtx.RLock()
	ackReplResCh, hasChannel := reactor.ackReplResChs[chainID]
	reactor.ackReplResMtx.RUnlock()
	if !hasChannel {
		return
	}

	// Close the channel first
	close(ackReplResCh)

	// And free memory space
	reactor.ackReplResMtx.Lock()
	delete(reactor.ackReplResChs, chainID)
	reactor.ackReplResMtx.Unlock()
}

// ChannelForAckTransaction creates or returns an unbuffered channel that accepts
// AckTransactionBroadcast messages for txHash.
//
// ackAcceptTxChs contains channels that are opened on-demand, when the
// application expects to receive [AckTransactionBroadcast] messages from
// relevant relays, about one transaction hash.
func (reactor *Reactor) ChannelForAckTransaction(txHash string) chan *mxp2p.AckTransactionBroadcast {
	reactor.ackAcceptTxMtx.RLock()
	acceptChForTxHash, hasChannel := reactor.ackAcceptTxChs[txHash]
	reactor.ackAcceptTxMtx.RUnlock()

	if !hasChannel {
		acceptChForTxHash = make(chan *mxp2p.AckTransactionBroadcast)

		reactor.ackAcceptTxMtx.Lock()
		reactor.ackAcceptTxChs[txHash] = acceptChForTxHash
		reactor.ackAcceptTxMtx.Unlock()
	}

	return acceptChForTxHash
}

// CloseAckTransactionChannel closes the transaction acceptance channel
// for txHash and deletes it gracefully from the registry.
func (reactor *Reactor) CloseAckTransactionChannel(txHash string) {
	reactor.ackAcceptTxMtx.RLock()
	acceptCh, hasChannel := reactor.ackAcceptTxChs[txHash]
	reactor.ackAcceptTxMtx.RUnlock()
	if !hasChannel {
		return
	}

	// Close the channel first
	close(acceptCh)

	// And free memory space
	reactor.ackAcceptTxMtx.Lock()
	delete(reactor.ackAcceptTxChs, txHash)
	reactor.ackAcceptTxMtx.Unlock()
}

// ChannelForRuntimeUpdates creates or returns an unbuffered channel that accepts
// ChainReplicationComplete messages for chainID.
//
// runtimeUpdatesChs contains channels that are opened on-demand, when the
// application expects to receive [ChainReplicationComplete] messages from
// relevant relays, about one chainID.
func (reactor *Reactor) ChannelForRuntimeUpdates(chainID string) chan *mxp2p.ChainReplicationComplete {
	reactor.runtimeUpdatesMtx.RLock()
	runtimeUpdatesChForChainID, hasChannel := reactor.runtimeUpdatesChs[chainID]
	reactor.runtimeUpdatesMtx.RUnlock()

	if !hasChannel {
		runtimeUpdatesChForChainID = make(chan *mxp2p.ChainReplicationComplete)

		reactor.runtimeUpdatesMtx.Lock()
		reactor.runtimeUpdatesChs[chainID] = runtimeUpdatesChForChainID
		reactor.runtimeUpdatesMtx.Unlock()
	}

	return runtimeUpdatesChForChainID
}

// CloseRuntimeUpdatesChannel closes the runtime updates channel
// for chainID and deletes it gracefully from the registry.
func (reactor *Reactor) CloseRuntimeUpdatesChannel(chainID string) {
	reactor.runtimeUpdatesMtx.RLock()
	runtimeUpdatesCh, hasChannel := reactor.runtimeUpdatesChs[chainID]
	reactor.runtimeUpdatesMtx.RUnlock()
	if !hasChannel {
		return
	}

	// Close the channel first
	close(runtimeUpdatesCh)

	// And free memory space
	reactor.runtimeUpdatesMtx.Lock()
	delete(reactor.runtimeUpdatesChs, chainID)
	reactor.runtimeUpdatesMtx.Unlock()
}

// SaveRelayInfo stores the RPC call response with information about the
// relay in a map where keys are CometBFT Node IDs.
func (reactor *Reactor) SaveRelayInfo(relayInfo *server.RPCResultRelayInfo) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()

	relayID := string(relayInfo.DefaultNodeID)
	reactor.knownRelayInfo[relayID] = relayInfo
}

// ----------------------------------------------------------------------------
// Service providers implementation

// GetGenesisProvider returns a genesisDocProviderFn instance.
func (reactor *Reactor) GetGenesisProvider() genesisDocProviderFn {
	// NOTE(midas): We don't need a functor to get an up-to-date genesisDocSet,
	// we now point to an updated genesisDocSet here, another iteration must
	// remove the need for a functor that returns the genesisDocProvider.
	icsGenesisDocSet := reactor.GetChecksummedGenesisDocSet() // locks genesisDocsMutex
	return func(chainId string) (*types.GenesisDoc, error) {
		genDoc, err := icsGenesisDocSet.GenesisDocByChainID(chainId)
		if err != nil {
			return nil, fmt.Errorf("could not load genesis doc for ChainID %s: %w", chainId, err)
		}

		return genDoc, nil
	}
}

// GetServicesProvider returns a [ServiceProvider] providereactor.
func (reactor *Reactor) GetServicesProvider() serviceProviderFn {
	return reactor.servicesProvider // locks servicesMutex
}

// GetMultiplexProvider returns a [MultiplexProvider] providereactor.
func (reactor *Reactor) GetMultiplexProvider() multiplexProviderFn {
	return reactor.multiplexProvider // locks multiplexMutex
}

// GetInstanceProvider returns a [InstanceProvider] providereactor.
func (reactor *Reactor) GetInstanceProvider(multiplexName string) instanceProviderFn {
	// Uses one of the multiplexRegistry entries
	multiplex := reactor.multiplexProvider(multiplexName)
	return func(chainId string) any {
		reactor.multiplexMutex.RLock()
		defer reactor.multiplexMutex.RUnlock()

		if _, ok := multiplex[chainId]; !ok {
			return nil
		}

		// Returns the underlying instance (castable)
		return multiplex[chainId].GetInstance()
	}
}

// ----------------------------------------------------------------------------
// Service, instance and networks registry implementations

// RegisterService inserts a [cmtlibs.Service] instance in the registry
// by a given name and ChainID.
//
// The servicesMutex is RW-locked during the time this function takes to run.
func (reactor *Reactor) RegisterService(
	serviceName string,
	chainID string,
	service cmtlibs.Service,
) {
	reactor.servicesMutex.Lock()
	defer reactor.servicesMutex.Unlock()

	// Allocate namespace if necessary
	if _, ok := reactor.servicesRegistry[serviceName]; !ok {
		reactor.servicesRegistry[serviceName] = MultiplexMap[cmtlibs.Service]{}
	}

	// Store a service by name and ChainID
	reactor.servicesRegistry[serviceName][chainID] = NewChainInstance[cmtlibs.Service](
		chainID,
		service,
	)

	// Add service to priorities list once
	if _, ok := reactor.servicesPriority[serviceName]; !ok {
		nextIndex := len(reactor.servicesPriority) + 1
		reactor.servicesPriority[serviceName] = uint32(nextIndex)
		reactor.servicesSequence = append(reactor.servicesSequence, serviceName)
	}
}

// RegisterInstance inserts a generic instance in the multiplexRegistry,
// by a given multiplexName and ChainID.
//
// The multiplexMutex is RW-locked during the time this function takes to run.
func (reactor *Reactor) RegisterInstance(
	multiplexName string,
	chainID string,
	instance any,
) {
	reactor.multiplexMutex.Lock()
	defer reactor.multiplexMutex.Unlock()

	// Allocate namespace if necessary
	if _, ok := reactor.multiplexRegistry[multiplexName]; !ok {
		reactor.multiplexRegistry[multiplexName] = MultiplexMap[any]{}
	}

	// Store a generic instance by name and ChainID in a multiplex
	reactor.multiplexRegistry[multiplexName][chainID] = NewChainInstance[any](
		chainID,
		instance,
	)
}

// RegisterMetrics inserts a generic metrics instance in the multiplexMetrics,
// by a given metricsName and ChainID.
//
// The multiplexMutex is RW-locked during the time this function takes to run.
func (reactor *Reactor) RegisterMetrics(
	moduleName string,
	metricsName string,
	providerFn func() interface{},
) interface{} {
	reactor.multiplexMutex.Lock()
	defer reactor.multiplexMutex.Unlock()

	// Allocate namespace if necessary
	if _, ok := reactor.multiplexMetrics[moduleName]; !ok {
		reactor.multiplexMetrics[moduleName] = MultiplexMap[interface{}]{}
	}

	if _, ok := reactor.multiplexMetrics[moduleName][metricsName]; !ok {
		// Store a generic instance by name and ChainID in a multiplex
		reactor.multiplexMetrics[moduleName][metricsName] = NewChainInstance[any](
			metricsName,
			providerFn(),
		)
	}

	return reactor.multiplexMetrics[moduleName][metricsName].GetInstance()
}

// RegisterNetwork updates the necessary resources to permit executing a
// new network node runtime for userAddress and chainID.
//
// This method notably mutates the chainRegistry, the networks list,
// the ABCI client and the MultiNetworkNodeInfo instance of the reactor.
//
// Subsequent dialing of this relay will include the new network.
func (reactor *Reactor) RegisterNetwork(
	userAddress string,
	chainID string,
) error {
	// Nothing to do if the ChainID is known.
	if reactor.HasNetwork(chainID) {
		return nil
	}

	// First things first, ChainRegistry must be updated.
	reactor.runtimesMutex.Lock()
	reactor.chainRegistry.AddChain(
		userAddress,
		chainID,
	)
	reactor.runtimesMutex.Unlock()

	// .. because it must reflect on our list of networks.
	reactor.SetNetworks(reactor.chainRegistry.GetChains())

	nodeConfig := reactor.GetNodeConfig()
	nodeKey := reactor.GetNodeKey()
	abciClient := reactor.GetABCIClient()

	// Injects new AppConns in MultiplexAppConn for ABCI.
	if abciClient != nil {
		reactor.envMutex.Lock()
		reactor.abciClient.AddNetwork(chainID)
		reactor.envMutex.Unlock()
	}

	// Update the MultiNetworkNodeInfo instance (just a re-make).
	updatedNodeInfo, err := makeNodeInfo(
		nodeConfig.Moniker,
		nodeKey,
		reactor,
	)
	if err != nil {
		return fmt.Errorf("could not update multi network node info: %w", err)
	}

	// Injects the updated node info instance in the reactor.
	reactor.SetNodeInfo(updatedNodeInfo)

	return nil
}

// ----------------------------------------------------------------------------
// Reactor implements snapsapp.Reactor

// GetNetworks returns an ordered slice of ChainID values.
// Internal mutex networkMutex is locked for read.
//
// GetNetworks implements [snapsapp.Reactor].
func (reactor *Reactor) GetNetworks() []string {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return reactor.networks
}

// SetNetworks sets a custom slice of ChainIDs.
// Internal mutex networkMutex is locked for write.
func (reactor *Reactor) SetNetworks(ns []string) {
	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	reactor.networks = make([]string, len(ns))
	copy(reactor.networks, ns)
}

// HasNetwork returns true if the ChainID can be found.
// Internal mutex networkMutex is locked for read.
//
// HasNetwork implements [snapsapp.Reactor].
func (reactor *Reactor) HasNetwork(chainID string) bool {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return slices.Contains(reactor.networks, chainID)
}

// HasNetworks returns true if the networks registry is not empty.
func (reactor *Reactor) HasNetworks() bool {
	return reactor.Size() > 0
}

// Size returns the number of ChainIDs in networks registry.
func (reactor *Reactor) Size() int {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()
	return len(reactor.networks)
}

// GetStateStore returns a [sm.Store].
//
// GetStateStore implements [snapsapp.Reactor].
func (reactor *Reactor) GetStateStore(chainID string) sm.Store {
	// Retrieves the "stateStore" instance map
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)

	// Returns the instance mapped by ChainID
	return stateStoreProvider(chainID).(sm.Store)
}

// GetReplayPool returns a [server.ReplayPool] which contains transactions
// batches to be replayed. These batches may contain one or many txes
// that will be forwarded to [Acceptor#ReplayBroadcastTxBatch].
//
// GetReplayPool implements [snapsapp.Reactor].
func (reactor *Reactor) GetReplayPool() *server.ReplayPool {
	reactor.replayPoolMtx.RLock()
	defer reactor.replayPoolMtx.RUnlock()

	return reactor.replayPool
}

// ----------------------------------------------------------------------------
// Reactor implements p2p.Reactor

// GetChannels implements p2p.Reactor.
func (*Reactor) GetChannels() []*p2p.ChannelDescriptor {
	return []*p2p.ChannelDescriptor{
		{
			ID: server.ReplicationChannel,
			// Lower priority than blocksync, evidence, mempool & consensus
			// i.e. This channel has priority to be gossiped on.
			Priority:    3,
			MessageType: &mxp2p.Message{},
		},
		{
			ID:          server.AckBroadcastChannel,
			Priority:    3,
			MessageType: &mxp2p.Receipt{},
		},
		{
			ID:          server.RuntimeChannel,
			Priority:    10, // This channel does not have priority.
			MessageType: &mxp2p.Message{},
		},
	}
}

// AddPeer implements p2p.Reactor.
func (r *Reactor) AddPeer(peer p2p.Peer) {}

// RemovePeer implements p2p.Reactor.
func (r *Reactor) RemovePeer(peer p2p.Peer, _ any) {}

// Receive implements p2p.Reactor.
func (r *Reactor) Receive(e p2p.Envelope) {
	r.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID)

	idleManager := r.GetRuntimeRegistry()

	switch extMsg := e.Message.(type) {
	// ChainReplicationRequest
	// ChainReplicationResponse
	// ChainReplicationComplete
	case *mxp2p.Message:
		msg := extMsg.GetSum()

		// Determine public source address from secret connection.
		sourceAddr, err := e.Src.NodeInfo().NetAddress()
		if err != nil {
			r.logger.Error("couldn't determine source address from message",
				"msg", msg, "err", err)
			return
		}

		r.logger.Debug("Received from", "src", sourceAddr.String())

		switch msg.(type) {
		// ChainReplicationRequest
		// Received a request to replicate a (new) chain.
		case *mxp2p.Message_ChainReplicationRequest:
			r.logger.Debug("Received ChainReplicationRequest", "msg", msg)
			replRequest := extMsg.GetChainReplicationRequest()

			// A ChainReplicationResponse will be sent to the source peer.
			r.networkMutex.RLock()
			sourcePeer := r.discoverySwitch.UniquePeers().Get(sourceAddr.ID)
			r.networkMutex.RUnlock()

			// After having acknowledged the chain replication, process it.
			//
			// CAUTION: This modifies the runtime and allocates the necessary resources
			// for the newly replicated ChainID, and then *dials* the source peer.
			if err := r.handleChainReplicationRequest(sourcePeer, replRequest); err != nil {
				r.logger.Error(
					"failed to process ChainReplicationRequest: error handling replication",
					"chain_id", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// AddConnectionChannels locks networkMutex for read.
			// Opens any missing CometBFT channels for injected network's consensus.
			csw := r.GetEventSwitchForCometBFT()
			chs := []byte{} // all channels
			ids := r.GetNetworks()
			if err := r.AddConnectionChannels(csw, ids, chs, true); err != nil {
				r.logger.Error(
					"failed to process ChainReplicationRequest: error opening CometBFT channels",
					"chain_id", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// Start consensus reactors for newly injected runtime.
			if err := r.StartConsensusInstanceReactors(
				context.Background(),
				replRequest.ChainID,
				true, // enable status updates to peers about replication (ChainReplicationComplete)
			); err != nil {
				r.logger.Error(
					"failed to process ChainReplicationRequest: error starting consensus reactors",
					"chain_id", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// Activate this runtime in our runtime registry.
			//
			// NOTE(midas): The OnComplete callback must be executed only when
			// we have completed the full chain replication.
			idleManager.OnActivate(replRequest.ChainID)

			// Dials the CometBFT relay to permit faster consensus startup.
			if err := r.DialBackReplicationPartner(e.Src, replRequest.ChainID); err != nil {
				r.logger.Error(
					"failed to process ChainReplicationRequest: error dialing replication partner",
					"chain_id", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// Now respond with a [ChainReplicationResponse].
			// This serves as a receipt for a chain replication request.
			//
			// NOTE(midas): A outbound connection with the peer *must* exist.
			if err = r.sendChainReplicationResponse(sourcePeer, replRequest.ChainID); err != nil {
				r.Logger.Error("failed to send ChainReplicationResponse",
					"chain_id", replRequest.ChainID,
					"from", r.GetNodeKey().ID(),
					"to", sourcePeer.ID(),
					"err", err,
				)
			}

			// Done.
			r.logger.Debug("This relay now replicates a new chain", "chain_id", replRequest.ChainID)
			return

		// ChainReplicationResponse
		// Received a receipt of replication from one of the relays.
		case *mxp2p.Message_ChainReplicationResponse:
			r.logger.Debug("Received ChainReplicationResponse", "msg", msg)
			replResponse := extMsg.GetChainReplicationResponse()

			// Dials the CometBFT relay to permit faster consensus startup and
			// make sure communication with this relay is possible for blocksync.
			if err := r.DialBackReplicationPartner(e.Src, replResponse.ChainID); err != nil {
				r.logger.Error(
					"failed to process ChainReplicationResponse: error dialing replication partner",
					"chain_id", replResponse.ChainID,
					"err", err,
				)
				return
			}

			// Channel is mapped by transaction hash
			replResChForChainID := r.ChannelForAckReplication(replResponse.ChainID)
			replResChForChainID <- replResponse

			// Done.
			return

		// ChainReplicationComplete
		// Received a receipt of replication completeness from a peer.
		case *mxp2p.Message_ChainReplicationComplete:
			r.logger.Debug("Received ChainReplicationComplete", "msg", msg)
			replComplete := extMsg.GetChainReplicationComplete()

			// Channel is mapped by transaction hash
			runtimeUpdatesChForChainID := r.ChannelForRuntimeUpdates(replComplete.ChainID)
			runtimeUpdatesChForChainID <- replComplete

		default:
			r.logger.Error(
				"Unknown internal message type",
				"src", e.Src,
				"chId", e.ChannelID,
				"msg", e.Message,
			)
			return
		}

	case *mxp2p.Receipt:
		msg := extMsg.GetSum()

		// Determine public source address from secret connection.
		sourceAddr, err := e.Src.NodeInfo().NetAddress()
		if err != nil {
			r.logger.Error("couldn't determine source address from receipt",
				"msg", msg, "err", err)
			return
		}

		switch msg.(type) {
		// AckTransactionBroadcast
		// Received a receipt of relay mempool inclusion for a transaction hash.
		case *mxp2p.Receipt_AckTransactionBroadcast:
			// r.logger.Debug("Now processing AckTransactionBroadcast", "msg", msg)
			ackTxBroadcast := extMsg.GetAckTransactionBroadcast()
			txHashes := []string{}
			for _, bzHash := range ackTxBroadcast.TxHashes {
				txHashes = append(txHashes, fmt.Sprintf("%X", bzHash))
			}

			relayId := ackTxBroadcast.NodeId

			// AckTransactionBroadcast is sent from mempool which always
			// processes transactions singularly.
			// TODO(midas): define AckTransactionBroadcast.TxHash instead
			txHash := fmt.Sprintf("%X", ackTxBroadcast.TxHashes[0])
			shouldProcessAckTx := false

			// If request was already noted for this tx, we should not wait.
			r.poolRequestsMtx.Lock()
			if peers, ok := r.poolRequestsSent[txHash]; ok {
				shouldProcessAckTx = slices.Contains(peers, relayId)
				r.poolRequestsSent[txHash] = slices.DeleteFunc(peers, func(s string) bool {
					return s == relayId
				})
			}
			r.poolRequestsMtx.Unlock()

			if shouldProcessAckTx {
				// TODO(midas): remove debug logs
				r.logger.Debug("Received AckTransactionBroadcast from remote relay",
					"relay_id", ackTxBroadcast.NodeId,
					"num_txes", len(txHashes),
					"txes", txHashes,
					"node_self", r.nodeKey.ID(),
					"from_peer", sourceAddr,
				)

				// Channel is mapped by transaction hash
				acceptChForTxHash := r.ChannelForAckTransaction(txHash)
				acceptChForTxHash <- ackTxBroadcast
			} else {
				// TODO(midas): remove debug logs
				r.logger.Debug("Skipping already processed AckTransactionBroadcast",
					"num_txs", len(txHashes),
					"relay_id", ackTxBroadcast.NodeId,
					"txes", txHashes,
					"node", r.nodeKey.ID(),
					"from", sourceAddr,
				)
			}
			// Done.
			return

		default:
			r.logger.Error(
				"Unknown internal receipt type",
				"src", e.Src,
				"chId", e.ChannelID,
				"msg", e.Message,
			)
			return
		}

	default:
		r.logger.Error(
			"Unknown message type",
			"src", e.Src,
			"chId", e.ChannelID,
			"msg", e.Message,
		)
		return
	}
}

// DialReplicationPartner uses dialWithSw to dial a relay using its address
// partnerAddr. Importantly, it will dial the port as passed with partnerAddr.
//
// Upon receiving a duplicate or already dialed error, we initialize the
// peer with reactors in case the switch was already running.
func (r *Reactor) DialReplicationPartner(
	dialWithSw *p2p.Switch,
	partnerAddr *server.RelayAddress,
	chainID string,
) error {
	// Parse the address into a NetAddress
	peerAddr, err := partnerAddr.NetAddress()
	if err != nil {
		return fmt.Errorf(
			"invalid cometbft relay address %s: %w", partnerAddr.String(), err)
	}

	// Storage/transports of switches are protected by networkMutex.
	r.networkMutex.RLock()
	defer r.networkMutex.RUnlock()

	// If the peer exists but is not running, cleanup before dialing.
	peerExist := dialWithSw.Peers(chainID).Get(peerAddr.ID)
	if peerExist != nil && !peerExist.IsRunning() {
		dialWithSw.StopPeerGracefully(peerExist)
	} else if peerExist != nil {
		// No dialing to do here, peer is running
		// Manually add peers when the switch was already running.
		dialWithSw.InitPeerForChain(peerExist, chainID)
		return nil
	}

	// Dial the peer for the given chainID
	if err := dialWithSw.DialPeerWithAddressAndChainID(peerAddr, chainID); err != nil {
		if !r.IsDialError(err) {
			// Not an error, handling a duplicate or existing address.
			dialedPeer := dialWithSw.Peers(chainID).Get(peerAddr.ID)

			// Manually add peers when the switch was already running.
			dialWithSw.InitPeerForChain(dialedPeer, chainID)
			err = nil
		}

		if err != nil {
			return fmt.Errorf(
				"could not dial relay %s: %w", peerAddr.DialString(), err)
		}
	}

	return nil
}

// DialBackReplicationPartner dials sourcePeer using its' NetAddressForCometBFT,
// i.e. `DiscoveryPort+1`.
//
// Parses the remote relay address, i.e. the source of a replication
// request, because we dial their CometBFT P2P address for block-sync.
// Note that sourcePeer may contain a secret connection port and must
// not be used as the DiscoveryPort to determine CometBFT ports.
// To find the correct discovery port when dialing *back* ("responding"),
// we must query the RelayInfo RPC and use the returned `DiscoveryPort`.
func (r *Reactor) DialBackReplicationPartner(
	sourcePeer p2p.Peer,
	chainID string,
) error {
	// Determine "remote relay address" for discovery, this address is used
	// to determine the correct relay address for CometBFT.
	discoveryAddr, err := r.GetRemoteDiscoveryAddress(sourcePeer)
	if err != nil {
		return fmt.Errorf(
			"failed to determine remote discovery address for %s: %w", string(sourcePeer.ID()), err)
	}

	// We can now safely use discoveryAddr as it contains `DiscoveryPort`
	// of the relay and we need `DiscoveryPort+1` to interact with CometBFT.
	cometbftAddr, err := server.NewRelayAddress(discoveryAddr.AddressForCometBFT())
	if err != nil {
		return fmt.Errorf(
			"invalid cometbft relay address %s: %w", discoveryAddr.AddressForCometBFT(), err)
	}

	r.networkMutex.RLock()
	cometbftSwitch := r.cometbftSwitch
	r.networkMutex.RUnlock()

	// Uses `DiscoveryPort+1`
	return r.DialReplicationPartner(cometbftSwitch, cometbftAddr, chainID)
}

func (r *Reactor) IsDialError(err error) bool {
	return p2p.IsDialError(err)
}

// GetRemoteRelayInfo connects to relayAddress using a JSONRPC client,
// and calls the GetRelayInfo remote procedure to retrieve the Relay ID,
// the supported networks and the listen address for the remote relay.
//
// The relayAddress parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
func (r *Reactor) GetRemoteRelayInfo(
	clientCtx context.Context,
	relayAddress *server.RelayAddress,
	requestTimeout time.Duration,
) (*server.RPCResultRelayInfo, *http.Client, error) {
	c, connectErr := rpcclient.New(relayAddress.AddressForRelayInfo())
	if connectErr != nil {
		return nil, nil, connectErr
	}

	deadline := time.Now().Add(requestTimeout)
	// Each call should timeout after max requestTimeout.
	if ctxDeadline, withDeadline := clientCtx.Deadline(); withDeadline {
		deadline = ctxDeadline
	}
	timeoutCtx, cancelFn := context.WithDeadline(context.Background(), deadline)
	defer cancelFn()

	result := &server.RPCResultRelayInfo{}
	params := map[string]any{}
	_, callErr := c.Call(timeoutCtx, "info", params, result)

	select {
	// cancelled by caller
	case <-clientCtx.Done():
		cancelledErr := fmt.Errorf(
			"RelayInfo cancelled with %s", relayAddress.AddressForRelayInfo())
		r.logger.Error(cancelledErr.Error())
		return nil, nil, cancelledErr
	// context timeout (request took too long)
	case <-timeoutCtx.Done():
		timeoutErr := fmt.Errorf(
			"RelayInfo timed out with %s", relayAddress.AddressForRelayInfo())
		r.logger.Error(timeoutErr.Error())
		return nil, nil, timeoutErr
	default:
	}

	if callErr != nil {
		return nil, nil, callErr
	}

	return result, c.GetHTTPClient(), nil
}

// GetRemoteDiscoveryAddress calls the RelayInfo remote procedure for sourcePeer
// to determine its' discovery address and networks information.
func (r *Reactor) GetRemoteDiscoveryAddress(
	sourcePeer p2p.Peer,
) (*server.RelayAddress, error) {
	// Note that publicAddr may contain a secret connection port and must
	// not be used as the DiscoveryPort to determine CometBFT ports.
	publicAddr, err := sourcePeer.NodeInfo().NetAddress()
	if err != nil {
		return nil, fmt.Errorf(
			"invalid replication source address %s: %w", sourcePeer.SocketAddr(), err)
	}
	// CAUTION: do not use as `DiscoveryPort`, may contain secret conn port.
	sourceAddr, err := server.NewRelayAddress(publicAddr.String())
	if err != nil {
		return nil, fmt.Errorf(
			"invalid replication source relay address %s: %w", publicAddr.String(), err)
	}

	// Check if we have a RelayInfo and already know this peer.
	r.networkMutex.RLock()
	partnerRelayInfo, hasRelayInfo := r.knownRelayInfo[string(sourceAddr.ID())]
	r.networkMutex.RUnlock()

	var discoveryPort uint16

	// We may first need to call the RelayInfo RPC, to find DiscoveryPort.
	if !hasRelayInfo {
		relayInfo, _, infoErr := r.GetRemoteRelayInfo(
			context.TODO(),
			sourceAddr,
			r.relayInfoTimeout,
		)
		if infoErr != nil {
			return nil, infoErr
		}

		r.networkMutex.Lock()
		r.knownRelayInfo[string(sourceAddr.ID())] = relayInfo
		r.networkMutex.Unlock()

		discoveryPort = relayInfo.DiscoveryPort
	} else {
		discoveryPort = partnerRelayInfo.DiscoveryPort
	}

	// Now we know which port is the discovery port on this relay.
	sourceAddr.SetPort(discoveryPort)

	// We can now safely use sourceAddr as it contains `DiscoveryPort` of the relay.
	discoveryAddr, err := server.NewRelayAddress(sourceAddr.String())
	if err != nil {
		return nil, fmt.Errorf(
			"invalid discovery relay address %s: %w", publicAddr.String(), err)
	}

	return discoveryAddr, nil
}

// ----------------------------------------------------------------------------
// Reactor implements [cmtlibs.Service]

// OnStart starts the multiplex reactor and must initialize the filesystem and
// database instances, as well as the block and state stores such that after being
// started, the reactor can be used to configure the running node services.
//
// A custom deep-copied [*config.Config] is prepare for each replicated chain,
// and when all configuration is ready for a particular network, this method
// writes a message with the ChainID on its channel `chainReadyCh`.
//
// This method registers instances in the multiplexRegistry:
// - `config`: the configuration overwrite for each network.
// - `storage`: the filesystem paths for each network.
// - `state`: the [sm.State] state machine instances (InitMultiplexStates).
// - `stateStore`: the [sm.Store] instance attached (InitMultiplexStates).
// - `database/blockstore`: the blockstore databases (initMultiplexDatabases).
// - `database/state`: the state machine databases (initMultiplexDatabases).
// - `database/tx_index`: the tx_index databases (initMultiplexDatabases).
// - `database/evidence`: the evidence databases (initMultiplexDatabases).
// - `privValidator`: the PrivValidator instance (startNodeListeners).
//
// This method also registers services in the servicesRegistry:
// - `eventBus`: the event bus for block events (startNodeListeners).
// - `indexers`: the transaction- and block indexers service (startNodeListeners).
//
// CAUTION: This method spawns one new goroutine for every replicated chain.
func (reactor *Reactor) OnStart() error {
	nodeConfig := reactor.GetNodeConfig()
	chainRegistry := reactor.GetChainRegistry()

	// Initialize filesystem directory structure
	multiplexFS, err := NewMultiplexFS(nodeConfig, chainRegistry)
	if err != nil {
		return err
	}

	// Update the internal storagePaths
	reactor.SetStoragePaths(multiplexFS)

	// Open databases for: state, blockstore, tx_index, evidence
	// Then load state machines from database or genesis doc
	// And initialize block stores per replicated chain.
	if err := reactor.loadMultiplexState(); err != nil {
		return err
	}

	// For each ChainID, we run a node with a distinct listen address
	chainIds := reactor.GetNetworks()
	for _, chainID := range chainIds {
		configOverwrite, err := NewConfigOverwrite(
			nodeConfig,
			chainRegistry,
			chainID,
		)
		if err != nil {
			return err
		}

		reactor.RegisterInstance(InstanceKeyConfig, chainID, configOverwrite)
		reactor.RegisterInstance(InstanceKeyStorage, chainID, multiplexFS[chainID])

		reactor.chainReadyMtx.Lock()
		reactor.chainReadyChs[chainID] = make(chan bool, 1)
		reactor.chainReadyMtx.Unlock()

		// Non-blocking execution using different goroutine
		// i.e. one goroutine spawned per each replicated chain
		go func(network string) {
			// lock the filesystem mutex while creating priv val (fs)
			reactor.runtimesMutex.Lock()
			defer reactor.runtimesMutex.Unlock()

			// Start node listeners
			if err := reactor.startNodeListeners(network); err != nil {
				reactor.logger.Error(
					"error starting node runtime",
					"chain_id", network,
					"err", err,
				)
			}

			reactor.chainReadyMtx.RLock()
			chainReadyCh := reactor.chainReadyChs[chainID]
			reactor.chainReadyMtx.RUnlock()

			// Done starting node listeners
			chainReadyCh <- true
		}(chainID)
	}

	return nil
}

// OnStop stops the multiplex reactor and all the registered services that
// are running, including the ABCI client if it is running.
//
// A *reversed* services sequence is used to implement the LIFO strategy when
// shutting down services as it is usual for services that are started last
// to be using previously created instances of other services.
//
// As for the database multiplexes, they are used in an unordered format and
// no specific order is used to close the database connection because every
// database connection is independent of other database connections.
func (reactor *Reactor) OnStop() {
	// Shutdown all network resources atomically
	reactor.networkMutex.Lock()

	// Stop the P2P Discovery Server that is injected
	if reactor.discoverySwitch != nil && reactor.discoverySwitch.IsRunning() {
		// Must stop listening for P2P messages on broadcast port
		if ts := reactor.discoverySwitch.Transport(); ts != nil {
			ts.Close()
		}

		// Must stop reactors and listener channels
		reactor.discoverySwitch.Stop()
		reactor.discoverySwitch = nil
	}

	// Stop the P2P CometBFT Server that is injected
	if reactor.cometbftSwitch != nil && reactor.cometbftSwitch.IsRunning() {
		if ts := reactor.cometbftSwitch.Transport(); ts != nil {
			ts.Close()
		}

		reactor.cometbftSwitch.Stop()
		reactor.cometbftSwitch = nil
	}

	// Done shutting down network resources
	reactor.networkMutex.Unlock()

	// Shutdown all registered services atomically
	reactor.servicesMutex.RLock()

	// Uses LIFO strategy to shutdown registered services
	servicesLIFO := reactor.servicesSequence[:]
	sort.Sort(sort.Reverse(sort.StringSlice(
		servicesLIFO,
	)))

	for _, serviceName := range servicesLIFO {
		// Services multiplex contains one instance per ChainID
		servicesMultiplex := reactor.servicesRegistry[serviceName]

		// Every service instance must be stopped if running
		for _, chainService := range servicesMultiplex {
			service := chainService.GetInstance().(cmtlibs.Service)
			if service.IsRunning() {
				service.Stop()
			}
		}
	}
	reactor.servicesMutex.RUnlock()

	// Now nothing may perturb shutting down ABCI anymore.
	// Shutdown the ABCI client if running
	reactor.envMutex.Lock()
	if reactor.abciClient != nil && reactor.abciClient.IsRunning() {
		if err := reactor.abciClient.Stop(); err != nil {
			reactor.logger.Error(
				"Error stopping the ABCI client", "err", err)
		}
	}
	reactor.envMutex.Unlock()

	// ABCI is shutdown, we can safely close the replay pool.
	reactor.replayPoolMtx.Lock()
	if reactor.replayPool != nil && reactor.replayPool.IsRunning() {
		if err := reactor.replayPool.Stop(); err != nil {
			reactor.logger.Error(
				"Error stopping the replay pool", "err", err)
		}
	}
	reactor.replayPoolMtx.Unlock()

	// Each database multiplex opens x dbs, no ordering or reversing is
	// applied here as it doesn't matter which database is closed first.
	dbKeys := []string{
		InstanceKeyDatabaseBlock,
		InstanceKeyDatabaseState,
		InstanceKeyDatabaseIndex,
		InstanceKeyDatabaseEvidence,
	}

	// Close database connections
	for _, dbMultiplexKey := range dbKeys {
		// Close all open database connections individually
		reactor.multiplexMutex.RLock()

		// Instances multiplex contains one instance per ChainID
		if mx, ok := reactor.multiplexRegistry[dbMultiplexKey]; ok {
			// Every database connection must be stopped
			for _, chainInstance := range mx {
				db := chainInstance.GetInstance().(dbm.DB)
				db.Close()
			}
		}
		reactor.multiplexMutex.RUnlock()
	}

	// and close internal channels
	reactor.chainReadyMtx.RLock()
	restChainReadyChs := reactor.chainReadyChs
	reactor.chainReadyMtx.RUnlock()

	for _, chainReadyCh := range restChainReadyChs {
		close(chainReadyCh)
	}

	reactor.ackReplResMtx.RLock()
	restAckReplResChs := reactor.ackReplResChs
	reactor.ackReplResMtx.RUnlock()

	for chainID, _ := range restAckReplResChs {
		reactor.CloseAckReplicationChannel(chainID)
	}

	reactor.ackAcceptTxMtx.RLock()
	restAckAcceptTxChs := reactor.ackAcceptTxChs
	reactor.ackAcceptTxMtx.RUnlock()

	for txHash, _ := range restAckAcceptTxChs {
		reactor.CloseAckTransactionChannel(txHash)
	}

	reactor.runtimeUpdatesMtx.RLock()
	restRuntimeUpdatesChs := reactor.runtimeUpdatesChs
	reactor.runtimeUpdatesMtx.RUnlock()

	for chainID, _ := range restRuntimeUpdatesChs {
		reactor.CloseRuntimeUpdatesChannel(chainID)
	}
}

// OnReset implements Service.
func (reactor *Reactor) OnReset() error {
	reactor.logger.Debug("Reset multiplex reactor")
	return nil
}

// WaitForNetworks waits for *all* configured networks to be readily configured.
// This method expects updates on the chainReadyCh private channel for each
// of the configured replicated chains. A [sync.WaitGroup] is used.
//
// TODO(midas): add timeout functionality in case some networks are stuck?
func (reactor *Reactor) WaitForNetworks() error {
	reactor.chainReadyMtx.RLock()
	chainReadyChs := reactor.chainReadyChs
	reactor.chainReadyMtx.RUnlock()

	var wg sync.WaitGroup
	wg.Add(len(chainReadyChs))

	// Waits for all nodes to be configured
	for _, chainReadyCh := range chainReadyChs {
		go func(ch chan bool) {
			// The multiplex reactor communicates the ChainID on a channel
			// to tell about the readiness of an individual network config
			<-ch
			wg.Done() // one network is configured
		}(chainReadyCh)
	}

	// Waits for all networks to be ready (order doesn't matter).
	wg.Wait()
	return nil
}

// -----------------------------------------------------------------------------
// Reactor private implementation

// initMultiplexProviders initializes the genesisDocProvider around icsGenesisDocSet,
// and further initializes the services provider and multiplex providereactor.
//
// Note that providers always use *read-only locks* for the respective mutexes.
// Note also, that registries must be allocated separately.
func (reactor *Reactor) initMultiplexProviders(
	icsGenesisDocSet node.IChecksummedGenesisDoc,
) {
	// Use the services registry to load node services
	reactor.servicesProvider = func(serviceName string, chainId string) cmtlibs.Service {
		reactor.servicesMutex.RLock()
		defer reactor.servicesMutex.RUnlock()

		if _, ok := reactor.servicesRegistry[serviceName]; !ok {
			// allocate in-place
			reactor.servicesRegistry[serviceName] = MultiplexMap[cmtlibs.Service]{}
		}

		if _, ok := reactor.servicesRegistry[serviceName][chainId]; !ok {
			return nil
		}

		return reactor.servicesRegistry[serviceName][chainId].GetInstance().(cmtlibs.Service)
	}

	// Use the multiplex registry to load node services
	reactor.multiplexProvider = func(multiplexName string) MultiplexMap[any] {
		reactor.multiplexMutex.RLock()
		defer reactor.multiplexMutex.RUnlock()

		if _, ok := reactor.multiplexRegistry[multiplexName]; !ok {
			// allocate in-place
			reactor.multiplexRegistry[multiplexName] = MultiplexMap[any]{}
		}

		return reactor.multiplexRegistry[multiplexName]
	}
}

// initMultiplexDatabases initializes database tables for each replicated
// chain with table names: blockstore, state, tx_index and evidence.
//
// This method registers instances in the multiplexRegistry:
// - `database/blockstore`: the blockstore databases.
// - `database/state`: the state machine databases.
// - `database/tx_index`: the tx_index databases.
// - `database/evidence`: the evidence databases.
//
// TODO(midas): refactoring with MakeNetworkDatabases.
func (reactor *Reactor) initMultiplexDatabases() error {
	nodeConfig := reactor.GetNodeConfig()
	chainRegistry := reactor.GetChainRegistry()

	// Create blockstore databases
	bsMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "blockstore", Config: nodeConfig},
	}, chainRegistry)
	if err != nil {
		return err
	}

	// Create state databases
	stateMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "state", Config: nodeConfig},
	}, chainRegistry)
	if err != nil {
		return err
	}

	// Create indexer databases
	indexerMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "tx_index", Config: nodeConfig},
	}, chainRegistry)
	if err != nil {
		return err
	}

	// Create evidence databases
	evidenceMultiplexDB, err := NewMultiplexDB(&ChainDBContext{
		DBContext: config.DBContext{ID: "evidence", Config: nodeConfig},
	}, chainRegistry)
	if err != nil {
		return err
	}

	// Register the database instances with the Reactor (thread-safe)
	chainIds := reactor.GetNetworks()
	for _, chainID := range chainIds {
		reactor.RegisterInstance(InstanceKeyDatabaseBlock, chainID, bsMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseState, chainID, stateMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseIndex, chainID, indexerMultiplexDB[chainID])
		reactor.RegisterInstance(InstanceKeyDatabaseEvidence, chainID, evidenceMultiplexDB[chainID])
	}

	return nil
}

// loadMultiplexState opens the multiplex databases for multiple
// contexts: state, blockstore, indexer and evidence. Then loads state
// machines from database, config or genesis doc and initialize stores.
//
// This method registers instances in the multiplexRegistry:
// - `state`: the [sm.State] state machine instances.
// - `stateStore`: the [sm.Store] instance attached to the database.
// - `blockstore`: the created/opened block stores.
//
// TODO(midas): add multiplex metric "MultiplexStateLoadDurationSeconds".
// TODO(midas): refactoring with MakeNetworkStateMachine.
func (reactor *Reactor) loadMultiplexState() error {
	// Initialize database tables and instances
	err := reactor.initMultiplexDatabases()
	if err != nil {
		return err
	}

	// Load initial state multiplex from database or from genesis docs
	// Uses "database/state" instances
	err = reactor.InitMultiplexStates()
	if err != nil {
		return err
	}

	// Create a blockstore multiplex around "database/blockstore" instances
	// Uses "database/blockStore" instances
	err = reactor.InitMultiplexBlockStores()
	if err != nil {
		return err
	}

	return nil
}

// startNodeListeners is called in a newly spawned goroutine and is responsible
// for starting the following node listeners:
//
// - the event bus for block events [types.EventBus] ;
// - the transaction- and block indexers [txindex.IndexerService] ;
// - the priv validator (signer) instance [types.PrivValidator] ;
//
// This method registers instances in the multiplexRegistry:
// - `privValidator`: the PrivValidator instance.
//
// This method registers services in the servicesRegistry:
// - `eventBus`: the event bus for block events.
// - `indexers`: the transaction- and block indexers service.
//
// The caller must make sure about thread-safety of filesystem operations,
// i.e. caller should always lock runtimesMutex during call.
func (reactor *Reactor) startNodeListeners(chainID string) error {
	clogger := reactor.logger.With("chain_id", chainID)
	nodeKey := reactor.GetNodeKey()

	// Retrieve the node's config overwrite object
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	stateStoreProvider := reactor.GetInstanceProvider(InstanceKeyStateStore)
	blockStoreProvider := reactor.GetInstanceProvider(InstanceKeyBlockStore)

	// Casting to ChainInstance before is required because the *instanceProviderFn*
	// implementation provides a `any` typed variable which is not an interface.
	nodeConfig := configProvider(chainID).(*config.Config)
	stateStore := stateStoreProvider(chainID).(sm.Store)
	blockStore := blockStoreProvider(chainID).(*bs.BlockStore)

	// We can safely ignore the error as we know an address is available.
	userAddress, _ := reactor.chainRegistry.GetAddress(chainID)
	userConfDir := filepath.Join(nodeConfig.RootDir, config.DefaultConfigDir, userAddress)
	userDataDir := filepath.Join(nodeConfig.RootDir, config.DefaultDataDir, userAddress)

	// Prometheus does not allow hyphens in metrics names, it must match
	// following regexp: [a-zA-Z_:][a-zA-Z0-9_:]*
	// see also: https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels
	metricsNames := nodeConfig.Instrumentation.Namespace + "_" + string(nodeKey.ID()) + ":" + strings.ReplaceAll(chainID, "-", "_")
	stateMetricsProvider := reactor.RegisterMetrics("state", metricsNames, func() interface{} {
		return sm.PrometheusMetrics(metricsNames, "chain_id", chainID)
	}).(*sm.Metrics)

	// 1) Event Bus Service
	eventBus := types.NewEventBus()
	eventBus.SetLogger(clogger.With("module", "events"))
	if err := eventBus.Start(); err != nil {
		return fmt.Errorf("error starting event bus: %w", err)
	}

	// 2) Priv Validator Service
	//
	// Uses a separate priv validator for each supported network to prevent
	// signing blocks with the same private key multiple times.
	//
	// TODO(midas): Add compatibility for PrivValidator as external socket client.
	// Currently it's not possible to use external socket client as
	// PrivValidator and we ignore Config.PrivValidatorListenAddr
	privValKeyDir := filepath.Join(userConfDir, chainID)   // config/
	privValStateDir := filepath.Join(userDataDir, chainID) // data/
	privValidator, err := privval.LoadOrGenFilePV(
		filepath.Join(privValKeyDir, filepath.Base(nodeConfig.PrivValidatorKeyFile())),
		filepath.Join(privValStateDir, filepath.Base(nodeConfig.PrivValidatorStateFile())),
		func() (crypto.PrivKey, error) {
			return ed25519.GenPrivKey(), nil
		},
	)
	if err != nil {
		return err
	}

	// 3) Blocks and Transactions Indexers
	//
	// TODO(midas): Add per-chain postgresql indexer compatibility, currently only support kv.
	// The scoped indexer functionality is compatible only with the `kv` indexer for now,
	// postgresql compatibility must be added. Appending the scope hash to the chainID
	// in the NewEventSink() call may be enough to allow multiple indexers instances.
	var (
		txIndexer    txindex.TxIndexer
		blockIndexer indexer.BlockIndexer
	)
	if nodeConfig.TxIndex.Indexer == "kv" {
		databaseProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseIndex)
		indexerDatabase := databaseProvider(chainID).(dbm.DB)

		txIndexer = txidxkv.NewTxIndex(indexerDatabase)
		blockIndexer = blockidxkv.New(
			dbm.NewPrefixDB(indexerDatabase, []byte("block_events")),
			blockidxkv.WithCompaction(nodeConfig.Storage.Compact, nodeConfig.Storage.CompactionInterval),
		)
	} else {
		txIndexer = &txidxnull.TxIndex{}
		blockIndexer = &blockidxnull.BlockerIndexer{}
	}

	indexerService := txindex.NewIndexerService(txIndexer, blockIndexer, eventBus, false) // stopOnError
	indexerService.SetLogger(clogger.With("module", "txindex"))
	if err := indexerService.Start(); err != nil {
		return fmt.Errorf("error starting indexers: %w", err)
	}

	// 4) Storage pruner
	//
	// Creates a pruner with interval. Note that ABCI responses are not pruned
	// due to the multiplex features disabling the data companion all along.
	//
	// More generally, the multiplex features *do not permit* pruning of blocks
	// and this implementation disables pruning by setting a retain height of 0.
	if err := stateStore.SaveApplicationRetainHeight(0); err != nil {
		return fmt.Errorf("could not save application retain height: %w", err)
	}

	prunerOpts := []sm.PrunerOption{
		sm.WithPrunerInterval(nodeConfig.Storage.Pruning.Interval),
		sm.WithPrunerMetrics(stateMetricsProvider),
	}
	pruner := sm.NewPruner(
		stateStore,
		blockStore,
		blockIndexer,
		txIndexer,
		clogger.With("module", "state"),
		prunerOpts...,
	)

	// Register the services and instances with the Reactor
	reactor.RegisterInstance(InstanceKeyPrivValidator, chainID, privValidator)
	reactor.RegisterService(ServiceKeyEventBus, chainID, eventBus)
	reactor.RegisterService(ServiceKeyIndexers, chainID, indexerService)
	reactor.RegisterService(ServiceKeyPruner, chainID, pruner)
	return nil
}

// sendChainReplicationResponse sends a ChainReplicationResponse.
// This response object may be used to determine that a relay acknowledges
// the replication of a chain it doesn't know yet.
func (reactor *Reactor) sendChainReplicationResponse(
	sourcePeer p2p.Peer,
	chainID string,
) error {
	myPeerID := reactor.GetNodeKey().ID()

	sendResponseToPeer := func(fromID p2p.ID, toPeer p2p.Peer, withChainID string) error {
		if success := toPeer.Send(withChainID, p2p.Envelope{
			ChannelID: server.ReplicationChannel,
			Message: &mxp2p.Message{
				Sum: &mxp2p.Message_ChainReplicationResponse{
					ChainReplicationResponse: &mxp2p.ChainReplicationResponse{
						ChainID: withChainID,
						NodeId:  string(fromID),
					},
				},
			},
		}); !success {
			return fmt.Errorf(
				"could not send message to peer, sender: %s, recipient: %s",
				string(fromID), string(toPeer.ID()))
		}
		return nil
	}

	discoverySwitch := reactor.GetEventSwitchForDiscovery()
	peerOut := discoverySwitch.UniquePeers().Get(sourcePeer.ID())
	if peerOut == nil {
		// Open a OUTBOUND connection to this peer. This is required to send back
		// a ChainReplicationResponse message.
		peerAddr, _ := sourcePeer.NodeInfo().NetAddress()
		if err := discoverySwitch.DialPeerWithAddressAndChainID(peerAddr, chainID); err != nil {
			if p2p.IsDialError(err) {
				reactor.logger.Error("Failed to open outbound connection to discovery peer",
					"err", err,
					"peer", peerAddr.String(),
					"chain_id", chainID,
				)
				return errors.New("outbound peer connection cannot be opened")
			}
		}

		peerOut = discoverySwitch.Peers(chainID).GetOutbound(peerAddr.ID)
		if peerOut == nil {
			reactor.logger.Error("Failed to find outbound connection to discovery peer",
				"peer", peerAddr.String(),
				"chain_id", chainID,
			)
			return errors.New("outbound peer connection is not ready")
		}
	}

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Sending ChainReplicationResponse to peer",
		"from_id", myPeerID,
		"peer_id", sourcePeer.ID(),
		"peer", peerOut,
		"chain_id", chainID,
	)

	// We send the response using the discovery switch. Note that dialing
	// the peer *must* have happened before.
	if err := sendResponseToPeer(myPeerID, peerOut, chainID); err != nil {
		return err
	}

	return errors.New("outbound peer connection is not ready")
}

// handleChainReplicationRequest processes a ChainReplicationRequest.
// This method allocates the resources necessary to spawn a NEW thread which
// consists of running a complete node runtime. It will inject the parameters
// necessary for blocks production and it will spawn a parallel goroutine with
// a call to [node.Node#Start].
//
// Note that the source peer will be dialed to accelerate the activation
// of the block-sync process with this peer.
func (reactor *Reactor) handleChainReplicationRequest(
	source p2p.Peer,
	req *mxp2p.ChainReplicationRequest,
) error {
	// Build the ExtendedChainID to retrieve user address from ChainID.
	extChainID, err := NewExtendedChainIDFromLegacy(req.ChainID)
	if err != nil {
		return fmt.Errorf(
			"invalid ChainID %s: %w", req.ChainID, err)
	}
	userAddress := extChainID.GetUserAddress()

	// Pre-allocates filesystem, database and priv validator.
	if err := reactor.AllocateNetwork(req.ChainID); err != nil {
		return fmt.Errorf(
			"could not allocate network resources: %w", err)
	}

	// Initialize the network genesis parameters
	genesisDoc, err := GenesisDocFromChainParams(req.GetChainParams())
	if err != nil {
		return fmt.Errorf(
			"invalid genesis parameters: %w", err)
	}

	// Config folder is created in AllocateNetwork
	reactor.runtimesMutex.RLock()
	newConfDir := reactor.configsPaths[req.ChainID]
	reactor.runtimesMutex.RUnlock()

	icsGenesisDocSet, err := reactor.InjectGenesisDoc(req.ChainID, newConfDir, genesisDoc)
	if err != nil {
		return fmt.Errorf(
			"could not inject genesis doc: %w", err)
	}

	// Initialize the state machine and block store
	if err := reactor.InjectStateMachine(req.ChainID, icsGenesisDocSet); err != nil {
		return fmt.Errorf(
			"could not inject state machine: %w", err)
	}

	// Initialize custom configuration overwrites (ports, fs, etc.)
	configOverwrite, err := reactor.MakeNetworkConfigOverwrite(extChainID)
	if err != nil {
		return fmt.Errorf(
			"could not create config overwrite: %w", err)
	}

	// Inject the new ChainID in the running reactor.
	reactor.RegisterInstance(InstanceKeyConfig, req.ChainID, configOverwrite)

	// This call updates the internal chainRegistry, nodeInfo and ABCI.
	if err = reactor.RegisterNetwork(userAddress, req.ChainID); err != nil {
		return fmt.Errorf(
			"could not register new ChainID: %w", err)
	}

	// Inject a *running* node.Node for the new network.
	// TODO(midas): currently not passing any node options.
	if err = reactor.InjectNewRuntime(context.Background(), req.ChainID); err != nil {
		return fmt.Errorf(
			"could not spawn node runtime: %w", err)
	}

	return nil
}

// EnableNewRuntimeRPC adds RPC routes for networks in a running
// http request multiplexer.
//
// TODO(midas): TBI whether the server must be restarted.
func (reactor *Reactor) EnableNewRuntimeRPC(networks []string) error {
	reactor.networkMutex.RLock()
	rpcMultiplexer := reactor.rpcMultiplexer
	reactor.networkMutex.RUnlock()
	if rpcMultiplexer == nil {
		return errors.New(
			"could not enable RPC runtime, missing multiplexer")
	}

	reactor.envMutex.RLock()
	nodeCfg := reactor.nodeConfig
	reactor.envMutex.RUnlock()

	// We configure one RPC environment per running network,
	// i.e. contains reactors, stores and genesis.
	nodesProvider := reactor.GetServicesProvider()
	chainRoutes := map[string]rpccore.RoutesMap{}
	for _, chainID := range networks {
		nodeRuntime, ok := nodesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		if !ok {
			return fmt.Errorf(
				"could not get node runtime in EnableNewRuntimeRPC with ChainID %s", chainID)
		}

		env, err := nodeRuntime.ConfigureRPC()
		if err != nil {
			return fmt.Errorf(
				"could not create RPC environment with ChainID %s: %w", chainID, err)
		}

		nodeRoutes := env.GetRoutes()
		if nodeCfg.RPC.Unsafe {
			env.AddUnsafeRoutes(nodeRoutes)
		}

		chainRoutes[chainID] = nodeRoutes
	}

	// Each network's ChainID is appended to the route name.
	// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
	newRoutes := rpccore.RoutesMap{}
	for chainID, nodeRoutes := range chainRoutes {
		for route, rpcFunc := range nodeRoutes {
			routeKey := route + "/" + chainID

			reactor.networkMutex.RLock()
			_, hasRoute := reactor.rpcRoutes[routeKey]
			reactor.networkMutex.RUnlock()

			if hasRoute {
				continue
			}

			newRoutes[routeKey] = rpcFunc

			reactor.networkMutex.Lock()
			reactor.rpcRoutes[routeKey] = true
			reactor.networkMutex.Unlock()
		}
	}

	reactor.networkMutex.Lock()
	defer reactor.networkMutex.Unlock()
	rpcserver.RegisterAddedRPCFuncs(
		rpcMultiplexer,
		newRoutes,
		reactor.logger.With("module", "rpc-server"),
	)

	return nil
}
