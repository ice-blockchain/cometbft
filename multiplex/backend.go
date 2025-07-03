package multiplex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/libs/service"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
	"github.com/ice-blockchain/cometbft/state/txindex"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
	"github.com/rs/cors"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

const (
	// Collecting metrics every 10 seconds, this may need to be adapted
	// to equal the Prometheus scrape interval (1s) for better granularity.
	metricsTickerDuration = 10 * time.Second

	// Prometheus timeout configuration
	readHeaderTimeout = 10 * time.Second

	// Transaction events timeout configuration. This duration defines the
	// maximum waiting time for transactions to appear in our indexer.
	// Used as a failsafe to stop [WaitForTransactionEvents] from waiting
	// for transactions forever upon completion of broadcast operations.
	DefaultTransactionTimeout = 60 * time.Second

	// Remote replication timeout configuration. This duration defines the
	// maximum waiting time for remote replication to complete.
	// Used as a failsafe to stop [WaitForRelaysReplicationCompleted] from
	// waiting for runtime updates forever.
	//
	// Using a timeout of 2 hours permits to cover for networks that grow
	// above of 2 million blocks with a blocksync range of 200-400 blocks.
	//
	// NOTE(midas): For a production environment, it is recommended to set
	// this timeout to 0 using `WithReplicationTimeout(0)`.
	DefaultReplicationTimeout = 2 * time.Hour
)

// Assert that our implementation satisfies the [server.Backend] interface.
var _ server.Backend = (*MultiplexBackend)(nil)

// AckTransactionResult describes a remote transaction receipt.
// Used by [MultiplexBackend#WaitForRelaysAckTransactionBatch].
type AckTransactionResult struct {
	// Contains relay IDs of relays that acknowledged TxHash.
	Relays []string

	// Contains a transaction hash in hexadecimal.
	TxHash string

	// May contain an error
	Error error
}

// AckReplicationResult describes a remote replication receipt.
// Used by [MultiplexBackend#WaitForRelaysAckChainReplications].
type AckReplicationResult struct {
	// Contains relay IDs of relays that acknowledged ChainID.
	Relays  []string
	ChainID string

	// May contain an error
	Error error
}

// RuntimeUpdateResult describes a remote status update result.
// Used by [MultiplexBackend#WaitForRelaysReplicationCompleted].
type RuntimeUpdateResult struct {
	// Contains relay IDs of relays that have announced the completion
	// of their replication for ChainID.
	Relays  []string
	ChainID string

	// May contain an error
	Error error
}

// TransactionEventResult describes a transaction event result.
// Used by [MultiplexBackend#WaitForTransactionsEvents].
type TransactionEventResult struct {
	// Contains transaction hashes in hex format for transactions
	// which have been included in a block on ChainID.
	TxHashes []string
	ChainID  string

	// May contain an error
	Error error
}

// ----------------------------------------------------------------------------
// MultiplexBackend defines a multiplex backend adapter implementation
//
// MultiplexBackend implements the [server.Backend] interface for a multiplex.
// This implementation makes use of an internal [Reactor] instance to read
// local networks heights and uses instances of [p2p.Switch] to communicate
// with relays about chain replications and transaction broadcasts.
//
// Additionally, an internal [client.Acceptor] instance may be used to further
// extend the broadcast process, e.g. to call RollbackTx.
type MultiplexBackend struct {
	*service.BaseService
	// A mutex is locked for reactor and eventSwitch updates.
	relayMtx sync.Mutex

	// The multiplex reactor is used to find ChainID, last block heights,
	// and to retrieve the AddrBook and connect to unknown relays.
	reactor *Reactor

	eventSwitch  *p2p.Switch
	rpcListeners []net.Listener
	httpServers  map[string]*http.Server
	httpClients  []*http.Client

	multiNodeInfo   *MultiNetworkNodeInfo
	broadcastAddr   *p2p.NetAddress // P2P Discovery (:dp)
	discoveryAddr   *p2p.NetAddress // RPC Discovery (:dp-1)
	cometbftP2PAddr *p2p.NetAddress // CometBFT P2P  (:dp+1)
	cometbftRPCAddr *p2p.NetAddress // CometBFT RPC  (:dp+2)
	prometheusAddr  *p2p.NetAddress // Prometheus (:dp+3)

	// Defines the duration for timeout of transaction completion.
	// Used in [MultiplexBackend#WaitForTransactionEvents]
	transactionTimeout time.Duration

	// Defines the duration for timeout of replication completion.
	// Used in [MultiplexBackend#OnBroadcastComplete]
	replicationTimeout           time.Duration
	useDefaultReplicationTimeout bool

	// A map of events subscriber names by ChainID.
	txSubscribers map[string]string

	// An acceptor implementation to which transactions will be forwarded.
	acceptor client.Acceptor

	// Routines may be extended directly using the [server.Jobs] struct.
	routines *server.Jobs

	// Mapping of replication partners relay IDs by ChainID.
	replRequestsMtx  sync.RWMutex
	replRequestsSent map[string][]string

	// Mapping of relay IDs whom responded to replication, by it's ChainID.
	replResponsesMtx  sync.RWMutex
	replResponsesRcvd map[string][]string

	// Mapping of relay IDs whom completed replication, by their ChainID.
	replCompleteMtx  sync.RWMutex
	replCompleteRcvd map[string][]string

	// Mapping of relay IDs whom ack'd a transaction, by its' hash.
	ackResponsesMtx  sync.RWMutex
	ackResponsesRcvd map[string][]string

	// Internals
	logger     cmtlog.Logger
	errorsCh   chan error
	shutdownCh chan struct{}
	metrics    *Metrics
}

type MultiplexBackendOption func(*MultiplexBackend)

// WithRoutines is an option helper to overwrite the [server.Jobs] instance.
func WithRoutines(jobs *server.Jobs) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.routines = jobs
	}
}

// WithMetrics is an option helper to overwrite the [Metrics] instance.
func WithMetrics(metrics *Metrics) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.metrics = metrics
	}
}

// WithLogger is an option helper to inject a custom backend logger.
func WithLogger(
	logger cmtlog.Logger,
) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.logger = logger
	}
}

// WithRequestTimeout is an option helper to inject a custom timeout duration.
func WithRequestTimeout(t time.Duration) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.reactor.SetOptions(WithRelayInfoTimeout(t))
	}
}

// WithTransactionTimeout is an option helper to inject a custom timeout duration.
func WithTransactionTimeout(t time.Duration) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.transactionTimeout = t
	}
}

// WithReplicationTimeout is an option helper to inject a custom timeout duration.
func WithReplicationTimeout(t time.Duration) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.replicationTimeout = t
		b.useDefaultReplicationTimeout = false
	}
}

// WithRuntimeRegistryOptions is an option helper to inject custom options in the
// reactor's [RuntimeRegistry] just after its instance is created.
func WithRuntimeRegistryOptions(regOpts ...server.RuntimeRegistryOption) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.reactor.SetRuntimeRegistryOptions(regOpts...)
	}
}

func WithReactorOptions(reactorOpts ...ReactorOption) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.reactor.SetOptions(reactorOpts...)
	}
}

// NewServer initializes a new [MultiplexBackend] around an empty multiplex
// configuration and prepares the node backend by starting the reactor and
// configuring the necessary node services, i.e. event bus, mempool, etc.
//
// The internal [Reactor] instance will be started when calling this method,
// and individual [node.Node] instances can be retrieved using the reactor's
// services registry: [Reactor#GetServicesProvider].
//
// The added reactorOptions slices permits to inject custom option helper
// calls in the [NewReactor] method.
//
// See also: [NewNodesMultiplex]
func NewServer(
	ctx context.Context,
	impl client.Acceptor,
	nodeConfig *config.Config,
	nodeLogger cmtlog.Logger,
	options ...MultiplexBackendOption,
) (*MultiplexBackend, error) {
	nodeConfig.Consensus.CreateEmptyBlocks = false // Force to create blocks only if there is transactions.
	nodeConfig.Consensus.TimeoutCommit = 0         // Make progress as soon as the node has all the precommits.
	nodeConfig.P2P.AllowDuplicateIP = true

	initTime := time.Now()
	_, reactor, err := NewNodesMultiplex(
		ctx,
		impl,
		nodeConfig,
		nodeLogger,
		node.NodeWithStartRPC(false),     // delegates to OnStart()
		node.NodeWithStartP2P(false),     // delegates to OnStart()
		node.NodeWithStartMonitor(false), // delegates to OnStart()
	)
	if err != nil {
		return nil, fmt.Errorf(
			"SERVER PANIC: could not initialize node backend: %w", err)
	}

	server := &MultiplexBackend{
		reactor:       reactor,
		acceptor:      impl,
		rpcListeners:  []net.Listener{},
		httpServers:   make(map[string]*http.Server),
		httpClients:   []*http.Client{},
		txSubscribers: map[string]string{},

		useDefaultReplicationTimeout: true,

		logger: nodeLogger,
	}
	// Enable overwrite of optional properties
	for _, option := range options {
		option(server)
	}

	if server.reactor.relayInfoTimeout == 0 {
		server.reactor.relayInfoTimeout = DefaultRequestTimeout
	}

	// Don't overwrite the 0 timeout if it was updated with options.
	if server.replicationTimeout == 0 && server.useDefaultReplicationTimeout {
		server.replicationTimeout = DefaultReplicationTimeout
	}

	if server.transactionTimeout == 0 {
		server.transactionTimeout = DefaultTransactionTimeout
	}

	if server.metrics != nil {
		defer addTimeSample(server.metrics.InitDurationSeconds, initTime)()
	}
	server.BaseService = service.NewBaseService(ctx, server.logger, "backend", server)
	return server, nil
}

// GetLogger returns the [cmtlog.Logger] property.
//
// GetLogger implements [server.Backend]
func (b *MultiplexBackend) GetLogger() cmtlog.Logger {
	return b.logger
}

// SetLogger sets a custom [cmtlog.Logger].
func (b *MultiplexBackend) SetLogger(logger cmtlog.Logger) {
	b.logger = logger
}

// GetAcceptor returns the injected [client.Acceptor] implementation.
//
// GetAcceptor implements [server.Server]
func (b *MultiplexBackend) GetAcceptor() client.Acceptor {
	if b.acceptor == nil {
		return &client.DefaultAcceptor{}
	}

	return b.acceptor
}

func (b *MultiplexBackend) SetAcceptor(acceptorImpl client.Acceptor) {
	b.acceptor = acceptorImpl
	b.reactor.SetAcceptor(acceptorImpl)
}

// GetReactor returns the [Reactor] instance.
func (b *MultiplexBackend) GetReactor() *Reactor {
	return b.reactor
}

// GetRuntimeRegistry should return the active node runtime manager.
//
// GetRuntimeRegistry implements [server.Backend]
func (b *MultiplexBackend) GetRuntimeRegistry() *server.RuntimeRegistry {
	b.reactor.runtimesMutex.Lock()
	defer b.reactor.runtimesMutex.Unlock()

	return b.reactor.runtimeRegistry
}

// GetRelayID returns the node ID assigned in the reactor.
//
// GetRelayID implements [server.Backend]
func (b *MultiplexBackend) GetRelayID() p2p.ID {
	return b.reactor.GetNodeKey().ID()
}

// GetListenAddress should return the relay's listen address.
//
// GetRelayID implements [server.Backend]
func (b *MultiplexBackend) GetListenAddress() string {
	return b.broadcastAddr.String()
}

// GetNetworks should return a slice of supported ChainID values.
func (b *MultiplexBackend) GetNetworks() []string {
	if b.reactor == nil {
		return []string{}
	}

	// Unlocks the reactor mutex before returning
	chainIds := b.reactor.GetNetworks()
	return chainIds
}

// GetDiscoveryPort returns the port used for broadcastAddr,
// i.e. it should map to the relay's discovery port.
func (b *MultiplexBackend) GetDiscoveryPort() uint16 {
	return b.broadcastAddr.Port
}

// GetValidatorPubs returns all validator pubkeys available per ChainID.
func (b *MultiplexBackend) GetValidatorPubs() map[string]string {
	// Locks reactor.multiplexMutex
	multiplex := b.reactor.multiplexProvider(InstanceKeyPrivValidator)

	// Read-lock this time, as we won't transition.
	b.reactor.multiplexMutex.RLock()
	defer b.reactor.multiplexMutex.RUnlock()

	validators := make(map[string]string, len(multiplex))
	for chainID, pvInstance := range multiplex {
		privValidator := pvInstance.GetInstance().(types.PrivValidator)

		// Make sure we can access the priv validator
		privValPubKey, err := privValidator.GetPubKey()
		if err != nil {
			b.logger.Error("failed to read public key from validator", "err", err)
			continue
		}

		validators[chainID] = fmt.Sprintf("%X", privValPubKey.Bytes())
	}
	return validators
}

// GetReplRequestPeers returns a list of node IDs to whom we have previously
// sent a ChainReplicationRequest.
// See also: [DefaultNodeReplRequestRoutine]
func (b *MultiplexBackend) GetReplRequestPeers(chainID string) []string {
	b.replRequestsMtx.RLock()
	defer b.replRequestsMtx.RUnlock()

	if peerIds, ok := b.replRequestsSent[chainID]; ok {
		return peerIds
	}

	return []string{}
}

// GetReplResponsePeers returns a list of node IDs whom have previously
// responded to a replication request with a ChainReplicationResponse.
func (b *MultiplexBackend) GetReplResponsePeers(chainID string) []string {
	b.replResponsesMtx.RLock()
	defer b.replResponsesMtx.RUnlock()

	if peerIds, ok := b.replResponsesRcvd[chainID]; ok {
		return peerIds
	}

	return []string{}
}

// GetReplCompletePeers returns a list of node IDs whom have previously
// finalized a replication request and sent a ChainReplicationComplete.
func (b *MultiplexBackend) GetReplCompletePeers(chainID string) []string {
	b.replCompleteMtx.RLock()
	defer b.replCompleteMtx.RUnlock()

	if peerIds, ok := b.replCompleteRcvd[chainID]; ok {
		return peerIds
	}

	return []string{}
}

// GetPoolRequestPeers returns a list of node IDs to whom we have previously
// sent a transaction through the Mempool.
// See also: [DefaultRelaysBroadcastRoutine]
func (b *MultiplexBackend) GetPoolRequestPeers(txHash string) []string {
	b.reactor.poolRequestsMtx.RLock()
	defer b.reactor.poolRequestsMtx.RUnlock()

	if peerIds, ok := b.reactor.poolRequestsSent[txHash]; ok {
		return peerIds
	}

	return []string{}
}

// GetAckResponsePeers returns a list of node IDs which have sent us back a
// AckTransactionBroadcast upon receiving a transaction in their mempool.
// See also: [DefaultRelaysBroadcastRoutine]
func (b *MultiplexBackend) GetAckResponsePeers(txHash string) []string {
	b.ackResponsesMtx.RLock()
	defer b.ackResponsesMtx.RUnlock()

	if peerIds, ok := b.ackResponsesRcvd[txHash]; ok {
		return peerIds
	}

	return []string{}
}

// CreateOrLoadDiscoveryEventSwitch creates a local [p2p.Switch] instance
// which is used to determine the required channels and connection information.
//
// The relayMtx is expected to be locked by the caller.
func (b *MultiplexBackend) CreateOrLoadDiscoveryEventSwitch(ctx context.Context) *p2p.Switch {
	discoverySwitch := b.reactor.GetEventSwitchForDiscovery()
	if discoverySwitch != nil {
		return discoverySwitch
	}

	// TODO(midas): remove debug logs
	b.logger.Debug("Creating switch for P2P discovery",
		"addr", b.broadcastAddr.String(),
	)

	nodeConfig := b.reactor.GetNodeConfig()

	// In-place mutation of the listen address so that it always uses
	// the configured broadcast address.
	multiNodeInfo := NewMultiNetworkNodeInfoWithConfig(
		nodeConfig,
		b.reactor.GetNodeKey(),
		b.broadcastAddr,
		[]byte{server.ReplicationChannel},
	)

	b.multiNodeInfo = multiNodeInfo

	mConnConfig := p2p.MConnConfig(nodeConfig.P2P)
	nodeKey := b.reactor.GetNodeKey()
	localTransport := p2p.NewMultiplexTransportWithCustomHandshake(
		multiNodeInfo, // local nodeInfo
		*nodeKey,
		mConnConfig,
		MultiplexTransportHandshake,
	)

	sw := p2p.NewSwitch(
		ctx,
		nodeConfig.P2P,
		localTransport,
		func(s *p2p.Switch) {
			s.Typ = "discovery"
		},
	)
	localTransport.SetSwitch(sw)
	sw.SetLogger(b.reactor.logger.With("module", "p2p"))
	sw.SetNodeInfo(multiNodeInfo)
	sw.SetNodeKey(b.reactor.GetNodeKey())

	// Make sure we listen to ChainReplicationRequest messages
	// Note that this reactor is STARTED in NewNodesMultiplex.
	sw.AddReactor(conn.SharedChannelsNamespace, "MULTIPLEX", b.reactor)
	b.reactor.SetEventSwitchForDiscovery(sw)
	return sw
}

// OpenChannels updates the NodeInfo pointer and event switch
// to permit communications related to a given list of ChainIDs.
func (b *MultiplexBackend) UpdateMultiNetworkNodeInfo(
	requiredNetworks []string,
) error {
	// Lock and read currently known ChainIDs.
	b.relayMtx.Lock()
	availableNetworks := b.multiNodeInfo.Networks
	availableVersions := b.multiNodeInfo.ProtocolVersions
	b.relayMtx.Unlock()

	// Keep only missing ChainIDs.
	missingChainIds := slices.DeleteFunc(requiredNetworks, func(chainID string) bool {
		return slices.Contains(availableNetworks, chainID)
	})
	if len(missingChainIds) == 0 {
		return nil
	}

	b.logger.Debug("Updating available networks",
		"num_networks", len(availableNetworks),
		"num_missing", len(missingChainIds))

	for _, chainID := range missingChainIds {
		availableNetworks = append(availableNetworks, chainID)
		availableVersions = append(availableVersions,
			NewChainProtocolVersion(
				chainID,
				DefaultProtocolVersion,
			),
		)
	}

	// Updates the MultiNetworkNodeInfo instance
	b.relayMtx.Lock()
	b.multiNodeInfo.SetNetworks(availableNetworks)
	b.multiNodeInfo.SetProtocolVersions(availableVersions)
	updatedNodeInfo := b.multiNodeInfo
	b.relayMtx.Unlock()

	// Update DISCOVERY switch if available
	if sw := b.reactor.GetEventSwitchForDiscovery(); sw != nil {
		sw.SetNodeInfo(updatedNodeInfo)
	}

	// Update COMETBFT switch if available
	if sw := b.reactor.GetEventSwitchForCometBFT(); sw != nil {
		sw.SetNodeInfo(updatedNodeInfo)
	}

	return nil
}

// shutdownOnPanic tries to close the backend after a panic.
func (b *MultiplexBackend) shutdownOnPanic() {
	if r := recover(); r != nil {
		b.logger.Error("Multiplex panicked", "err", r, "stack", string(debug.Stack()))
		close(b.shutdownCh)
	}
}

// MustStart starts a replication backend basically selecting void
// and running forever.
//
// This method also opens a custom P2P "broadcast" port such that
// the relay may be communicated to, even without hosting any
// replicated chain.
//
// MustStart implements [server.Server]
func (b *MultiplexBackend) OnStart(ctx context.Context) error {
	startTime := time.Now()

	if b.metrics != nil {
		defer addTimeSample(b.metrics.StartDurationSeconds, startTime)()
	}

	// TODO(midas): remove debug logs
	b.logger.Debug("Process now starting a node backend",
		"id", b.reactor.GetNodeKey().ID(),
	)

	// Multiplex reactor is started in NewNodesMultiplex, so if the backend
	// was shutdown (and had to be reset), we restart the reactor here.
	if !b.reactor.IsRunning() {
		if err := b.reactor.Start(); err != nil {
			b.logger.Error(
				"Error starting multiplex reactor", "err", err)
		}
	}

	// Filled with relay IDs upon sending ChainReplicationRequest.
	b.replRequestsMtx.Lock()
	b.replRequestsSent = map[string][]string{}
	b.replRequestsMtx.Unlock()

	// Filled with relay IDs upon receiving ChainReplicationResponse.
	b.replResponsesMtx.Lock()
	b.replResponsesRcvd = map[string][]string{}
	b.replResponsesMtx.Unlock()

	// Filled with relay IDs upon receiving ChainReplicationComplete.
	b.replCompleteMtx.Lock()
	b.replCompleteRcvd = map[string][]string{}
	b.replCompleteMtx.Unlock()

	// Filled with relay IDs upon sending mempool.Tx.
	b.reactor.poolRequestsMtx.Lock()
	b.reactor.poolRequestsSent = map[string][]string{}
	b.reactor.poolRequestsMtx.Unlock()

	// Filled with relay IDs upon receiving AckTransactionBroadcast.
	b.ackResponsesMtx.Lock()
	b.ackResponsesRcvd = map[string][]string{}
	b.ackResponsesMtx.Unlock()

	go b.metricsReporter()

	// Panics recovery closes shutdownCh to stop goroutines
	// and attempt a graceful shutdown of the backend.
	defer b.shutdownOnPanic()

	// Channel used to intercept errors during startup.
	b.errorsCh = make(chan error, 1)
	// Channel used to shutdown local goroutines on quit.
	b.shutdownCh = make(chan struct{}, 1)

	// Start P2P and RPC servers for Discovery.
	//
	// P2P: DiscoveryPort, accepts messages on [server.ReplicationChannel].
	// RPC: DiscoveryPort-1, accepts calls to [server.RelayInfo].
	discoveryWg := new(sync.WaitGroup)
	discoveryWg.Add(1)
	go func(wg *sync.WaitGroup) {
		// Since we'll modify the reactor and eventSwitch internals,
		// we lock the mutex to ensure that initialization completes.
		b.relayMtx.Lock()
		nodeCfg := b.reactor.GetNodeConfig()
		nodeKey := b.reactor.GetNodeKey()

		// We open a discovery port which is required such that the relay may
		// be communicated to, even without hosting any replicated chain.
		if _, err := b.StartP2PServerDiscovery(ctx, nodeCfg, nodeKey); err != nil {
			b.errorsCh <- fmt.Errorf("error with discovery P2P server: %w", err)
		}

		// Additionally, a RPC server is started which permits to read
		// node information such as the node ID, with [server.RelayInfo].
		if _, err := b.StartRPCServerDiscovery(nodeCfg, nodeKey); err != nil {
			b.errorsCh <- fmt.Errorf("error with discovery RPC server: %w", err)
		}

		// Done setting up Discovery
		b.relayMtx.Unlock()
		wg.Done()

		b.logger.Info("Discovery servers started - waiting to replicate chains",
			"time", cmttime.Now(),
			"id", b.reactor.GetNodeKey().ID(),
			"p2p", b.broadcastAddr.DialString(),
			"rpc", b.discoveryAddr.DialString(),
		)

		// Keep alive until shutdown
		select {
		case <-b.shutdownCh:
			return
		}
	}(discoveryWg)

	// Complete setup of discovery, then proceed.
	discoveryWg.Wait()

	// Start the Prometheus server.
	//
	// HTTP: DiscoveryPort+3.
	monitoringWg := new(sync.WaitGroup)
	monitoringWg.Add(1)
	go func(wg *sync.WaitGroup) {
		// Since we'll modify the reactor and eventSwitch internals,
		// we lock the mutex to ensure that initialization completes.
		b.relayMtx.Lock()

		// Start the Prometheus server, if enabled.
		if err := b.StartPrometheusServer(); err != nil {
			b.errorsCh <- fmt.Errorf("error with Prometheus server: %w", err)
		}

		// Done setting up Prometheus
		b.relayMtx.Unlock()
		wg.Done()

		b.logger.Info("Prometheus server started",
			"time", cmttime.Now(),
			"id", b.reactor.GetNodeKey().ID(),
			"mon", b.prometheusAddr.DialString(),
		)

		// Keep alive until shutdown
		select {
		case <-b.shutdownCh:
			return
		}
	}(monitoringWg)

	// Complete setup of Prometheus, then proceed.
	monitoringWg.Wait()

	// Start P2P and RPC servers for CometBFT.
	//
	// P2P: DiscoveryPort+1, accepts messages on CometBFT reactors channels.
	// RPC: DiscoveryPort+2, accepts requests to CometBFT RPC, by ChainID.
	cometbftWg := new(sync.WaitGroup)
	cometbftWg.Add(1)
	go func(wg *sync.WaitGroup) {
		// Since we'll modify the reactor and eventSwitch internals,
		// we lock the mutex to ensure that initialization completes.
		b.relayMtx.Lock()

		// Start the RPC server before the P2P server
		// so we can eg. receive txs for the first block
		if err := b.StartRPCServerCometBFT(); err != nil {
			b.errorsCh <- fmt.Errorf("error with CometBFT RPC server: %w", err)
		}

		// Then start the P2P server
		if err := b.StartP2PServerCometBFT(ctx); err != nil {
			b.errorsCh <- fmt.Errorf("error with CometBFT P2P server: %w", err)
		}

		b.relayMtx.Unlock()
		wg.Done()

		cometbftSwitch := b.reactor.GetEventSwitchForCometBFT()
		b.logger.Info("CometBFT servers started - waiting to activate runtimes",
			"time", cmttime.Now(),
			"id", b.reactor.GetNodeKey().ID(),
			"p2p", b.cometbftP2PAddr.DialString(),
			"rpc", b.cometbftRPCAddr.DialString(),
			"info", cometbftSwitch.NodeInfo(),
			"len", b.reactor.Size(),
		)

		select {
		case <-b.shutdownCh:
			return
		}
	}(cometbftWg)

	// Complete setup of CometBFT, then proceed.
	cometbftWg.Wait()

	// Start networks that are currently replaying on other relays.
	// We don't need for this goroutine to complete before we proceed.
	go func(runtimeRegistry *server.RuntimeRegistry) {
		replayingBuckets := b.reactor.GetReplayPool().GetBuckets()
		if b.reactor.Size() == 0 || len(replayingBuckets) == 0 {
			return
		}

		// Start only nodes that are currently replaying on some other relays.
		availableChainIds := b.reactor.GetNetworks()
		replayingChainIds := slices.DeleteFunc(availableChainIds, func(replayingChainID string) bool {
			chainAddr, _ := NewExtendedChainIDFromLegacy(replayingChainID)
			return !slices.Contains(replayingBuckets, chainAddr.GetUserAddress())
		})
		if len(replayingChainIds) == 0 {
			return
		}

		b.logger.Info("CometBFT replay pool - activating node runtimes",
			"len", len(replayingChainIds),
		)

		for _, replayingChainID := range replayingChainIds {
			if err := b.StartConsensusInstance(replayingChainID); err != nil {
				b.errorsCh <- fmt.Errorf(
					"error activating node runtime for %s: %w", replayingChainID, err,
				)
			}
			runtimeRegistry.OnActivate(replayingChainID)
		}
	}(b.GetRuntimeRegistry())

	if b.metrics != nil {
		addTimeSample(b.metrics.StartDurationSeconds, startTime)()
	}

	close(b.errorsCh)

	// We do not STOP when errors happen, instead only report.
	for err := range b.errorsCh {
		b.logger.Error("Error during multiplex backend initialization", "err", err)
	}

	// TODO(midas): remove debug logs
	b.logger.Debug("Done starting a node backend",
		"id", b.reactor.GetNodeKey().ID(),
		"len", b.reactor.Size(),
	)
	return nil
}

// Close stops the multiplex reactor and listeners, as well
// as internal channels.
// The relayMtx mutex is locked during execution.
//
// Close implements io.Closer
func (b *MultiplexBackend) OnStop() {
	// TODO(midas): remove debug logs
	b.logger.Debug("Shutting down node backend",
		"id", b.reactor.GetNodeKey().ID(),
	)

	b.relayMtx.Lock()
	if b.shutdownCh != nil {
		// Shutdown goroutines started by MustStart().
		close(b.shutdownCh)
	}
	b.relayMtx.Unlock()

	// Close transaction listeners (event bus) before reactor.
	serviceProvider := b.reactor.GetServicesProvider()
	for chainID, txSubscriber := range b.txSubscribers {
		ebService := serviceProvider(ServiceKeyEventBus, chainID)
		if ebService != nil {
			chainEventBus := ebService.(*types.EventBus)
			chainEventBus.UnsubscribeAll(context.Background(), txSubscriber)
		}
	}

	b.relayMtx.Lock()
	defer b.relayMtx.Unlock()

	if b.reactor != nil && b.reactor.IsRunning() {
		b.reactor.Stop()
	}

	for _, rpcListener := range b.rpcListeners {
		rpcListener.Close()
	}

	for _, httpClient := range b.httpClients {
		httpClient.CloseIdleConnections()
	}

	// Stop any custom HTTP servers (e.g. prometheus)
	for _, httpServer := range b.httpServers {
		// Shutdown instantly stops [http#Server.ListenAndServe].
		if err := httpServer.Shutdown(context.Background()); err != nil {
			b.logger.Error(
				"Error stopping HTTP server while shutting down", "err", err)
		}
	}
}

// OnReset implements Service.
func (b *MultiplexBackend) OnReset() error {
	b.logger.Debug("Reset multiplex backend")

	if err := b.reactor.Reset(); err != nil {
		b.logger.Error(
			"Error resetting the multiplex reactor", "err", err)
	}

	b.relayMtx.Lock()
	b.rpcListeners = []net.Listener{}
	b.httpServers = make(map[string]*http.Server)
	b.httpClients = []*http.Client{}
	b.txSubscribers = map[string]string{}
	b.relayMtx.Unlock()

	b.reactor.networkMutex.Lock()
	b.reactor.discoverySwitch = nil
	b.reactor.cometbftSwitch = nil
	b.reactor.networkMutex.Unlock()

	b.logger.Debug("Done resetting multiplex backend")
	return nil
}

// OnBroadcastError updates a runtime completion status and attaches
// an error which happened during the consensus instance.
//
// All node runtimes activated by the consensus instance should be
// marked as completed with error.
func (b *MultiplexBackend) OnBroadcastError(
	reason error,
	userAddress string,
	transactions ...client.Transaction,
) error {
	// Track active runtimes and relax some resources with idle manager.
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)
	transactionHashes := txHashesToHex(transactions...)

	// TODO(midas): remove debug logs
	b.logger.Debug("Node runtimes will be marked as completed with error",
		"num_networks", len(relevantChainIds),
		"chain_ids", relevantChainIds,
		"tx_batch", transactionHashes,
		"err", reason,
	)

	// Completes the runtimes activated by client.BroadcastTx.
	for _, chainID := range relevantChainIds {
		b.GetRuntimeRegistry().OnComplete(chainID)
	}

	return reason
}

// OnBroadcastComplete updates a runtime completion status. It accepts a batch
// of transactions and a list of remoteRelays that we may have to wait for
// until they have completed replication.
//
// When replication is announced as completed for all the relays currently
// catching up AND when all transactions are indexed, we proceed to mark the
// active runtime as completed, i.e. `OnComplete` is executed.
//
// Node runtimes that are not currently processing replications may be safely
// completed upon querying the indexerService until all transactions
// are included, and then proceed to mark the active runtime as completed.
func (b *MultiplexBackend) OnBroadcastComplete(
	ctx context.Context,
	userAddress string,
	remoteRelays []*server.RelayAddress,
	transactions ...client.Transaction,
) error {
	// Track active runtimes and relax some resources with idle manager.
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)
	transactionHashes := txHashesToHex(transactions...)
	transactionsByChain := mapTransactionsByChainID(userAddress, transactions...)

	syncingPeersChainIds := []string{}
	totalNumReplications := 0
	for _, chainID := range relevantChainIds {
		// Did we send any ChainReplicationRequest for this ChainiD?
		chainReplRequests := b.GetReplRequestPeers(chainID)
		if len(chainReplRequests) == 0 {
			continue
		}

		totalNumReplications += len(chainReplRequests)
		syncingPeersChainIds = append(syncingPeersChainIds, chainID)
	}

	// Removes syncing chains, as we won't need to check transactions.
	relevantChainIds = slices.DeleteFunc(relevantChainIds, func(cid string) bool {
		return slices.Contains(syncingPeersChainIds, cid)
	})

	// TODO(midas): remove debug logs
	b.logger.Debug("Now evaluating consensus instance completion",
		"num_networks", len(relevantChainIds),
		"num_remotes", len(remoteRelays),
		"num_waiting", len(syncingPeersChainIds),
		"num_syncing", totalNumReplications,
		"tx_batch", transactionHashes,
	)

	// If any replication (sync) is in progress for one of the relevant
	// ChainID values, then we must wait for completion before we may idle.
	if len(syncingPeersChainIds) > 0 {
		// TODO(midas): remove debug logs
		b.logger.Debug("Delaying the idle manager until relays have caught up",
			"num_networks", len(syncingPeersChainIds),
			"chain_ids", syncingPeersChainIds,
			"tx_batch", transactionHashes,
		)

		// Wait for the syncing relays to announce a ChainReplicationComplete.
		go func(withReg *server.RuntimeRegistry, syncingChainIds []string) {
			// Upon completion or error, we may plan to idle the active runtime.
			defer func() {
				for _, chainID := range syncingChainIds {
					cliTxes := transactionsByChain[chainID]
					rawTxes := [][]byte{}
					for _, tx := range cliTxes {
						rawTxes = append(rawTxes, []byte(client.TransactionToRawTx(tx)))
					}

					// Note that this method will check that the transaction
					// got indexed locally, otherwise it will keep querying
					// the tx indexer until the transaction is indexed.
					//
					// This blocks the goroutine until shutdown and/or indexing.
					b.reactor.OnCompleteRuntime(chainID, rawTxes)
				}
			}()

			// This goroutine will be locked until relevant relays are done with replication
			var (
				numCompleted int
				err          error
			)
			if numCompleted, err = b.WaitForRelaysReplicationCompleted(b.Context(),
				syncingChainIds,
				transactions...,
			); err != nil {
				b.logger.Error("Failed to wait for finalization of chain replication",
					"num_networks", len(syncingChainIds),
					"num_completed", numCompleted,
					"chain_ids", syncingChainIds,
					"tx_batch", transactionHashes,
					"err", err,
				)
				return
			}

			// TODO(midas): remove debug logs
			b.logger.Debug("All relays have caught up and completed chain replications",
				"num_networks", len(syncingChainIds),
				"num_synced", numCompleted,
				"chain_ids", syncingChainIds,
				"tx_batch", transactionHashes,
			)
		}(b.GetRuntimeRegistry(), syncingPeersChainIds)
	}

	// All other relays participated in consensus, thus transactions for
	// these ChainID should have been indexed by now, if they were not
	// we shall be listening for transaction events, i.e. `EventQueryTx`.
	servicesProvider := b.reactor.GetServicesProvider()
	completedChainIds := map[string]bool{}
	transactionsNotFound := make([]client.Transaction, 0, len(transactions))
	for _, chainID := range relevantChainIds {
		txesPerChain := transactionsByChain[chainID]

		for _, tx := range txesPerChain {
			txChainID := chainIdsFromTransactions(userAddress, tx)[0]
			indexerService := servicesProvider(ServiceKeyIndexers, txChainID).(*txindex.IndexerService)

			// The indexer Get() method will need the database open.
			indexDatabase := servicesProvider(ServiceKeyDatabaseIndex, txChainID)
			if err := EnsureStartDBService(indexDatabase); err != nil {
				b.logger.Error("Failed to search for indexed transaction",
					"chain_ids", txChainID,
					"tx_hash", txHashesToHex(tx)[0],
					"err", err,
				)
				continue
			}

			// First try to find transaction with tx indexer.
			if idxTx, err := indexerService.GetTxIndexer().Get(
				tx.Hash(),
			); idxTx == nil || err != nil {
				// Transaction is not yet indexed
				b.logger.Debug("Failed to find indexed transaction (not an error)",
					"chain_id", txChainID,
					"tx_hash", txHashesToHex(tx)[0],
				)
				transactionsNotFound = append(transactionsNotFound, tx)
				continue
			}

			// Transaction is not yet indexed
			b.logger.Debug("Found indexed transaction",
				"chain_id", txChainID,
				"tx_hash", txHashesToHex(tx)[0],
			)

			if _, ok := completedChainIds[txChainID]; !ok {
				// We may call OnComplete directly here, this tx is indexed.
				b.GetRuntimeRegistry().OnComplete(txChainID)
				completedChainIds[txChainID] = true
			}
		}
	}

	// If any transaction is still missing (not indexed), we must wait for
	// it to be included in a block, before we may idle.
	if len(transactionsNotFound) > 0 {
		// TODO(midas): remove debug logs
		b.logger.Debug("Delaying the idle manager until transactions are included",
			"num_waiting", len(transactionsNotFound),
			"waiting_tx", txHashesToHex(transactionsNotFound...),
			"tx_batch", transactionHashes,
		)

		// Wait for the transaction to be announced (indexed locally).
		go func(withReg *server.RuntimeRegistry, txesWaiting []client.Transaction) {
			// Upon completion or error, we may plan to idle the active runtime.
			defer func() {
				waitingChainIds := chainIdsFromTransactions(userAddress, txesWaiting...)
				for _, chainID := range waitingChainIds {
					withReg.OnComplete(chainID)
				}
			}()

			// This goroutine will be locked until all waiting transactions are indexed.
			//
			// NOTE(midas): We use a fallback timeout of transactionTimeout to stop waiting
			// in case some transactions take too long to be included in a block.
			var (
				numCompleted int
				err          error
			)
			if numCompleted, err = b.WaitForTransactionsEvents(b.Context(),
				userAddress,
				txesWaiting...,
			); err != nil {
				b.logger.Error("Failed to wait for inclusion of transaction",
					"num_waiting_tx", len(txesWaiting),
					"waiting_txes", txHashesToHex(txesWaiting...),
					"num_completed", numCompleted,
					"tx_batch", transactionHashes,
					"err", err,
				)
				return
			}

			// TODO(midas): remove debug logs
			b.logger.Debug("All transactions have been included",
				"num_waiting_tx", len(txesWaiting),
				"num_completed", numCompleted,
				"tx_batch", transactionHashes,
			)
		}(b.GetRuntimeRegistry(), transactionsNotFound)
	}

	msgSuccess := "Consensus instance completed"
	if len(syncingPeersChainIds) > 0 {
		msgSuccess += " - some relays are replicating in background"
	}
	if len(transactionsNotFound) > 0 {
		msgSuccess += " - some transactions have yet to be included"
	}

	// TODO(midas): remove debug logs
	b.logger.Debug(msgSuccess,
		"num_networks", len(relevantChainIds),
		"num_remotes", len(remoteRelays),
		"num_waiting", len(syncingPeersChainIds),
		"num_syncing", totalNumReplications,
		"num_waiting_tx", len(transactionsNotFound),
		"tx_batch", transactionHashes,
	)

	return nil
}

// WaitForRelaysAckChainReplications waits for a number of healthy relays
// to respond to replication requests. For this we use a combination of
// the reactor's `ackReplResChs` which receives updates upon processing
// ChainReplicationResponse messages from peers, and a `localReplResCh`
// channel to process the messages.
//
// WaitForRelaysAckChainReplications implements [server.Backend].
func (b *MultiplexBackend) WaitForRelaysAckChainReplications(
	ctx context.Context,
	catchupRelays map[string][]*server.RelayAddress,
	transactions ...client.Transaction,
) (
	relaysPerChain map[string][]string,
	numExpected int,
	numReceived int,
	err error,
) {
	totalNumReplRequests := 0
	numChainReplications := 0
	for _, addrs := range catchupRelays {
		totalNumReplRequests += len(addrs)
		if len(addrs) > 0 {
			numChainReplications++
		}
	}

	relaysPerChain = map[string][]string{}
	numReceived = 0
	numExpected = totalNumReplRequests
	transactionHashes := txHashesToHex(transactions...)

	// Closed at the end of this method, when results are returned.
	shutdownWaitChs := make(map[string]chan struct{}, numChainReplications)

	// Written on by [remoteAckReplicationConsumer] when it processes
	// a relevant ChainReplicationResponse message from a relevant relay.
	localReplResChs := make(map[string]chan *mxp2p.ChainReplicationResponse, numChainReplications)

	// Written on by [multiplex.Reactor#Receive] when it intercepts
	// a relevant ChainReplicationResponse message from a relay.
	remoteReplResChs := make(map[string]chan *mxp2p.ChainReplicationResponse, numChainReplications)

	// Written on by [remoteAckReplicationConsumer] when it errors, and also
	// written on by [localAckReplicationConsumer] when it errors and when it
	// is done processing (enough) replication acknowledgments.
	asyncResultsCh := make(chan AckReplicationResult, numChainReplications)

	for chainID, chainCatchupRelays := range catchupRelays {
		if len(chainCatchupRelays) == 0 {
			continue
		}

		relevantRelayIds := make([]string, 0, len(chainCatchupRelays))
		for _, catchupAddr := range chainCatchupRelays {
			relevantRelayIds = append(relevantRelayIds, string(catchupAddr.ID()))
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Waiting for replication response from relevant relays",
			"num_relays", len(chainCatchupRelays),
			"num_relevant", len(relevantRelayIds),
			"relay_ids", relevantRelayIds,
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)

		// Used to permit expiration of context or forcing shutdown of goroutines.
		shutdownWaitChs[chainID] = make(chan struct{}, 1)

		// Used to share response message internally
		// and forward the acknowledgment to [localAckReplicationConsumer].
		localReplResChs[chainID] = make(chan *mxp2p.ChainReplicationResponse, len(relevantRelayIds))

		// Used to intercept ChainReplicationResponse messages.
		remoteReplResChs[chainID] = b.reactor.ChannelForAckReplication(chainID)

		// NOTE(midas): The order of execution of the following goroutines
		// does not matter, because the local consumer reads messages that
		// are issued by the remote consumer, i.e. if the local consumer is
		// started first, it will lock its goroutine until consuming.

		// Collects ChainReplicationResponse messages.
		// Stopped on shutdownWaitCh.
		go b.remoteAckReplicationConsumer(ctx,
			chainID,                   // Accept response only for this ChainID
			remoteReplResChs[chainID], // Consuming this channel
			localReplResChs[chainID],  // Forwarding to local consumer
			asyncResultsCh,
			shutdownWaitChs[chainID],
			transactions...,
		)

		// Collects localReplResChs messages and create result object.
		// Stopped on shutdownWaitCh.
		go b.localAckReplicationConsumer(ctx,
			relevantRelayIds,         // Wait for response only for these relays
			chainID,                  // ... and for this ChainID
			localReplResChs[chainID], // Consuming this channel
			asyncResultsCh,
			shutdownWaitChs[chainID],
			transactions...,
		)
	}

	// Gracefully shutdown any living goroutines for a particular
	// ChainID. This method is called in deferral process, once per ChainID.
	shutdownFn := func(
		chainID string,
		shutdownChs map[string]chan struct{},
		localResChs map[string]chan *mxp2p.ChainReplicationResponse,
		processErr error,
	) {
		if ch, ok := shutdownChs[chainID]; ok && ch != nil {
			close(ch)
		}

		b.reactor.CloseAckReplicationChannel(chainID)

		if ch, ok := localResChs[chainID]; ok && ch != nil {
			close(ch)
		}

		msgStatus := ""
		if processErr == nil {
			msgStatus = " (SUCCESS)"
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Stopped replication response processor"+msgStatus,
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)
	}

	// Cancels any remaining goroutine in case of error, we must iterate
	// through all chains to make sure we shutdown all remaining goroutines.
	shutdownForError := func(
		replRelays map[string][]*server.RelayAddress,
		shutdownChs map[string]chan struct{},
		localResChs map[string]chan *mxp2p.ChainReplicationResponse,
		reasonErr error,
	) {
		for chainID, _ := range replRelays {
			shutdownFn(chainID, shutdownChs, localResChs, reasonErr)
		}

		// TODO(midas): remove debug logs
		b.logger.Error("Replication response processor stopped with error",
			"tx_batch", transactionHashes,
			"err", reasonErr,
		)
	}

	// Waits until we have all required results (or errors).
	for i := 0; i < numChainReplications; i++ {
		select {
		case replResult := <-asyncResultsCh:
			if replResult.Error != nil {
				// We stop waiting at first error that occurs.
				err = replResult.Error
				shutdownForError(catchupRelays, shutdownWaitChs, localReplResChs, err)
				return
			}

			relaysPerChain[replResult.ChainID] = make([]string, 0, len(replResult.Relays))
			relaysPerChain[replResult.ChainID] = append(relaysPerChain[replResult.ChainID], replResult.Relays...)
			numReceived += len(replResult.Relays)

			// Shutdown any living goroutine for this ChainID
			shutdownFn(replResult.ChainID, shutdownWaitChs, localReplResChs, nil)

		case <-b.reactor.Quit():
			err = errors.New("interrupted by shutdown process")
			shutdownForError(catchupRelays, shutdownWaitChs, localReplResChs, err)
			return
		}
	}

	return
}

// WaitForRelaysAckTransactionBatch waits for a number of healthy relays
// to ack a complete transaction batch. For this we use a combination of
// the reactor's `ackTxAcceptCh` which receives updates upon processing
// AckTransactionBroadcast messages from peers, and the `remoteRelayTxCh`
// channel to process the messages into a relay ID and transaction hash.
//
// Note that err will NOT be set for individual ACK errors because we may
// be able to reach consensus without ALL relays sending ACK responses.
// The returned err field will only be set given a larger potion of relays
// do not respond with a transaction ACK, more than 2/3+1 of relays.
//
// WaitForRelaysAckTransactionBatch implements [server.Backend].
func (b *MultiplexBackend) WaitForRelaysAckTransactionBatch(
	ctx context.Context,
	chainRelays map[string][]*server.RelayAddress,
	catchupRelays map[string][]*server.RelayAddress,
	transactions ...client.Transaction,
) (
	expectedRelaysPerTx map[string][]string,
	relaysPerTx map[string][]string,
	numExpected int,
	numReceived int,
	err error,
) {
	relaysPerTx = map[string][]string{}
	expectedRelaysPerTx = map[string][]string{}
	numReceived = 0
	transactionHashes := txHashesToHex(transactions...)

	// Contains only relay IDs for which we must wait
	relevantRelays := b.ApplyFilterAckTransactionRelayIds(
		chainRelays,   // Healthy relays
		catchupRelays, // Relays received ReplRequest
	)

	// Each relevant (healthy) relay should acknowledge each transaction once.
	numExpected = len(relevantRelays) * len(transactions)
	if numExpected == 0 {
		return
	}

	// Closed at the end of this method, when results are returned.
	shutdownWaitChs := make(map[string]chan struct{}, len(transactions))

	// Written on by [remoteAckTransactionConsumer] when it processes
	// a relevant AckTransactionBroadcast message from a relevant relay.
	// Consumed by [localAckTransactionConsumer].
	localAckAcceptTxChs := make(map[string]chan string, len(transactions))

	// Written on by [multiplex.Reactor#Receive] when it intercepts
	// a relevant AckTransactionBroadcast message from a relevant relay.
	// Consumed by [remoteAckTransactionConsumer].
	remoteAckAcceptTxChs := make(map[string]chan *mxp2p.AckTransactionBroadcast, len(transactions))

	// Written on by [remoteAckTransactionConsumer] when it errors, and also
	// written on by [localAckTransactionConsumer] when it errors and when it
	// is done processing (enough) transaction acknowledgments for this batch.
	// Consumed at the end of this method.
	asyncResultsCh := make(chan AckTransactionResult, len(transactions))

	// For every transaction that must be acknowledged, we open a channel
	// that will be used by [multiplex.Reactor#Receive] when it intercepts
	// a [AckTransactionBroadcast] message on the [server.AckBroadcastChannel].
	for _, transaction := range transactions {
		txHash := fmt.Sprintf("%X", transaction.Hash())
		relevantRelaysForTx := relevantRelays
		expectedRelaysPerTx[txHash] = relevantRelaysForTx

		// TODO(midas): remove debug logs
		b.logger.Debug("Waiting for transactions ACK from relevant relays",
			"num_relays", len(relevantRelaysForTx),
			"relay_ids", relevantRelaysForTx,
			"acks_tx", len(relevantRelaysForTx),
			"tx_hash", txHash,
			"tx_batch", transactionHashes,
		)

		// Used to permit expiration of context or forcing shutdown of goroutines.
		shutdownWaitChs[txHash] = make(chan struct{}, 1) // buffered

		// Used to share acceptance message `id:tx_hash_hex` internally
		// and forward the acknowledgment to [localAckTransactionConsumer].
		localAckAcceptTxChs[txHash] = make(chan string, len(relevantRelays)) // buffered

		// Used to intercept AckTransactionBroadcast messages.
		remoteAckAcceptTxChs[txHash] = b.reactor.ChannelForAckTransaction(txHash) // unbuffered

		// NOTE(midas): The order of execution of the following goroutines
		// does not matter, because the local consumer reads messages that
		// are issued by the remote consumer, i.e. if the local consumer is
		// started first, it will lock its goroutine until consuming.

		// Collects AckTransactionBroadcast messages and proxy to remoteRelayTxCh.
		// Stopped on shutdownWaitCh.
		go b.remoteAckTransactionConsumer(ctx,
			relevantRelaysForTx,          // Accept ACK only from these relays
			transaction,                  // ... and for this transaction
			remoteAckAcceptTxChs[txHash], // Consuming this channel
			localAckAcceptTxChs[txHash],  // Forwarding to local consumer
			asyncResultsCh,
			shutdownWaitChs[txHash],
		)

		// Collects remoteRelayTxCh messages and create result object.
		// Stopped on shutdownWaitCh.
		go b.localAckTransactionConsumer(ctx,
			relevantRelaysForTx,         // Wait for ACK only for these relays
			transaction,                 // ... and for these transactions
			localAckAcceptTxChs[txHash], // Consuming this channel
			asyncResultsCh,
			shutdownWaitChs[txHash],
		)
	}

	// Gracefully shutdown any living goroutines for a particular
	// transaction hash txHash. This method is called in deferral
	// process, once per *completed* transaction.
	shutdownFn := func(
		txHash string,
		shutdownChs map[string]chan struct{},
		remoteTxChs map[string]chan string,
		processErr error,
	) {
		if ch, ok := shutdownChs[txHash]; ok && ch != nil {
			close(ch)
		}

		b.reactor.CloseAckTransactionChannel(txHash)

		if ch, ok := remoteTxChs[txHash]; ok && ch != nil {
			close(ch)
		}

		msgStatus := ""
		if processErr == nil {
			msgStatus = " (SUCCESS)"
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Stopped transaction ACK processor"+msgStatus,
			"tx_hash", txHash,
		)
	}

	// Cancels any remaining goroutine in case of error, we must iterate
	// through transactions to make sure we shutdown all remaining goroutines.
	shutdownForError := func(
		transactions []client.Transaction,
		shutdownChs map[string]chan struct{},
		remoteTxChs map[string]chan string,
		reasonErr error,
	) {
		for _, tx := range transactions {
			txHash := fmt.Sprintf("%X", tx.Hash())
			shutdownFn(txHash, shutdownChs, remoteTxChs, reasonErr)
		}

		// TODO(midas): remove debug logs
		b.logger.Error("Transaction ACK processor stopped with error",
			"tx_batch", transactionHashes,
			"err", reasonErr,
		)
	}

	// Waits until we have all required results (or errors).
	numErrors := 0
	maxErrorsPerTx := len(relevantRelays) - (len(relevantRelays)*2/3 + 1)
	errsPerTxHash := map[string]int{}
	for (numReceived + numErrors) < numExpected {
		select {
		case txResult := <-asyncResultsCh: // Wait for one result (it doesn't matter which)
			if txResult.Error != nil {
				// At best, we just account for this error, but don't stop.
				numErrors++
				if _, ok := errsPerTxHash[txResult.TxHash]; !ok {
					errsPerTxHash[txResult.TxHash] = 1
				} else {
					errsPerTxHash[txResult.TxHash]++
				}

				// We stop waiting if we didn't reach 2/3+1 relays to ACK a tx.
				err = txResult.Error
				if errsPerTxHash[txResult.TxHash] > maxErrorsPerTx {
					shutdownForError(transactions, shutdownWaitChs, localAckAcceptTxChs, err)
					return
				}

				continue // continue processing results
			}

			relaysPerTx[txResult.TxHash] = make([]string, 0, len(txResult.Relays))
			relaysPerTx[txResult.TxHash] = append(relaysPerTx[txResult.TxHash], txResult.Relays...)
			numReceived += len(txResult.Relays)

			// Shutdown any living goroutine for this txHash
			shutdownFn(txResult.TxHash, shutdownWaitChs, localAckAcceptTxChs, nil)

		case <-b.reactor.Quit():
			err = errors.New("interrupted by shutdown process")
			shutdownForError(transactions, shutdownWaitChs, localAckAcceptTxChs, err)
			return
		}
	}

	return
}

// WaitForRelaysReplicationCompleted waits for a number of syncing relays
// to finalize replication requests. For this we use a combination of
// the reactor's `runtimeUpdatesChs` which receives updates upon processing
// ChainReplicatioComplete messages from peers, and a `localReplFinCh`
// channel to process the messages.
//
// CAUTION: A chain replication may take hours to complete given a higher
// number of blocks to synchronize with the network. Use accordingly.
//
// WaitForRelaysReplicationCompleted implements [server.Backend].
func (b *MultiplexBackend) WaitForRelaysReplicationCompleted(
	ctx context.Context,
	syncingChainIds []string,
	transactions ...client.Transaction,
) (numCompleted int, err error) {
	numCompleted = 0
	transactionHashes := txHashesToHex(transactions...)

	// Closed at the end of this method, when results are returned.
	shutdownWaitChs := make(map[string]chan struct{}, len(syncingChainIds))

	// Written on by [remoteRuntimeUpdatesConsumer] when it processes
	// a relevant ChainReplicationComplete message from a relevant relay.
	localReplFinChs := make(map[string]chan *mxp2p.ChainReplicationComplete, len(syncingChainIds))

	// Written on by [multiplex.Reactor#Receive] when it intercepts
	// a relevant ChainReplicationComplete message from a relay.
	remoteReplFinChs := make(map[string]chan *mxp2p.ChainReplicationComplete, len(syncingChainIds))

	// Written on by [remoteRuntimeUpdatesConsumer] when it errors, and also
	// written on by [localRuntimeUpdatesConsumer] when it errors and when it
	// is done processing (enough) replication completion messages.
	asyncResultsCh := make(chan RuntimeUpdateResult, len(syncingChainIds))

	for _, chainID := range syncingChainIds {
		// Uses the relays that acknowledged the ChainReplicationRequest.
		relevantRelayIds := b.GetReplResponsePeers(chainID)

		b.logger.Debug("Waiting for completed replication from syncing relays",
			"num_relays", len(relevantRelayIds),
			"relay_ids", relevantRelayIds,
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)

		// Used to permit expiration of context or forcing shutdown of goroutines.
		shutdownWaitChs[chainID] = make(chan struct{}, 1)

		// Used to share response message internally and forward the status
		// to [localRuntimeUpdatesConsumer].
		localReplFinChs[chainID] = make(chan *mxp2p.ChainReplicationComplete, len(relevantRelayIds))

		// Used to intercept ChainReplicationComplete messages.
		remoteReplFinChs[chainID] = b.reactor.ChannelForRuntimeUpdates(chainID)

		// NOTE(midas): The order of execution of the following goroutines
		// does not matter, because the local consumer reads messages that
		// are issued by the remote consumer, i.e. if the local consumer is
		// started first, it will lock its goroutine until consuming.

		// Collects ChainReplicationComplete messages.
		// Stopped on shutdownWaitCh.
		go b.remoteRuntimeUpdatesConsumer(ctx,
			chainID,                   // Accept response only for this ChainID
			remoteReplFinChs[chainID], // Consuming this channel
			localReplFinChs[chainID],  // Forwarding to local consumer
			asyncResultsCh,
			shutdownWaitChs[chainID],
			transactions...,
		)

		// Collects localReplFinChs messages and create result object.
		// Stopped on shutdownWaitCh.
		go b.localRuntimeUpdatesConsumer(ctx,
			relevantRelayIds,         // Wait for response only for these relays
			chainID,                  // ... and for this ChainID
			localReplFinChs[chainID], // Consuming this channel
			asyncResultsCh,
			shutdownWaitChs[chainID],
			transactions...,
		)
	}

	// Gracefully shutdown any living goroutines for a particular
	// ChainID. This method is called in deferral process, once per ChainID.
	shutdownFn := func(
		chainID string,
		shutdownChs map[string]chan struct{},
		localFinChs map[string]chan *mxp2p.ChainReplicationComplete,
		processErr error,
	) {
		if ch, ok := shutdownChs[chainID]; ok && ch != nil {
			close(ch)
		}

		b.reactor.CloseRuntimeUpdatesChannel(chainID)

		if ch, ok := localFinChs[chainID]; ok && ch != nil {
			close(ch)
		}

		msgStatus := ""
		if processErr == nil {
			msgStatus = " (SUCCESS)"
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Stopped completed replications processor"+msgStatus,
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)
	}

	// Cancels any remaining goroutine in case of error, we must iterate
	// through all chains to make sure we shutdown all remaining goroutines.
	shutdownForError := func(
		chainIds []string,
		shutdownChs map[string]chan struct{},
		localFinChs map[string]chan *mxp2p.ChainReplicationComplete,
		reasonErr error,
	) {
		for _, chainID := range chainIds {
			shutdownFn(chainID, shutdownChs, localFinChs, reasonErr)
		}

		// TODO(midas): remove debug logs
		b.logger.Error("Completed replications processor stopped with error",
			"tx_batch", transactionHashes,
			"err", reasonErr,
		)
	}

	// Waits until we have all required results (or errors).
	for i := 0; i < len(syncingChainIds); i++ {
		select {
		case updateResult := <-asyncResultsCh: // Wait for one result (it doesn't matter which)
			if updateResult.Error != nil {
				// We stop waiting at first error that occurs.
				err = updateResult.Error
				shutdownForError(syncingChainIds, shutdownWaitChs, localReplFinChs, err)
				return
			}

			numCompleted += len(updateResult.Relays)

			// Shutdown any living goroutine for this ChainID
			shutdownFn(updateResult.ChainID, shutdownWaitChs, localReplFinChs, nil)

		case <-b.reactor.Quit():
			err = errors.New("interrupted by shutdown process")
			shutdownForError(syncingChainIds, shutdownWaitChs, localReplFinChs, err)
			return
		}
	}

	return
}

// WaitForTransactionsEvents waits for a number of transaction events
// to confirm that transactions got included.
//
// WaitForTransactionsEvents implements [server.Backend].
func (b *MultiplexBackend) WaitForTransactionsEvents(
	ctx context.Context,
	userAddress string,
	transactions ...client.Transaction,
) (numCompleted int, err error) {
	numCompleted = 0
	transactionHashes := txHashesToHex(transactions...)
	transactionsByChain := mapTransactionsByChainID(userAddress, transactions...)
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)

	// Closed at the end of this method, when results are returned.
	shutdownWaitChs := make(map[string]chan struct{}, len(transactionsByChain))

	// Written on by [localTransactionEventsConsumer] when it errors and when it
	// is done processing (enough) transaction events.
	asyncResultsCh := make(chan TransactionEventResult, len(transactionsByChain))

	serviceProvider := b.reactor.GetServicesProvider()
	for chainID, txesForChainID := range transactionsByChain {
		// Make sure we can retrieve the EventBus, otherwise we won't be able
		// to wait for the inclusion of txesForChainID, and should error here.
		serviceEventBus := serviceProvider(ServiceKeyEventBus, chainID)
		if serviceEventBus == nil {
			asyncResultsCh <- TransactionEventResult{Error: fmt.Errorf(
				"Failed to retrieve EventBus for %s - can't wait for transaction events", chainID,
			)}
			continue
		}
		chainEventBus := serviceEventBus.(*types.EventBus)

		b.logger.Debug("Waiting for transaction events locally",
			"num_txes", len(txesForChainID),
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)

		// Used to permit expiration of context or forcing shutdown of goroutines.
		shutdownWaitChs[chainID] = make(chan struct{}, 1)

		// Collects localReplFinChs messages and create result object.
		// Stopped on shutdownWaitCh.
		go b.localTransactionEventsConsumer(ctx,
			chainEventBus, // Wait for event using this EventBus
			chainID,       // ... and for this ChainID
			asyncResultsCh,
			shutdownWaitChs[chainID],
			txesForChainID...,
		)
	}

	// Gracefully shutdown any living goroutines for a particular
	// ChainID. This method is called in deferral process, once per ChainID.
	shutdownFn := func(
		chainID string,
		shutdownChs map[string]chan struct{},
		processErr error,
	) {
		if ch, ok := shutdownChs[chainID]; ok && ch != nil {
			close(ch)
		}

		msgStatus := ""
		if processErr == nil {
			msgStatus = " (SUCCESS)"
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Stopped transaction events processor"+msgStatus,
			"chain_id", chainID,
			"tx_batch", transactionHashes,
		)
	}

	// Cancels any remaining goroutine in case of error, we must iterate
	// through all chains to make sure we shutdown all remaining goroutines.
	shutdownForError := func(
		chainIds []string,
		shutdownChs map[string]chan struct{},
		reasonErr error,
	) {
		for _, chainID := range chainIds {
			shutdownFn(chainID, shutdownChs, reasonErr)
		}

		// TODO(midas): remove debug logs
		b.logger.Error("Transaction events processor stopped with error",
			"tx_batch", transactionHashes,
			"err", reasonErr,
		)
	}

	// Waits until we have all required results (or errors).
	for i := 0; i < len(transactionsByChain); i++ {
		select {
		case txEventResult := <-asyncResultsCh: // Wait for one result (it doesn't matter which)
			if txEventResult.Error != nil {
				// We stop waiting at first error that occurs.
				err = txEventResult.Error
				shutdownForError(relevantChainIds, shutdownWaitChs, err)
				return
			}

			numCompleted += len(txEventResult.TxHashes)

			// Shutdown any living goroutine for this ChainID
			shutdownFn(txEventResult.ChainID, shutdownWaitChs, nil)

		case <-b.reactor.Quit():
			err = errors.New("interrupted by shutdown process")
			shutdownForError(relevantChainIds, shutdownWaitChs, err)
			return
		}
	}

	return
}

// CancelBroadcastOperation executes the CancelBroadcast routine
// and the RemoveTransactions method to remove transactions
// from the local mempool.
func (b *MultiplexBackend) CancelBroadcastOperation(
	ctx context.Context,
	userAddress string,
	transactions ...client.Transaction,
) error {
	routineCancelBroadcast := b.GetRoutines().CancelBroadcast
	go routineCancelBroadcast(ctx,
		userAddress,
		transactions,
		b.logger.With("tx_batch", txHashesToHex(transactions...)),
	)

	return b.RemoveTransactions(userAddress, transactions...)
}

// getLocalNetworkHeights finds out about the last block height and determines
// a list of networks that must be created. The list of networks that must be
// created will also be present in the list of required networks.
// GetLocalNetworkHeights implements [server.Backend].
func (b *MultiplexBackend) GetLocalNetworkHeights(
	userAddress string,
	transactions ...client.Transaction,
) (requiredNetworks []string, mustCreateNetworks []string) {
	requiredNetworks = []string{}
	mustCreateNetworks = []string{}

	uniqueNetworks := map[string]bool{}
	unknownNetworks := map[string]bool{}
	for _, tx := range transactions {
		chainID := client.GetChainID(userAddress, tx.Fingerprint)
		uniqueNetworks[chainID] = true

		// If we don't know this network, we either need a background-sync
		// or we must create a new network if other relays also don't know it.
		if !b.reactor.HasNetwork(chainID) {
			unknownNetworks[chainID] = true
		}
	}

	// Returns as a slice of unique ChainIDs
	for unknownChainID := range unknownNetworks {
		mustCreateNetworks = append(mustCreateNetworks, unknownChainID)
	}

	for requiredChainID := range uniqueNetworks {
		requiredNetworks = append(requiredNetworks, requiredChainID)
	}

	return requiredNetworks, mustCreateNetworks
}

// InitValidators initialize validators for networks and returns a map
// of public keys per ChainID. It uses [Reactor.AllocateNetwork] to init
// the missing [types.PrivValidator] instances.
//
// InitValidators implements [server.Backend].
func (b *MultiplexBackend) InitValidators(
	networks []string,
) (pubKeysPerChainID map[string]string, err error) {
	myValidatorPubKeys := b.GetValidatorPubs()
	pubKeysPerChainID = make(map[string]string, len(networks))
	for _, chainID := range networks {
		// If GetValidatorPubs() already has this ChainID
		if pubKey, ok := myValidatorPubKeys[chainID]; ok {
			pubKeysPerChainID[chainID] = pubKey
			continue
		}

		// Otherwise, pre-allocates priv validator instance.
		if err = b.reactor.AllocateNetwork(chainID); err != nil {
			b.logger.Error("Failed to allocate new priv validator",
				"chain_id", chainID,
				"err", err,
			)
			return
		}

		// Retrieve pre-allocated resources for priv validator and fs
		privValProvider := b.reactor.GetInstanceProvider(InstanceKeyPrivValidator)
		privValidator := privValProvider(chainID).(types.PrivValidator)

		// Make sure we can access the priv validator
		var privValPubKey crypto.PubKey
		if privValPubKey, err = privValidator.GetPubKey(); err != nil {
			b.logger.Error("Failed to read public key from validator", "err", err)
			return
		}

		pubKeysPerChainID[chainID] = fmt.Sprintf("%X", privValPubKey.Bytes())
	}

	return // pubKeysPerChainID, nil
}

// GetRemoteValidatorsInfo connects to relayAddress using a JSONRPC client,
// and calls the InitValidators remote procedure to retrieve public keys.
//
// The relayAddress parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
//
// GetRemoteValidatorsInfo implements [server.Backend].
func (b *MultiplexBackend) GetRemoteValidatorsInfo(
	clientCtx context.Context,
	relayAddress *server.RelayAddress,
	requiredNetworks []string,
) (*server.RPCResultInitValidators, error) {
	valsInfo,
		httpClient,
		infoErr := b.reactor.GetRemoteValidatorsInfo(
		clientCtx,
		relayAddress,
		requiredNetworks,
		b.reactor.relayInfoTimeout,
	)
	if infoErr != nil {
		return nil, infoErr
	}

	b.relayMtx.Lock()
	b.httpClients = append(b.httpClients, httpClient)
	b.relayMtx.Unlock()

	return valsInfo, nil
}

// GetValidatorsByNetwork maps each supported network to a slice of validator
// public keys in string (hex) format.
//
// This method uses [GetRemoteValidatorsInfo] to orchestrate the InitValidators
// call if necessary. Given a correct response, we fill a map where keys contain
// ChainID and values are slices of validator public keys.
//
// GetValidatorsByNetwork implements [server.Backend].
func (b *MultiplexBackend) GetValidatorsByNetwork(
	clientCtx context.Context,
	relayAddresses []*server.RelayAddress,
	requiredNetworks []string,
) (validatorsByChain map[string][]string, err error) {
	validatorsByChain = make(map[string][]string, len(requiredNetworks))

	// This method should block until it processed all relays' validators.
	var valsWg sync.WaitGroup
	valsWg.Add(len(relayAddresses))

	validatorsCh := make(chan struct {
		start  time.Time
		result *server.RPCResultInitValidators
		addr   *server.RelayAddress
	}, len(relayAddresses))

	// Connect to all other relays using RPC (discovery server) to find
	// out their validator public key for requiredNetworks.
	for _, relAddr := range relayAddresses {
		// Open ephemeral goroutines to request RelayInfo RPC from all relays.
		go func(relayAddr *server.RelayAddress) {
			defer valsWg.Done()
			startTz := time.Now()
			addrRPC := relayAddr.AddressForRelayInfo()

			// Discover this relay's validator public key.
			// This executes a RPC request for InitValidators.
			result, valsErr := b.GetRemoteValidatorsInfo(clientCtx,
				relayAddr, // expects DiscoveryPort
				requiredNetworks,
			)
			if valsErr != nil {
				b.logger.Error("Error discovering relay validator public keys",
					"relay", addrRPC,
					"err", valsErr,
				)
				return
			}

			validatorsCh <- struct {
				start  time.Time
				result *server.RPCResultInitValidators
				addr   *server.RelayAddress
			}{result: result, addr: relayAddr, start: startTz}
		}(relAddr)
	}

	// Block this process until all relays have responded or timed out.
	valsWg.Wait()
	close(validatorsCh) // No more responses/timeouts expected.

	for result := range validatorsCh {
		durationMs := time.Since(result.start).Milliseconds()
		relayAddr := result.addr

		// Every relay may return one validator public key per requiredNetworks.
		for chainID, validatorPubKey := range result.result.ValidatorPubs {
			if _, ok := validatorsByChain[chainID]; !ok {
				validatorsByChain[chainID] = make([]string, 0, len(relayAddresses))
			}

			validatorsByChain[chainID] = append(validatorsByChain[chainID], validatorPubKey)
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Retrieved validators information from relay",
			"relay", relayAddr,
			"validators", result.result.ValidatorPubs,
			"time", strconv.Itoa(int(durationMs))+"ms",
		)
	}

	return // validatorsByChain, nil
}

// GetRemoteRelayInfo connects to relayAddress using a JSONRPC client,
// and calls the GetRelayInfo remote procedure to retrieve the Relay ID,
// the supported networks and the listen address for the remote relay.
//
// The relayAddress parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
//
// GetRemoteRelayInfo implements [server.Backend].
func (b *MultiplexBackend) GetRemoteRelayInfo(
	clientCtx context.Context,
	relayAddress *server.RelayAddress,
) (*server.RPCResultRelayInfo, error) {
	relayInfo,
		httpClient,
		infoErr := b.reactor.GetRemoteRelayInfo(
		clientCtx,
		relayAddress,
		b.reactor.relayInfoTimeout,
	)
	if infoErr != nil {
		return nil, infoErr
	}

	b.relayMtx.Lock()
	b.httpClients = append(b.httpClients, httpClient)
	b.relayMtx.Unlock()

	return relayInfo, nil
}

// GetRelaysByNetwork maps each supported network to a slice of relay addresses
// and it also returns a slice of relays that produced errors, e.g. network error.
//
// This method uses [GetRemoteRelayInfo] to find the relay's ID. Given a correct
// response, we use the [RPCResultRelayInfo] to fill the [RelayAddress#ID] and
// the full address (with ID) is added to healthyRelays.
//
// GetRelaysByNetwork implements [server.Backend].
func (b *MultiplexBackend) GetRelaysByNetwork(
	clientCtx context.Context,
	relayAddresses []*server.RelayAddress,
) (
	healthyRelays []*server.RelayAddress,
	chainRelays map[string][]*server.RelayAddress,
	errorRelays []string,
) {
	healthyRelays = make([]*server.RelayAddress, 0, len(relayAddresses))
	chainRelays = map[string][]*server.RelayAddress{}
	errorRelays = []string{}

	relaysWithFailure := map[string]bool{}

	// This method should block until it processed all relays' responses.
	var wg sync.WaitGroup
	wg.Add(len(relayAddresses))

	// We will call RelayInfo concurrently on every relay. A nil result
	// means that the relay did not respond (in time) and is unhealthy.
	relayInfoCh := make(chan struct {
		start  time.Time
		result *server.RPCResultRelayInfo
		addr   *server.RelayAddress
	}, len(relayAddresses))

	// Connect to all other relays using RPC (discovery server) to find
	// out their relay ID (CometBFT Node ID) before we can connect with P2P.
	for _, relAddr := range relayAddresses {
		// Open ephemeral goroutines to request RelayInfo RPC from all relays.
		go func(relayAddr *server.RelayAddress) {
			defer wg.Done()
			startTz := time.Now()
			addrRPC := relayAddr.AddressForRelayInfo()

			// Discover this relay's ID (CometBFT Node ID).
			// This executes a RPC request for RelayInfo.
			result, err := b.GetRemoteRelayInfo(clientCtx, relayAddr) // expects DiscoveryPort
			if err != nil {
				b.logger.Error("Error discovering relay information",
					"relay", addrRPC,
					"err", err,
				)
				relayInfoCh <- struct {
					start  time.Time
					result *server.RPCResultRelayInfo
					addr   *server.RelayAddress
				}{result: nil, addr: relayAddr, start: startTz}
				return
			}

			relayInfoCh <- struct {
				start  time.Time
				result *server.RPCResultRelayInfo
				addr   *server.RelayAddress
			}{result: result, addr: relayAddr, start: startTz}
		}(relAddr)
	}

	// Block this process until all relays have responded or timed out.
	wg.Wait()
	close(relayInfoCh) // No more responses/timeouts expected.

	// Now process responses from relays and extend RelayAddress instances
	// to contain the CometBFT Node ID when the relay responded correctly.
	for result := range relayInfoCh {
		relayAddr := result.addr
		if result.result == nil {
			// This error case is logged in above for-loop.
			relaysWithFailure[relayAddr.String()] = true
			continue
		}
		durationMs := time.Since(result.start).Milliseconds()

		// TODO(midas): remove debug logs
		b.logger.Debug("Retrieved networks information from relay",
			"relay", relayAddr,
			"node_id", result.result.DefaultNodeID,
			"networks", result.result.Networks,
			"laddr", result.result.ListenAddress,
			"dport", strconv.FormatUint(uint64(result.result.DiscoveryPort), 10),
			"time", strconv.Itoa(int(durationMs))+"ms",
		)

		// Fill the CometBFT Node ID
		relayAddr.SetID(result.result.DefaultNodeID)
		healthyRelays = append(healthyRelays, relayAddr)

		// Save the RelayInfo result as we need it for dialing.
		b.reactor.SaveRelayInfo(result.result)

		// Also, populate a map of relay addresses by ChainID.
		for _, chainID := range result.result.Networks {
			if _, ok := chainRelays[chainID]; !ok {
				chainRelays[chainID] = []*server.RelayAddress{}
			}

			chainRelays[chainID] = append(chainRelays[chainID], relayAddr)
		}
	}

	// Populate a slice of unique relay addresses which produced errors
	for errRelay, _ := range relaysWithFailure {
		if !slices.Contains(errorRelays, errRelay) {
			errorRelays = append(errorRelays, errRelay)
		}
	}

	return healthyRelays, chainRelays, errorRelays
}

// CheckDialCompatibleRelay dials the relay using a local [p2p.Switch] instance
// to perform a handshake and determine whether relayAddress is compatible.
//
// Ignore existing address errors here in case of long-living process
// broadcasting more transactions, when peer is already dialed or being dialed.
//
// CheckDialCompatibleRelay implements [server.Backend].
func (b *MultiplexBackend) CheckDialCompatibleRelay(
	_ context.Context,
	dialWithSw *p2p.Switch,
	relayAddr *server.RelayAddress,
) error {
	// If this is us, nothing to do.
	if relayAddr.ID() == b.reactor.GetNodeKey().ID() {
		return nil
	}

	// (1)
	// Dial the relay to find out whether it is compatible (handshake).

	// TODO(midas): remove debug logs
	b.logger.Debug("Process now dialing remote relay (discovery)",
		"relay", relayAddr.String(),
	)

	if err := b.reactor.DialRelayForScope(dialWithSw, relayAddr, p2p.ScopeForDiscovery); err != nil {
		if b.reactor.IsDialError(err) {
			return fmt.Errorf(
				"could not dial relay %s for discovery: %w", relayAddr.String(), err)
		}
	}

	return nil
}

// ApplyFilterAckTransactionRelayIds is a RelayID filter function which
// fills a relevantRelays slice that contains only relay IDs that must
// be waited for during the AckTransaction process. In case a relay ID does not
// appear in the resulting slice, it means that they must first handle a
// replication and we should not wait for their acknowledgement.
//
// TODO(midas): TBI if more than 2/3 relays must replicate. Transactions won't
// broadcast because of dependency on successfull remote replications by too
// many relays. Note that this is an edge case and the current response of this
// implementation to those conditions is to *deny the transaction broadcast*,
// because too many healthy (required) relays are failing (not syncd).
func (b *MultiplexBackend) ApplyFilterAckTransactionRelayIds(
	chainRelays map[string][]*server.RelayAddress,
	catchupRelays map[string][]*server.RelayAddress,
) []string {
	relevantRelays := []string{}
	// Any healthy relay should be waited for initially.
	for _, relaysForChain := range chainRelays {
		healthyRelayIds := func() (relayIds []string) {
			relayIds = make([]string, 0, len(relaysForChain))
			for _, relayAddr := range relaysForChain {
				relayIds = append(relayIds, string(relayAddr.ID()))
			}
			return relayIds
		}()
		relevantRelays = slices.DeleteFunc(healthyRelayIds, func(relayId string) bool {
			return len(relayId) == 0 || relayId == string(b.GetRelayID())
		})
	}

	// ... but make sure that if only some of them have to replicate, and others
	// don't have to replicate, we won't wait for the relays that need replication.
	for _, catchupForChain := range catchupRelays {
		catchupRelayIds := func() (relayIds []string) {
			relayIds = make([]string, 0, len(catchupForChain))
			for _, relayAddr := range catchupForChain {
				relayIds = append(relayIds, string(relayAddr.ID()))
			}
			return relayIds
		}()
		if len(relevantRelays) > 0 {
			// Some have chain, some don't. The ones that are missing it
			// will replicate, but we shouldn't be waiting for them.
			relevantRelays = slices.DeleteFunc(relevantRelays, func(relayId string) bool {
				return slices.Contains(catchupRelayIds, relayId)
			})
		}
	}

	return removeDuplicates(relevantRelays)
}

// ApplyFilterReplRequestRelays filters relays and returns a map of relays
// by ChainID which contains only relays that need to catchup, i.e. it returns
// relays that will receive a chain replication request.
//
// ApplyFilterReplRequestRelays implements [server.Backend].
func (b *MultiplexBackend) ApplyFilterReplRequestRelays(
	requiredNetworks []string,
	relays []*server.RelayAddress,
	chainRelays map[string][]*server.RelayAddress,
) map[string][]*server.RelayAddress {
	// Makes sure to avoid mistakenly including self.
	relaysWithoutSelf := []*server.RelayAddress{}
	for _, relayAddr := range relays {
		if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
			relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
		}
	}

	catchupRelays := map[string][]*server.RelayAddress{}
	for chainID, relaysByChain := range chainRelays {
		// Did all relays report to know this ChainID?
		if len(relaysByChain) >= len(relaysWithoutSelf) {
			catchupRelays[chainID] = nil
			continue
		}

		// Build a (searchable) slice of relay IDs
		relayIdsByChain := []string{}
		for _, relayAddr := range relaysByChain {
			if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
				relayIdsByChain = append(relayIdsByChain, string(relayAddr.ID()))
			}
		}

		// Find out which relays are missing for this chain.
		// Those are relays that need to catchup with the chain.
		for _, relayAddr := range relaysWithoutSelf {
			if !slices.Contains(relayIdsByChain, string(relayAddr.ID())) {
				catchupRelays[chainID] = append(catchupRelays[chainID], relayAddr)
			}
		}
	}

	// Handling case when chainRelays is empty (0 networks on remote relays).
	for _, chainID := range requiredNetworks {
		if _, has := catchupRelays[chainID]; !has {
			catchupRelays[chainID] = append(catchupRelays[chainID], relaysWithoutSelf...)
		}

		// Reset the sent requests cache for required networks
		// TODO(midas): It is preferrable to move this registry over to the Reactor.
		b.replRequestsMtx.Lock()
		b.replRequestsSent[chainID] = []string{}
		b.replRequestsMtx.Unlock()
	}

	return catchupRelays
}

// AddTransactions executes the CheckTx call to add individual
// transactions to the mempool by ChainID.
//
// This method is called by [BroadcastTx] when the transaction is ready
// to be broadcast to all other relays. Adding the transaction to the
// mempool effectively marks the transaction as locally accepted.
// AddTransactions implements [server.Backend].
func (b *MultiplexBackend) AddTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With("chain_id", chainID)

		reactorsProvider := b.reactor.GetServicesProvider()
		memplReactor, ok := reactorsProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		if !ok {
			return fmt.Errorf(
				"could not get local mempool reactor instance in AddTransactions with ChainID %s", chainID)
		}

		chainMempool := memplReactor.GetMempoolPtr()

		checkTxRes, err := chainMempool.CheckTx(
			client.TransactionToRawTx(transaction),
			b.reactor.GetNodeKey().ID(),
		)
		if err != nil {
			switch {
			case err == mempl.ErrTxInCache:
			case err == mempl.ErrTxInMempool:
			case err == mempl.ErrTxAlreadyReceivedFromSender:
				continue
			default:
				return err
			}
		}

		// Inform about local mempool addition result
		clogger.Info("Received CheckTx response", "res", checkTxRes)
	}

	return nil
}

// RemoveTransactions remove a transaction from the local mempool
// if it has been added already, e.g. using addTransactionToMempool.
//
// This method is called by [BroadcastTx] when a transaction rollback must
// be executed due to some of the healthy relays not accepting a batch.
//
// RemoveTransactions implements [server.Backend].
func (b *MultiplexBackend) RemoveTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With("chain_id", chainID)

		reactorsProvider := b.reactor.GetServicesProvider()
		memplReactor, ok := reactorsProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		if !ok {
			return fmt.Errorf(
				"could not get local mempool reactor instance in RemoveTransactions with ChainID %s", chainID)
		}

		chainMempool := memplReactor.GetMempoolPtr()

		memTx := client.TransactionToRawTx(transaction)
		chainMempool.Lock()
		if err := chainMempool.RemoveTxByKey(memTx.Key()); err != nil {
			clogger.Debug("Rollback transaction not in local mempool (not an error)",
				"tx", cmtlog.NewLazySprintf("%X", memTx.Hash()),
				"error", err.Error())
		}
		chainMempool.Unlock()
	}

	return nil
}

// StartConsensusInstance calls the Start method of consensus reactors,
// including mempool, blocksync, consensus and evidence reactors, and
// starts the node services afterwards.
//
// StartConsensusInstance implements [server.Backend].
func (b *MultiplexBackend) StartConsensusInstance(
	chainID string,
) error {
	ctx := b.Context()
	clogger := b.logger.With("chain_id", chainID)

	// In case this ChainID has not been activated yet, we need to do it
	// here so that we may proceed with starting consensus reactors.
	b.reactor.chainReadyMtx.RLock()
	_, hasConfiguredChainID := b.reactor.chainReadyChs[chainID]
	b.reactor.chainReadyMtx.RUnlock()

	if !hasConfiguredChainID {
		// calls AllocateNetwork, InjectNewNetwork, InjectNewRuntime
		ReactorWithActiveRuntimes([]string{chainID}, map[string][]string{})(
			b.reactor,
		)
	}

	if err := b.reactor.StartConsensusInstanceReactors(ctx,
		chainID,
		false, // disables status updates to peers about replication (ChainReplicationComplete)
	); err != nil {
		clogger.Error("failed to initialize consensus reactors", "err", err)
	}

	if err := b.reactor.InitAndStartNode(ctx, chainID); err != nil {
		clogger.Error("failed to initialize node instance", "err", err)
	}

	return nil
}

// ----------------------------------------------------------------------------
// Servers

// StartP2PServerDiscovery creates a [p2p.Switch] instance that may be used
// to transport [ChainReplicationRequest] messages to nodes that do not have
// network ports open yet (due to not replicating any chain).
// Creates a transport listening on DiscoveryPort.
func (b *MultiplexBackend) StartP2PServerDiscovery(
	ctx context.Context,
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	promoteAddr := nodeCfg.P2P.ExternalAddress
	if promoteAddr == "" {
		promoteAddr = nodeCfg.P2P.ListenAddress
	}
	p2pListenAddr := overwriteListenPort(
		promoteAddr,
		int(nodeCfg.DiscoveryPort),
	)

	relayAddr, err := server.NewRelayAddress(p2pListenAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"could not create relay address for P2P: %w", err)
	}

	relayAddr.SetID(nodeKey.ID())
	if b.broadcastAddr, err = relayAddr.NetAddress(); err != nil {
		return nil, fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	b.logger.Info("Process is now setting up P2P discovery",
		"addr", relayAddr.String(),
	)

	// Initializes the local p2p.Switch
	// Creates a global P2P switch to respond even without chain info.
	eventSwitch := b.CreateOrLoadDiscoveryEventSwitch(ctx)

	// And start the switch (the P2P server).
	err = eventSwitch.Start()
	if err != nil {
		return nil, fmt.Errorf(
			"could not start p2p switch: %w", err)
	}

	// Open the broadcast port for listening continuously
	if listenTransport := eventSwitch.Transport(); listenTransport != nil {
		listenAddr := overwriteListenPort(
			nodeCfg.P2P.ListenAddress,
			int(nodeCfg.DiscoveryPort),
		)

		relayListenAddr, err := server.NewRelayAddress(listenAddr)
		if err != nil {
			return nil, fmt.Errorf(
				"could not create relay address for P2P: %w", err)
		}
		relayListenAddr.SetID(nodeKey.ID())
		netListenAddr, err := relayListenAddr.NetAddress()
		if err != nil {
			return nil, fmt.Errorf(
				"could not create p2p listen address: %w", err)
		}
		if err := listenTransport.Listen(*netListenAddr); err != nil {
			return nil, fmt.Errorf(
				"could not start listening on %s: %w", netListenAddr.DialString(), err)
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Process is now listening on broadcast port",
			"addr", netListenAddr.DialString(),
		)
	}

	return b.broadcastAddr, nil
}

// StartRPCServerDiscovery starts a RPC server with a RelayInfo function that
// may be used to retrieve node information, including the node ID.
// This method sets the listen address in discoveryAddr.
// Creates a transport listening on DiscoveryPort-1.
func (b *MultiplexBackend) StartRPCServerDiscovery(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	// RPC Discovery Port is always: `discovery_port-1`
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.DiscoveryPort-1), // always DiscoveryPort-1
	)

	relayAddr, err := server.NewRelayAddress(rpcListenAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"could not create relay address for RPC: %w", err)
	}

	relayAddr.SetID(nodeKey.ID())
	if b.discoveryAddr, err = relayAddr.NetAddress(); err != nil {
		return nil, fmt.Errorf(
			"could not create rpc listen address: %w", err)
	}

	b.logger.Info("Process is now setting up RPC discovery",
		"addr", relayAddr.StringWithoutId(),
	)

	// Initializes a local RPC server
	nodeRpc := b.reactor.GetNodeConfig().RPC
	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = nodeRpc.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = nodeRpc.MaxBodyBytes
	rpcConf.MaxHeaderBytes = nodeRpc.MaxHeaderBytes
	rpcConf.MaxOpenConnections = nodeRpc.MaxOpenConnections

	mux := http.NewServeMux()
	rpcLogger := b.logger.With("module", "rpc-server")

	// Enabled procedures:
	// - "info": POST /info to retrieve RelayInfo.

	infoImpl := server.NewRelayInfoServer(b)
	rpcserver.RegisterRPCFuncs(mux, map[string]*rpcserver.RPCFunc{
		"info":       rpcserver.NewRPCFunc(infoImpl.GetRelayInfo, ""),
		"validators": rpcserver.NewRPCFunc(infoImpl.InitValidators, "networks"),
	}, rpcLogger)

	rpcListener, err := rpcserver.Listen(
		relayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return nil, err
	}

	var rootHandler http.Handler = mux
	go func() {
		if err := rpcserver.Serve(
			rpcListener,
			rootHandler,
			rpcLogger,
			rpcConf,
		); err != nil && !errors.Is(err, net.ErrClosed) {
			b.logger.Error("Error serving RPC discovery server", "err", err)
		}
	}()

	b.rpcListeners = append(b.rpcListeners, rpcListener)

	return b.discoveryAddr, nil
}

// StartP2PServerCometBFT creates the CometBFT P2P Server that may be used
// to interact directly with a CometBFT node runtime, e.g to broadcast a
// transaction.
// Creates a transport listening on DiscoveryPort+1.
// This method sets the listen address in cometbftP2PAddr.
func (b *MultiplexBackend) StartP2PServerCometBFT(ctx context.Context) error {
	nodeConfig := b.reactor.GetNodeConfig()

	promoteAddr := nodeConfig.P2P.ExternalAddress
	if promoteAddr == "" {
		promoteAddr = nodeConfig.P2P.ListenAddress
	}

	// P2P CometBFT Port is always: `discovery_port+1`
	p2pListenAddr := overwriteListenPort(
		promoteAddr,
		int(nodeConfig.DiscoveryPort+1), // always DiscoveryPort+1
	)

	relayAddr, err := server.NewRelayAddress(p2pListenAddr)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for P2P: %w", err)
	}

	relayAddr.SetID(b.reactor.GetNodeKey().ID())
	if b.cometbftP2PAddr, err = relayAddr.NetAddress(); err != nil {
		return fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	b.logger.Info("Process is now setting up P2P cometbft",
		"addr", relayAddr.String(),
	)

	// uses DiscoveryPort+1
	sw := b.reactor.CreateOrLoadCometBFTEventSwitch(ctx, b.cometbftP2PAddr)

	// Start the transport.
	if err := sw.Transport().Listen(*b.cometbftP2PAddr); err != nil {
		return err
	}

	// Start the switch (the P2P server).
	err = sw.Start()
	if err != nil {
		return err
	}

	// Always connect to chain seed nodes, if any available
	if len(nodeConfig.P2P.Seeds) > 0 {
		knownSeeds := splitAndTrimEmpty(nodeConfig.P2P.Seeds, ",", " ")
		seedIds := []string{}
		for _, seedNodeAddr := range knownSeeds {
			seedAddr, err := server.NewRelayAddress(seedNodeAddr)
			if err != nil {
				return fmt.Errorf(
					"could not create seed node address for P2P: %w", err)
			}

			seedIds = append(seedIds, string(seedAddr.ID()))
		}

		sw.AddUnconditionalPeerIDs(seedIds)

		if err := sw.DialPeersAsync(knownSeeds); err != nil {
			return fmt.Errorf("could not dial peers from seeds field: %w", err)
		}
	}

	return nil
}

// StartRPCServerCometBFT starts a CometBFT RPC server that may be used
// to interact directly with a CometBFT node runtime, e.g to request the
// status of a node.
//
// CAUTION:
// Each network's ChainID is appended to RPC route names,
// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
// This permits us to use legacy RPC function implementation without
// modification apart from the route paths.
//
// Creates a transport listening on DiscoveryPort+2.
// This method sets the listen address in cometbftRPCAddr.
func (b *MultiplexBackend) StartRPCServerCometBFT() error {
	nodeCfg := b.reactor.GetNodeConfig()

	// RPC CometBFT Port is always: `discovery_port+2`
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.DiscoveryPort+2), // always DiscoveryPort+2
	)

	// We configure one RPC environment per running network,
	// i.e. contains reactors, stores and genesis.
	nodesProvider := b.reactor.GetServicesProvider()
	chainRoutes := map[string]rpccore.RoutesMap{}
	chainIds := b.GetNetworks()
	for _, chainID := range chainIds {
		nodeRuntime, ok := nodesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		if !ok {
			// Not activating the RPC now, it will be activated when
			// [Reactor#EnableNewRuntimeRPC] is called.
			continue
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
	routes := rpccore.RoutesMap{}
	for chainID, nodeRoutes := range chainRoutes {
		for route, rpcFunc := range nodeRoutes {
			routeKey := route + "/" + chainID
			routes[routeKey] = rpcFunc

			b.reactor.networkMutex.Lock()
			b.reactor.rpcRoutes[routeKey] = true
			b.reactor.networkMutex.Unlock()
		}
	}

	relayAddr, err := server.NewRelayAddress(rpcListenAddr)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for RPC: %w", err)
	}

	relayAddr.SetID(b.reactor.GetNodeKey().ID())
	if b.cometbftRPCAddr, err = relayAddr.NetAddress(); err != nil {
		return fmt.Errorf(
			"could not create rpc listen address: %w", err)
	}

	b.logger.Info("Process is now setting up CometBFT RPC",
		"addr", relayAddr.StringWithoutId(),
	)

	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = nodeCfg.RPC.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = nodeCfg.RPC.MaxBodyBytes
	rpcConf.MaxHeaderBytes = nodeCfg.RPC.MaxHeaderBytes
	rpcConf.MaxOpenConnections = nodeCfg.RPC.MaxOpenConnections
	if rpcConf.WriteTimeout <= nodeCfg.RPC.TimeoutBroadcastTxCommit {
		rpcConf.WriteTimeout = nodeCfg.RPC.TimeoutBroadcastTxCommit + 1*time.Second
	}

	rpcMultiplexer := http.NewServeMux()
	rpcLogger := b.logger.With("module", "rpc-server")
	wmLogger := rpcLogger.With("protocol", "websocket")
	wm := rpcserver.NewWebsocketManager(routes,
		// TODO(midas): many instances of eventBus exist
		// rpcserver.OnDisconnect(func(remoteAddr string) {
		// 	err := n.eventBus.UnsubscribeAll(context.Background(), remoteAddr)
		// 	if err != nil && err != cmtpubsub.ErrSubscriptionNotFound {
		// 		wmLogger.Error("Failed to unsubscribe addr from events", "addr", remoteAddr, "err", err)
		// 	}
		// }),
		rpcserver.ReadLimit(rpcConf.MaxBodyBytes),
		rpcserver.WriteChanCapacity(nodeCfg.RPC.WebSocketWriteBufferSize),
	)
	wm.SetLogger(wmLogger)
	rpcMultiplexer.HandleFunc("/websocket", wm.WebsocketHandler)
	rpcMultiplexer.HandleFunc("/v1/websocket", wm.WebsocketHandler)
	rpcserver.RegisterRPCFuncs(rpcMultiplexer, routes, rpcLogger)
	rpcListener, err := rpcserver.Listen(
		relayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return err
	}

	var rootHandler http.Handler = rpcMultiplexer
	if nodeCfg.RPC.IsCorsEnabled() {
		corsMiddleware := cors.New(cors.Options{
			AllowedOrigins: nodeCfg.RPC.CORSAllowedOrigins,
			AllowedMethods: nodeCfg.RPC.CORSAllowedMethods,
			AllowedHeaders: nodeCfg.RPC.CORSAllowedHeaders,
		})
		rootHandler = corsMiddleware.Handler(rpcMultiplexer)
	}
	if nodeCfg.RPC.IsTLSEnabled() {
		go func() {
			if err := rpcserver.ServeTLS(
				rpcListener,
				rootHandler,
				nodeCfg.RPC.CertFile(),
				nodeCfg.RPC.KeyFile(),
				rpcLogger,
				rpcConf,
			); err != nil && !errors.Is(err, net.ErrClosed) {
				b.logger.Error("Error serving server with TLS", "err", err)
			}
		}()
	} else {
		go func() {
			if err := rpcserver.Serve(
				rpcListener,
				rootHandler,
				rpcLogger,
				rpcConf,
			); err != nil && !errors.Is(err, net.ErrClosed) {
				b.logger.Error("Error serving server", "err", err)
			}
		}()
	}

	b.reactor.SetRPCMultiplexer(rpcMultiplexer)
	b.rpcListeners = append(b.rpcListeners, rpcListener)
	return nil
}

// StartPrometheusServer starts a Prometheus HTTP server, listening for metrics
// collectors on addr.
// Creates a transport listening on DiscoveryPort+3.
// This method sets the listen address in prometheusAddr.
func (b *MultiplexBackend) StartPrometheusServer() error {
	nodeCfg := b.reactor.GetNodeConfig()
	prometheusCfg := nodeCfg.Instrumentation

	// Allows disabling prometheus through legacy config.
	if !prometheusCfg.Prometheus {
		return nil
	}

	if len(prometheusCfg.PrometheusListenAddr) == 0 {
		return nil
	}

	// Prometheus Port is always: `discovery_port+3`
	monListenAddr := overwriteListenPort(
		prometheusCfg.PrometheusListenAddr,
		int(nodeCfg.DiscoveryPort+3), // always DiscoveryPort+3
	)

	relayAddr, err := server.NewRelayAddress(monListenAddr)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for Prometheus: %w", err)
	}

	relayAddr.SetID(b.reactor.GetNodeKey().ID())
	if b.prometheusAddr, err = relayAddr.NetAddress(); err != nil {
		return fmt.Errorf(
			"could not create Prometheus listen address: %w", err)
	}

	b.logger.Info("Process is now setting up Prometheus HTTP",
		"addr", relayAddr.StringHostname(),
	)

	srv := &http.Server{
		Addr: relayAddr.StringHostname(),
		Handler: promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				prometheus.DefaultGatherer,
				promhttp.HandlerOpts{MaxRequestsInFlight: prometheusCfg.MaxOpenConnections},
			),
		),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	b.httpServers[relayAddr.StringHostname()] = srv

	go func(host string, httpServer *http.Server) {
		// defer func() {
		// 	b.httpServers[host] = nil // inaccessible (GC)
		// 	delete(b.httpServers, host)
		// }()

		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener:
			b.logger.Error("Error serving Prometheus HTTP server", "err", err)
		}
	}(relayAddr.StringHostname(), srv)

	return nil
}

// ----------------------------------------------------------------------------

// remoteAckReplicationConsumer reacts to ChainReplicationResponse messages
// about ChainIDs and proxies to localReplResCh.
//
// Note that remoteReplResCh is expected to be written on separately,
// namely by [Reactor#Receive], when it intercepts a ChainReplicationResponse.
func (b *MultiplexBackend) remoteAckReplicationConsumer(
	ctx context.Context,
	chainID string,
	remoteReplResCh chan *mxp2p.ChainReplicationResponse,
	localReplResCh chan *mxp2p.ChainReplicationResponse,
	resultsCh chan AckReplicationResult,
	shutdownCh chan struct{},
	transactions ...client.Transaction,
) {
	transactionHashes := txHashesToHex(transactions...)

	for {
		select {
		case res := <-remoteReplResCh:
			// In case of channel closing early.
			if res == nil {
				// TODO(midas): remove debug logs
				b.logger.Debug("CAUTION: Intercepted nil ChainReplicationResponse (channel closed early)",
					"chain_id", chainID,
					"tx_batch", transactionHashes,
				)
				return
			}

			b.logger.Debug("Intercepted relevant ChainReplicationResponse",
				"relay_id", res.NodeId,
				"chain_id", res.ChainID,
				"tx_batch", transactionHashes,
			)

			localReplResCh <- res

		case <-ctx.Done():
			err := fmt.Errorf(
				"process timed out waiting for remote replication for ChainID: %s", chainID)

			resultsCh <- AckReplicationResult{Error: err}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// localAckReplicationConsumer reacts to internal updates on localReplResCh,
// which are issued after parsing a ChainReplicationResponse message in method
// remoteAckReplicationConsumer.
//
// Collects localReplResCh messages and creates a result object.
// The resultsCh channel is also used to transmit errors when cancelled.
func (b *MultiplexBackend) localAckReplicationConsumer(
	ctx context.Context,
	relevantRelays []string,
	chainID string,
	localReplResCh chan *mxp2p.ChainReplicationResponse,
	resultsCh chan AckReplicationResult,
	shutdownCh chan struct{},
	transactions ...client.Transaction,
) {
	relaysPerChain := make(map[string][]string, 1)
	numExpected := len(relevantRelays)
	numReceived := 0
	transactionHashes := txHashesToHex(transactions...)
	if numExpected == 0 {
		resultsCh <- AckReplicationResult{
			Relays:  []string{},
			ChainID: chainID,
		}
		return
	}

	for {
		select {
		case res := <-localReplResCh:
			// In case of channel closing early.
			if res == nil {
				// TODO(midas): remove debug logs
				b.logger.Debug("Intercepted nil ChainReplicationResponse (local channel closed)",
					"chain_id", chainID,
					"tx_batch", transactionHashes,
				)
				return
			}

			relayId := res.NodeId
			resChainID := res.ChainID

			b.replResponsesMtx.RLock()
			_, hasReplResponses := b.replResponsesRcvd[resChainID]
			b.replResponsesMtx.RUnlock()

			b.replResponsesMtx.Lock()
			if !hasReplResponses {
				b.replResponsesRcvd[resChainID] = make([]string, 0, numExpected)
			}
			b.replResponsesRcvd[resChainID] = append(b.replResponsesRcvd[resChainID], relayId)
			b.replResponsesMtx.Unlock()

			numReceived++

			// TODO(midas): remove debug logs
			b.logger.Debug("Done processing chain replication response",
				"relay_id", res.NodeId,
				"chain_id", res.ChainID,
				"num_rcvd", numReceived,
				"num_expect", numExpected,
				"tx_batch", transactionHashes,
			)

			if numReceived >= numExpected {
				// TODO(midas): remove debug logs
				b.logger.Debug("Processed enough ChainReplicationResponse",
					"chain_id", resChainID,
					"num_rcvd", numReceived,
					"num_expect", numExpected,
					"tx_batch", transactionHashes,
				)

				// Result should contain only relevant transactions
				b.replResponsesMtx.RLock()
				relaysPerChain[resChainID] = make([]string, 0, len(b.replResponsesRcvd[resChainID]))
				relaysPerChain[resChainID] = append(relaysPerChain[resChainID], b.replResponsesRcvd[resChainID]...)
				b.replResponsesMtx.RUnlock()

				resultsCh <- AckReplicationResult{
					Relays:  relaysPerChain[resChainID],
					ChainID: resChainID,
				}
				return
			}

		case <-ctx.Done():
			err := fmt.Errorf(
				"process timed out waiting for replication responses for %s", chainID)

			resultsCh <- AckReplicationResult{Error: err}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// remoteAckTransactionConsumer reacts to AckTransactionBroadcast messages
// about transaction and proxies to localAcceptTxCh in a message formatted
// to contain the relay ID and tx hash: `id:tx_hash_hex`.
//
// Note that remoteAcceptTxCh is expected to be written on separately,
// namely by [Reactor#Receive], when it intercepts a AckTransactionBroadcast.
func (b *MultiplexBackend) remoteAckTransactionConsumer(
	ctx context.Context,
	relevantRelays []string,
	transaction client.Transaction,
	remoteAcceptTxCh chan *mxp2p.AckTransactionBroadcast,
	localAcceptTxCh chan string,
	resultsCh chan AckTransactionResult,
	shutdownCh chan struct{},
) {
	consumerTxHash := fmt.Sprintf("%X", transaction.Hash())
	numExpected := len(relevantRelays)
	if numExpected == 0 {
		return
	}

	for {
		select {
		// Note: AckTransactionBroadcast always contains exactly one tx hash
		// because the remote mempool processes one transaction at a time.
		case ackResponse := <-remoteAcceptTxCh:
			// In case of channel closing early.
			if ackResponse == nil {
				// TODO(midas): remove debug logs
				b.logger.Debug("Intercepted nil AckTransactionBroadcast (remote channel closed)",
					"consumer_tx", consumerTxHash,
				)
				return
			}

			relayId := ackResponse.NodeId
			ackTxHash := fmt.Sprintf("%X", ackResponse.TxHashes[0])

			// TODO(midas): remove debug logs
			b.logger.Debug("Intercepted relevant AckTransactionBroadcast",
				"relay_id", relayId,
				"tx_hash", ackTxHash,
			)

			acceptMsg := fmt.Sprintf("%s:%s", relayId, ackTxHash)
			localAcceptTxCh <- acceptMsg

		case <-ctx.Done():
			err := fmt.Errorf(
				"process timed out waiting for ack messages (remote) for tx: %s", consumerTxHash)

			resultsCh <- AckTransactionResult{
				TxHash: consumerTxHash,
				Error:  err,
			}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// localAckTransactionConsumer reacts to internal updates on localAcceptTxCh,
// which are issued after parsing a AckTransactionBroadcast message in method
// remoteAckTransactionConsumer.
//
// Collects localAcceptTxCh messages and creates a result object.
// The resultsCh channel is also used to transmit errors when cancelled.
func (b *MultiplexBackend) localAckTransactionConsumer(
	ctx context.Context,
	relevantRelays []string,
	transaction client.Transaction,
	localAcceptTxCh chan string,
	resultsCh chan AckTransactionResult,
	shutdownCh chan struct{},
) {
	consumerTxHash := fmt.Sprintf("%X", transaction.Hash())

	relaysPerTx := make(map[string][]string, 1)
	numExpected := len(relevantRelays)
	numReceived := 0
	if numExpected == 0 {
		resultsCh <- AckTransactionResult{
			Relays: []string{},
			TxHash: consumerTxHash,
		}
		return
	}

	for {
		select {
		// Note: acceptTxMsg contains one relay ID and one tx hash.
		case acceptTxMsg := <-localAcceptTxCh:
			parts := strings.Split(acceptTxMsg, ":")
			if len(parts) != 2 {
				err := fmt.Errorf(
					"could not parse ack transaction message: '%s'", acceptTxMsg)

				resultsCh <- AckTransactionResult{
					TxHash: consumerTxHash,
					Error:  err,
				}
				return
			}

			relayId, txHash := parts[0], parts[1]

			// TODO(midas): remove debug logs
			b.logger.Debug("Locally processing remote transaction ACK",
				"relay_id", relayId,
				"tx_hash", txHash,
			)

			b.ackResponsesMtx.RLock()
			ackResponsesRcvdForTx, hasAckResponsesForTx := b.ackResponsesRcvd[txHash]
			b.ackResponsesMtx.RUnlock()

			relayAlreadyAckedTx := false
			b.ackResponsesMtx.Lock()
			if !hasAckResponsesForTx {
				b.ackResponsesRcvd[txHash] = make([]string, 0, numExpected)
				b.ackResponsesRcvd[txHash] = append(b.ackResponsesRcvd[txHash], relayId)
			} else if !slices.Contains(ackResponsesRcvdForTx, relayId) {
				b.ackResponsesRcvd[txHash] = append(b.ackResponsesRcvd[txHash], relayId)
			} else { // already acked
				relayAlreadyAckedTx = true
			}
			b.ackResponsesMtx.Unlock()

			if !relayAlreadyAckedTx {
				numReceived++

				// TODO(midas): remove debug logs
				b.logger.Debug("Done processing relevant AckTransactionBroadcast",
					"relay_id", relayId,
					"tx_hash", txHash,
					"num_rcvd", numReceived,
					"num_expect", numExpected,
				)
			}

			if numReceived >= numExpected {
				// TODO(midas): remove debug logs
				b.logger.Debug("Processed enough AckTransactionBroadcast",
					"tx_hash", txHash,
					"num_rcvd", numReceived,
					"num_expect", numExpected,
				)

				// Result should contain only relevant transactions
				b.ackResponsesMtx.RLock()
				relaysPerTx[txHash] = make([]string, 0, len(b.ackResponsesRcvd[txHash]))
				relaysPerTx[txHash] = append(relaysPerTx[txHash], b.ackResponsesRcvd[txHash]...)
				b.ackResponsesMtx.RUnlock()

				resultsCh <- AckTransactionResult{
					Relays: relaysPerTx[txHash],
					TxHash: txHash,
				}
				return
			}

		case <-ctx.Done():
			err := fmt.Errorf(
				"process timed out waiting for ack messages (local) for tx: %s", consumerTxHash)

			resultsCh <- AckTransactionResult{
				TxHash: consumerTxHash,
				Error:  err,
			}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// remoteRuntimeUpdatesConsumer reacts to ChainReplicationComplete messages
// about ChainIDs and proxies to localReplFinCh.
//
// Note that remoteReplFinCh is expected to be written on separately,
// namely by [Reactor#Receive], when it intercepts a ChainReplicationComplete.
func (b *MultiplexBackend) remoteRuntimeUpdatesConsumer(
	ctx context.Context,
	chainID string,
	remoteReplFinCh chan *mxp2p.ChainReplicationComplete,
	localReplFinCh chan *mxp2p.ChainReplicationComplete,
	resultsCh chan RuntimeUpdateResult,
	shutdownCh chan struct{},
	transactions ...client.Transaction,
) {
	transactionHashes := txHashesToHex(transactions...)

	// Wait a maximum duration of replicationTimeout. With a replicationTimeout
	// of 0, this method will block until shutdown or parent context expiration.
	var cancelFn func()
	clientCtxOrTimeout := ctx
	if b.replicationTimeout != 0 {
		clientCtxOrTimeout, cancelFn = context.WithTimeout(ctx, b.replicationTimeout)
		defer cancelFn()
	}

	for {
		select {
		case res := <-remoteReplFinCh:
			// In case of channel closing early.
			if res == nil {
				// TODO(midas): remove debug logs
				b.logger.Debug("Intercepted nil ChainReplicationComplete (remote channel closed)",
					"chain_id", chainID,
					"tx_batch", transactionHashes,
				)
				return
			}

			b.logger.Debug("Intercepted relevant runtime status update (ChainReplicationComplete)",
				"relay_id", res.NodeId,
				"chain_id", res.ChainID,
				"tx_batch", transactionHashes,
			)

			localReplFinCh <- res

		case <-clientCtxOrTimeout.Done():
			err := fmt.Errorf(
				"process timed out waiting for runtime status (remote) for: %s", chainID)

			resultsCh <- RuntimeUpdateResult{Error: err}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// localRuntimeUpdatesConsumer reacts to internal updates on localReplFinCh,
// which are issued after parsing a ChainReplicationComplete message in method
// remoteRuntimeUpdatesConsumer.
//
// Collects localReplFinCh messages and creates a result object.
// The resultsCh channel is also used to transmit errors when cancelled.
func (b *MultiplexBackend) localRuntimeUpdatesConsumer(
	ctx context.Context,
	relevantRelays []string,
	chainID string,
	localReplFinCh chan *mxp2p.ChainReplicationComplete,
	resultsCh chan RuntimeUpdateResult,
	shutdownCh chan struct{},
	transactions ...client.Transaction,
) {
	relaysPerChain := make(map[string][]string, 1)
	numExpected := len(relevantRelays)
	numReceived := 0
	if numExpected == 0 {
		resultsCh <- RuntimeUpdateResult{
			Relays:  []string{},
			ChainID: chainID,
		}
		return
	}

	transactionHashes := txHashesToHex(transactions...)

	// Wait a maximum duration of replicationTimeout. With a replicationTimeout
	// of 0, this method will block until shutdown or parent context expiration.
	var cancelFn func()
	clientCtxOrTimeout := ctx
	if b.replicationTimeout != 0 {
		clientCtxOrTimeout, cancelFn = context.WithTimeout(ctx, b.replicationTimeout)
		defer cancelFn()
	}

	for {
		select {
		case res := <-localReplFinCh:
			// In case of channel closing early.
			if res == nil {
				// TODO(midas): remove debug logs
				b.logger.Debug("Intercepted nil ChainReplicationComplete (local channel closed)",
					"chain_id", chainID,
					"tx_batch", transactionHashes,
				)
				return
			}

			relayId := res.NodeId
			resChainID := res.ChainID

			b.replCompleteMtx.RLock()
			_, hasReplComplete := b.replCompleteRcvd[resChainID]
			b.replCompleteMtx.RUnlock()

			b.replCompleteMtx.Lock()
			if !hasReplComplete {
				b.replCompleteRcvd[resChainID] = make([]string, 0, numExpected)
			}
			b.replCompleteRcvd[resChainID] = append(b.replCompleteRcvd[resChainID], relayId)
			b.replCompleteMtx.Unlock()

			numReceived++

			// TODO(midas): remove debug logs
			b.logger.Debug("Done processing runtime status update",
				"relay_id", res.NodeId,
				"chain_id", res.ChainID,
				"num_rcvd", numReceived,
				"num_expect", numExpected,
				"tx_batch", transactionHashes,
			)

			if numReceived >= numExpected {
				// TODO(midas): remove debug logs
				b.logger.Debug("Processed enough ChainReplicationComplete",
					"chain_id", resChainID,
					"num_rcvd", numReceived,
					"num_expect", numExpected,
					"tx_batch", transactionHashes,
				)

				// Result should contain only relevant transactions
				b.replResponsesMtx.RLock()
				relaysPerChain[resChainID] = make([]string, 0, len(b.replCompleteRcvd[resChainID]))
				relaysPerChain[resChainID] = append(relaysPerChain[resChainID], b.replCompleteRcvd[resChainID]...)
				b.replResponsesMtx.RUnlock()

				resultsCh <- RuntimeUpdateResult{
					Relays:  relaysPerChain[resChainID],
					ChainID: resChainID,
				}
				return
			}

		case <-clientCtxOrTimeout.Done():
			err := fmt.Errorf(
				"process timed out waiting for runtime status (local) for %s", chainID)

			resultsCh <- RuntimeUpdateResult{Error: err}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// localTransactionEventsConsumer reacts to internal updates on a transaction
// events subscriber, which are issued by CometBFT when transactions are
// included in bocks.
//
// Collects [types.EventDataTx] messages and creates a result object.
// The resultsCh channel is also used to transmit errors when cancelled.
func (b *MultiplexBackend) localTransactionEventsConsumer(
	ctx context.Context,
	chainEventBus *types.EventBus,
	chainID string,
	resultsCh chan TransactionEventResult,
	shutdownCh chan struct{},
	transactions ...client.Transaction,
) {
	txHashesFound := make([]string, 0, len(transactions))
	numExpected := len(transactions)
	numReceived := 0
	if numExpected == 0 {
		resultsCh <- TransactionEventResult{
			TxHashes: []string{},
			ChainID:  chainID,
		}
		return
	}

	transactionHashes := txHashesToHex(transactions...)
	broadcastId := strings.Join(transactionHashes, "_")

	subscriberName := strings.Join([]string{
		"broadcastCompletion",
		chainID,
		broadcastId,
	}, "_")

	cancelTimer := time.NewTimer(b.transactionTimeout)
	txsSub, err := chainEventBus.Subscribe(context.Background(), subscriberName, types.EventQueryTx, len(transactionHashes))
	if err != nil {
		resultsCh <- TransactionEventResult{Error: err}
		return
	}
	defer chainEventBus.UnsubscribeAll(ctx, subscriberName)

	defer cancelTimer.Stop()

	b.txSubscribers[chainID] = subscriberName
	for {
		select {
		case tx, ok := <-txsSub.Out():
			if !ok {
				return
			}

			// Interpret received transaction result
			txResult := tx.Data().(types.EventDataTx).TxResult
			rawTx := types.Tx(txResult.Tx)
			foundTx := client.RawTxToTransaction(rawTx)

			txHashesFound = append(txHashesFound, txHashesToHex(foundTx)[0])
			numReceived++

			// TODO(midas): remove debug logs
			b.logger.Debug("Done processing transaction event",
				"chain_id", chainID,
				"num_rcvd", numReceived,
				"num_expect", numExpected,
				"tx_batch", transactionHashes,
			)

			if numReceived >= numExpected {
				// Transaction is not yet indexed
				b.logger.Debug("Found all indexed transactions",
					"chain_id", chainID,
					"tx_batch", transactionHashes,
				)

				resultsCh <- TransactionEventResult{
					TxHashes: txHashesFound,
					ChainID:  chainID,
				}
				return
			}

		case <-cancelTimer.C:
			err := fmt.Errorf(
				"process timed out waiting for transaction events for %s (fallback)", chainID)

			resultsCh <- TransactionEventResult{Error: err}
			return

		case <-ctx.Done():
			err := fmt.Errorf(
				"process timed out waiting for transaction events for %s", chainID)

			resultsCh <- TransactionEventResult{Error: err}
			return

		case <-b.reactor.Quit():
		case <-shutdownCh:
			return
		}
	}
}

// metricsReporter runs a reporter every metricsTickerDuration until
// the backend is stopped.
//
// - Report ProcessorUsage as the current CPU load percentage.
// - Report MemoryUsage as the total allocated bytes across all networks.
// - Report TotalNetworkBytes as the total of bytes received and sent across all networks.
// - Report TotalBlocks as the total number of blocks across all networks.
// - Report TotalTxs as the total number of transactions across all networks.
// - Report Errors as the total number of failed transactions across all networks.
// - Report TotalBlocksPerUser as the total number of blocks by each user.
// - Report TotalTxsPerUser as the total number of transactions by each user.
// - Report ErrorsPerUser as the total number of failed transactions by each user.
func (b *MultiplexBackend) metricsReporter() {
	metricsTicker := time.NewTicker(metricsTickerDuration)
	defer metricsTicker.Stop()

	for b.Context().Err() == nil {
		select {
		case <-metricsTicker.C:
			if b.metrics == nil {
				return
			}

			// If we are (also) shutting down, stop here.
			select {
			case <-b.reactor.Quit():
				return
			default:
			}

			prometheusCfg := b.reactor.GetNodeConfig().Instrumentation
			relayMetricsPrefix := prometheusCfg.Namespace + "_" + string(b.reactor.GetNodeKey().ID())

			// Resources sampling for CPU and RAM
			collectSampleCPU(b.metrics.ProcessorUsage)()
			collectSampleRAM(b.metrics.MemoryUsage)()

			// Bandwidth usage sampling
			collectSampleP2P(relayMetricsPrefix, b.metrics, []string{
				"message_receive_bytes_total",
				"message_send_bytes_total",
			})()

			// For each network, collect CometBFT metrics (blocks, txes)
			chainIds := b.GetNetworks()
			for _, chainID := range chainIds {
				// See also: multiplex/consensus.go
				chainMetricsPrefix := relayMetricsPrefix + ":" + strings.ReplaceAll(chainID, "-", "_")

				// TODO(midas): enable user-grouped metrics with ChainID and RelayID.
				collectSampleCometBFT(
					// string(b.reactor.GetNodeKey().ID()),
					// chainID,
					chainMetricsPrefix,
					b.metrics,
				)()
			}

		case <-b.reactor.Quit():
			return
		}
	}
}
