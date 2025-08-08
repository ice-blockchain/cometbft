package multiplex

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	cmtrpcclient "github.com/ice-blockchain/cometbft/rpc/jsonrpc/client"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/replay"
	mxrpc "github.com/ice-blockchain/cometbft/multiplex/rpc"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// Assert that our implementation satisfies the [types.Backend] interface.
var _ types.Backend = (*MultiplexBackend)(nil)

// Assert that our implementation satisfies the [snapsapp.Backend] interface.
var _ snapsapp.Backend = (*MultiplexBackend)(nil)

// ----------------------------------------------------------------------------
// MultiplexBackend defines a multiplex backend adapter implementation
//
// MultiplexBackend implements the [types.Backend] interface for a multiplex
// using the CometBFT blockchain engine to replicate data.
//
// This implementation makes use of instances of [cmtp2p.Switch] to communicate
// with other relays about chain replications and transaction broadcasts.
//
// Additionally, an internal [client.Acceptor] instance may be used to further
// extend the broadcast verification process, e.g. [client.CommitBroadcastTx].
type MultiplexBackend struct {
	*service.BaseService

	// A mutex is locked for reactor and eventSwitch transitions.
	mtx sync.Mutex

	// An acceptor implementation to which transactions will be forwarded.
	acceptor client.Acceptor
	// A background tasks implementation used during broadcast operations.
	routines *types.Jobs
	// Our own relay address, it contains a Relay ID (CometBFT Node ID).
	relayAddr *helpers.RelayAddress // uses `DiscoveryPort`.
	// Our p2p node key, it contains an ed25519 public key.
	nodeKey *cmtp2p.NodeKey

	// An ABCI client implementation, forcefully enabled.
	snapsApp *snapsapp.SnapsApp
	// An ABCI client connected to the SnapsApp application.
	chainConns proxy.ChainConns
	// A dynamic chain registry used to read known ChainID values.
	chainRegistry helpers.ChainRegistry

	// An event switch listening on `DiscoveryPort`, the attached [cmtp2p.NodeInfo],
	// and a [p2p.ConnectionPool] to handle P2P connections between peers.
	discoverySwitch *cmtp2p.Switch
	discoveryInfo   *p2p.MultiNetworkNodeInfo
	discoveryPool   *p2p.ConnectionPool
	// An event switch listening on `DiscoveryPort+1`.
	cometbftSwitch *cmtp2p.Switch
	cometbftInfo   *p2p.MultiNetworkNodeInfo
	cometbftPool   *p2p.ConnectionPool
	// A prometheus HTTP server listening on `DiscoveryPort+3`.
	prometheusHttp *http.Server
	// A RPC multiplexer listening on `DiscoveryPort+2`.
	rpcMultiplexer *http.ServeMux
	// A list of active RPC servers as [net.Listener] instances.
	rpcListeners []net.Listener
	// A list of open (or idle) HTTP client connections (RelayInfo RPC).
	httpClients []*http.Client
	// A list of open RPC client connections (RelayInfo RPC).
	jsonRpcClients map[string]*cmtrpcclient.Client

	// A broadcast operations manager, or processor for `AckTransactionBroadcast`.
	broadcastMgr types.BroadcastManager
	// A replications manager, or processor for `ChainReplicationRequest`.
	replicationMgr types.ReplicationManager
	// A resources manager is responsible for all services and resources.
	resourceMgr types.ResourceManager

	// A runtime manager used to orchestrate networks and start consensus.
	runtimeRegistry *runtime.Registry
	// A catchup replay pool to process missed events using the acceptor.
	replayPool *replay.ReplayPool

	// A map of RPC routes where keys contain RPC paths, e.g. `info/test-chain`.
	knownRPCRoutes map[string]*rpcserver.RPCFunc
	// A map of [mxrpc.RPCResultRelayInfo] by relay ID, caches relays info.
	knownRelayInfo map[string]*mxrpc.RPCResultRelayInfo
	// A relay's P2P listen address, it contains a Relay ID.
	listenAddress string

	// The multiplex reactor is used to process runtime, broadcast and
	// replication messages from other relays.
	reactor *Reactor

	// Internals
	backendCfg *config.Config
	logger     cmtlog.Logger
	errorsCh   chan error
	shutdownCh chan struct{}
	metrics    *Metrics
}

type MultiplexBackendOption func(*MultiplexBackend)

// ----------------------------------------------------------------------------
// Option helpers implementation

// WithRoutines is an option helper to overwrite the [types.Jobs] instance.
func WithRoutines(jobs *types.Jobs) MultiplexBackendOption {
	return func(b *MultiplexBackend) {
		b.routines = jobs
	}
}

// WithMetrics is an option helper to overwrite the [Metrics] instance.
func WithMetrics(metrics *Metrics) MultiplexBackendOption {
	return func(b *MultiplexBackend) {
		b.metrics = metrics
	}
}

// WithLogger is an option helper to inject a custom backend logger.
func WithLogger(
	logger cmtlog.Logger,
) MultiplexBackendOption {
	return func(b *MultiplexBackend) {
		b.logger = logger
	}
}

// ----------------------------------------------------------------------------
// Constructor

// NewServer initializes a new [MultiplexBackend] around an empty multiplex
// configuration and prepares the node backend by starting the reactor and
// configuring the necessary backend services, i.e. runtime, broadcast and
// replications managers.
//
// The internal [types.RuntimeManager] instance will be started when calling
// this method, and resources and services instances can be retrieved using
// the [types.ResourceManager] instance.
//
// See also: [NewNodesMultiplex]
func NewServer(
	ctx context.Context,
	impl client.Acceptor,
	nodeConfig *config.Config,
	nodeLogger cmtlog.Logger,
	options ...MultiplexBackendOption,
) (*MultiplexBackend, error) {
	initTime := time.Now()

	nodeConfig.DBBackend = "goleveldb"
	nodeConfig.Consensus.CreateEmptyBlocks = false // Force to create blocks only if there are transactions.
	nodeConfig.Consensus.TimeoutCommit = 0         // Make progress as soon as the node has all the precommits.
	nodeConfig.P2P.AllowDuplicateIP = true

	// Creates or re-use config/ and data/ folders.
	if _, _, err := helpers.EnsureBackendFS(nodeConfig); err != nil {
		return nil, fmt.Errorf(
			"failed to initialize filesystem: %w", err)
	}

	// Creates one [cmtp2p.NodeKey] instance per backend.
	nodeKey, err := cmtp2p.LoadOrGenNodeKey(nodeConfig.NodeKeyFile())
	if err != nil {
		return nil, fmt.Errorf(
			"failed to load or gen node key %s: %w", nodeConfig.NodeKeyFile(), err)
	}

	// Creates one manager service instance per backend.
	resourceMgr := runtime.NewResourceManager(ctx, nodeLogger)
	replicationMgr := runtime.NewReplicationManager(ctx, nodeLogger)
	broadcastMgr := runtime.NewBroadcastManager(ctx, nodeLogger)

	// Initialize the multiplex reactor, responsible for broadcast and
	// replication messages. Created exactly once per backend server.
	reactor := NewReactor(ctx,
		nodeKey,
		nodeConfig,
		resourceMgr,
		replicationMgr,
		broadcastMgr,
		nodeLogger.With("module", "multiplex"),
	)

	backend := &MultiplexBackend{
		reactor:  reactor,
		acceptor: impl,
		nodeKey:  nodeKey,

		resourceMgr:    resourceMgr,
		replicationMgr: replicationMgr,
		broadcastMgr:   broadcastMgr,

		rpcListeners:   []net.Listener{},
		httpClients:    []*http.Client{},
		jsonRpcClients: map[string]*cmtrpcclient.Client{},

		knownRPCRoutes: map[string]*rpcserver.RPCFunc{},
		knownRelayInfo: map[string]*mxrpc.RPCResultRelayInfo{},

		logger:     nodeLogger.With("module", "multiplex"),
		backendCfg: nodeConfig,
	}

	// Enable overwrite of optional properties
	for _, option := range options {
		option(backend)
	}

	backend.BaseService = service.NewBaseService(ctx,
		backend.logger,
		"MultiplexBackend",
		backend,
	)

	if backend.acceptor == nil {
		backend.acceptor = &client.DefaultAcceptor{}
	}

	// Initializes the relayAddr property and internal (shared) services:
	// - chainRegistry
	// - snapsApp
	// - runtimeRegistry
	// - discoverySwitch, discoveryPool
	// - cometbftSwitch, cometbftPool
	if err := backend.Init(); err != nil {
		return nil, fmt.Errorf(
			"SERVER PANIC: could not initialize node backend: %w", err)
	}

	// The pools' dispatchers need a multiplex reactor to create channels.
	backend.discoveryPool.Dispatcher().SetMultiplexReactor(reactor)
	backend.cometbftPool.Dispatcher().SetMultiplexReactor(reactor)

	// The registry's composer needs a switch to create [node.Node].
	backend.runtimeRegistry.Composer().SetSwitch(backend.cometbftSwitch)

	// The registry's consensus pool needs a switch for consensus reactors.
	backend.runtimeRegistry.ConsensusPool().SetSwitch(backend.cometbftSwitch)

	// The multiplex reactor needs the runtime manager when it intercepts
	// mempool messages that must be forwarded to a *running* mempool.
	backend.reactor.SetRuntimeManager(backend.runtimeRegistry)
	// The multiplex reactor uses discoveryPool and cometbftPool to dial
	// back replication partners and broadcast operation partners.
	backend.reactor.SetDiscoveryPool(backend.discoveryPool)
	backend.reactor.SetRuntimePool(backend.cometbftPool)

	if backend.metrics != nil {
		defer addTimeSample(backend.metrics.InitDurationSeconds, initTime)()
	}

	// TODO(midas): remove debug logs
	backend.logger.Debug("NewServer",
		"addr", backend.relayAddr.String(),
		"took", time.Since(initTime).String(),
	)
	return backend, nil
}

// ----------------------------------------------------------------------------
// types.Server API implementation

// Config returns a [config.Config] instance.
func (b *MultiplexBackend) Config() *config.Config {
	return b.backendCfg
}

// GetLogger returns a [cmtlog.Logger].
func (b *MultiplexBackend) GetLogger() cmtlog.Logger {
	return b.logger
}

// SetLogger sets a custom [cmtlog.Logger].
func (b *MultiplexBackend) SetLogger(logger cmtlog.Logger) {
	b.logger = logger
}

// Acceptor returns a [client.Acceptor] implementation.
func (b *MultiplexBackend) Acceptor() client.Acceptor {
	return b.acceptor
}

// SetAcceptor sets a custom [client.Acceptor] implementation.
func (b *MultiplexBackend) SetAcceptor(acceptorImpl client.Acceptor) {
	b.acceptor = acceptorImpl
}

// RuntimeManager should return the runtime manager's idler implementation.
func (b *MultiplexBackend) RuntimeManager() types.RuntimeManager {
	return b.runtimeRegistry
}

// IdleManager should return the runtime manager's idler implementation.
func (b *MultiplexBackend) IdleManager() types.IdleManager {
	return b.runtimeRegistry
}

// Discovery returns the switch listening on `DiscoveryPort`.
func (b *MultiplexBackend) Discovery() *cmtp2p.Switch {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.discoverySwitch
}

// CometBFT returns the switch listening on `DiscoveryPort+2`.
func (b *MultiplexBackend) CometBFT() *cmtp2p.Switch {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.cometbftSwitch
}

// SnapsApp returns the local SnapsApp application, i.e. [snapsapp.SnapsApp].
func (b *MultiplexBackend) SnapsApp() *snapsapp.SnapsApp {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.snapsApp
}

// ChainConns returns the ABCI client as defined with [proxy.ChainConns.]
func (b *MultiplexBackend) ChainConns() proxy.ChainConns {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	return b.chainConns
}

// ----------------------------------------------------------------------------
// snapsapp.Backend API implementation

// HasNetwork should return true if a ChainID is known to a node.
func (b *MultiplexBackend) HasNetwork(chainID string) bool {
	return b.chainRegistry.HasChain(chainID)
}

// StateStore should return the state store for ChainID.
func (b *MultiplexBackend) StateStore(chainID string) sm.Store {
	return b.resourceMgr.Get(
		chainID,
		types.InstanceKeyStateStore,
	).(sm.Store)
}

// Mempool should return the mempool for ChainID.
func (b *MultiplexBackend) Mempool(chainID string) snapsapp.TxAcceptor {
	mempoolReactor := b.resourceMgr.Get(
		chainID,
		types.ServiceKeyMempoolReactor,
	).(*mempl.Reactor)
	return mempoolReactor.GetMempoolPtr()
}

// ReplayPool returns a [replay.ReplayPool] which contains transactions
// batches to be replayed. These batches may contain one or many txes
// that will be forwarded to [Acceptor#ReplayBroadcastTxBatch].
func (b *MultiplexBackend) ReplayPool() *replay.ReplayPool {
	return b.replayPool
}

// ----------------------------------------------------------------------------
// service.Service API implementation

// OnStart starts a replication backend, then selects void and runs forever.
// errorsCh and shutdownCh are used to shutdown operations gracefully.
func (b *MultiplexBackend) OnStart(ctx context.Context) error {
	startTime := time.Now()

	// TODO(midas): remove debug logs
	b.logger.Debug("OnStart",
		"addr", b.relayAddr.String(),
		"path", b.backendCfg.RootDir,
	)

	// Starts ABCI, RuntimeManager, ReplayPool, Reactor.
	if err := b.StartSharedServices(); err != nil {
		return fmt.Errorf(
			"failed to start shared services: %w", err)
	}

	// Setup metrics exporter (prometheus) and panics recovery.
	go b.metricsReporter()
	defer b.shutdownOnPanic()

	b.mtx.Lock()
	{
		// Channel used to intercept errors during startup.
		b.errorsCh = make(chan error, 1)
		// Channel used to shutdown local goroutines on quit.
		b.shutdownCh = make(chan struct{}, 1)
	}
	b.mtx.Unlock()

	// Start P2P and RPC servers for Discovery.
	//
	// P2P: DiscoveryPort, accepts messages on [types.ReplicationChannel].
	// RPC: DiscoveryPort-1, accepts calls to e.g. [rpc.RPCServer#GetRelayInfo].
	discoveryWg := new(sync.WaitGroup)
	discoveryWg.Add(1)
	go func(wg *sync.WaitGroup) {
		// Since we'll modify the internal discoverySwitch and listeners,
		// we lock the mutex to ensure the transition is thread-safe.
		b.mtx.Lock()

		if err := b.StartP2PServerDiscovery(); err != nil {
			b.errorsCh <- fmt.Errorf("error with discovery P2P server: %w", err)
		}

		if err := b.StartRPCServerDiscovery(); err != nil {
			b.errorsCh <- fmt.Errorf("error with discovery RPC server: %w", err)
		}

		b.mtx.Unlock()
		wg.Done()

		b.logger.Info("Discovery servers started",
			"time", cmttime.Now(),
			"info", b.discoverySwitch.NodeInfo(),
			"p2p", b.relayAddr.String(),
			"rpc", b.relayAddr.AddressForRelayInfo(),
		)

		// Keep this goroutine alive until shutdown explicitely.
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
		// Since we'll modify the internal RPC listeners,
		// we lock the mutex to ensure the transition is thread-safe.
		b.mtx.Lock()

		if err := b.StartPrometheusServer(); err != nil {
			b.errorsCh <- fmt.Errorf("error with Prometheus server: %w", err)
		}

		b.mtx.Unlock()
		wg.Done()

		b.logger.Info("Prometheus server started",
			"time", cmttime.Now(),
			"addr", b.relayAddr.AddressForMonitoring(),
		)

		// Keep this goroutine alive until shutdown explicitely.
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
		// Since we'll modify the internal cometbftSwitch and listeners,
		// we lock the mutex to ensure the transition is thread-safe.
		b.mtx.Lock()

		// Start the RPC server before the P2P server
		// so we can e.g. receive txs for the first block.
		if err := b.StartRPCServerCometBFT(); err != nil {
			b.errorsCh <- fmt.Errorf("error with CometBFT RPC server: %w", err)
		}

		if err := b.StartP2PServerCometBFT(); err != nil {
			b.errorsCh <- fmt.Errorf("error with CometBFT P2P server: %w", err)
		}

		b.mtx.Unlock()
		wg.Done()

		b.logger.Info("CometBFT servers started",
			"time", cmttime.Now(),
			"info", b.cometbftSwitch.NodeInfo(),
			"p2p", b.relayAddr.AddressForCometBFT(),
			"rpc", b.relayAddr.AddressForLightRPC(),
		)

		// Keep this goroutine alive until shutdown explicitely.
		select {
		case <-b.shutdownCh:
			return
		}
	}(cometbftWg)

	// Complete setup of CometBFT, then proceed.
	cometbftWg.Wait()
	close(b.errorsCh)

	// Just report when errors are issued in setup goroutines.
	for err := range b.errorsCh {
		b.logger.Error("Error during multiplex backend initialization", "err", err)
	}

	if b.metrics != nil {
		addTimeSample(b.metrics.StartDurationSeconds, startTime)()
	}

	b.logStartupInfo()
	return nil
}

// OnStop stops the event switches and servers, as well
func (b *MultiplexBackend) OnStop() {
	// TODO(midas): remove debug logs
	b.logger.Debug("OnStop",
		"addr", b.relayAddr.String(),
		"path", b.backendCfg.RootDir,
	)

	// Since we shall modify the event switches and internal channels,
	// we lock the mutex to make sure the transition is thread-safe.
	b.mtx.Lock()
	if b.shutdownCh != nil {
		// Shutdown goroutines started by MustStart().
		close(b.shutdownCh)
	}

	if b.discoverySwitch != nil {
		b.discoverySwitch.Stop()
		b.discoveryPool.Stop()
		b.discoverySwitch = nil // Reset must re-create
	}

	if b.cometbftSwitch != nil {
		b.cometbftSwitch.Stop()
		b.cometbftPool.Stop()
		b.cometbftSwitch = nil // Reset must re-create
	}
	b.mtx.Unlock()

	// Stops ABCI, RuntimeManager, ReplayPool, Reactor.
	// Takes a temporary lock on the mutex during shutdowns.
	if err := b.StopSharedServices(); err != nil {
		b.logger.Error(
			"failed to stop shared services", "err", err)
	}

	b.mtx.Lock()
	for _, rpcListener := range b.rpcListeners {
		rpcListener.Close()
	}

	for _, httpClient := range b.httpClients {
		httpClient.CloseIdleConnections()
	}

	if b.prometheusHttp != nil {
		if err := b.prometheusHttp.Shutdown(b.Context()); err != nil {
			b.logger.Error(
				"Error stopping HTTP server while shutting down", "err", err)
		}
	}
	b.mtx.Unlock()
}

// OnReset resets the runtime manager and shared services.
func (b *MultiplexBackend) OnReset(ctx context.Context) error {
	// TODO(midas): remove debug logs
	b.logger.Debug("OnReset",
		"addr", b.relayAddr.String(),
		"path", b.backendCfg.RootDir,
	)

	if b.chainConns != nil && b.chainConns.IsStopped() {
		if err := b.chainConns.Reset(ctx); err != nil {
			b.logger.Error(
				"failed to reset the ABCI client", "err", err)
		}
	}

	if b.replayPool != nil && b.replayPool.IsStopped() {
		if err := b.replayPool.Reset(ctx); err != nil {
			b.logger.Error(
				"failed to reset the replay pool", "err", err)
		}
	}

	if b.runtimeRegistry != nil && b.runtimeRegistry.IsStopped() {
		if err := b.runtimeRegistry.Reset(ctx); err != nil {
			b.logger.Error(
				"failed to reset the runtime manager", "err", err)
		}
	}

	if b.reactor != nil && b.reactor.IsStopped() {
		if err := b.reactor.Reset(ctx); err != nil {
			b.logger.Error(
				"failed to reset the multiplex reactor", "err", err)
		}
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	b.rpcListeners = []net.Listener{}
	b.httpClients = []*http.Client{}

	b.logger.Debug("Reset multiplex backend")
	return nil
}

// ----------------------------------------------------------------------------

// shutdownOnPanic tries to close the backend after a panic.
func (b *MultiplexBackend) shutdownOnPanic() {
	if r := recover(); r != nil {
		b.logger.Error("Multiplex panicked", "err", r, "stack", string(debug.Stack()))
		close(b.shutdownCh)
	}
}

// logStartupInfo logs a message with addresses information.
func (b *MultiplexBackend) logStartupInfo() {
	discoveryP2PAddr := b.relayAddr.NetAddress().DialString()
	discoveryRPCAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForRelayInfo())
	prometheusAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForMonitoring())
	cometbftP2PAddr := b.relayAddr.NetAddressForCometBFT().DialString()
	cometbftRPCAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForLightRPC())

	b.logger.Info("Started a multiplex backend",
		"id", b.nodeKey.ID(),
		"discovery", discoveryP2PAddr,
		"relayInfo", discoveryRPCAddr.StringWithoutId(),
		"cometbft", cometbftP2PAddr,
		"cometRPC", cometbftRPCAddr.StringWithoutId(),
		"prometheus", prometheusAddr.StringHostname(),
	)
}

// waitForInterval waits for i using time.After, or shutdown channels.
func (b *MultiplexBackend) waitForInterval(i time.Duration) (waited bool) {
	for b.Context().Err() == nil {
		select {
		case <-time.After(i):
			return true
		case <-b.Quit():
			return false
		}
	}

	return false
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
func (b *MultiplexBackend) metricsReporter() {
	metricsTicker := time.NewTicker(DefaultMetricsTickerDuration)
	defer metricsTicker.Stop()

	if b.metrics == nil {
		return
	}

	for b.Context().Err() == nil {
		select {
		case <-metricsTicker.C:
			prometheusCfg := b.backendCfg.Instrumentation
			relayMetricsPrefix := prometheusCfg.Namespace + "_" + string(b.nodeKey.ID())

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
				// See also: runtime/consensus.go
				chainMetricsPrefix := relayMetricsPrefix + ":" + strings.ReplaceAll(chainID, "-", "_")

				// TODO(midas): enable user-grouped metrics with ChainID and RelayID.
				collectSampleCometBFT(
					chainMetricsPrefix,
					b.metrics,
				)()
			}
		}
	}
}
