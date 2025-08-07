package multiplex

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/cors"

	cmtpubsub "github.com/ice-blockchain/cometbft/libs/pubsub"
	"github.com/ice-blockchain/cometbft/node"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/replay"
	mxrpc "github.com/ice-blockchain/cometbft/multiplex/rpc"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ----------------------------------------------------------------------------
// types.RelayComposer API implementation

// Init initializes shared global resources for the backend.
func (b *MultiplexBackend) Init() error {
	promoteAddr := b.backendCfg.P2P.ExternalAddress
	if promoteAddr == "" {
		promoteAddr = b.backendCfg.P2P.ListenAddress
	}
	b.listenAddress = overwriteListenPort(
		promoteAddr,
		int(b.backendCfg.DiscoveryPort),
	)

	// Sets the relay address (P2P DiscoveryPort)
	b.relayAddr, _ = helpers.NewRelayAddress(b.listenAddress)
	b.relayAddr.SetID(b.nodeKey.ID())

	// Uses a singleton chain registry to interpret multiplex configurations.
	b.chainRegistry, _ = helpers.NewChainRegistry(
		&b.backendCfg.MultiplexConfig,
		b.backendCfg.GenesisFile(),
	)

	// Create the local ABCI client for the SnapsApp application.
	if err := b.InitSnapsAppClient(); err != nil {
		return err.(ErrSetupSnapsapp)
	}

	// Create the RuntimeManager instance.
	if err := b.InitRuntimeManager(); err != nil {
		return err.(ErrSetupRuntime)
	}

	// Create the cmtp2p.Switch instance for Discovery.
	if err := b.InitDiscoverySwitch(); err != nil {
		return err.(ErrSetupDiscovery)
	}

	// Create the cmtp2p.Switch instance for Discovery.
	if err := b.InitCometBFTSwitch(); err != nil {
		return err.(ErrSetupCometBFT)
	}

	// TODO(midas): remove debug logs
	b.logger.Debug("Done initializing multiplex backend",
		"addr", b.relayAddr.String(),
		"size", len(b.GetNetworks()),
		"nodeId", b.nodeKey.ID(),
	)
	return nil
}

// InitSnapsAppClient initializes the local SnapsApp application.
func (b *MultiplexBackend) InitSnapsAppClient() error {
	b.snapsApp = snapsapp.NewSnapsApplication(b,
		b.logger.With("module", "snapsapp"),
		snapsapp.WithAcceptor(b.acceptor),
	)

	metricsName := b.backendCfg.Instrumentation.Namespace + "_" + string(b.nodeKey.ID())
	clientCreator := proxy.NewLocalClientCreator(b.snapsApp)
	b.chainConns = proxy.NewMultiplexAppConn(b.Context(),
		b.GetNetworks(),
		clientCreator,
		proxy.PrometheusMetrics(metricsName),
	)
	b.chainConns.SetLogger(b.logger.With("module", "proxy"))

	return nil
}

// InitRuntimeManager initializes the internal [RuntimeManager].
func (b *MultiplexBackend) InitRuntimeManager() error {
	b.runtimeRegistry = runtime.NewRegistry(b.Context(),
		b.backendCfg,
		b.chainRegistry,
		b.nodeKey,
		b.chainConns,
		b.resourceMgr,
		b.logger.With("module", "runtime"),
	)
	b.replayPool = replay.NewReplayPool(b.Context(),
		b.logger.With("module", "replay"),
		replay.ReplayPoolThreshold(10),
		replay.ReplayPoolAcceptor(b.acceptor),
	)

	return nil
}

// InitDiscoverySwitch initializes the Discovery event switch.
func (b *MultiplexBackend) InitDiscoverySwitch() error {
	discoveryLogger := b.logger.With("module", "discovery")

	// Prepare the discovery NodeInfo and connection manager.
	b.discoveryInfo = p2p.NewMultiNetworkNodeInfoWithConfig(
		b.backendCfg,
		b.nodeKey,
		b.relayAddr.NetAddress(),
		[]byte{types.ReplicationChannel},
	)

	localTransport := cmtp2p.NewMultiplexTransport(b.Context(),
		b.discoveryInfo,
		*b.nodeKey,
	)

	b.discoveryPool = p2p.NewConnectionManager(b.Context(),
		b.nodeKey,
		localTransport,
		b.resourceMgr,
		discoveryLogger,
	)

	b.discoverySwitch = cmtp2p.NewSwitch(b.Context(),
		b.backendCfg.P2P,
		b.discoveryPool,
	)
	b.discoverySwitch.SetLogger(discoveryLogger)
	b.discoverySwitch.SetNodeInfo(b.discoveryInfo)
	b.discoverySwitch.SetNodeKey(b.nodeKey)

	// TODO(midas): add reactor MULTIPLEX?
	return nil
}

// InitCometBFTSwitch initializes the CometBFT event switch.
func (b *MultiplexBackend) InitCometBFTSwitch() error {
	cometLogger := b.logger.With("module", "cometbft")

	// Prepare the CometBFT NodeInfo and connection manager.
	b.cometbftInfo = p2p.NewMultiNetworkNodeInfoWithConfig(
		b.backendCfg,
		b.nodeKey,
		b.relayAddr.NetAddressForCometBFT(),
		p2p.GetChannelIds(p2p.GetRuntimeChannels()),
	)

	localTransport := cmtp2p.NewMultiplexTransport(b.Context(),
		b.cometbftInfo,
		*b.nodeKey,
	)

	b.cometbftPool = p2p.NewConnectionManager(b.Context(),
		b.nodeKey,
		localTransport,
		b.resourceMgr,
		cometLogger,
	)

	b.cometbftSwitch = cmtp2p.NewSwitch(b.Context(),
		b.backendCfg.P2P,
		b.cometbftPool,
	)
	b.cometbftSwitch.SetLogger(cometLogger)
	b.cometbftSwitch.SetNodeInfo(b.cometbftInfo)
	b.cometbftSwitch.SetNodeKey(b.nodeKey)

	// TODO(midas): add reactor MULTIPLEX?
	return nil
}

// InitLightRPCRoutes initializes the CometBFT RPC routes.
func (b *MultiplexBackend) InitLightRPCRoutes() error {
	// We configure one RPC environment per running network,
	// i.e. contains reactors, stores and genesis.
	chainRoutes := map[string]rpccore.RoutesMap{}
	chainIds := b.runtimeRegistry.ActiveRuntimes()
	for chainID := range chainIds {
		nodeRuntime, ok := b.resourceMgr.Get(
			chainID,
			types.ServiceKeyNodeRuntime,
		).(*node.Node)
		if !ok {
			// Not activating the RPC now, it will be activated when
			// [mxrpc.#EnableNewRuntimeRPC] is called.
			continue
		}

		env, err := nodeRuntime.ConfigureRPC()
		if err != nil {
			return fmt.Errorf(
				"could not create RPC environment with ChainID %s: %w", chainID, err)
		}

		nodeRoutes := env.GetRoutes()
		if b.backendCfg.RPC.Unsafe {
			env.AddUnsafeRoutes(nodeRoutes)
		}

		chainRoutes[chainID] = nodeRoutes
	}

	// Each network's ChainID is appended to the route name.
	// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
	for chainID, nodeRoutes := range chainRoutes {
		for route, rpcFunc := range nodeRoutes {
			routePath := route + "/" + chainID

			b.knownRPCRoutes[routePath] = rpcFunc
		}
	}

	return nil
}

// StartP2PServerDiscovery starts a [cmtp2p.Switch] instance that may be used
// to transport [ChainReplicationRequest] messages to nodes that do not have
// network ports open yet (due to not replicating any network).
// Creates a transport listening on `DiscoveryPort`.
func (b *MultiplexBackend) StartP2PServerDiscovery() error {
	netListenAddr := b.relayAddr.NetAddress()

	b.logger.Info("Process is now setting up P2P discovery",
		"addr", netListenAddr.DialString(),
	)

	eventSwitch := b.Discovery()
	if err := eventSwitch.Start(); err != nil {
		return fmt.Errorf(
			"could not start p2p switch: %w", err)
	}

	if listenTransport := eventSwitch.Transport(); listenTransport != nil {
		if err := listenTransport.Listen(*netListenAddr); err != nil {
			return fmt.Errorf(
				"could not start listening on %s: %w", netListenAddr.DialString(), err)
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Process is now listening on discovery port",
			"addr", netListenAddr.DialString(),
		)
	}

	return nil
}

// StartRPCServerDiscovery starts a RPC server with a RelayInfo function that
// may be used to retrieve node information, including the node ID.
// This method sets the listen address in discoveryAddr.
// Creates a transport listening on `DiscoveryPort-1`.
func (b *MultiplexBackend) StartRPCServerDiscovery() error {
	rpcRelayAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForRelayInfo())
	b.logger.Info("Process is now setting up RPC discovery",
		"addr", rpcRelayAddr.StringWithoutId(),
	)

	// Initializes a local RPC server
	nodeRpc := b.backendCfg.RPC
	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = nodeRpc.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = nodeRpc.MaxBodyBytes
	rpcConf.MaxHeaderBytes = nodeRpc.MaxHeaderBytes
	rpcConf.MaxOpenConnections = nodeRpc.MaxOpenConnections

	mux := http.NewServeMux()
	rpcLogger := b.logger.With("module", "rpc-server")

	// Enabled procedures:
	// - "info": POST /info to retrieve RelayInfo.

	infoImpl := mxrpc.NewRPCServer(b)
	rpcserver.RegisterRPCFuncs(mux, map[string]*rpcserver.RPCFunc{
		"info":       rpcserver.NewRPCFunc(infoImpl.GetRelayInfo, ""),
		"validators": rpcserver.NewRPCFunc(infoImpl.InitValidators, "networks"),
	}, rpcLogger)

	rpcListener, err := rpcserver.Listen(
		rpcRelayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return err
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

	return nil
}

// StartP2PServerCometBFT starts a [cmtp2p.Switch] instance that may be used
// to interact directly with a CometBFT node runtime, e.g to broadcast a
// transaction.
// Creates a transport listening on `DiscoveryPort+1`.
func (b *MultiplexBackend) StartP2PServerCometBFT() error {
	netListenAddr := b.relayAddr.NetAddressForCometBFT()

	b.logger.Info("Process is now setting up P2P cometbft",
		"addr", netListenAddr.DialString(),
	)

	eventSwitch := b.CometBFT()
	if err := eventSwitch.Start(); err != nil {
		return fmt.Errorf(
			"could not start p2p switch: %w", err)
	}

	if listenTransport := eventSwitch.Transport(); listenTransport != nil {
		if err := listenTransport.Listen(*netListenAddr); err != nil {
			return fmt.Errorf(
				"could not start listening on %s: %w", netListenAddr.DialString(), err)
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Process is now listening on cometbft port",
			"addr", netListenAddr.DialString(),
		)
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
// Creates a transport listening on `DiscoveryPort+2`.
func (b *MultiplexBackend) StartRPCServerCometBFT() error {
	rpcRelayAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForLightRPC())
	rpcLogger := b.logger.With("module", "rpc-server")
	wmLogger := rpcLogger.With("protocol", "websocket")

	b.logger.Info("Process is now setting up CometBFT RPC",
		"addr", rpcRelayAddr.StringWithoutId(),
	)

	if b.runtimeRegistry.NumRuntimes() > 0 {
		if err := b.InitLightRPCRoutes(); err != nil {
			return fmt.Errorf(
				"failed to initialize RPC routes: %w", err)
		}
	}

	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = b.backendCfg.RPC.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = b.backendCfg.RPC.MaxBodyBytes
	rpcConf.MaxHeaderBytes = b.backendCfg.RPC.MaxHeaderBytes
	rpcConf.MaxOpenConnections = b.backendCfg.RPC.MaxOpenConnections
	if rpcConf.WriteTimeout <= b.backendCfg.RPC.TimeoutBroadcastTxCommit {
		rpcConf.WriteTimeout = b.backendCfg.RPC.TimeoutBroadcastTxCommit + 1*time.Second
	}

	wm := rpcserver.NewWebsocketManager(b.knownRPCRoutes,
		rpcserver.OnDisconnect(func(remoteAddr string) {
			for chainID := range b.runtimeRegistry.ActiveRuntimes() {
				eventBus := b.runtimeRegistry.Composer().EventBus(chainID)
				err := eventBus.UnsubscribeAll(b.Context(), remoteAddr)
				if err != nil && err != cmtpubsub.ErrSubscriptionNotFound {
					wmLogger.Error("Failed to unsubscribe addr from events", "addr", remoteAddr, "err", err)
				}
			}
		}),
		rpcserver.ReadLimit(rpcConf.MaxBodyBytes),
		rpcserver.WriteChanCapacity(b.backendCfg.RPC.WebSocketWriteBufferSize),
	)
	wm.SetLogger(wmLogger)

	// Creates the RPC server (HTTP multiplexer).
	b.rpcMultiplexer = http.NewServeMux()
	b.rpcMultiplexer.HandleFunc("/websocket", wm.WebsocketHandler)
	b.rpcMultiplexer.HandleFunc("/v1/websocket", wm.WebsocketHandler)
	rpcserver.RegisterRPCFuncs(b.rpcMultiplexer, b.knownRPCRoutes, rpcLogger)

	// Creates a listener later attached to Serve method.
	rpcListener, err := rpcserver.Listen(
		rpcRelayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return fmt.Errorf(
			"failed to start CometBFT RPC listener: %w", err)
	}

	var rootHandler http.Handler = b.rpcMultiplexer
	if b.backendCfg.RPC.IsCorsEnabled() {
		corsMiddleware := cors.New(cors.Options{
			AllowedOrigins: b.backendCfg.RPC.CORSAllowedOrigins,
			AllowedMethods: b.backendCfg.RPC.CORSAllowedMethods,
			AllowedHeaders: b.backendCfg.RPC.CORSAllowedHeaders,
		})
		rootHandler = corsMiddleware.Handler(b.rpcMultiplexer)
	}

	// Server the RPC endpoints and listen until shutdown.
	if b.backendCfg.RPC.IsTLSEnabled() {
		go func() {
			if err := rpcserver.ServeTLS(
				rpcListener,
				rootHandler,
				b.backendCfg.RPC.CertFile(),
				b.backendCfg.RPC.KeyFile(),
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

	b.rpcListeners = append(b.rpcListeners, rpcListener)
	return nil
}

// StartPrometheusServer starts a Prometheus HTTP server, listening for metrics
// collectors on addr. Creates a transport listening on `DiscoveryPort+3`.
func (b *MultiplexBackend) StartPrometheusServer() error {
	// Config allows disabling prometheus.
	prometheusCfg := b.backendCfg.Instrumentation
	if !prometheusCfg.Prometheus || len(prometheusCfg.PrometheusListenAddr) == 0 {
		return nil
	}

	monRelayAddr, _ := helpers.NewRelayAddress(b.relayAddr.AddressForMonitoring())

	b.logger.Info("Process is now setting up Prometheus HTTP",
		"addr", monRelayAddr.StringHostname(),
	)

	b.prometheusHttp = &http.Server{
		Addr: monRelayAddr.StringHostname(),
		Handler: promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				prometheus.DefaultGatherer,
				promhttp.HandlerOpts{MaxRequestsInFlight: prometheusCfg.MaxOpenConnections},
			),
		),
		ReadHeaderTimeout: DefaultReadHeaderTimeout,
	}

	go func(httpServer *http.Server) {
		if err := httpServer.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener.
			b.logger.Error("Error serving Prometheus HTTP server", "err", err)
		}
	}(b.prometheusHttp)

	return nil
}

// StartSharedServices starts the global services shared amongst networks.
func (b *MultiplexBackend) StartSharedServices() error {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	// We share one ABCI client amongst all replication chains.
	if !b.chainConns.IsRunning() {
		if err := b.chainConns.Start(); err != nil {
			return fmt.Errorf(
				"failed to start proxy app connections: %w", err)
		}
	}

	// We share one runtime manager amongst all replication chains.
	if !b.runtimeRegistry.IsRunning() {
		if err := b.runtimeRegistry.Start(); err != nil {
			return fmt.Errorf(
				"failed to start runtime manager service: %w", err)
		}
	}

	// A replay pool forwards missed events over to the acceptor.
	if !b.replayPool.IsRunning() {
		if err := b.replayPool.Start(); err != nil {
			return fmt.Errorf(
				"failed to start replay pool service: %w", err)
		}
	}

	// Make sure we have the multiplex reactor up and running.
	if !b.reactor.IsRunning() {
		if err := b.reactor.Start(); err != nil {
			return fmt.Errorf(
				"failed to start multiplex reactor: %w", err)
		}
	}

	return nil
}

// StopSharedServices stops the global services shared amongst networks.
func (b *MultiplexBackend) StopSharedServices() error {
	// At first, stop the runtimes registry as it shouldn't interfere with shutdown.
	if b.runtimeRegistry != nil && b.runtimeRegistry.IsRunning() {
		// Try to shutdown gracefully (each ChainID individually).
		restNodeRuntimes := b.runtimeRegistry.ActiveRuntimes()
		if len(restNodeRuntimes) > 0 {
			b.logger.Info("Shutting down remaining node runtimes",
				"networks", restNodeRuntimes,
			)

			for chainID, _ := range restNodeRuntimes {
				b.runtimeRegistry.OnIdle(chainID)
			}
		}

		go func() {
			b.mtx.Lock()
			defer b.mtx.Unlock()

			if err := b.runtimeRegistry.Stop(); err != nil {
				b.logger.Error(
					"failed to stop the runtime manager", "err", err)
			}
		}()
	}

	if b.replayPool != nil && b.replayPool.IsRunning() {
		go func() {
			b.mtx.Lock()
			defer b.mtx.Unlock()

			if err := b.replayPool.Stop(); err != nil {
				b.logger.Error(
					"failed to stop the replay pool", "err", err)
			}
		}()
	}

	if b.chainConns != nil && b.chainConns.IsRunning() {
		go func() {
			b.mtx.Lock()
			defer b.mtx.Unlock()

			if err := b.chainConns.Stop(); err != nil {
				b.logger.Error(
					"failed to stop the ABCI client", "err", err)
			}
		}()
	}

	if b.reactor != nil && b.reactor.IsRunning() {
		go func() {
			b.mtx.Lock()
			defer b.mtx.Unlock()

			if err := b.reactor.Stop(); err != nil {
				b.logger.Error(
					"failed to stop the replay pool", "err", err)
			}
		}()
	}

	return nil
}
