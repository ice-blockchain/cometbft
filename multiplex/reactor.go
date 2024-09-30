package multiplex

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"sync"

	cmtcfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/libs/log"
	cmtlibs "github.com/cometbft/cometbft/libs/service"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/store"
	"github.com/cometbft/cometbft/types"
)

const (
	// ReplicationChannel is a channel for blockchain replication updates.
	ReplicationChannel = byte(0x90)

	// Max message size for multiplex reactor messages
	maxMsgSize = 1048576 // 1MB; NOTE/TODO: keep in sync with types.PartSet sizes.
)

// Reactor handles chain replication by user scope hash for the local node.
// Reactor is responsible for handling incoming messages on one or more
// Channel. Switch calls GetChannels when reactor is added to it. When a new
// peer joins our node, InitPeer and AddPeer are called. RemovePeer is called
// when the peer is stopped. Receive is called when a message is received on a
// channel associated with this reactor.
//
// Peer#Send or Peer#TrySend should be used to send the message to a peer.
type Reactor struct {
	p2p.BaseReactor // BaseService + p2p.Switch

	// Listen addresses multiplex
	laddrMutex      sync.RWMutex
	ListenAddresses MultiplexServiceAddress

	// Node services/config
	nodeKey    *p2p.NodeKey
	nodeConfig *cmtcfg.Config
	userConfig *ScopedUserConfig
	replChains []string

	confMutex  sync.RWMutex
	replConfig MultiplexNodeConfig

	pvMutex    sync.RWMutex
	mxPrivVals MultiplexPrivValidator
	peerFilter MultiplexPeerFilterFunc

	// Database(s)
	dbBackendProvider cmtcfg.DBProvider
	databasesProvider ScopedDBProvider
	multiplexProvider DBMultiplexProvider

	// Metrics (prometheus)
	shareMetricsProvider SharedMetricsProvider
	chainMetricsProvider ReplicatedMetricsProvider
	logger               log.Logger

	// Registries for node services and network
	scopeRegistry   *ScopeRegistry
	serviceProvider ServiceProvider

	serviceMutex    sync.RWMutex
	serviceRegistry MultiplexService

	// State and consensus providers
	statesProvider     ScopedStateProvider
	stateStoreProvider ScopedStateStoreProvider
	blockStoreProvider ScopedBlockStoreProvider

	// Network metadata for multiplex
	icsGenesisProvider node.IChecksummedGenesisDoc
	genesisDocProvider ScopedGenesisProvider
	seedNodesProvider  ScopedSeedNodesProvider
	networksSyncStates map[string]bool
	networksSyncBlocks map[string]bool

	// Channels
	listenersStartedCh chan string
}

// NewReactor creates a new state sync reactor.
func NewReactor(
	nodeKey *p2p.NodeKey,
	config *cmtcfg.Config,
	logger log.Logger,
	dbBackendProvider cmtcfg.DBProvider,
	genesisDocProvider node.GenesisDocProvider,
	shareMetricsProvider SharedMetricsProvider,
	chainMetricsProvider ReplicatedMetricsProvider,
) *Reactor {
	// Uses a singleton scope registry to create SHA256 once
	scopeRegistry, err := DefaultScopeHashProvider(&config.UserConfig)
	if err != nil {
		panic(err)
	}

	// Augment user configuration to multiplex capacity
	mxUserConfig := NewUserConfig(config.Replication, config.UserScopes, config.ListenPort)

	// Configures a multiplex reactor with config and metrics
	r := &Reactor{
		// configuration
		nodeKey:    nodeKey,
		nodeConfig: config,
		userConfig: mxUserConfig,
		replChains: mxUserConfig.GetScopeHashes(),
		replConfig: MultiplexNodeConfig{},
		mxPrivVals: MultiplexPrivValidator{},
		peerFilter: MultiplexPeerFilterFunc{},

		// environment
		dbBackendProvider: dbBackendProvider,
		serviceRegistry:   MultiplexService{},
		logger:            logger,

		// services, metrics
		scopeRegistry:        scopeRegistry,
		shareMetricsProvider: shareMetricsProvider,
		chainMetricsProvider: chainMetricsProvider,
	}
	r.BaseReactor = *p2p.NewBaseReactor("Multiplex", r)

	// Validates genesis configuration
	icsGenDoc, err := genesisDocProvider()
	if err != nil {
		panic(err)
	}

	// Make genesis doc provider accessible
	r.icsGenesisProvider = icsGenDoc
	r.genesisDocProvider = func(scopeHash string) (*types.GenesisDoc, error) {
		return icsGenDoc.GenesisDocByScope(scopeHash)
	}

	// Use the service registry to load node services
	r.serviceProvider = func(scopeHash string, serviceName string) cmtlibs.Service {
		r.serviceMutex.RLock()
		defer r.serviceMutex.RUnlock()

		if _, ok := r.serviceRegistry[scopeHash]; !ok {
			panic(fmt.Errorf("could not load services for scope hash %s", scopeHash))
		}

		if _, ok := r.serviceRegistry[scopeHash][serviceName]; !ok {
			panic(fmt.Errorf("could not find a service with name: %s", serviceName))
		}

		return r.serviceRegistry[scopeHash][serviceName]
	}

	// Make seeds accessible per replicated chain
	r.seedNodesProvider = MultiplexSeedNodesProvider(config)

	// From here on, node listeners can be started
	r.listenersStartedCh = make(chan string)
	return r
}

// ----------------------------------------------------------------------------
// Reactor public getters

// GetUserConfig returns a [ScopedUserConfig] instance (multiplex users config)
func (r *Reactor) GetUserConfig() *ScopedUserConfig { return r.userConfig }

// GetScopeRegistry returns a [ScopeRegistry] instance (scope hash registry)
func (r *Reactor) GetScopeRegistry() *ScopeRegistry { return r.scopeRegistry }

// GetGenesisProvider returns a [ScopedGenesisProvider] instance (genesis doc set)
func (r *Reactor) GetGenesisProvider() ScopedGenesisProvider { return r.genesisDocProvider }

// GetGenesisProvider returns a [ScopedGenesisProvider] instance (genesis doc set)
func (r *Reactor) GetChecksummedGenesisDoc() node.IChecksummedGenesisDoc { return r.icsGenesisProvider }

// GetServiceProvider returns a [ServiceProvider] provider.
func (r *Reactor) GetServiceProvider() ServiceProvider { return r.serviceProvider }

// GetDatabasesProvider returns a [ScopedDBProvider] provider.
func (r *Reactor) GetDatabasesProvider() ScopedDBProvider { return r.databasesProvider }

// GetDatabasesProvider returns a [DBMultiplexProvider] provider.
func (r *Reactor) GetDBMultiplexProvider() DBMultiplexProvider { return r.multiplexProvider }

// GetStateStoreProvider returns a [ScopedStateStoreProvider] provider.
func (r *Reactor) GetStateStoreProvider() ScopedStateStoreProvider { return r.stateStoreProvider }

// GetBlockStoreProvider returns a [ScopedBlockStoreProvider] provider.
func (r *Reactor) GetBlockStoreProvider() ScopedBlockStoreProvider { return r.blockStoreProvider }

// GetStatesProvider returns a [ScopedStateProvider] provider.
func (r *Reactor) GetStatesProvider() ScopedStateProvider { return r.statesProvider }

// GetStateSync returns a bool value depending on whether the chain is state syncing.
func (r *Reactor) GetStateSync(userScopeHash string) bool {
	val, ok := r.networksSyncStates[userScopeHash]
	return ok && val
}

// GetBlockSync returns a bool value depending on whether the chain is block syncing.
func (r *Reactor) GetBlockSync(userScopeHash string) bool {
	val, ok := r.networksSyncBlocks[userScopeHash]
	return ok && val
}

// GetReplNodeConfig returns a [cmtcfg.Config] instance (per-chain node config)
func (r *Reactor) GetReplNodeConfig(userScopeHash string) *cmtcfg.Config {
	r.confMutex.RLock()
	defer r.confMutex.RUnlock()
	return r.replConfig[userScopeHash]
}

// GetReplPrivValidator returns a [types.PrivValidator] instance (per-chain priv validator)
func (r *Reactor) GetReplPrivValidator(userScopeHash string) types.PrivValidator {
	r.pvMutex.RLock()
	defer r.pvMutex.RUnlock()

	return r.mxPrivVals[userScopeHash]
}

// GetReplPeerFilters returns a slice of [p2p.PeerFilterFunc] instances (per-chain p2p filter functions)
func (r *Reactor) GetReplPeerFilters(userScopeHash string) []p2p.PeerFilterFunc {
	return r.peerFilter[userScopeHash]
}

// GetStateServices returns
func (r *Reactor) GetStateServices(userScopeHash string) (StateServices, error) {
	stateDB := r.databasesProvider("state", userScopeHash)
	stateMachine := r.statesProvider(userScopeHash)
	stateStore := r.stateStoreProvider(userScopeHash)
	blockStore := r.blockStoreProvider(userScopeHash)

	return StateServices{
		DB:           stateDB,
		StateMachine: stateMachine,
		StateStore:   stateStore,
		BlockStore:   blockStore,
	}, nil
}

// ----------------------------------------------------------------------------
// Reactor public setters

// SetStoresProviders defines store providers for the multiplex
func (r *Reactor) SetStoresProviders(
	multiplexStateStore MultiplexStateStore,
	multiplexBlockStore MultiplexBlockStore,
) {
	r.stateStoreProvider = func(scopeHash string) *ScopedStateStore {
		store, err := GetScopedStateStore(multiplexStateStore, scopeHash)
		if err != nil {
			panic(err)
		}

		return store
	}

	r.blockStoreProvider = func(scopeHash string) *ScopedBlockStore {
		store, err := GetScopedBlockStore(multiplexBlockStore, scopeHash)
		if err != nil {
			panic(err)
		}

		return store
	}
}

// SetStatesProviders defines state machine providers for the multiplex
func (r *Reactor) SetStatesProviders(multiplexState MultiplexState) {
	r.statesProvider = func(scopeHash string) *ScopedState {
		state, err := GetScopedState(multiplexState, scopeHash)
		if err != nil {
			panic(err)
		}

		return state
	}
}

// SetDatabasesProviders defines database providers for the multiplex
// with 4 different contexts: blockstore, state, indexer, evidence.
func (r *Reactor) SetDatabasesProviders(
	bsMultiplexDB MultiplexDB,
	stateMultiplexDB MultiplexDB,
	indexerMultiplexDB MultiplexDB,
	evidenceMultiplexDB MultiplexDB,
) {
	multiplexByTable := map[string]*MultiplexDB{
		"blockstore": &bsMultiplexDB,
		"state":      &stateMultiplexDB,
		"indexer":    &indexerMultiplexDB,
		"evidence":   &evidenceMultiplexDB,
	}

	r.databasesProvider = func(tableName string, scopeHash string) *ScopedDB {
		multiplex, ok := multiplexByTable[tableName]
		if !ok {
			panic(fmt.Errorf("could not find databases with table name %s", tableName))
		}

		db, err := GetScopedDB(*multiplex, scopeHash)
		if err != nil {
			panic(err)
		}

		return db
	}

	r.multiplexProvider = func(tableName string) MultiplexDB {
		multiplex, ok := multiplexByTable[tableName]
		if !ok {
			panic(fmt.Errorf("could not find databases with table name %s", tableName))
		}

		return *multiplex
	}
}

// RegisterService inserts a [cmtlibs.Service] instance in the service registry by a given name.
func (r *Reactor) RegisterService(
	scopeHash string,
	serviceName string,
	service cmtlibs.Service,
) {
	r.serviceMutex.Lock()
	defer r.serviceMutex.Unlock()

	// Allocate if necessary
	if _, ok := r.serviceRegistry[scopeHash]; !ok {
		r.serviceRegistry[scopeHash] = map[string]cmtlibs.Service{}
	}

	// TODO: add validation/encoding for serviceName
	r.serviceRegistry[scopeHash][serviceName] = service
}

// RegisterPrivVal inserts a [types.PrivValidator] instance in the priv validators registry using
// a mutex for concurrency-safe handling of the internal map.
func (r *Reactor) RegisterPrivVal(
	scopeHash string,
	privVal types.PrivValidator,
) {
	r.pvMutex.Lock()
	defer r.pvMutex.Unlock()

	r.mxPrivVals[scopeHash] = privVal
}

// SetPeerFilters inserts a slice of [p2p.PeerFilterFunc] helper functions in a mutex-locked map.
func (r *Reactor) SetPeerFilters(scopeHash string, peerFilters []p2p.PeerFilterFunc) {
	r.pvMutex.Lock()
	defer r.pvMutex.Unlock()

	r.peerFilter[scopeHash] = peerFilters
}

// SetStateSync updates the stateSync flag for a replicated chain.
func (r *Reactor) SetStateSync(scopeHash string, stateSync bool) {
	r.networksSyncStates[scopeHash] = stateSync
}

// SetStateSync updates the stateSync flag for a replicated chain.
func (r *Reactor) SetBlockSync(scopeHash string, blockSync bool) {
	r.networksSyncBlocks[scopeHash] = blockSync
}

// ----------------------------------------------------------------------------
// Reactor implements Service

// OnStart implements Service.
//
// Starting the multiplex reactor must initialize the filesystem and db
// instances, as well as the block and state stores such that after being
// started, the reactor can be used to configure the running node services.
func (r *Reactor) OnStart() error {
	// Initialize filesystem directory structure
	if err := initDataDir(r.nodeConfig); err != nil {
		return err
	}

	// Open databases for: state, blockstore, indexer, evidence
	// Then load state from database, config or genesis doc
	// And initiliaze block and state stores
	if err := r.loadMultiplexState(); err != nil {
		return err
	}

	// ------------------------------------------------------------------------
	// Replicated chains

	r.laddrMutex.Lock()
	r.ListenAddresses = make(MultiplexServiceAddress, len(r.replChains))
	r.laddrMutex.Unlock()

	// For each scope hash, we run a node with a distinct listen address
	for _, userScopeHash := range r.replChains {
		scopeId := NewScopeIDFromHash(userScopeHash)
		r.logger.With("scope", scopeId.Fingerprint())

		// Overwrite the p2p and rpc ports
		r.confMutex.Lock()
		r.replConfig[userScopeHash] = NewMultiplexNodeConfig(
			r.nodeConfig,
			r.userConfig,
			r.scopeRegistry,
			userScopeHash,
			r.seedNodesProvider(userScopeHash),
		)
		nodeConfig := r.replConfig[userScopeHash]
		r.confMutex.Unlock()

		r.laddrMutex.Lock()
		r.ListenAddresses[userScopeHash] = map[string]string{
			"P2P":      nodeConfig.P2P.ListenAddress,
			"RPC":      nodeConfig.RPC.ListenAddress,
			"GRPC":     nodeConfig.GRPC.ListenAddress,
			"GRPCPriv": nodeConfig.GRPC.Privileged.ListenAddress,
		}
		r.laddrMutex.Unlock()

		// Non-blocking execution using different goroutine
		// i.e. one goroutine per each replicated chain
		go func(scopeHash string) {
			// Start node listeners
			if err := r.startNodeListeners(scopeHash); err != nil {
				panic(err)
			}

			// TODO: move inside startNodeListeners
			// Done starting node listeners
			r.listenersStartedCh <- scopeHash
		}(userScopeHash)
	}

	return nil
}

// String implements Service.
//
// String returns a string representation of the Reactor.
func (*Reactor) String() string {
	// better not to access shared variables
	return "MultiplexReactor"
}

// ----------------------------------------------------------------------------
// Reactor implements Stringer

// StringIndented returns an indented string representation of the Reactor.
func (r *Reactor) StringIndented(indent string) string {
	s := r.String() + "{\n"

	// add ScopedUserConfig
	jsonConf, _ := json.Marshal(r.userConfig)
	s += indent + "  " + string(jsonConf) + "\n"

	// add Peers information
	r.Switch.Peers().ForEach(func(peer p2p.Peer) {
		ps, ok := peer.Get(types.PeerStateKey).(*cs.PeerState)
		if !ok {
			panic(fmt.Sprintf("Peer %v has no state", peer))
		}
		s += indent + "  " + ps.StringIndented(indent+"  ") + "\n"
	})

	s += indent + "}"
	return s
}

// ----------------------------------------------------------------------------
// Reactor implements p2p.Reactor

// GetChannels implements p2p.Reactor.
// TODO: define channels for multiplex reactor
func (*Reactor) GetChannels() []*p2p.ChannelDescriptor { return nil }

// AddPeer implements p2p.Reactor.
func (r *Reactor) AddPeer(peer p2p.Peer) {}

// RemovePeer implements p2p.Reactor.
func (r *Reactor) RemovePeer(peer p2p.Peer, _ any) {}

// Receive implements p2p.Reactor.
// TODO: implement receive handler
func (r *Reactor) Receive(e p2p.Envelope) {
	if !r.IsRunning() {
		r.Logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID)
		return
	}
}

// ----------------------------------------------------------------------------
// Reactor protected API

// loadMultiplexState opens the multiplex databases for multiple
// contexts: state, blockstore, indexer and evidence. Then load state
// from database, config or genesis doc and initiliaze stores.
//
// This method must be called before one of databasesProvider,
// statesProvider or storesProvider may be used.
func (r *Reactor) loadMultiplexState() error {
	// Initialize database multiplex instances
	bsMultiplexDB,
		stateMultiplexDB,
		indexMultiplexDB,
		evidenceMultiplexDB, err := initDBs(r.nodeConfig, r.dbBackendProvider)
	if err != nil {
		return err
	}

	// Load initial state multiplex from database or from genesis docs
	multiplexState, _, err := LoadMultiplexStateFromDBOrGenesisDocProviderWithConfig(
		stateMultiplexDB,
		"", // empty operatorGenesisHashHex
		r.nodeConfig,
		r,
	)
	if err != nil {
		return err
	}

	// Initialize shared metrics (global scope)
	globalMetrics := r.shareMetricsProvider(string(r.nodeKey.ID()))

	// Load initial state store multiplex
	multiplexStateStore := NewMultiplexStateStore(stateMultiplexDB, sm.StoreOptions{
		DiscardABCIResponses: r.nodeConfig.Storage.DiscardABCIResponses,
		Metrics:              globalMetrics.StateMetrics,
		Compact:              r.nodeConfig.Storage.Compact,
		CompactionInterval:   r.nodeConfig.Storage.CompactionInterval,
		Logger:               r.logger,
		DBKeyLayout:          r.nodeConfig.Storage.ExperimentalKeyLayout,
	})

	// Load initial block store multiplex
	multiplexBlockStore := NewMultiplexBlockStore(
		bsMultiplexDB,
		store.WithMetrics(globalMetrics.StoreMetrics),
		store.WithCompaction(r.nodeConfig.Storage.Compact, r.nodeConfig.Storage.CompactionInterval),
		store.WithDBKeyLayout(r.nodeConfig.Storage.ExperimentalKeyLayout),
		store.WithDBKeyLayout(r.nodeConfig.Storage.ExperimentalKeyLayout),
	)

	// Update database providers by context
	r.SetDatabasesProviders(bsMultiplexDB, stateMultiplexDB, indexMultiplexDB, evidenceMultiplexDB)

	// Update the state machines provider
	r.SetStatesProviders(multiplexState)

	// Update block stores and state stores
	r.SetStoresProviders(multiplexStateStore, multiplexBlockStore)
	return nil
}

// startNodeListeners is called in a parallel goroutine and is responsible for
// starting the following node listeners:
//
// - the ABCI server through [proxy.AppConns] ;
// - the event bus for block events [types.EventBus] ;
// - the transaction- and block indexers [txindex.IndexerService] ;
// - the priv validator (signer) instance [types.PrivValidator] ;
//
// The services are then inserted in the reactor's service registry.
// TODO(midas): make sure that this method uses only mutex-locked maps.
func (r *Reactor) startNodeListeners(scopeHash string) error {
	// Read replicated chain resources
	genesisDoc, _ := r.genesisDocProvider(scopeHash)
	nodeConfig := r.GetReplNodeConfig(scopeHash)
	replicatedMetrics := r.chainMetricsProvider(genesisDoc.ChainID, scopeHash)
	indexerMultiplexDB := r.multiplexProvider("indexer")

	scopeId := NewScopeIDFromHash(scopeHash)
	userAddress, err := r.scopeRegistry.GetAddress(scopeHash)
	if err != nil {
		return err
	}

	userDataDir := filepath.Join(nodeConfig.RootDir, cmtcfg.DefaultDataDir, userAddress)
	clientCreator := proxy.DefaultClientCreator(nodeConfig.ProxyApp, nodeConfig.ABCI, userDataDir)

	// 1) ABCI Server (--proxy_app)
	// TODO(midas): the ABCI server doesn't need to be replicated for each chain.
	proxyApp := proxy.NewAppConns(clientCreator, replicatedMetrics.ProxyMetrics)
	proxyApp.SetLogger(r.logger.With("module", "proxy"))
	if err := proxyApp.Start(); err != nil {
		return fmt.Errorf("error starting proxy app connections: %v", err)
	}

	// 2) Event Bus
	eventBus := types.NewEventBus()
	eventBus.SetLogger(r.logger.With("module", "events"))
	if err := eventBus.Start(); err != nil {
		return fmt.Errorf("error starting event bus: %v", err)
	}

	// 3) Indexers
	indexerService, _, _, err := createAndStartIndexerService(
		nodeConfig,
		genesisDoc.ChainID,
		indexerMultiplexDB,
		eventBus,
		r.logger,
		scopeHash,
	)
	if err != nil {
		return err
	}

	// 4) Priv Validator
	// If no address is provided, we use the default file-based priv validator
	// implementation and create one FilePV per replicated chain.
	var privValidator types.PrivValidator
	if nodeConfig.PrivValidatorListenAddr == "" {
		folderName := scopeId.Fingerprint()
		privValKeyDir := filepath.Join(userDataDir, folderName)
		privValStateDir := filepath.Join(userDataDir, folderName)

		privValidator, err = privval.LoadOrGenFilePV(
			filepath.Join(privValKeyDir, filepath.Base(nodeConfig.PrivValidatorKeyFile())),
			filepath.Join(privValStateDir, filepath.Base(nodeConfig.PrivValidatorStateFile())),
			func() (crypto.PrivKey, error) {
				return ed25519.GenPrivKey(), nil
			},
		)
		if err != nil {
			return err
		}
	} else {
		// If an address is provided, listen on the socket for a connection from an
		// external signing process.
		// FIXME: we should start services inside OnStart
		privValidator, err = createAndStartPrivValidatorSocketClient(nodeConfig.PrivValidatorListenAddr, genesisDoc.ChainID, r.logger)
		if err != nil {
			return fmt.Errorf("error with private validator socket client: %w", err)
		}
	}

	r.RegisterService(scopeHash, "proxyApp", proxyApp)
	r.RegisterService(scopeHash, "eventBus", eventBus)
	r.RegisterService(scopeHash, "indexerService", indexerService)

	// Locks pvMutex
	r.RegisterPrivVal(scopeHash, privValidator)
	return nil
}

// // RequestChains asks peer for replicated chains metadata.
// func (r *Reactor) RequestChains(p p2p.Peer) {
// 	id := string(p.ID())
// 	if r.requestsSent.Has(id) {
// 		return
// 	}
// 	r.Logger.Debug("Request chains", "from", p)
// 	r.requestsSent.Set(id, struct{}{})
// 	p.Send(p2p.Envelope{
// 		ChannelID: ReplicationChannel,
// 		Message:   &mxp2p.BasicReplicationRequest{},
// 	})
// }

// // SendAddrs sends addrs to the peer.
// func (*Reactor) SendChains(p p2p.Peer, userConfig ScopedUserConfig) {
// 	for _, scopeHash := range userConfig.GetScopeHashes() {
// 		e := p2p.Envelope{
// 			ChannelID: ReplicationChannel,
// 			Message: &mxp2p.ChainReplicationResponse{
// 				ScopeHash: scopeHash,
// 				// ...
// 			},
// 		}
// 		p.Send(e)
// 	}
// }

// ChainReplicationResponse {
// 	string scope_hash = 1;
// 	string chain_id = 2 [(gogoproto.customname) = "ChainID"];
// 	cometbft.p2p.v1.ProtocolVersion protocol_version = 3;
// 	cometbft.types.v1.ValidatorSet validators = 4;
// 	uint64 height   = 5;
// }
