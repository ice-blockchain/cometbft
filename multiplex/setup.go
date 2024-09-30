package multiplex

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "net/http/pprof" //nolint: gosec,gci // securely exposed on separate, optional port

	"github.com/go-kit/kit/metrics"

	abci "github.com/cometbft/cometbft/abci/types"
	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/tmhash"
	"github.com/cometbft/cometbft/internal/blocksync"
	cs "github.com/cometbft/cometbft/internal/consensus"
	"github.com/cometbft/cometbft/internal/evidence"
	"github.com/cometbft/cometbft/libs/log"
	mempl "github.com/cometbft/cometbft/mempool"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/p2p/pex"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/state/indexer"
	"github.com/cometbft/cometbft/state/txindex"
	"github.com/cometbft/cometbft/statesync"
	"github.com/cometbft/cometbft/types"
	"github.com/cometbft/cometbft/version"
)

// PluralUserGenesisDocProviderFunc returns a GenesisDocProvider that loads
// the GenesisDocSet from the config.GenesisFile() on the filesystem.
// CAUTION: this method expects the genesis file to contain a **set** genesis docs.
func PluralUserGenesisDocProviderFunc(config *cfg.Config) node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		// FIXME: find a way to stream the file incrementally,
		// for the JSON	parser and the checksum computation.
		// https://github.com/cometbft/cometbft/issues/1302
		jsonBlob, err := os.ReadFile(config.GenesisFile())
		if err != nil {
			return nil, fmt.Errorf("couldn't read GenesisDocSet from file: %w", err)
		}

		genDocSet, err := GenesisDocSetFromJSON(jsonBlob)
		if err != nil {
			return nil, err
		}

		// TODO(midas): SHA256 of each GenesisDoc, not the GenesisDocSet
		// doc, ok, err := genDocSet.SearchGenesisDocByUser(config.UserAddress)
		// if !ok || err != nil {
		// 	return &ChecksummedUserGenesisDoc{}, err
		// }
		// genDocJSONBlob, err := cmtjson.Marshal(doc.GenesisDoc)
		// if err != nil {
		// 	return ChecksummedUserGenesisDoc{}, err
		// }

		//incomingChecksum := tmhash.Sum(genDocJSONBlob)
		incomingChecksum := tmhash.Sum(jsonBlob)
		return &ChecksummedGenesisDocSet{GenesisDocs: *genDocSet, Sha256Checksum: incomingChecksum}, nil
	}
}

// ------------------------------------------------------------------------------
// DATABASE
//
// Modified version of private database initializer to cope with the DB multiplex
// The behaviour in SingularReplicationMode() returns solitary multiplex entries

func initDBs(
	config *cfg.Config,
	dbProvider cfg.DBProvider,
) (
	bsMultiplexDB MultiplexDB,
	stateMultiplexDB MultiplexDB,
	indexerMultiplexDB MultiplexDB,
	evidenceMultiplexDB MultiplexDB,
	err error,
) {
	// When replication is in plural mode, we will create one blockstore
	// and one state database per each pair of user address and scope
	if config.Replication == cfg.PluralReplicationMode() {
		// Initialize many databases
		return initMultiplexDBs(config)
	}

	// Default behaviour should fallback to the basic node implementation
	// to initialize only one blockstore and one state database such that
	// in singular replication mode, the multiplexes contain just one entry
	// Default behaviour does not create indexerDB here (see IndexerFromConfig)
	// Default behaviour does not create evidenceDB here (see IndexerFromConfig)

	bsDB, err := dbProvider(&cfg.DBContext{ID: "blockstore", Config: config})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stateDB, err := dbProvider(&cfg.DBContext{ID: "state", Config: config})
	if err != nil {
		return nil, nil, nil, nil, err
	}

	bsMultiplexDB = MultiplexDB{"": &ScopedDB{DB: bsDB}}
	stateMultiplexDB = MultiplexDB{"": &ScopedDB{DB: stateDB}}
	return bsMultiplexDB, stateMultiplexDB, nil, nil, nil
}

func initMultiplexDBs(
	config *cfg.Config,
) (
	bsMultiplexDB MultiplexDB,
	stateMultiplexDB MultiplexDB,
	indexerMultiplexDB MultiplexDB,
	evidenceMultiplexDB MultiplexDB,
	err error,
) {
	bsMultiplexDB, _ = OpenMultiplexDB(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "blockstore", Config: config},
	})

	stateMultiplexDB, _ = OpenMultiplexDB(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "state", Config: config},
	})

	indexerMultiplexDB, _ = OpenMultiplexDB(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "tx_index", Config: config},
	})

	evidenceMultiplexDB, _ = OpenMultiplexDB(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "evidence", Config: config},
	})

	return bsMultiplexDB, stateMultiplexDB, indexerMultiplexDB, evidenceMultiplexDB, nil
}

// ----------------------------------------------------------------------------
// STORAGE

func initDataDir(config *cfg.Config) error {
	// MultiplexFSProvider creates an instances of MultiplexFS and checks folders.
	// Note: the filesystem folders are created in EnsureRootMultiplex()
	_, err := MultiplexFSProvider(config)
	if err != nil {
		return fmt.Errorf("could not create multiplex fs structure: %w", err)
	}

	return nil
}

// ------------------------------------------------------------------------------
// Starter helper functions

func createAndStartProxyAppConns(clientCreator proxy.ClientCreator, logger log.Logger, metrics *proxy.Metrics) (proxy.AppConns, error) {
	proxyApp := proxy.NewAppConns(clientCreator, metrics)
	proxyApp.SetLogger(logger.With("module", "proxy"))
	if err := proxyApp.Start(); err != nil {
		return nil, fmt.Errorf("error starting proxy app connections: %v", err)
	}
	return proxyApp, nil
}

func createAndStartEventBus(logger log.Logger) (*types.EventBus, error) {
	eventBus := types.NewEventBus()
	eventBus.SetLogger(logger.With("module", "events"))
	if err := eventBus.Start(); err != nil {
		return nil, err
	}
	return eventBus, nil
}

func createAndStartIndexerService(
	config *cfg.Config,
	chainID string,
	indexerMultiplexDB MultiplexDB,
	eventBus *types.EventBus,
	logger log.Logger,
	userScopeHash string,
) (*txindex.IndexerService, txindex.TxIndexer, indexer.BlockIndexer, error) {
	var (
		txIndexer    txindex.TxIndexer
		blockIndexer indexer.BlockIndexer
	)

	txIndexer, blockIndexer, err := GetScopedIndexer(config, indexerMultiplexDB, chainID, userScopeHash)
	if err != nil {
		return nil, nil, nil, err
	}

	txIndexer.SetLogger(logger.With("module", "txindex"))
	blockIndexer.SetLogger(logger.With("module", "txindex"))
	indexerService := txindex.NewIndexerService(txIndexer, blockIndexer, eventBus, false)
	indexerService.SetLogger(logger.With("module", "txindex"))

	if err := indexerService.Start(); err != nil {
		return nil, nil, nil, err
	}

	return indexerService, txIndexer, blockIndexer, nil
}

func createAndStartPrivValidatorSocketClient(
	listenAddr,
	chainID string,
	logger log.Logger,
) (types.PrivValidator, error) {
	pve, err := privval.NewSignerListener(listenAddr, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to start private validator: %w", err)
	}

	pvsc, err := privval.NewSignerClient(pve, chainID)
	if err != nil {
		return nil, fmt.Errorf("failed to start private validator: %w", err)
	}

	// try to get a pubkey from private validator first time
	_, err = pvsc.GetPubKey()
	if err != nil {
		return nil, fmt.Errorf("can't get pubkey: %w", err)
	}

	const (
		retries = 50 // 50 * 100ms = 5s total
		timeout = 100 * time.Millisecond
	)
	pvscWithRetries := privval.NewRetrySignerClient(pvsc, retries, timeout)

	return pvscWithRetries, nil
}

func doHandshake(
	ctx context.Context,
	stateStore ScopedStateStore,
	state ScopedState,
	blockStore *ScopedBlockStore,
	genDoc *types.GenesisDoc,
	eventBus types.BlockEventPublisher,
	proxyApp proxy.AppConns,
	consensusLogger log.Logger,
) error {
	handshaker := cs.NewHandshaker(stateStore, state.GetState(), blockStore, genDoc)
	handshaker.SetLogger(consensusLogger)
	handshaker.SetEventBus(eventBus)
	if err := handshaker.Handshake(ctx, proxyApp); err != nil {
		return fmt.Errorf("error during handshake: %v", err)
	}
	return nil
}

func logNodeStartupInfo(state ScopedState, pubKey crypto.PubKey, logger, consensusLogger log.Logger) {
	// Log the version info.
	logger.Info("Multiplex info",
		"scope_hash", state.ScopeHash,
		"chain_id", state.ChainID,
		"height", state.LastBlockHeight,
	)

	// Log the version info.
	logger.Info("Version info",
		"tendermint_version", version.CMTSemVer,
		"abci", version.ABCISemVer,
		"block", version.BlockProtocol,
		"p2p", version.P2PProtocol,
		"commit_hash", version.CMTGitCommitHash,
	)

	// If the state and software differ in block version, at least log it.
	if state.Version.Consensus.Block != version.BlockProtocol {
		logger.Info("Software and state have different block protocols",
			"software", version.BlockProtocol,
			"state", state.Version.Consensus.Block,
		)
	}

	addr := pubKey.Address()
	// Log whether this node is a validator or an observer
	if state.Validators.HasAddress(addr) {
		consensusLogger.Info("This node is a validator", "addr", addr, "pubKey", pubKey)
	} else {
		consensusLogger.Info("This node is not a validator", "addr", addr, "pubKey", pubKey)
	}
}

// createMempoolAndMempoolReactor creates a mempool and a mempool reactor based on the config.
func createMempoolAndMempoolReactor(
	config *cfg.Config,
	proxyApp proxy.AppConns,
	state ScopedState,
	waitSync bool,
	memplMetrics *mempl.Metrics,
	logger log.Logger,
) (mempl.Mempool, p2p.Reactor) {
	switch config.Mempool.Type {
	// allow empty string for backward compatibility
	case cfg.MempoolTypeFlood, "":
		logger = logger.With("module", "mempool")
		mp := mempl.NewCListMempool(
			config.Mempool,
			proxyApp.Mempool(),
			state.LastBlockHeight,
			mempl.WithMetrics(memplMetrics),
			mempl.WithPreCheck(sm.TxPreCheck(state.GetState())),
			mempl.WithPostCheck(sm.TxPostCheck(state.GetState())),
		)
		mp.SetLogger(logger)
		reactor := mempl.NewReactor(
			config.Mempool,
			mp,
			waitSync,
		)
		if config.Consensus.WaitForTxs() {
			mp.EnableTxsAvailable()
		}
		reactor.SetLogger(logger)

		return mp, reactor
	case cfg.MempoolTypeNop:
		// Strictly speaking, there's no need to have a `mempl.NopMempoolReactor`, but
		// adding it leads to a cleaner code.
		return &mempl.NopMempool{}, mempl.NewNopMempoolReactor()
	default:
		panic(fmt.Sprintf("unknown mempool type: %q", config.Mempool.Type))
	}
}

func createEvidenceReactor(
	config *cfg.Config,
	multiplexDB MultiplexDB,
	stateStore *ScopedStateStore,
	blockStore *ScopedBlockStore,
	logger log.Logger,
	userScopeHash string,
) (*evidence.Reactor, *evidence.Pool, error) {
	evidenceDB, err := GetScopedDB(multiplexDB, userScopeHash)
	if err != nil {
		return nil, nil, err
	}
	evidenceLogger := logger.With("module", "evidence")
	evidencePool, err := evidence.NewPool(evidenceDB, stateStore, blockStore, evidence.WithDBKeyLayout(config.Storage.ExperimentalKeyLayout))
	if err != nil {
		return nil, nil, err
	}
	evidenceReactor := evidence.NewReactor(evidencePool)
	evidenceReactor.SetLogger(evidenceLogger)
	return evidenceReactor, evidencePool, nil
}

func createBlocksyncReactor(config *cfg.Config,
	state *ScopedState,
	blockExec *sm.BlockExecutor,
	blockStore *ScopedBlockStore,
	blockSync bool,
	localAddr crypto.Address,
	logger log.Logger,
	metrics *blocksync.Metrics,
	offlineStateSyncHeight int64,
) (bcReactor p2p.Reactor, err error) {
	switch config.BlockSync.Version {
	case "v0":
		bcReactor = blocksync.NewReactor(state.GetState(), blockExec, blockStore.BlockStore, blockSync, localAddr, metrics, offlineStateSyncHeight)
	case "v1", "v2":
		return nil, fmt.Errorf("block sync version %s has been deprecated. Please use v0", config.BlockSync.Version)
	default:
		return nil, fmt.Errorf("unknown block sync version %s", config.BlockSync.Version)
	}

	bcReactor.SetLogger(logger.With("module", "blocksync"))
	return bcReactor, nil
}

func createConsensusReactor(config *cfg.Config,
	state *ScopedState,
	blockExec *sm.BlockExecutor,
	blockStore *ScopedBlockStore,
	mempool mempl.Mempool,
	evidencePool *evidence.Pool,
	privValidator types.PrivValidator,
	csMetrics *cs.Metrics,
	waitSync bool,
	eventBus *types.EventBus,
	consensusLogger log.Logger,
	offlineStateSyncHeight int64,
) (*cs.Reactor, *cs.State) {
	consensusState := cs.NewState(
		config.Consensus,
		state.GetState(),
		blockExec,
		blockStore,
		mempool,
		evidencePool,
		cs.StateMetrics(csMetrics),
		cs.OfflineStateSyncHeight(offlineStateSyncHeight),
		cs.UseScopeHash(state.ScopeHash),
	)
	consensusState.SetLogger(consensusLogger)
	if privValidator != nil {
		consensusState.SetPrivValidator(privValidator)
	}
	consensusReactor := cs.NewReactor(consensusState, waitSync, cs.ReactorMetrics(csMetrics))
	consensusReactor.SetLogger(consensusLogger)
	// services which will be publishing and/or subscribing for messages (events)
	// consensusReactor will set it on consensusState and blockExecutor
	consensusReactor.SetEventBus(eventBus)
	return consensusReactor, consensusState
}

func createTransports(
	config *cfg.Config,
	nodeInfo p2p.NodeInfo,
	nodeKey *p2p.NodeKey,
	reactor *Reactor,
) error {
	// Service provider is used to retrieve proxyApp
	serviceProvider := reactor.GetServiceProvider()

	for _, userScopeHash := range reactor.replChains {
		var (
			mConnConfig = p2p.MConnConfig(config.P2P)
			transport   = p2p.NewMultiplexTransportWithCustomHandshake(nodeInfo, *nodeKey, mConnConfig, MultiplexTransportHandshake)
			connFilters = []p2p.ConnFilterFunc{}
			peerFilters = []p2p.PeerFilterFunc{}
		)

		if !config.P2P.AllowDuplicateIP {
			connFilters = append(connFilters, p2p.ConnDuplicateIPFilter())
		}

		// Filter peers by addr or pubkey with an ABCI query.
		// If the query return code is OK, add peer.
		if config.FilterPeers {
			connFilters = append(
				connFilters,
				// ABCI query for address filtering.
				func(_ p2p.ConnSet, c net.Conn, _ []net.IP) error {
					proxyApp := serviceProvider(userScopeHash, "proxyApp").(proxy.AppConns)
					res, err := proxyApp.Query().Query(context.TODO(), &abci.QueryRequest{
						Path: "/p2p/filter/addr/" + c.RemoteAddr().String(),
					})
					if err != nil {
						return err
					}
					if res.IsErr() {
						return fmt.Errorf("error querying abci app: %v", res)
					}

					return nil
				},
			)

			peerFilters = append(
				peerFilters,
				// ABCI query for ID filtering.
				func(_ p2p.IPeerSet, p p2p.Peer) error {
					proxyApp := serviceProvider(userScopeHash, "proxyApp").(proxy.AppConns)
					res, err := proxyApp.Query().Query(context.TODO(), &abci.QueryRequest{
						Path: fmt.Sprintf("/p2p/filter/id/%s", p.ID()),
					})
					if err != nil {
						return err
					}
					if res.IsErr() {
						return fmt.Errorf("error querying abci app: %v", res)
					}

					return nil
				},
			)
		}

		p2p.MultiplexTransportConnFilters(connFilters...)(transport)

		// TODO(midas): due to multiplex of multiplex, we must probably divide the max peers by the number of replicated chains.
		// Limit the number of incoming connections.
		max := config.P2P.MaxNumInboundPeers + len(splitAndTrimEmpty(config.P2P.UnconditionalPeerIDs, ",", " "))
		p2p.MultiplexTransportMaxIncomingConnections(max)(transport)

		reactor.RegisterService(userScopeHash, "transport", transport)
		reactor.SetPeerFilters(userScopeHash, peerFilters)
	}

	return nil
}

func createSwitches(
	config *cfg.Config,
	p2pMetrics *p2p.Metrics,
	nodeInfo p2p.NodeInfo,
	nodeKey *p2p.NodeKey,
	p2pLogger log.Logger,
	reactor *Reactor,
) error {
	// Service provider is used to retrieve reactors
	serviceProvider := reactor.GetServiceProvider()
	userScopeHashes := reactor.GetScopeRegistry().GetScopeHashes()

	for _, userScopeHash := range userScopeHashes {

		nodeConfig := reactor.GetReplNodeConfig(userScopeHash)
		transport := serviceProvider(userScopeHash, "transport")
		peerFilters := reactor.GetReplPeerFilters(userScopeHash)

		blockSyncReactor := serviceProvider(userScopeHash, "blockSyncReactor").(*blocksync.Reactor)
		stateSyncReactor := serviceProvider(userScopeHash, "stateSyncReactor").(*statesync.Reactor)
		consensusReactor := serviceProvider(userScopeHash, "consensusReactor").(*cs.Reactor)
		evidenceReactor := serviceProvider(userScopeHash, "evidenceReactor").(*evidence.Reactor)

		sw := NewScopedSwitch(
			nodeConfig.P2P,
			transport.(*p2p.MultiplexTransport),
			userScopeHash,
			p2p.WithMetrics(p2pMetrics),
			p2p.SwitchPeerFilters(peerFilters...),
		)

		sw.SetLogger(p2pLogger)
		if nodeConfig.Mempool.Type != cfg.MempoolTypeNop {
			mempoolReactor := serviceProvider(userScopeHash, "mempoolReactor").(*mempl.Reactor)
			sw.AddReactor("MEMPOOL", mempoolReactor)
		}
		sw.AddReactor("BLOCKSYNC", blockSyncReactor)
		sw.AddReactor("CONSENSUS", consensusReactor)
		sw.AddReactor("EVIDENCE", evidenceReactor)
		sw.AddReactor("STATESYNC", stateSyncReactor)

		sw.SetNodeInfo(nodeInfo)
		sw.SetNodeKey(nodeKey)

		if len(nodeConfig.P2P.PersistentPeers) > 0 {
			err := sw.AddPersistentPeers(splitAndTrimEmpty(nodeConfig.P2P.PersistentPeers, ",", " "))
			if err != nil {
				return fmt.Errorf("could not add peers from persistent_peers field: %w", err)
			}
		}

		if len(nodeConfig.P2P.UnconditionalPeerIDs) > 0 {
			err := sw.AddUnconditionalPeerIDs(splitAndTrimEmpty(nodeConfig.P2P.UnconditionalPeerIDs, ",", " "))
			if err != nil {
				return fmt.Errorf("could not add peer ids from unconditional_peer_ids field: %w", err)
			}
		}

		reactor.RegisterService(userScopeHash, "eventSwitch", sw)
	}

	p2pLogger.Info("P2P Node ID", "ID", nodeKey.ID(), "file", config.NodeKeyFile())
	return nil
}

func createAddressBooksAndSetOnSwitches(
	config *cfg.Config,
	p2pLogger log.Logger,
	nodeKey *p2p.NodeKey,
	reactor *Reactor,
) error {
	// Service provider is used to retrieve event switches
	serviceProvider := reactor.GetServiceProvider()
	scopeRegistry := reactor.GetScopeRegistry()
	userScopeHashes := scopeRegistry.GetScopeHashes()
	addressBookPaths := make(map[string]string, len(userScopeHashes))

	for _, userAddress := range config.GetAddresses() {
		for _, scope := range config.UserScopes[userAddress] {
			// Query scope hash from registry to avoid re-creating SHA256
			scopeHash, err := scopeRegistry.GetScopeHash(userAddress, scope)
			if err != nil {
				return err
			}

			// The folder name is the hex representation of the fingerprint
			scopeId := NewScopeIDFromHash(scopeHash)
			folderName := scopeId.Fingerprint()

			// Uses one subfolder by user and one subfolder by scope
			bookPath := filepath.Join(config.RootDir, cfg.DefaultConfigDir, userAddress, folderName)

			addressBookPaths[scopeHash] = bookPath
		}
	}

	for _, userScopeHash := range userScopeHashes {
		nodeConfig := reactor.GetReplNodeConfig(userScopeHash)

		if _, ok := addressBookPaths[userScopeHash]; !ok {
			return fmt.Errorf("could not find address book path with scope hash %s", userScopeHash)
		}

		scopedDir := addressBookPaths[userScopeHash]
		addrBookFile := filepath.Join(scopedDir, cfg.DefaultAddrBookName)

		if _, err := os.Stat(scopedDir); err != nil {
			return fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
		}

		addrBook := pex.NewAddrBook(addrBookFile, nodeConfig.P2P.AddrBookStrict)
		addrBook.SetLogger(p2pLogger.With("book", addrBookFile))

		// Add ourselves to addrbook to prevent dialing ourselves
		if nodeConfig.P2P.ExternalAddress != "" {
			addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), nodeConfig.P2P.ExternalAddress))
			if err != nil {
				return fmt.Errorf("p2p.external_address is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}
		if nodeConfig.P2P.ListenAddress != "" {
			addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), nodeConfig.P2P.ListenAddress))
			if err != nil {
				return fmt.Errorf("p2p.laddr is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}

		// Add private IDs to addrbook to block those peers being added
		addrBook.AddPrivateIDs(splitAndTrimEmpty(nodeConfig.P2P.PrivatePeerIDs, ",", " "))

		sw := serviceProvider(userScopeHash, "eventSwitch").(*ScopedSwitch)
		sw.SetAddrBook(addrBook)
	}

	return nil
}

func createPEXReactorsAndAddToSwitches(
	config *cfg.Config,
	logger log.Logger,
	reactor *Reactor,
) error {
	// Service provider is used to retrieve event switches
	serviceProvider := reactor.GetServiceProvider()
	scopeRegistry := reactor.GetScopeRegistry()
	userScopeHashes := scopeRegistry.GetScopeHashes()

	for _, userScopeHash := range userScopeHashes {
		sw := serviceProvider(userScopeHash, "eventSwitch").(*ScopedSwitch)
		addrBook := sw.GetAddrBook().(pex.AddrBook)
		nodeConfig := reactor.GetReplNodeConfig(userScopeHash)

		// TODO persistent peers ? so we can have their DNS addrs saved
		pexReactor := pex.NewReactor(addrBook,
			&pex.ReactorConfig{
				Seeds:    splitAndTrimEmpty(nodeConfig.P2P.Seeds, ",", " "),
				SeedMode: nodeConfig.P2P.SeedMode,
				// See consensus/reactor.go: blocksToContributeToBecomeGoodPeer 10000
				// blocks assuming 10s blocks ~ 28 hours.
				// TODO (melekes): make it dynamic based on the actual block latencies
				// from the live network.
				// https://github.com/tendermint/tendermint/issues/3523
				SeedDisconnectWaitPeriod:     28 * time.Hour,
				PersistentPeersMaxDialPeriod: nodeConfig.P2P.PersistentPeersMaxDialPeriod,
			})
		pexReactor.SetLogger(logger.With("module", "pex"))

		sw.AddReactor("PEX", pexReactor)
	}

	return nil
}

// ----------------------------------------------------------------------------
// Utils

func onlyValidatorIsUs(state ScopedState, pubKey crypto.PubKey) bool {
	if state.Validators.Size() > 1 {
		return false
	}
	addr, _ := state.Validators.GetByIndex(0)
	return bytes.Equal(pubKey.Address(), addr)
}

// addTimeSample returns a function that, when called, adds an observation to m.
// The observation added to m is the number of seconds elapsed since addTimeSample
// was initially called. addTimeSample is meant to be called in a defer to calculate
// the amount of time a function takes to complete.
func addTimeSample(m metrics.Histogram, start time.Time) func() {
	return func() { m.Observe(time.Since(start).Seconds()) }
}

// splitAndTrimEmpty slices s into all subslices separated by sep and returns a
// slice of the string s with all leading and trailing Unicode code points
// contained in cutset removed. If sep is empty, SplitAndTrim splits after each
// UTF-8 sequence. First part is equivalent to strings.SplitN with a count of
// -1.  also filter out empty strings, only return non-empty strings.
func splitAndTrimEmpty(s, sep, cutset string) []string {
	if s == "" {
		return []string{}
	}

	spl := strings.Split(s, sep)
	nonEmptyStrings := make([]string, 0, len(spl))
	for i := 0; i < len(spl); i++ {
		element := strings.Trim(spl[i], cutset)
		if element != "" {
			nonEmptyStrings = append(nonEmptyStrings, element)
		}
	}
	return nonEmptyStrings
}

// ------------------------------------------------------------------------------
// Factories

var (
	genesisDocKey     = []byte("mxGenesisDoc")
	genesisDocHashKey = []byte("mxGenesisDocHash")
)

// Provider takes a config and a logger and returns a ready to go [NodeRegistry].
type Provider func(*cfg.Config, log.Logger) (*NodeRegistry, error)

// DefaultMultiplexNode returns a CometBFT node with default settings for the
// PrivValidator, ClientCreator, GenesisDoc, and DBProvider.
// It implements NodeProvider.
func DefaultMultiplexNode(config *cfg.Config, logger log.Logger) (*NodeRegistry, error) {
	nodeKey, err := p2p.LoadOrGenNodeKey(config.NodeKeyFile())
	if err != nil {
		return nil, fmt.Errorf("failed to load or gen node key %s: %w", config.NodeKeyFile(), err)
	}

	return NewMultiplexNode(
		context.Background(),
		config,
		nodeKey,
		proxy.DefaultClientCreator(config.ProxyApp, config.ABCI, config.DBDir()),
		PluralUserGenesisDocProviderFunc(config),
		cfg.DefaultDBProvider,
		node.DefaultMetricsProvider(config.Instrumentation),
		logger,
	)
}

// LoadMultiplexStateFromDBOrGenesisDocProviderWithConfig load a state multiplex
// using a database multiplex and a genesis doc provider. This factory adds one
// or many state instances in the resulting state multiplex.
func LoadMultiplexStateFromDBOrGenesisDocProviderWithConfig(
	stateMultiplexDB MultiplexDB,
	operatorGenesisHashHex string,
	config *cfg.Config,
	reactor *Reactor,
) (multiplexState MultiplexState, icsGenDoc node.IChecksummedGenesisDoc, err error) {
	mxConfig := NewUserConfig(config.Replication, config.UserScopes, config.ListenPort)
	replicatedChains := mxConfig.GetScopeHashes()
	numReplicatedChains := len(replicatedChains)
	multiplexState = make(MultiplexState, numReplicatedChains)

	genesisDocProvider := reactor.GetGenesisProvider()

	// Validate plural genesis configuration
	// Then get genesis doc set hashes from dbs or update
	// FIXME: currently saves GenesisDocSet SHA256 in *all* state DBs, instead
	// it should save each GenesisDoc's SHA256 in the corresponding DB.
	var genDocSetHashes [][]byte
	for _, userScopeHash := range replicatedChains {
		icsGenDoc := reactor.GetChecksummedGenesisDoc()
		csGenDoc, err := genesisDocProvider(userScopeHash)
		if err != nil {
			return MultiplexState{}, nil, fmt.Errorf(
				"error retrieving genesis doc for scope hash %s: %w", userScopeHash, err,
			)
		}

		// Validate per-scope genesis doc
		if err = csGenDoc.ValidateAndComplete(); err != nil {
			return MultiplexState{}, nil, fmt.Errorf("error in genesis doc for scope hash %s: %w", userScopeHash, err)
		}

		stateDB, err := GetScopedDB(stateMultiplexDB, userScopeHash)
		if err != nil {
			return MultiplexState{}, nil, fmt.Errorf(
				"error retrieving DB for scope hash %s: %w", userScopeHash, err,
			)
		}

		// Get genesis doc set hash from user scoped DB
		genDocSetHashFromDB, err := stateDB.Get(genesisDocHashKey)
		if err != nil {
			return MultiplexState{}, nil, fmt.Errorf("error retrieving genesis doc set hash: %w", err)
		}

		// Validate that existing or recently saved genesis file hash matches optional --genesis_hash passed by operator
		if operatorGenesisHashHex != "" {
			decodedOperatorGenesisHash, err := hex.DecodeString(operatorGenesisHashHex)
			if err != nil {
				return MultiplexState{}, nil, errors.New("genesis hash provided by operator cannot be decoded")
			}
			if !bytes.Equal(icsGenDoc.GetChecksum(), decodedOperatorGenesisHash) {
				return MultiplexState{}, nil, errors.New("genesis doc set hash in db does not match passed --genesis_hash value")
			}
		}

		if len(genDocSetHashFromDB) == 0 {
			// Save the genDoc hash in the store if it doesn't already exist for future verification
			if err = stateDB.SetSync(genesisDocHashKey, icsGenDoc.GetChecksum()); err != nil {
				return MultiplexState{}, nil, fmt.Errorf("failed to save genesis doc hash to db: %w", err)
			}

			// Add from IChecksummedGenesisDoc
			genDocSetHashes = append(genDocSetHashes, icsGenDoc.GetChecksum())
		} else {
			if !bytes.Equal(genDocSetHashFromDB, icsGenDoc.GetChecksum()) {
				return MultiplexState{}, nil, errors.New("genesis doc hash in db does not match loaded genesis doc")
			}

			// Add from database
			genDocSetHashes = append(genDocSetHashes, genDocSetHashFromDB)
		}

		dbKeyLayoutVersion := ""
		if config != nil {
			dbKeyLayoutVersion = config.Storage.ExperimentalKeyLayout
		}
		stateStore := sm.NewStore(stateDB, sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          dbKeyLayoutVersion,
		})

		userState, err := stateStore.LoadFromDBOrGenesisDoc(csGenDoc)
		if err != nil {
			return MultiplexState{}, nil, err
		}

		multiplexState[userScopeHash] = &ScopedState{
			ScopeHash: userScopeHash,
			State:     userState,
		}
	}

	numGenDocSetHashes := len(genDocSetHashes)
	if numGenDocSetHashes != numReplicatedChains {
		return MultiplexState{}, nil, fmt.Errorf("missing genesis docs in set, got %d docs, expected %d", numGenDocSetHashes, numReplicatedChains)
	}

	return multiplexState, icsGenDoc, nil
}
