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

		// doc, ok, err := genDocSet.SearchGenesisDocByUser(config.UserAddress)
		// if !ok || err != nil {
		// 	return &ChecksummedUserGenesisDoc{}, err
		// }

		// XXX
		// SHA256 of the GenesisDoc, not the GenesisDocSet
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
	bsMultiplexDB, _ = MultiplexDBProvider(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "blockstore", Config: config},
	})

	stateMultiplexDB, _ = MultiplexDBProvider(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "state", Config: config},
	})

	indexerMultiplexDB, _ = MultiplexDBProvider(&ScopedDBContext{
		DBContext: cfg.DBContext{ID: "tx_index", Config: config},
	})

	evidenceMultiplexDB, _ = MultiplexDBProvider(&ScopedDBContext{
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

// ----------------------------------------------------------------------------
// Scoped instance getters

func GetScopedStateServices(
	stateMultiplexDB *MultiplexDB,
	multiplexState *MultiplexState,
	multiplexStateStore *MultiplexStateStore,
	multiplexBlockStore *MultiplexBlockStore,
	userScopeHash string,
) (*ScopedDB, *ScopedState, *ScopedStateStore, *ScopedBlockStore, error) {
	stateDB, err := GetScopedDB(*stateMultiplexDB, userScopeHash)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stateMachine, err := GetScopedState(*multiplexState, userScopeHash)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	stateStore, err := GetScopedStateStore(*multiplexStateStore, userScopeHash)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	blockStore, err := GetScopedBlockStore(*multiplexBlockStore, userScopeHash)
	if err != nil {
		return nil, nil, nil, nil, err
	}

	return stateDB, stateMachine, stateStore, blockStore, nil
}

func GetScopedReactors(
	multiplexBlockSyncReactor MultiplexBlockSyncReactor,
	multiplexStateSyncReactor MultiplexStateSyncReactor,
	multiplexConsensusReactor MultiplexConsensusReactor,
	multiplexEvidenceReactor MultiplexEvidenceReactor,
	userScopeHash string,
) (*p2p.Reactor, *statesync.Reactor, *cs.Reactor, *evidence.Reactor, error) {
	blockSyncReactor, ok := multiplexBlockSyncReactor[userScopeHash]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("blocksync reactor for scope hash %s not found", userScopeHash)
	}

	stateSyncReactor, ok := multiplexStateSyncReactor[userScopeHash]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("statesync reactor for scope hash %s not found", userScopeHash)
	}

	consensusReactor, ok := multiplexConsensusReactor[userScopeHash]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("consensus reactor for scope hash %s not found", userScopeHash)
	}

	evidenceReactor, ok := multiplexEvidenceReactor[userScopeHash]
	if !ok {
		return nil, nil, nil, nil, fmt.Errorf("evidence reactor for scope hash %s not found", userScopeHash)
	}

	return blockSyncReactor, stateSyncReactor, consensusReactor, evidenceReactor, nil
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

	// try to get a pubkey from private validate first time
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
	logger log.Logger,
	metrics *blocksync.Metrics,
	offlineStateSyncHeight int64,
) (bcReactor p2p.Reactor, err error) {
	switch config.BlockSync.Version {
	case "v0":
		bcReactor = blocksync.NewReactor(state.GetState(), blockExec, blockStore.BlockStore, blockSync, metrics, offlineStateSyncHeight)
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

func createTransport(
	config *cfg.Config,
	nodeInfo p2p.NodeInfo,
	nodeKey *p2p.NodeKey,
	multiplexAppConns map[string]*proxy.AppConns,
) (
	*p2p.MultiplexTransport,
	[]p2p.PeerFilterFunc,
) {
	var (
		mConnConfig = p2p.MConnConfig(config.P2P)
		transport   = p2p.NewMultiplexTransport(nodeInfo, *nodeKey, mConnConfig)
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
				for _, proxyApp := range multiplexAppConns {
					res, err := (*proxyApp).Query().Query(context.TODO(), &abci.QueryRequest{
						Path: "/p2p/filter/addr/" + c.RemoteAddr().String(),
					})
					if err != nil {
						return err
					}
					if res.IsErr() {
						return fmt.Errorf("error querying abci app: %v", res)
					}
				}

				return nil
			},
		)

		peerFilters = append(
			peerFilters,
			// ABCI query for ID filtering.
			func(_ p2p.IPeerSet, p p2p.Peer) error {
				for _, proxyApp := range multiplexAppConns {
					res, err := (*proxyApp).Query().Query(context.TODO(), &abci.QueryRequest{
						Path: fmt.Sprintf("/p2p/filter/id/%s", p.ID()),
					})
					if err != nil {
						return err
					}
					if res.IsErr() {
						return fmt.Errorf("error querying abci app: %v", res)
					}
				}

				return nil
			},
		)
	}

	p2p.MultiplexTransportConnFilters(connFilters...)(transport)

	// Limit the number of incoming connections.
	max := config.P2P.MaxNumInboundPeers + len(splitAndTrimEmpty(config.P2P.UnconditionalPeerIDs, ",", " "))
	p2p.MultiplexTransportMaxIncomingConnections(max)(transport)

	return transport, peerFilters
}

func createSwitches(
	config *cfg.Config,
	transport p2p.Transport,
	p2pMetrics *p2p.Metrics,
	peerFilters []p2p.PeerFilterFunc,
	userScopeHashes []string,
	multiplexMempoolReactor map[string]*mempl.Reactor,
	multiplexBlockSyncReactor map[string]*p2p.Reactor,
	multiplexStateSyncReactor map[string]*statesync.Reactor,
	multiplexConsensusReactor map[string]*cs.Reactor,
	multiplexEvidenceReactor map[string]*evidence.Reactor,
	nodeInfo p2p.NodeInfo,
	nodeKey *p2p.NodeKey,
	p2pLogger log.Logger,
) (MultiplexSwitch, error) {

	switchMultiplex := make(MultiplexSwitch, len(userScopeHashes))

	for _, userScopeHash := range userScopeHashes {
		blockSyncReactor,
			stateSyncReactor,
			consensusReactor,
			evidenceReactor,
			err := GetScopedReactors(
			multiplexBlockSyncReactor,
			multiplexStateSyncReactor,
			multiplexConsensusReactor,
			multiplexEvidenceReactor,
			userScopeHash,
		)

		// missing reactors not allowed
		if err != nil {
			return nil, err
		}

		sw := NewScopedSwitch(
			config.P2P,
			transport,
			userScopeHash,
			p2p.WithMetrics(p2pMetrics),
			p2p.SwitchPeerFilters(peerFilters...),
		)

		sw.SetLogger(p2pLogger)
		if config.Mempool.Type != cfg.MempoolTypeNop {
			mempoolReactor, ok := multiplexMempoolReactor[userScopeHash]
			if !ok {
				return nil, fmt.Errorf("mempool reactor for scope hash %s not found", userScopeHash)
			}

			sw.AddReactor("MEMPOOL", mempoolReactor)
		}
		sw.AddReactor("BLOCKSYNC", *blockSyncReactor)
		sw.AddReactor("CONSENSUS", consensusReactor)
		sw.AddReactor("EVIDENCE", evidenceReactor)
		sw.AddReactor("STATESYNC", stateSyncReactor)

		sw.SetNodeInfo(nodeInfo)
		sw.SetNodeKey(nodeKey)

		if len(config.P2P.PersistentPeers) > 0 {
			err = sw.AddPersistentPeers(splitAndTrimEmpty(config.P2P.PersistentPeers, ",", " "))
			if err != nil {
				return nil, fmt.Errorf("could not add peers from persistent_peers field: %w", err)
			}
		}

		if len(config.P2P.UnconditionalPeerIDs) > 0 {
			err = sw.AddUnconditionalPeerIDs(splitAndTrimEmpty(config.P2P.UnconditionalPeerIDs, ",", " "))
			if err != nil {
				return nil, fmt.Errorf("could not add peer ids from unconditional_peer_ids field: %w", err)
			}
		}

		switchMultiplex[userScopeHash] = sw
	}

	p2pLogger.Info("P2P Node ID", "ID", nodeKey.ID(), "file", config.NodeKeyFile())
	return switchMultiplex, nil
}

// XXX:
//
// Passing []string is not enough to determine the subfolders structure, the
// current implementation iterates through subfolders using the config and
// *re-creates SHA256* hashes, which should not be necessary given a different
// parameter set, e.g. adding the addressBookPaths to the method.
func createAddressBooksAndSetOnSwitches(
	config *cfg.Config,
	userScopeHashes []string,
	switchMultiplex MultiplexSwitch,
	p2pLogger log.Logger,
	nodeKey *p2p.NodeKey,
) (MultiplexAddressBook, error) {
	addressBookMultiplex := make(MultiplexAddressBook, len(userScopeHashes))
	addressBookPaths := make(map[string]string, len(userScopeHashes))

	// FIXME: instead of re-creating SHA256 hashes, the MultiplexFS can be used
	// to retrieve the correct config files path per scope.
	for _, userAddress := range config.GetAddresses() {
		for _, scope := range config.UserScopes[userAddress] {
			// XXX re-creating SHA256 should not be necessary

			// Create scopeID, then SHA256 and create 8-bytes fingerprint
			// The folder name is the hex representation of the fingerprint
			scopeId := NewScopeID(userAddress, scope)
			scopeHash := scopeId.Hash()
			folderName := scopeId.Fingerprint()

			// Uses one subfolder by user and one subfolder by scope
			bookPath := filepath.Join(config.RootDir, cfg.DefaultConfigDir, userAddress, folderName)

			addressBookPaths[scopeHash] = bookPath
		}
	}

	for _, userScopeHash := range userScopeHashes {
		if _, ok := switchMultiplex[userScopeHash]; !ok {
			return nil, fmt.Errorf("could not find switch in multiplex with scope hash %s", userScopeHash)
		}

		if _, ok := addressBookPaths[userScopeHash]; !ok {
			return nil, fmt.Errorf("could not find address book path with scope hash %s", userScopeHash)
		}

		scopedDir := addressBookPaths[userScopeHash]
		addrBookFile := filepath.Join(scopedDir, cfg.DefaultAddrBookName)

		if _, err := os.Stat(scopedDir); err != nil {
			return nil, fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
		}

		addrBook := pex.NewAddrBook(addrBookFile, config.P2P.AddrBookStrict)
		addrBook.SetLogger(p2pLogger.With("book", addrBookFile))

		// Add ourselves to addrbook to prevent dialing ourselves
		if config.P2P.ExternalAddress != "" {
			addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), config.P2P.ExternalAddress))
			if err != nil {
				return nil, fmt.Errorf("p2p.external_address is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}
		if config.P2P.ListenAddress != "" {
			addr, err := p2p.NewNetAddressString(p2p.IDAddressString(nodeKey.ID(), config.P2P.ListenAddress))
			if err != nil {
				return nil, fmt.Errorf("p2p.laddr is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}

		sw := switchMultiplex[userScopeHash]
		sw.SetAddrBook(addrBook)
		addressBookMultiplex[userScopeHash] = addrBook
	}

	return addressBookMultiplex, nil
}

func createPEXReactorsAndAddToSwitches(
	userScopeHashes []string,
	addressBookMultiplex MultiplexAddressBook,
	config *cfg.Config,
	switchMultiplex MultiplexSwitch,
	logger log.Logger,
) (map[string]*pex.Reactor, error) {
	pexReactors := make(map[string]*pex.Reactor, len(userScopeHashes))
	for _, userScopeHash := range userScopeHashes {
		if _, ok := switchMultiplex[userScopeHash]; !ok {
			return nil, fmt.Errorf("could not find switch in multiplex with scope hash %s", userScopeHash)
		}

		addrBook, ok := addressBookMultiplex[userScopeHash]
		if !ok {
			return nil, fmt.Errorf("could not find address book in multiplex with scope hash %s", userScopeHash)
		}

		// TODO persistent peers ? so we can have their DNS addrs saved
		pexReactor := pex.NewReactor(addrBook,
			&pex.ReactorConfig{
				Seeds:    splitAndTrimEmpty(config.P2P.Seeds, ",", " "),
				SeedMode: config.P2P.SeedMode,
				// See consensus/reactor.go: blocksToContributeToBecomeGoodPeer 10000
				// blocks assuming 10s blocks ~ 28 hours.
				// TODO (melekes): make it dynamic based on the actual block latencies
				// from the live network.
				// https://github.com/tendermint/tendermint/issues/3523
				SeedDisconnectWaitPeriod:     28 * time.Hour,
				PersistentPeersMaxDialPeriod: config.P2P.PersistentPeersMaxDialPeriod,
			})
		pexReactor.SetLogger(logger.With("module", "pex"))

		sw := switchMultiplex[userScopeHash]
		sw.AddReactor("PEX", pexReactor)

		pexReactors[userScopeHash] = pexReactor
	}

	return pexReactors, nil
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
		privval.LoadOrGenFilePV(config.PrivValidatorKeyFile(), config.PrivValidatorStateFile()),
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
	genesisDocProvider node.GenesisDocProvider,
	operatorGenesisHashHex string,
	config *cfg.Config,
) (multiplexState MultiplexState, icsGenDoc node.IChecksummedGenesisDoc, err error) {
	icsGenDoc, err = genesisDocProvider()
	if err != nil {
		return MultiplexState{}, nil, err
	}

	mxConfig := NewUserConfig(config.Replication, config.UserScopes)
	replicatedChains := mxConfig.GetScopeHashes()
	numReplicatedChains := len(replicatedChains)
	multiplexState = make(MultiplexState, numReplicatedChains)

	// Validate plural genesis configuration
	// Then get genesis doc set hashes from dbs or update
	// FIXME: currently saves GenesisDocSet SHA256 in *all* state DBs, instead
	// it should save each GenesisDoc's SHA256 in the corresponding DB.
	var genDocSetHashes [][]byte
	for _, userScopeHash := range replicatedChains {
		csGenDoc, err := icsGenDoc.GenesisDocByScope(userScopeHash)
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
