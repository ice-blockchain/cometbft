package runtime

import (
	"context"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"sync"

	dbm "github.com/cometbft/cometbft-db"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtcodec "github.com/ice-blockchain/cometbft/crypto/encoding"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/privval"
	"github.com/ice-blockchain/cometbft/proxy"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/state/indexer"
	blockidxkv "github.com/ice-blockchain/cometbft/state/indexer/block/kv"
	"github.com/ice-blockchain/cometbft/state/txindex"
	txidxkv "github.com/ice-blockchain/cometbft/state/txindex/kv"
	bs "github.com/ice-blockchain/cometbft/store"
	cmttypes "github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// runtimeComposer defines a runtime composer.
type runtimeComposer struct {
	service.BaseService
	mtx *sync.Mutex

	// Resources
	runtimeBaseConf  *config.Config
	validatorsByNet  map[string][]string // ChainID:[]ed25519PubKey
	genesisDocSet    *helpers.ChecksummedGenesisDocSet
	composedChainIds map[string]struct{}
	injectedChainIds map[string]struct{}
	cometbftSwitch   *cmtp2p.Switch

	// Services
	resourceMgr    types.ResourceManager
	connectionPool *p2p.ConnectionPool

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.RuntimeComposer = (*runtimeComposer)(nil)

type ComposerOption func(*runtimeComposer)

// NewComposer creates a new runtime composer.
func NewComposer(
	ctx context.Context,
	baseConfig *config.Config,
	connectionPool *p2p.ConnectionPool,
	resourceMgr types.ResourceManager,
	logger cmtlog.Logger,
	options ...ComposerOption,
) *runtimeComposer {
	c := &runtimeComposer{
		mtx: new(sync.Mutex),

		// Resources
		runtimeBaseConf:  baseConfig,
		validatorsByNet:  map[string][]string{},
		composedChainIds: map[string]struct{}{},
		injectedChainIds: map[string]struct{}{},

		// Options
		logger: logger,

		// Services
		connectionPool: connectionPool,
		resourceMgr:    resourceMgr,
	}

	// Use option helpers
	c.SetOptions(options...)

	c.BaseService = *service.NewBaseService(ctx, logger, "runtimeComposer", c)
	return c
}

// ComposerWithLogger injects a custom logger instance.
func ComposerWithLogger(logger cmtlog.Logger) ComposerOption {
	return func(c *runtimeComposer) {
		c.logger = logger
	}
}

func ComposerWithGenesisDocSet(docSet *helpers.ChecksummedGenesisDocSet) ComposerOption {
	return func(c *runtimeComposer) {
		c.genesisDocSet = docSet
	}
}

// ----------------------------------------------------------------------------

// UserChainID returns a parsed extended ChainID.
func (c *runtimeComposer) UserChainID(chainID string) helpers.ExtendedChainID {
	return helpers.NewExtendedChainIDFromString(chainID)
}

// SetOptions uses custom option helpers.
func (c *runtimeComposer) SetOptions(options ...ComposerOption) {
	for _, option := range options {
		option(c)
	}
}

// Logger returns the logger instance.
func (c *runtimeComposer) Logger() cmtlog.Logger {
	return c.logger
}

// SetSwitch is used to set a cmtp2p.Switch for CometBFT.
func (c *runtimeComposer) SetSwitch(sw *cmtp2p.Switch) {
	c.cometbftSwitch = sw
}

// Switch returns the cmtp2p.Switch instance for CometBFT.
func (c *runtimeComposer) Switch() *cmtp2p.Switch {
	return c.cometbftSwitch
}

// ----------------------------------------------------------------------------
// runtimeComposer implements [service.Service]

// OnStart implements [service.Service] by opening a database.
//
// The mutex is locked during the execution time of this method.
func (c *runtimeComposer) OnStart(ctx context.Context) error {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	// CAUTION: this method expects the genesis file to contain a GenesisDocSet.
	genesisDocSetProvider := helpers.GenesisDocSetProvider(c.runtimeBaseConf)

	// Loads GenesisDocSet from filesystem if available.
	if genesisDocSet, err := genesisDocSetProvider(); err == nil {
		if len(genesisDocSet.GenesisDocs) == 0 {
			c.logger.Info("CAUTION: Using empty GenesisDocSet (not an error)")
		}
		c.SetOptions(ComposerWithGenesisDocSet(genesisDocSet))
	} else {
		switch err.(type) {
		case helpers.ErrMissingGenesisDocSet:
			c.logger.Info("CAUTION: Creating empty GenesisDocSet (not an error)")
			return nil
		case helpers.ErrEmptyGenesisDocSet:
			c.logger.Info("CAUTION: Using empty GenesisDocSet (not an error)")
			return nil
		}
		return err
	}

	return nil
}

// OnStop implements [service.Service] by closing the database.
func (c *runtimeComposer) OnStop() {
	c.mtx.Lock()
	composedChainIds := c.composedChainIds
	c.mtx.Unlock()

	for chainID := range composedChainIds {
		go func() {
			c.Unload(chainID)
		}()
	}
}

// OnReset implements [service.Service] by resetting the service.
func (c *runtimeComposer) OnReset(ctx context.Context) error {
	c.mtx.Lock()
	composedChainIds := c.composedChainIds
	c.mtx.Unlock()

	serviceResetFn := func(s service.Service) {
		if s.IsStopped() {
			s.Reset(ctx)
		}
	}

	for chainID := range composedChainIds {
		if eventBus := c.EventBus(chainID); eventBus != nil {
			serviceResetFn(eventBus)
		}

		if memR := c.resourceMgr.Get(chainID,
			types.ServiceKeyMempoolReactor,
		).(*mempl.Reactor); memR != nil {
			serviceResetFn(memR)
		}

		if bsR := c.resourceMgr.Get(chainID,
			types.ServiceKeyBlockSyncReactor,
		).(*blocksync.Reactor); bsR != nil {
			serviceResetFn(bsR)
		}

		if conR := c.resourceMgr.Get(chainID,
			types.ServiceKeyConsensusReactor,
		).(*cs.Reactor); conR != nil {
			serviceResetFn(conR)
		}

		if evR := c.resourceMgr.Get(chainID,
			types.ServiceKeyEvidenceReactor,
		).(*evidence.Reactor); evR != nil {
			serviceResetFn(evR)
		}

		if idxS := c.resourceMgr.Get(chainID,
			types.ServiceKeyIndexers,
		).(*txindex.IndexerService); idxS != nil {
			serviceResetFn(idxS)
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// RuntimeComposer API implementation
//
// The mutex is locked during the execution time of the methods listed below.

// Compose initializes a runtime for chainID.
func (c *runtimeComposer) Compose(
	chainID string,
	remoteValidatorPubKeys []string,
) error {
	// TODO(midas): remove debug logs
	c.logger.Debug("runtimeComposer#Compose", "chainId", chainID)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	// Create a config.Config for this ChainID.
	if err := c.makeNetworkConfig(chainID); err != nil {
		return err
	}

	// Create helpers.DBService instances for this ChainID.
	if err := c.makeNetworkDatabases(chainID); err != nil {
		return err
	}

	// Create a cmttypes.PrivValidator for this ChainID.
	if err := c.makeNetworkValidator(chainID); err != nil {
		return err
	}

	// Update the validators list for this ChainID.
	localValidatorPubKey, _ := c.Validator(chainID).GetPubKey()
	localValidatorPubKeyHex := hex.EncodeToString(localValidatorPubKey.Bytes())

	if _, ok := c.validatorsByNet[chainID]; !ok {
		c.validatorsByNet[chainID] = make([]string, 0, len(remoteValidatorPubKeys)+1) // add local
	}
	c.validatorsByNet[chainID] = remoteValidatorPubKeys[:]
	c.validatorsByNet[chainID] = append(c.validatorsByNet[chainID], localValidatorPubKeyHex)

	// Create a cmttypes.GenesisDoc with validatorPubKeys.
	if err := c.makeNetworkGenesis(chainID); err != nil {
		return err
	}

	c.composedChainIds[chainID] = struct{}{}
	return nil
}

// Inject injects a running state machine and block store.
func (c *runtimeComposer) Inject(chainID string) error {
	// TODO(midas): remove debug logs
	c.logger.Debug("runtimeComposer#Inject", "chainId", chainID)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	// Create a [sm.Store] and [bs.Store] for state and blocks.
	// This opens the state machine and blocks store databases.
	if err := c.startNetworkStateMachine(chainID); err != nil {
		return err
	}

	// Create and start a [cmttypes.EventBus] for chainID.
	if err := c.startNetworkEventBus(chainID); err != nil {
		return err
	}

	// Create and start a [txindex.IndexerService] for chainID.
	if err := c.startNetworkIndexers(chainID); err != nil {
		return err
	}

	// Create a [sm.Pruner] and set 0 retain height to disable.
	if err := c.makeNetworkPruner(chainID); err != nil {
		return err
	}

	c.injectedChainIds[chainID] = struct{}{}
	return nil
}

// Build packages a node runtime and injects a [node.Node].
func (c *runtimeComposer) Build(
	chainID string,
	abciClient proxy.ChainConns,
) error {
	// TODO(midas): remove debug logs
	c.logger.Debug("runtimeComposer#Build", "chainId", chainID)

	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.resourceMgr.Has(chainID, types.ServiceKeyNodeRuntime) {
		return nil // Nothing to do
	}

	// Resources/Services
	runtimeConfig := c.Config(chainID)
	genesisDoc := c.GenesisDoc(chainID)
	privValidator := c.Validator(chainID)
	eventBus := c.EventBus(chainID)
	mempoolPtr := c.Mempool(chainID)
	evidencePtr := c.EvidencePool(chainID)
	stateStore := c.StateStore(chainID)
	blockStore := c.BlockStore(chainID)
	stateMachine := c.StateMachine(chainID)
	pruner := c.resourceMgr.Get(chainID, types.ServiceKeyPruner).(*sm.Pruner)
	indexerService := c.resourceMgr.Get(chainID, types.ServiceKeyIndexers).(*txindex.IndexerService)
	consensusReactor := c.resourceMgr.Get(chainID, types.ServiceKeyConsensusReactor).(*cs.Reactor)
	pexAddrBook := c.resourceMgr.Get(chainID, types.ServiceKeyAddressesReactor).(*pex.Reactor).AddrBook()
	proxyApp := abciClient.ToAppConns(chainID)

	// Connections
	withNodeInfo := c.connectionPool.NodeInfo()
	withNodeKey := c.connectionPool.NodeKey()
	eventSwitch := c.Switch()
	connTransport := eventSwitch.Transport()

	eventSwitch.SetAddrBook(pexAddrBook)

	// Create the [node.Node] instance.
	nodeInstance := node.NewNodeWithServices(
		runtimeConfig,
		&genesisDoc,
		withNodeInfo,
		withNodeKey,
		privValidator,
		pexAddrBook,
		connTransport,
		eventSwitch,
		eventBus,
		proxyApp,
		mempoolPtr,
		evidencePtr,
		pruner,
		indexerService,
		stateStore,
		blockStore,
		consensusReactor.GetState(), // cs.State
		false,                       // state-sync is disabled
		stateMachine,                // stateSyncGenesis (sm.State)
	)

	nodeInstance.BaseService = *service.NewBaseService(
		c.Context(),
		c.logger.With("nodeId", withNodeKey.ID()),
		"Node",
		nodeInstance,
	)

	c.resourceMgr.Set(chainID, types.ServiceKeyNodeRuntime, nodeInstance)
	return nil
}

// Unload decomposes resources and services for chainID.
func (c *runtimeComposer) Unload(chainID string) error {
	// TODO(midas): remove debug logs
	c.logger.Debug("runtimeComposer#Unload", "chainId", chainID)

	defer func() {
		c.mtx.Lock()
		defer c.mtx.Unlock()

		delete(c.composedChainIds, chainID)
		delete(c.injectedChainIds, chainID)
	}()

	// Close database connections.
	dbServiceKeys := []string{
		types.ServiceKeyDatabaseBlock,
		types.ServiceKeyDatabaseState,
		types.ServiceKeyDatabaseIndex,
		types.ServiceKeyDatabaseEvidence,
	}
	for _, dbServiceKey := range dbServiceKeys {
		if dbS := c.resourceMgr.Get(
			chainID,
			dbServiceKey,
		); dbS != nil {
			dbService := dbS.(service.Service)
			if dbService.IsRunning() || dbService.IsStarted() {
				go func() {
					c.mtx.Lock()
					defer c.mtx.Unlock()

					dbService.Stop()
				}()
			}
		}
	}

	if eventBus := c.EventBus(chainID); eventBus != nil {
		if eventBus.IsRunning() {
			eventBus.Stop()
		}
	}

	if indexerService := c.IndexerService(chainID); indexerService != nil {
		if indexerService.IsRunning() {
			indexerService.Stop()
		}
	}

	if blocksPruner := c.BlockPruner(chainID); blocksPruner != nil {
		if blocksPruner.IsRunning() {
			blocksPruner.Stop()
		}
	}

	return nil
}

// ConfPath returns the filesystem path to config for chainID.
func (c *runtimeComposer) ConfPath(chainID string) string {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyPathConf) {
		return ""
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyPathConf,
	).(string)
}

// DataPath returns the filesystem path to data for chainID.
func (c *runtimeComposer) DataPath(chainID string) string {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyPathData) {
		return ""
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyPathData,
	).(string)
}

// Config returns the configuration instance for chainID.
func (c *runtimeComposer) Config(chainID string) *config.Config {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyConfig) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyConfig,
	).(*config.Config)
}

// GenesisDoc returns the genesis configuration for chainID.
func (c *runtimeComposer) GenesisDoc(chainID string) cmttypes.GenesisDoc {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyGenesisDoc) {
		return cmttypes.GenesisDoc{}
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyGenesisDoc,
	).(cmttypes.GenesisDoc)
}

// Database returns a database for chainID.
// Uses dbServiceKey as registered service key.
func (c *runtimeComposer) Database(chainID, dbServiceKey string) *helpers.DBService {
	if !c.resourceMgr.Has(chainID, dbServiceKey) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		dbServiceKey,
	).(*helpers.DBService)
}

// Validator returns the priv validator for chainID.
func (c *runtimeComposer) Validator(chainID string) cmttypes.PrivValidator {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyPrivValidator) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyPrivValidator,
	).(cmttypes.PrivValidator)
}

// EventBus returns the event bus for chainID.
func (c *runtimeComposer) EventBus(chainID string) *cmttypes.EventBus {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyEventBus) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.ServiceKeyEventBus,
	).(*cmttypes.EventBus)
}

// IndexerService returns the tx indexer for chainID.
func (c *runtimeComposer) IndexerService(chainID string) *txindex.IndexerService {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyIndexers) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.ServiceKeyIndexers,
	).(*txindex.IndexerService)
}

// StateMachine returns the state machine for chainID.
func (c *runtimeComposer) StateMachine(chainID string) sm.State {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyStateMachine) {
		return sm.State{}
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyStateMachine,
	).(sm.State)
}

// StateStore returns the state store for chainID.
func (c *runtimeComposer) StateStore(chainID string) sm.Store {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyStateStore) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyStateStore,
	).(sm.Store)
}

// BlockStore returns the blocks store for chainID.
func (c *runtimeComposer) BlockStore(chainID string) *bs.BlockStore {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyBlockStore) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyBlockStore,
	).(*bs.BlockStore)
}

// BlockPruner returns the blocks store for chainID.
func (c *runtimeComposer) BlockPruner(chainID string) *sm.Pruner {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyPruner) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.ServiceKeyPruner,
	).(*sm.Pruner)
}

// Mempool returns the mempool for chainID.
func (c *runtimeComposer) Mempool(chainID string) mempl.Mempool {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyMempoolReactor) {
		return nil
	}

	mempoolReactor := c.resourceMgr.Get(
		chainID,
		types.ServiceKeyMempoolReactor,
	).(*mempl.Reactor)
	return mempoolReactor.GetMempoolPtr()
}

// EvidencePool returns the evidence pool for chainID.
func (c *runtimeComposer) EvidencePool(chainID string) *evidence.Pool {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyEvidenceReactor) {
		return nil
	}

	evidenceReactor := c.resourceMgr.Get(
		chainID,
		types.ServiceKeyEvidenceReactor,
	).(*evidence.Reactor)
	return evidenceReactor.GetPoolPtr()
}

// BlockExecutor returns the blocks executor for chainID.
func (c *runtimeComposer) BlockExecutor(chainID string) *sm.BlockExecutor {
	if !c.resourceMgr.Has(chainID, types.InstanceKeyBlockExecutor) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.InstanceKeyBlockExecutor,
	).(*sm.BlockExecutor)
}

// Node returns the node service for chainID.
func (c *runtimeComposer) Node(chainID string) *node.Node {
	if !c.resourceMgr.Has(chainID, types.ServiceKeyNodeRuntime) {
		return nil
	}

	return c.resourceMgr.Get(
		chainID,
		types.ServiceKeyNodeRuntime,
	).(*node.Node)
}

// ----------------------------------------------------------------------------
// Orchestration methods

func (c *runtimeComposer) makeNetworkConfig(
	chainID string,
) error {
	// Ensures filesystem, i.e. %root%/(config|data)/%address%/%ChainID%.
	rootDir := c.runtimeBaseConf.RootDir
	confDir := filepath.Join(rootDir, config.DefaultConfigDir)
	dataDir := filepath.Join(rootDir, config.DefaultDataDir)

	confDir,
		dataDir, _ = helpers.EnsureNetworkFS(c.UserChainID(chainID), confDir, dataDir)

	// Creates config overwrite for chainID.
	cfg := NewConfig(
		c.runtimeBaseConf,
		chainID,
		"", // empty seed nodes (always)
		config.DefaultStateSyncConfig(),
		int(c.runtimeBaseConf.DiscoveryPort),
	)

	c.resourceMgr.Set(chainID, types.InstanceKeyPathData, dataDir)
	c.resourceMgr.Set(chainID, types.InstanceKeyPathConf, confDir)
	c.resourceMgr.Set(chainID, types.InstanceKeyConfig, cfg)
	return nil
}

func (c *runtimeComposer) makeNetworkDatabases(
	chainID string,
) error {
	extChainID := c.UserChainID(chainID)

	// Prepare database backend parameters.
	dbBackend := dbm.BackendType(c.runtimeBaseConf.DBBackend)
	dbStorage := filepath.Join(
		c.runtimeBaseConf.DBDir(),
		extChainID.GetUserAddress(),
		extChainID.String(),
	)

	// For every ChainID, we create multiple dbs.
	dbServices := map[string]string{
		"blockstore": types.ServiceKeyDatabaseBlock,
		"state":      types.ServiceKeyDatabaseState,
		"txindex":    types.ServiceKeyDatabaseIndex,
		"evidence":   types.ServiceKeyDatabaseEvidence,
	}

	// Initialize database services (restartable).
	for dbName, dbServiceKey := range dbServices {
		databaseService := c.Database(chainID, dbServiceKey)
		databaseLogger := c.logger.With("module", "database")

		if databaseService == nil {
			databaseService = helpers.NewDBService(c.Context(),
				dbName,
				dbStorage,
				string(dbBackend),
				databaseLogger,
			)

			c.resourceMgr.Set(chainID, dbServiceKey, databaseService)
		}
	}

	return nil
}

func (c *runtimeComposer) makeNetworkValidator(
	chainID string,
) error {
	confDir := c.ConfPath(chainID)
	dataDir := c.DataPath(chainID)

	// Uses filenames from configuration
	keyFile := filepath.Base(c.runtimeBaseConf.PrivValidatorKeyFile())
	stateFile := filepath.Base(c.runtimeBaseConf.PrivValidatorStateFile())

	// NOTE: We force the ed25519 key type here.
	privValidator, err := privval.LoadOrGenFilePV(
		filepath.Join(confDir, keyFile),
		filepath.Join(dataDir, stateFile),
		func() (crypto.PrivKey, error) {
			return ed25519.GenPrivKey(), nil
		},
	)
	if err != nil {
		return err
	}

	c.resourceMgr.Set(chainID, types.InstanceKeyPrivValidator, privValidator)
	return nil
}

func (c *runtimeComposer) makeNetworkGenesis(
	chainID string,
) error {
	var genesisDoc cmttypes.GenesisDoc
	if !c.resourceMgr.Has(chainID, types.InstanceKeyGenesisDoc) {
		validatorPubKeys := c.validatorsByNet[chainID]

		powerPerValidator := 10
		genesisValidators := make([]cmttypes.GenesisValidator, 0, len(validatorPubKeys))
		for _, valPubKeyHex := range validatorPubKeys {
			validatorPubBz, _ := hex.DecodeString(valPubKeyHex)
			validatorPubKey, _ := cmtcodec.PubKeyFromTypeAndBytes(ed25519.KeyType, validatorPubBz)

			genesisValidators = append(genesisValidators, cmttypes.GenesisValidator{
				Address: validatorPubKey.Address(),
				PubKey:  validatorPubKey,
				Power:   int64(powerPerValidator),
			})
		}

		// Create new GenesisDoc with genesis time "now" and validators.
		genesisDoc = cmttypes.GenesisDoc{
			ChainID:         chainID,
			GenesisTime:     cmttime.Now(),
			ConsensusParams: cmttypes.DefaultConsensusParams(),
			Validators:      genesisValidators,
		}

		c.resourceMgr.Set(chainID, types.InstanceKeyGenesisDoc, genesisDoc)
	} else {
		genesisDoc = c.resourceMgr.Get(
			chainID,
			types.InstanceKeyGenesisDoc,
		).(cmttypes.GenesisDoc)
	}

	confDir := c.ConfPath(chainID)

	// Store individual genesis docs per ChainID.
	// i.e.: %root%/config/%address%/%ChainID%/genesis.json
	genesisDocFile := filepath.Join(confDir, "genesis.json")
	genesisDoc.SaveAs(genesisDocFile)

	if genDoc, _ := c.genesisDocSet.GenesisDocByChainID(chainID); genDoc == nil {
		// Store GenesisDocSet in multiplex root dir.
		// i.e.: %root%/config/genesis.json
		c.genesisDocSet.GenesisDocs = append(c.genesisDocSet.GenesisDocs, genesisDoc)
		genDocSetBytes, _ := cmtjson.Marshal(c.genesisDocSet.GenesisDocs)
		c.genesisDocSet.Sha256Checksum = tmhash.Sum(genDocSetBytes)
		c.genesisDocSet.GenesisDocs.SaveAs(c.runtimeBaseConf.GenesisFile())
	}

	return nil
}

func (c *runtimeComposer) makeNetworkPruner(
	chainID string,
) error {
	if c.resourceMgr.Has(chainID, types.ServiceKeyPruner) {
		return nil // Nothing to do.
	}

	// Updates the application retain height to 0, to disable pruner.
	stateStore := c.resourceMgr.Get(chainID, types.InstanceKeyStateStore).(sm.Store)
	blockStore := c.resourceMgr.Get(chainID, types.InstanceKeyBlockStore).(*bs.BlockStore)
	indexerService := c.resourceMgr.Get(chainID, types.ServiceKeyIndexers).(*txindex.IndexerService)

	// NOTE(midas): Disables the blocks pruner completely.
	if err := stateStore.SaveApplicationRetainHeight(0); err != nil {
		return fmt.Errorf("could not save application retain height: %w", err)
	}

	txIndexer := indexerService.GetTxIndexer()
	blockIndexer := indexerService.GetBlockIndexer()
	pruner := sm.NewPruner(
		c.Context(),
		stateStore,
		blockStore,
		blockIndexer,
		txIndexer,
		c.logger.With("module", "state"),
	)

	c.resourceMgr.Set(chainID, types.ServiceKeyPruner, pruner)
	return nil
}

// ----------------------------------------------------------------------------
// Starter methods

func (c *runtimeComposer) startNetworkStateMachine(
	chainID string,
) error {
	stateDatabaseService := c.resourceMgr.Get(chainID, types.ServiceKeyDatabaseState).(service.Service)
	blockDatabaseService := c.resourceMgr.Get(chainID, types.ServiceKeyDatabaseBlock).(service.Service)

	// Ensure that state and blockstore databases are open.
	if err := helpers.EnsureStartDBService(c.Context(), stateDatabaseService); err != nil {
		return fmt.Errorf(
			"failed to open state database for %s: %w", chainID, err)
	}

	if err := helpers.EnsureStartDBService(c.Context(), blockDatabaseService); err != nil {
		return fmt.Errorf(
			"failed to open block database for %s: %w", chainID, err)
	}

	stateDB := stateDatabaseService.(*helpers.DBService).DB()
	blockDB := blockDatabaseService.(*helpers.DBService).DB()

	dbCfg := c.runtimeBaseConf.Storage
	dbKeyLayoutVersion := dbCfg.ExperimentalKeyLayout

	// Initialize a replicable [sm.Store].
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DBKeyLayout: dbKeyLayoutVersion,
	})

	// Try to load state machine from database.
	stateMachine, err := stateStore.Load()
	if err != nil {
		return fmt.Errorf(
			"error loading state machine for ChainID %s: %w", chainID, err)
	}

	// .. or fill it from genesis doc.
	if stateMachine.IsEmpty() {
		var createErr error
		genesisDoc := c.GenesisDoc(chainID)

		// Load the state from database or GenesisDoc
		stateMachine, createErr = stateStore.LoadFromDBOrGenesisDoc(&genesisDoc)
		if createErr != nil {
			return createErr
		}

		// State machine was empty, update now
		if createErr = stateStore.Save(stateMachine); createErr != nil {
			return fmt.Errorf(
				"could not save newly initialized state machine: %w", createErr)
		}
	}

	// Configure blockstore
	dbCompactionMethod := dbCfg.Compact
	dbCompactionPeriod := dbCfg.CompactionInterval

	// Also, initialize a [bs.BlockStore] (not snapshottable)
	blockStore := bs.NewBlockStore(
		blockDB,
		bs.WithCompaction(dbCompactionMethod, dbCompactionPeriod),
		bs.WithDBKeyLayout(dbKeyLayoutVersion),
	)

	c.resourceMgr.Set(chainID, types.InstanceKeyStateMachine, stateMachine) // *sm.State
	c.resourceMgr.Set(chainID, types.InstanceKeyStateStore, stateStore)
	c.resourceMgr.Set(chainID, types.InstanceKeyBlockStore, blockStore)
	return nil
}

func (c *runtimeComposer) startNetworkEventBus(
	chainID string,
) error {
	var eventBus *cmttypes.EventBus
	if !c.resourceMgr.Has(chainID, types.ServiceKeyEventBus) {
		eventBus = cmttypes.NewEventBus(c.Context())
		eventBus.SetLogger(c.logger.With("module", "events"))
		if err := eventBus.Start(); err != nil {
			return fmt.Errorf("error starting event bus: %w", err)
		}

		c.resourceMgr.Set(chainID, types.ServiceKeyEventBus, eventBus)
	} else {
		eventBus = c.resourceMgr.Get(chainID, types.ServiceKeyEventBus).(*cmttypes.EventBus)
		if !eventBus.IsRunning() {
			if eventBus.IsStopped() {
				eventBus.Reset(c.Context()) // permit re-start
			}

			if err := eventBus.Start(); err != nil && err != service.ErrAlreadyStarted {
				return fmt.Errorf("error starting event bus: %w", err)
			}
		}
	}

	return nil
}

func (c *runtimeComposer) startNetworkIndexers(
	chainID string,
) error {
	// Ensure that tx_index database is open.
	indexerDbService := c.resourceMgr.Get(chainID, types.ServiceKeyDatabaseIndex).(service.Service)
	if err := helpers.EnsureStartDBService(c.Context(), indexerDbService); err != nil {
		return fmt.Errorf(
			"failed to open tx_index database for %s: %w", chainID, err)
	}

	var (
		txIndexer    txindex.TxIndexer
		blockIndexer indexer.BlockIndexer
	)

	if !c.resourceMgr.Has(chainID, types.ServiceKeyIndexers) {
		indexerDatabase := indexerDbService.(*helpers.DBService).DB()
		txIndexer = txidxkv.NewTxIndex(indexerDatabase)
		blockIndexer = blockidxkv.New(
			dbm.NewPrefixDB(indexerDatabase, []byte("block_events")),
			blockidxkv.WithCompaction(c.runtimeBaseConf.Storage.Compact, c.runtimeBaseConf.Storage.CompactionInterval),
		)

		eventBus := c.EventBus(chainID)
		indexerService := txindex.NewIndexerService(c.Context(), txIndexer, blockIndexer, eventBus, false) // stopOnError
		indexerService.SetLogger(c.logger.With("module", "txindex"))
		if err := indexerService.Start(); err != nil && err != service.ErrAlreadyStarted {
			return fmt.Errorf("error starting indexers: %w", err)
		}

		c.resourceMgr.Set(chainID, types.ServiceKeyIndexers, indexerService)
	} else {
		indexerService := c.resourceMgr.Get(chainID, types.ServiceKeyIndexers).(*txindex.IndexerService)
		txIndexer = indexerService.GetTxIndexer()
		blockIndexer = indexerService.GetBlockIndexer()

		if !indexerService.IsRunning() {
			if indexerService.IsStopped() {
				indexerService.Reset(c.Context()) // permit re-start
			}

			if err := indexerService.Start(); err != nil && err != service.ErrAlreadyStarted {
				return fmt.Errorf("error starting indexers: %w", err)
			}
		}
	}

	return nil
}
