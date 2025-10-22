package runtime

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/proxy"
	cmttypes "github.com/ice-blockchain/cometbft/types"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

const (
	// DefaultRuntimeCleanerInterval contains the period of time after which the
	// registry should garbage collect any idle node runtimes.
	DefaultRuntimeCleanerInterval = 600 * time.Second

	// DefaultRuntimeIdleDuration contains the period of time after which a
	// node runtime must be considered idle when it has no more active workers.
	DefaultRuntimeIdleDuration = 300 * time.Second
)

// Registry defines a registry for parallel node runtimes.
type Registry struct {
	service.BaseService
	mtx *sync.Mutex

	// The number of active runtimes across all ChainID values (concurrently).
	numr uint64 // atomic
	// The number of sleeping runtimes.
	nums uint64 // atomic

	// Active runtimes list, contains counter mapped by ChainID.
	Runtimes map[string]uint64
	// Inactive runtimes list, contains ChainID values.
	Sleeping []string
	// Schedules the idling of a sleeping runtime.
	Scheduler map[string]time.Time

	// Services
	chainRegistry   helpers.ChainRegistry
	abciClient      proxy.ChainConns
	runtimeBaseConf *config.Config
	resourceMgr     *ResourceRegistry
	broadcastMgr    *BroadcastPool
	replicationMgr  *ReplicationPool
	runtimeComposer *runtimeComposer
	consensusPool   *ConsensusPool
	discoveryPool   *p2p.ConnectionPool
	cometbftPool    *p2p.ConnectionPool

	// Options
	cleanerInterval time.Duration
	runIdleDuration time.Duration
	logger          cmtlog.Logger

	// Unbuffered channel that may be written on to shutdown idling,
	// this channel is consumed alongside the [Quit] channel.
	goShutdownCh chan bool
}

// Ensure that our implementation satisfies interface.
var _ types.RuntimeManager = (*Registry)(nil)
var _ types.IdleManager = (*Registry)(nil)

type RegistryOption func(*Registry)

// NewRegistry creates a new nodes runtime registry.
func NewRegistry(
	ctx context.Context,
	baseConfig *config.Config,
	chainRegistry helpers.ChainRegistry,
	nodeKey *cmtp2p.NodeKey,
	abciClient proxy.ChainConns,
	discoveryPool *p2p.ConnectionPool,
	cometbftPool *p2p.ConnectionPool,
	resourceMgr types.ResourceManager,
	broadcastMgr types.BroadcastManager,
	replicationMgr types.ReplicationManager,
	logger cmtlog.Logger,
	options ...RegistryOption,
) *Registry {
	runtimeComposer := NewComposer(ctx,
		baseConfig,
		cometbftPool, // composes CometBFT consensus
		resourceMgr,
		logger.With("module", "composer"),
	)

	reg := &Registry{
		mtx: new(sync.Mutex),

		// Options
		cleanerInterval: DefaultRuntimeCleanerInterval,
		runIdleDuration: DefaultRuntimeIdleDuration,
		logger:          logger,

		// Services
		abciClient:      abciClient,
		runtimeBaseConf: baseConfig,
		chainRegistry:   chainRegistry,
		runtimeComposer: runtimeComposer,
		discoveryPool:   discoveryPool,
		cometbftPool:    cometbftPool,
		consensusPool: NewConsensusHandler(ctx,
			cometbftPool.NodeKey(),
			abciClient,
			resourceMgr,
			runtimeComposer,
			logger.With("module", "consensus"),
		),
		resourceMgr:    resourceMgr.(*ResourceRegistry),
		broadcastMgr:   broadcastMgr.(*BroadcastPool),
		replicationMgr: replicationMgr.(*ReplicationPool),

		// Storage
		Runtimes:  map[string]uint64{},
		Sleeping:  []string{},
		Scheduler: map[string]time.Time{},
	}

	atomic.StoreUint64(&reg.numr, uint64(0))
	atomic.StoreUint64(&reg.nums, uint64(0))

	// Use option helpers
	reg.SetOptions(options...)

	reg.BaseService = *service.NewBaseService(ctx, logger, "Registry", reg)

	// The consensus pool requires an idle manager for mempool and consensus.
	reg.consensusPool.SetIdleManager(reg)

	// The connection managers require a runtime manager to start runtimes.
	reg.discoveryPool.SetRuntimeManager(reg)
	reg.cometbftPool.SetRuntimeManager(reg)

	return reg
}

// RegistryCleanerInterval sets a custom cleaner interval. This interval
// is used to determine when the cleaner procedure should find idle runtimes.
func RegistryCleanerInterval(si time.Duration) RegistryOption {
	return func(rr *Registry) {
		rr.cleanerInterval = si
	}
}

// RegistryIdleDuration sets a custom idle duration. This period of time
// is used to determine how much time has to pass before a node runtime must be
// considered idle, given it has no more active workers.
func RegistryIdleDuration(dur time.Duration) RegistryOption {
	return func(rr *Registry) {
		rr.runIdleDuration = dur
	}
}

// RegistryLogger injects a custom logger instance.
func RegistryLogger(logger cmtlog.Logger) RegistryOption {
	return func(rr *Registry) {
		rr.logger = logger
	}
}

func RegistryWithComposerOptions(opts ...ComposerOption) RegistryOption {
	return func(rr *Registry) {
		rr.runtimeComposer.SetOptions(opts...)
	}
}

func RegistryWithConsensusOptions(opts ...ConsensusPoolOption) RegistryOption {
	return func(rr *Registry) {
		rr.consensusPool.SetOptions(opts...)
	}
}

// ----------------------------------------------------------------------------
// Registry implements [service.Service]

// OnStart implements [service.Service] by spawning the sleeper routine.
func (reg *Registry) OnStart(ctx context.Context) error {
	reg.logger.Debug("Starting runtime registry",
		"numActive", reg.NumRuntimes(),
		"numSleeping", reg.NumSleeping(),
		"idleAfter", reg.IdleDuration(),
		"timer", reg.CleanerInterval(),
	)

	if err := reg.discoveryPool.Start(); err != nil {
		return fmt.Errorf("failed to start discovery ConnectionManager: %w", err)
	}

	if err := reg.cometbftPool.Start(); err != nil {
		return fmt.Errorf("failed to start CometBFT ConnectionManager: %w", err)
	}

	if err := reg.runtimeComposer.Start(); err != nil {
		return fmt.Errorf("failed to start RuntimeComposer: %w", err)
	}

	if err := reg.consensusPool.Start(); err != nil {
		return fmt.Errorf("failed to start ConsensusHandler: %w", err)
	}

	reg.goShutdownCh = make(chan bool) // unbuffered
	go reg.cleanerRoutine()

	return nil
}

// OnStop implements [service.Service] by closing all open channels and
// freeing memory resources allocated for all runtimes.
func (reg *Registry) OnStop() {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if err := reg.runtimeComposer.Stop(); err != nil {
		reg.logger.Error("failed to stop RuntimeComposer",
			"err", err,
		)
	}

	if err := reg.discoveryPool.Stop(); err != nil {
		reg.logger.Error("failed to stop discovery ConnectionManager",
			"err", err,
		)
	}

	if err := reg.cometbftPool.Stop(); err != nil {
		reg.logger.Error("failed to stop CometBFT ConnectionManager",
			"err", err,
		)
	}

	if err := reg.consensusPool.Stop(); err != nil {
		reg.logger.Error("failed to stop ConsensusHandler",
			"err", err,
		)
	}

	// TODO(midas): should we free memory in ResourceManager?

	// Make sure all goroutines are stopped.
	close(reg.goShutdownCh)
}

// OnReset implements [service.Service] by resetting the registry.
func (reg *Registry) OnReset(ctx context.Context) error {
	reg.mtx.Lock()
	reg.Runtimes = map[string]uint64{}
	reg.Sleeping = []string{}
	reg.Scheduler = map[string]time.Time{}
	reg.mtx.Unlock()

	atomic.StoreUint64(&reg.numr, uint64(0))
	atomic.StoreUint64(&reg.nums, uint64(0))

	if err := reg.runtimeComposer.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset RuntimeComposer: %w", err)
	}

	if err := reg.discoveryPool.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset discovery ConnectionManager: %w", err)
	}

	if err := reg.cometbftPool.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset CometBFT ConnectionManager: %w", err)
	}

	if err := reg.consensusPool.Reset(ctx); err != nil {
		return fmt.Errorf("failed to reset ConsensusHandler: %w", err)
	}

	reg.logger.Debug("Reset runtime registry")
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (reg *Registry) SetOptions(options ...RegistryOption) {
	for _, option := range options {
		option(reg)
	}
}

// Logger returns the logger instance.
func (reg *Registry) Logger() cmtlog.Logger {
	return reg.logger
}

// ----------------------------------------------------------------------------
// IdleManager API implementation

// NumRuntimes returns the number of active runtimes across all ChainID values.
// i.e. if more than one runtime is active for a given ChainID, it will be
// counted as many time as there are active runtimes.
func (reg *Registry) NumRuntimes() uint64 {
	return atomic.LoadUint64(&reg.numr)
}

// NumSleeping returns the number of sleeping runtimes.
func (reg *Registry) NumSleeping() uint64 {
	return atomic.LoadUint64(&reg.nums)
}

// ActiveRuntimes returns the active node runtimes counters by ChainID.
func (reg *Registry) ActiveRuntimes() map[string]uint64 {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Runtimes
}

// SleepingRuntimes returns the sleeping node runtimes.
func (reg *Registry) SleepingRuntimes() []string {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Sleeping
}

// IdleScheduler returns the map of sleep start by ChainID.
func (reg *Registry) IdleScheduler() map[string]time.Time {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Scheduler
}

// CleanerInterval returns the interval for the execution of the cleaner.
func (reg *Registry) CleanerInterval() time.Duration {
	return reg.cleanerInterval
}

// IdleDuration returns the period of inactivity to consider a runtime idle.
func (reg *Registry) IdleDuration() time.Duration {
	return reg.runIdleDuration
}

// OnActivate marks a runtime for chainID as being active. A call to this
// method increments the internal counter of active runtimes for chainID.
// If the runtime is found sleeping, we re-activate it and remove its'
// scheduler entry so that a re-activation delays its idling to completion.
func (reg *Registry) OnActivate(chainID string) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#OnActivate", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	// If this runtime is scheduled for garbage collect, remove sleeping.
	if _, ok := reg.Scheduler[chainID]; ok {
		if err := reg.removeSleeping(chainID); err != nil {
			return err
		}
	}

	// Now set active counter and increment
	if _, ok := reg.Runtimes[chainID]; !ok {
		reg.Runtimes[chainID] = uint64(0)
	}

	reg.Runtimes[chainID] += uint64(1)
	atomic.AddUint64(&reg.numr, uint64(1))
	return nil
}

// OnComplete marks a runtime for chainID as being completed. A call to this
// method decrements the internal counter of active runtimes for chainID.
// If after decrementing the counter, we find no more active runtimes for
// chainID, we shall put it asleep so that it gets idled after runIdleDuration.
func (reg *Registry) OnComplete(chainID string) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#OnComplete", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if _, ok := reg.Runtimes[chainID]; !ok {
		return nil
	}

	// Decrement active runtimes counter for this ChainID.
	if reg.Runtimes[chainID] > 0 {
		reg.Runtimes[chainID] -= uint64(1)
		atomic.AddUint64(&reg.numr, ^uint64(0)) // -1
	}

	// If we have no more runtimes for this ChainID, set sleeping.
	if reg.Runtimes[chainID] == 0 {
		delete(reg.Runtimes, chainID)
		reg.Sleeping = append(reg.Sleeping, chainID)
		atomic.AddUint64(&reg.nums, uint64(1))
	}

	// Update scheduler to the last time we called OnComplete.
	reg.Scheduler[chainID] = time.Now()
	return nil
}

// OnIdle executes the callback cbOnIdle to idle a sleeping runtime by chainID.
func (reg *Registry) OnIdle(chainID string) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#OnIdle", "chainId", chainID)

	// StopRuntime takes a mutex lock.
	if err := reg.StopRuntime(chainID); err != nil {
		reg.logger.Error("failed to stop runtime",
			"chainId", chainID,
			"err", err)
		return err
	}

	return nil
}

// WaitForIndexedTransactions creates goroutines that wait for indexing events
// with relevantChainIds and all transactions for each ChainID.
//
// CAUTION:
// The main thread is blocked using a WaitGroup, and this method completes
// only when *all* transactions for relevantChainIds are indexed or when
// the broadcast pool is shutdown (general shutdown).
//
// Additionally, runtimes are marked complete when all txes are indexed.
func (reg *Registry) WaitForIndexedTransactions(
	relevantChainIds []string,
	transactionsByChain map[string][]client.Transaction,
) (numCompleted int) {
	txHashes := []string{}

	// CAUTION:
	// The caller thread will be locked until transactions are indexed.
	chainsWg := new(sync.WaitGroup)
	chainsWg.Add(len(relevantChainIds))

	for _, chainID := range relevantChainIds {
		cliTxes := transactionsByChain[chainID]

		// One goroutine per syncing ChainID, blocked until all txes indexed.
		// The runtime is marked complete upon completion of all tx indexing.
		go func() {
			defer chainsWg.Done()
			defer func() {
				reg.OnComplete(chainID)
			}()

			txesWg := new(sync.WaitGroup)
			txesWg.Add(len(cliTxes))

			for _, tx := range cliTxes {
				// One goroutine per txHash, blocked until tx indexed.
				go func() {
					defer txesWg.Done()

					txHash := bytesToHex(tx.Hash())
					txHashes = append(txHashes, txHash)

					if ok := reg.broadcastMgr.WaitIndexed(txHash); ok {
						numCompleted++
					}
				}()
			}
			txesWg.Wait()
		}()
	}
	chainsWg.Wait()

	if numCompleted > 0 && numCompleted == len(txHashes) {
		reg.logger.Info("All transactions have been indexed locally",
			"numNetworks", len(relevantChainIds),
			"numIndexed", numCompleted,
			"chainIds", relevantChainIds,
			"txHashes", txHashes,
		)
	}

	return // numCompleted
}

// WaitForChainReplications creates goroutines that wait for replications
// with relevantChainIds and all transactions for each ChainID.
//
// CAUTION:
// The main thread is blocked using a WaitGroup, and this method completes
// only when *all* transactions for relevantChainIds are also indexed.
//
// Eventually, it should execute waitForIndexedTransactions upon deferral.
func (reg *Registry) WaitForChainReplications(
	relevantChainIds []string,
	transactionsByChain map[string][]client.Transaction,
) (numCompleted int) {
	defer reg.WaitForIndexedTransactions(
		relevantChainIds,
		transactionsByChain,
	)

	txHashes := []string{}

	// CAUTION:
	// This goroutine will be locked until relevant relays are done with replication.
	completionWg := new(sync.WaitGroup)
	completionWg.Add(len(relevantChainIds))
	for _, syncingChainID := range relevantChainIds {
		cliTxes := transactionsByChain[syncingChainID]
		txHashes = append(txHashes, txHashesToHex(cliTxes...)...)

		go func() {
			defer completionWg.Done()

			//XXX add logs

			// This blocks the goroutine until shutdown and/or replication done.
			if ok := reg.replicationMgr.WaitCompleted(syncingChainID); ok {
				numCompleted++
			}
		}()
	}
	completionWg.Wait()

	reg.logger.Info("All relays have caught up and completed chain replications",
		"numNetworks", len(relevantChainIds),
		"numSynced", numCompleted,
		"chainIds", relevantChainIds,
		"txHashes", txHashes,
	)

	return // numCompleted
}

// ----------------------------------------------------------------------------
// RuntimeManager API implementation
//
// The mutex is locked during the execution time of the methods listed below.

// Resources returns the resource manager.
func (reg *Registry) Resources() types.ResourceManager {
	return reg.resourceMgr
}

// Validators returns a map of [cmttypes.PrivValidator] by ChainID.
func (reg *Registry) Validators() (out map[string]cmttypes.PrivValidator) {
	privValMultiplex := reg.Resources().Multiplex(types.InstanceKeyPrivValidator)
	out = make(map[string]cmttypes.PrivValidator, len(privValMultiplex))
	for chainID, instance := range privValMultiplex {
		out[chainID] = instance.GetInstance().(cmttypes.PrivValidator)
	}
	return // out
}

// Composer returns the runtime composer instance.
func (reg *Registry) Composer() types.RuntimeComposer {
	return reg.runtimeComposer
}

// ConsensusPool returns the consensus pool.
func (reg *Registry) ConsensusPool() types.ConsensusHandler {
	return reg.consensusPool
}

// AddRuntime should add a genesisDoc for chainID.
func (reg *Registry) AddRuntime(
	chainID string,
	genesisDoc cmttypes.GenesisDoc,
) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("AddRuntime", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	// Injects a GenesisDoc in runtimeComposer.genesisDocSet.
	reg.resourceMgr.Set(chainID, types.InstanceKeyGenesisDoc, genesisDoc)
	if err := reg.runtimeComposer.makeNetworkGenesis(chainID); err != nil {
		return fmt.Errorf("failed to inject genesis doc for %s: %w", chainID, err)
	}

	// Updates the internal chainRegistry instance.
	extChainID := helpers.NewExtendedChainIDFromString(chainID)
	reg.chainRegistry.AddChain(
		extChainID.GetUserAddress(),
		chainID,
	)

	// NOTE(midas): we do not need to update the NodeInfo anymore,
	// because the Networks list was removed from exported fields.

	return nil
}

// InitRuntime should initialize all services and resources for chainID.
func (reg *Registry) InitRuntime(
	chainID string,
	otherValPubKeys []string,
	createNetworkGenesis bool,
) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#InitRuntime", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if err := reg.runtimeComposer.Compose(chainID, otherValPubKeys, createNetworkGenesis); err != nil {
		return fmt.Errorf("failed to compose network for %s: %w", chainID, err)
	}

	// TODO(midas): remove debug logs
	reg.logger.Debug("RuntimeComposer#Compose done", "chainId", chainID)

	// Updates the internal chainRegistry instance.
	extChainID := helpers.NewExtendedChainIDFromString(chainID)
	reg.chainRegistry.AddChain(
		extChainID.GetUserAddress(),
		chainID,
	)

	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#InitRuntime done", "chainId", chainID)
	return nil
}

// StartRuntime should start all services for chainID.
func (reg *Registry) StartRuntime(chainID string) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#StartRuntime", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	// Injects new AppConns in MultiplexAppConn for ABCI.
	if reg.abciClient != nil {
		reg.abciClient.AddNetwork(chainID)
	}

	// Injects the state machine and block stores
	if err := reg.runtimeComposer.Inject(chainID); err != nil {
		return fmt.Errorf("failed to inject network for %s: %w", chainID, err)
	}

	// Execute the consensus/ABCI handshake for chainID.
	// This will notably set the App version.
	if err := reg.consensusPool.Handshake(chainID); err != nil {
		return fmt.Errorf("failed consensus handshake for %s: %w", chainID, err)
	}

	// Create the mempool, blocksync and consensus reactors.
	if err := reg.consensusPool.Inject(chainID); err != nil {
		return fmt.Errorf("failed to create services for %s: %w", chainID, err)
	}

	// Create the node runtime, i.e. [node.Node] implementing CometBFT.
	if err := reg.runtimeComposer.Build(chainID, reg.abciClient); err != nil {
		return fmt.Errorf("failed to build runtime for %s: %w", chainID, err)
	}

	// Starts the [node.Node] and all consensus reactors.
	if err := reg.consensusPool.Execute(chainID); err != nil {
		return fmt.Errorf("failed to start consensus for %s: %w", chainID, err)
	}

	return nil
}

// StopRuntime should stop all services for chainID.
func (reg *Registry) StopRuntime(chainID string) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#StopRuntime", "chainId", chainID)

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	// Stops the [node.Node] and all consensus reactors.
	if err := reg.consensusPool.Shutdown(chainID); err != nil {
		return fmt.Errorf("failed to stop consensus for %s: %w", chainID, err)
	}

	// Stop the databases and services owned by composer.
	if err := reg.runtimeComposer.Unload(chainID); err != nil {
		return fmt.Errorf("failed to unload runtime for %s: %w", chainID, err)
	}

	return nil
}

// ----------------------------------------------------------------------------

// removeSleeping removes an inactive runtime from the list.
// CAUTION: The caller is responsible for locking the mutex.
func (reg *Registry) removeSleeping(chainID string) error {
	if _, ok := reg.Scheduler[chainID]; ok {
		delete(reg.Scheduler, chainID)
	}

	if len(reg.Sleeping) == 0 {
		return nil
	}

	// Find runtime index, we always remove the last item.
	lastRuntimeIdx := len(reg.Sleeping) - 1
	runtimeIndex := slices.IndexFunc(reg.Sleeping, func(r string) bool {
		return r == chainID
	})
	if runtimeIndex == -1 {
		return nil
	}

	// If it's not the last item, swap it.
	if runtimeIndex != lastRuntimeIdx {
		lastRuntime := reg.Sleeping[lastRuntimeIdx]
		reg.Sleeping[runtimeIndex] = lastRuntime
	}

	// Remove the last item from reg.Sleeping.
	reg.Sleeping = reg.Sleeping[:lastRuntimeIdx]
	atomic.AddUint64(&reg.nums, ^uint64(0)) // -1
	return nil
}

// ----------------------------------------------------------------------------
// Routines

// cleanerRoutine waits for cleanerInterval, then finds node runtimes that have
// been idle for at least runIdleDuration and executes the OnIdle() method.
func (reg *Registry) cleanerRoutine() {
	// TODO(midas): remove debug logs
	reg.logger.Debug("Registry#cleanerRoutine",
		"numActive", reg.NumRuntimes(),
		"numSleeping", reg.NumSleeping(),
		"timer", reg.CleanerInterval())

	// Loops and garbage collects runtimes when timer ticks.
	for reg.Context().Err() == nil {
		cleanerInterval := reg.CleanerInterval()

		select {
		case <-time.After(cleanerInterval): // Every cleanerInterval, we garbage collect.
			if atomic.LoadUint64(&reg.nums) == uint64(0) {
				reg.logger.Debug("No sleeping runtime to garbage collect",
					"numActive", reg.NumRuntimes(),
					"numSleeping", reg.NumSleeping(),
					"timer", reg.CleanerInterval(),
				)
				continue
			}

			reg.mtx.Lock()
			runtimes := reg.Sleeping   // []string
			scheduler := reg.Scheduler // map[string]time.Time
			reg.mtx.Unlock()

			for _, idleChainID := range runtimes {
				idleSinceTz, ok := scheduler[idleChainID]
				if !ok { // too fast to idle now
					continue
				}

				secondsIdle := time.Since(idleSinceTz).Seconds()

				// If this runtime has been inactive for at least runIdleDuration,
				// we execute the OnIdle callback to idle this node runtime.
				if time.Since(idleSinceTz) >= reg.runIdleDuration {
					reg.logger.Debug("Runtime has been idle and will now shutdown",
						"chainId", idleChainID,
						"idleSince", strconv.Itoa(int(secondsIdle))+"s",
					)

					if err := reg.OnIdle(idleChainID); err != nil {
						reg.logger.Error("Failed to execute OnIdle callback",
							"chainId", idleChainID,
							"idleSince", strconv.Itoa(int(secondsIdle))+"s",
							"err", err,
						)
					}

					reg.mtx.Lock()
					if err := reg.removeSleeping(idleChainID); err != nil {
						reg.logger.Error("Failed to remove inactive runtime",
							"chainId", idleChainID,
							"idleSince", strconv.Itoa(int(secondsIdle))+"s",
							"err", err,
						)
					}
					reg.mtx.Unlock()
				}
			}

		case <-reg.Quit():
			return

		case <-reg.goShutdownCh:
			return
		}
	}
}
