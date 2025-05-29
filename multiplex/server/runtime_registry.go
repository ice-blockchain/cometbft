package server

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
)

const (
	// DefaultRuntimeCleanerInterval contains the period of time after which the
	// registry should garbage collect any idle node runtimes.
	DefaultRuntimeCleanerInterval = 60 * time.Second

	// DefaultRuntimeIdleDuration contains the period of time after which a
	// node runtime must be considered idle when it has no more active workers.
	DefaultRuntimeIdleDuration = 30 * time.Second
)

type OnIdleFn func(chainID string) error

type Idleable interface {
	OnIdle(chainID string) error
}

// RuntimeRegistry defines a registry for parallel node runtimes.
type RuntimeRegistry struct {
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

	// Options
	OnIdle          OnIdleFn
	cleanerInterval time.Duration
	runIdleDuration time.Duration
	logger          cmtlog.Logger

	// Unbuffered channel that may be written on to shutdown idling,
	// this channel is consumed alongside the [Quit] channel.
	goShutdownCh chan bool
}

type RuntimeRegistryOption func(*RuntimeRegistry)

// NewRuntimeRegistry creates a new nodes runtime registry.
func NewRuntimeRegistry(logger cmtlog.Logger, options ...RuntimeRegistryOption) *RuntimeRegistry {
	reg := &RuntimeRegistry{
		mtx: new(sync.Mutex),

		// Options
		cleanerInterval: DefaultRuntimeCleanerInterval,
		runIdleDuration: DefaultRuntimeIdleDuration,
		logger:          logger,

		// Storage
		Runtimes:  map[string]uint64{},
		Sleeping:  []string{},
		Scheduler: map[string]time.Time{},

		// Channels
		goShutdownCh: make(chan bool), // unbuffered
	}

	atomic.StoreUint64(&reg.numr, uint64(0))
	atomic.StoreUint64(&reg.nums, uint64(0))

	// Use option helpers
	reg.SetOptions(options...)

	reg.BaseService = *service.NewBaseService(nil, "RuntimeRegistry", reg)

	return reg
}

// RuntimeRegistryCleanerInterval sets a custom cleaner interval. This interval
// is used to determine when the cleaner procedure should find idle runtimes.
func RuntimeRegistryCleanerInterval(si time.Duration) RuntimeRegistryOption {
	return func(rr *RuntimeRegistry) {
		rr.cleanerInterval = si
	}
}

// RuntimeRegistryIdleDuration sets a custom idle duration. This period of time
// is used to determine how much time has to pass before a node runtime must be
// considered idle, given it has no more active workers.
func RuntimeRegistryIdleDuration(dur time.Duration) RuntimeRegistryOption {
	return func(rr *RuntimeRegistry) {
		rr.runIdleDuration = dur
	}
}

// RuntimeRegistryLogger injects a custom logger instance.
func RuntimeRegistryLogger(logger cmtlog.Logger) RuntimeRegistryOption {
	return func(rr *RuntimeRegistry) {
		rr.logger = logger
	}
}

// RuntimeRegistryOnIdle injects a custom OnIdle callback.
func RuntimeRegistryOnIdle(onIdle OnIdleFn) RuntimeRegistryOption {
	return func(rr *RuntimeRegistry) {
		rr.OnIdle = onIdle
	}
}

// ----------------------------------------------------------------------------
// RuntimeRegistry implements [service.Service]

// OnStart implements [service.Service] by spawning the sleeper routine.
//
// Setting a nil OnIdle disables the cleaner routine.
func (reg *RuntimeRegistry) OnStart() error {
	reg.logger.Debug("Starting runtime registry",
		"num_active", reg.NumRuntimes(),
		"num_sleeping", reg.NumSleeping(),
		"idle_after", reg.IdleDuration(),
		"timer", reg.CleanerInterval(),
	)

	go reg.cleanerRoutine()

	return nil
}

// OnStop implements [service.Service] by closing all open channels and
// freeing memory resources allocated for all runtimes.
func (reg *RuntimeRegistry) OnStop() {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	// Make sure all goroutines are stopped.
	close(reg.goShutdownCh)
}

// OnReset implements [service.Service] by resetting the registry.
func (reg *RuntimeRegistry) OnReset() error {
	reg.mtx.Lock()
	reg.Runtimes = map[string]uint64{}
	reg.Sleeping = []string{}
	reg.Scheduler = map[string]time.Time{}
	reg.mtx.Unlock()

	atomic.StoreUint64(&reg.numr, uint64(0))
	atomic.StoreUint64(&reg.nums, uint64(0))

	reg.logger.Debug("Reset runtime registry")
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers to configure a ReplayPool instance.
func (reg *RuntimeRegistry) SetOptions(options ...RuntimeRegistryOption) {
	for _, option := range options {
		option(reg)
	}
}

// Logger returns the logger instance.
func (reg *RuntimeRegistry) Logger() cmtlog.Logger {
	return reg.logger
}

// NumRuntimes returns the number of active runtimes across all ChainID values.
// i.e. if more than one runtime is active for a given ChainID, it will be
// counted as many time as there are active runtimes.
func (reg *RuntimeRegistry) NumRuntimes() uint64 {
	return atomic.LoadUint64(&reg.numr)
}

// NumSleeping returns the number of sleeping runtimes.
func (reg *RuntimeRegistry) NumSleeping() uint64 {
	return atomic.LoadUint64(&reg.nums)
}

// ActiveRuntimes returns the active node runtimes counters by ChainID.
func (reg *RuntimeRegistry) ActiveRuntimes() map[string]uint64 {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Runtimes
}

// SleepingRuntimes returns the sleeping node runtimes.
func (reg *RuntimeRegistry) SleepingRuntimes() []string {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Sleeping
}

// IdleScheduler returns the map of sleep start by ChainID.
func (reg *RuntimeRegistry) IdleScheduler() map[string]time.Time {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	return reg.Scheduler
}

// CleanerInterval returns the interval for the execution of the cleaner.
func (reg *RuntimeRegistry) CleanerInterval() time.Duration {
	return reg.cleanerInterval
}

// IdleDuration returns the period of inactivity to consider a runtime idle.
func (reg *RuntimeRegistry) IdleDuration() time.Duration {
	return reg.runIdleDuration
}

// ----------------------------------------------------------------------------
// RuntimeRegistry API implementation
//
// The mutex is locked during the execution time of the methods listed below.

// OnActivate marks a runtime for chainID as being active. A call to this
// method increments the internal counter of active runtimes for chainID.
// If the runtime is found sleeping, we re-activate it and remove its'
// scheduler entry so that a re-activation delays its idling to completion.
func (reg *RuntimeRegistry) OnActivate(chainID string) error {
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
func (reg *RuntimeRegistry) OnComplete(chainID string) error {
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

// ----------------------------------------------------------------------------

// removeSleeping removes an inactive runtime from the list.
// CAUTION: The caller is responsible for locking the mutex.
func (reg *RuntimeRegistry) removeSleeping(chainID string) error {
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
// been idle for at least runIdleDuration and executes the OnIdle() extension.
//
// Setting a nil OnIdle disables the cleaner routine.
func (reg *RuntimeRegistry) cleanerRoutine() {
	if reg.OnIdle == nil {
		reg.logger.Debug("Disabled sleeping runtime cleaner routine")
		return
	}

	// Loops and garbage collects runtimes when timer ticks.
	for {
		cleanerInterval := reg.CleanerInterval()

		select {
		case <-time.After(cleanerInterval): // Every cleanerInterval, we garbage collect.
			if atomic.LoadUint64(&reg.nums) == uint64(0) {
				reg.logger.Debug("No sleeping runtime to garbage collect",
					"num_active", reg.NumRuntimes(),
					"num_sleeping", reg.NumSleeping(),
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
						"chain_id", idleChainID,
						"idle_since", strconv.Itoa(int(secondsIdle))+"s",
					)

					if err := reg.OnIdle(idleChainID); err != nil {
						reg.logger.Error("Failed to execute OnIdle callback",
							"chain_id", idleChainID,
							"idle_since", strconv.Itoa(int(secondsIdle))+"s",
							"err", err,
						)
					}

					reg.mtx.Lock()
					if err := reg.removeSleeping(idleChainID); err != nil {
						reg.logger.Error("Failed to remove inactive runtime",
							"chain_id", idleChainID,
							"idle_since", strconv.Itoa(int(secondsIdle))+"s",
							"err", err,
						)
					}
					reg.mtx.Unlock()
				}
			}

		case <-reg.Quit():
		case <-reg.goShutdownCh:
			return
		}
	}
}
