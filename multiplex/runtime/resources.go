package runtime

import (
	"context"
	"sync"
	"sync/atomic"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ResourceRegistry defines a resources manager.
type ResourceRegistry struct {
	mtx *sync.Mutex

	// resourceMap is a map which is searchable by resource name.
	resourceMap  helpers.NamedMultiplexMap[any]
	numResources uint64 // atomic

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.ResourceManager = (*ResourceRegistry)(nil)

type ResourceRegistryOption func(*ResourceRegistry)

// NewResourceManager creates a new database service.
func NewResourceManager(
	ctx context.Context,
	logger cmtlog.Logger,
	options ...ResourceRegistryOption,
) *ResourceRegistry {
	mgr := &ResourceRegistry{
		mtx:         new(sync.Mutex),
		resourceMap: helpers.NamedMultiplexMap[any]{},

		// Options
		logger: logger,
	}

	atomic.StoreUint64(&mgr.numResources, uint64(0))

	// Use option helpers
	mgr.SetOptions(options...)
	return mgr
}

// ResourceRegistryWithLogger injects a custom logger instance.
func ResourceRegistryWithLogger(logger cmtlog.Logger) ResourceRegistryOption {
	return func(mgr *ResourceRegistry) {
		mgr.logger = logger
	}
}

// ----------------------------------------------------------------------------
// ResourceManager API implementation
//
// The mutex is locked during the execution time of the methods listed below.

// Has returns true if a resource with name exists for chainID.
func (reg *ResourceRegistry) Has(chainID, name string) bool {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if _, ok := reg.resourceMap[name]; ok {
		if _, ok := reg.resourceMap[name][chainID]; ok {
			return true
		}
	}

	return false
}

// Set adds a resource res with name for chainID.
func (reg *ResourceRegistry) Set(chainID, name string, res any) error {
	// TODO(midas): remove debug logs
	reg.logger.Debug("ResourceRegistry#Set",
		"chainId", chainID,
		"name", name,
	)

	defer atomic.AddUint64(&reg.numResources, uint64(1))

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if _, ok := reg.resourceMap[name]; !ok {
		reg.resourceMap[name] = helpers.MultiplexMap[any]{}
	}

	// Store a generic instance by name and ChainID in a map
	reg.resourceMap[name][chainID] = helpers.NewChainInstance[any](
		chainID,
		res,
	)

	return nil
}

// Get returns a resource by chainID and name or returns nil.
func (reg *ResourceRegistry) Get(chainID, name string) any {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if _, ok := reg.resourceMap[name]; ok {
		if res, ok := reg.resourceMap[name][chainID]; ok {
			return res.GetInstance()
		}
	}

	return nil
}

// Multiplex returns a multiplex map for name resources by ChainID.
func (reg *ResourceRegistry) Multiplex(name string) helpers.MultiplexMap[any] {
	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if _, ok := reg.resourceMap[name]; ok {
		return reg.resourceMap[name]
	}

	return helpers.MultiplexMap[any]{}
}

// Delete removes a resource or service with name for ChainID.
func (reg *ResourceRegistry) Delete(chainID, name string) error {
	if !reg.Has(chainID, name) {
		return nil
	}

	defer atomic.AddUint64(&reg.numResources, ^uint64(0)) // -1

	reg.mtx.Lock()
	defer reg.mtx.Unlock()

	if res, ok := reg.resourceMap[name][chainID]; ok && res != nil {
		s, isService := res.GetInstance().(service.Service)
		if isService {
			if s.IsRunning() || s.IsStarted() {
				go s.Stop()
			}
		}

		delete(reg.resourceMap[name], chainID)
		if len(reg.resourceMap[name]) == 0 {
			delete(reg.resourceMap, name)
		}

		// TODO(midas): remove debug logs
		reg.logger.Debug("ResourceRegistry#Delete",
			"chainId", chainID,
			"name", name,
		)
		return nil
	}

	return nil
}

// Reset resets the resource map and counter.
func (reg *ResourceRegistry) Reset() error {
	reg.mtx.Lock()
	allResources := reg.resourceMap
	reg.mtx.Unlock()

	for name, resources := range allResources {
		for chainID := range resources {
			reg.Delete(chainID, name)
		}
	}

	reg.mtx.Lock()
	reg.resourceMap = helpers.NamedMultiplexMap[any]{}
	reg.mtx.Unlock()

	atomic.StoreUint64(&reg.numResources, uint64(0))
	return nil
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (reg *ResourceRegistry) SetOptions(options ...ResourceRegistryOption) {
	for _, option := range options {
		option(reg)
	}
}

// Logger returns the logger instance.
func (reg *ResourceRegistry) Logger() cmtlog.Logger {
	return reg.logger
}
