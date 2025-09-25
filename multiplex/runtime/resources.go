package runtime

import (
	"context"
	"sync"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ResourceRegistry defines a resources manager.
type ResourceRegistry struct {
	mtx *sync.Mutex

	// resourceMap is a map which is searchable by resource name.
	resourceMap helpers.NamedMultiplexMap[any]

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
func (reg *ResourceRegistry) Set(chainID string, name string, res any) error {
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
func (reg *ResourceRegistry) Get(chainID string, name string) any {
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
