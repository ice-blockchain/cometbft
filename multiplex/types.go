package multiplex

// -----------------------------------------------------------------------------
// Interface

// The ChainInstance interface defines the contract for a generic instance
// mapped by ChainID and the instance may be of any type.
type ChainInstance interface {
	// GetChainID should return the ChainID.
	GetChainID() string

	// GetInstance should return the instance.
	GetInstance() any
}

// MultiplexMap is a generic map for which keys are ChainID values and values
// are of the type passed with ImplT - the underlying ChainInstance.
type MultiplexMap[ImplT any] map[string]*chainInstance[ImplT]

// NamedMultiplexMap is a wrapper around MultiplexMap that maps multiple
// instances of it by name. The first-level of keys are "multiplex names"
// and the second-level are from the MultiplexMap, i.e. ChainID.
//
//	NamedMultiplexMap[*ChainDB]{ "database": MultiplexMap[*ChainDB]{
//	  "mx-chain-...-...": &ChainDB{ ChainID: "...", DB: ... }
//	}}
type NamedMultiplexMap[ImplT any] map[string]MultiplexMap[ImplT]

// -----------------------------------------------------------------------------
// chainInstance implements ChainInstance

// chainInstance is an implementation of [ChainInstance] that maps a ChainID
// value to a generic instance. The instance may be of any type.
type chainInstance[ImplT any] struct {
	ChainID  string
	instance ImplT
}

// chainInstance must satisfy the [ChainInstance] interface.
var _ ChainInstance = (*chainInstance[string])(nil)

// NewChainInstance creates a new generic instance attached to a ChainID.
func NewChainInstance[ImplT any](
	chainID string,
	instance ImplT,
) *chainInstance[ImplT] {
	return &chainInstance[ImplT]{
		ChainID:  chainID,
		instance: instance,
	}
}

// GetChainID returns the attached ChainID.
func (mx *chainInstance[ImplT]) GetChainID() string {
	return mx.ChainID
}

// GetInstances returns the attached instance.
func (mx *chainInstance[ImplT]) GetInstance() any {
	return mx.instance
}
