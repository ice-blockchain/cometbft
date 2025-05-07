package multiplex

import (
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"sort"
	"sync"

	"github.com/ice-blockchain/cometbft/config"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
)

// -----------------------------------------------------------------------------
// ChainRegistry
//
// ChainRegistry defines a registry pattern contract which should be searchable
// by ChainID and by user address.
//
// Note that it is preferable that the ChainID slice returned with [GetChains]
// is ordered to enable determinism on listing supported replicated chains.
//
// A cacheable chain provider is provided with [NewChainRegistry].
type ChainRegistry interface {
	// AddChain should add a new ChainID for userAddress.
	AddChain(userAddress string, chainID string) ChainRegistry

	// HasChain should return true if the ChainID can be found.
	HasChain(chainID string) bool

	// GetChains should return an ordered slice of unique chain identifiers.
	GetChains() []string

	// GetStateSyncConfig should return the required configuration for state-sync.
	// This method should return an error if no sync config can be found.
	GetStateSyncConfig(chainID string) (*config.StateSyncConfig, error)

	// GetSeeds should return a comma-separated list of seed nodes (id@host:port).
	// This method should return an error if no seed nodes can be found.
	GetSeeds(chainID string) (string, error)

	// GetAddress should return the user address attached to a ChainID.
	// This method should return an error if no address can be found.
	GetAddress(chainID string) (string, error)

	// FindChain should search for a ChainID and return its index or -1.
	// This method should return an error if no address can be found.
	FindChain(chainID string) (int, error)
}

// Assert internal singletonChainRegistry satisfies ChainRegistry.
var _ ChainRegistry = (*singletonChainRegistry)(nil)

type singletonChainRegistry struct {
	mtx *sync.Mutex

	// SyncConfig maps a ChainID to trust options for the state-sync service.
	SyncConfig map[string]*config.StateSyncConfig

	// ChainSeeds maps a ChainID to a comma-separated list of seed nodes (id@host:port).
	ChainSeeds map[string]string

	// ReplicatedChains contains a slice of chain identifiers ordered alphabetically.
	ReplicatedChains []string

	// userChains maps user addresses to a slice of ChainIDs
	// This field is unexported to prevent unordered iteration.
	userChains map[string][]string
}

// AddChain adds a new ChainID to the ReplicatedChains slice.
//
// This method may be used to add a new ChainID to a ChainRegistry after it
// was initialized around a configuration object.
//
// NOTE: The ChainSeeds and SyncConfig configuration objects for the added
// network are set to empty values. The ChainSeeds is obviously empty because
// the added (new) network doesn't have peers yet.
//
// This method accepts a userAddress and chainID.
// AddChain implements ChainRegistry.
func (r *singletonChainRegistry) AddChain(
	userAddress string,
	chainID string,
) ChainRegistry {
	// Nothing to do if the ChainID is known.
	if r.HasChain(chainID) {
		return r
	}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	// Append to list of available ChainID
	r.ReplicatedChains = append(r.ReplicatedChains, chainID)
	r.ChainSeeds[chainID] = ""
	// r.SyncConfig[chainID] = config.DefaultStateSyncConfig()

	// Also append to user mapped ChainID
	if _, ok := r.userChains[userAddress]; !ok {
		r.userChains[userAddress] = []string{}
	}
	r.userChains[userAddress] = append(r.userChains[userAddress], chainID)

	// Sort networks slices alphabetically by ChainID
	sort.Strings(r.ReplicatedChains)
	sort.Strings(r.userChains[userAddress])
	return r
}

// HasChain returns true if the ChainID can be found
// HasChain implements ChainRegistry.
func (r *singletonChainRegistry) HasChain(chainID string) bool {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return slices.Contains(r.ReplicatedChains, chainID)
}

// GetChains returns a flattened slice of ChainIDs.
// Note that the ChainID slice is ordered in ascending alphabetical order.
// GetChains implements ChainRegistry.
func (r *singletonChainRegistry) GetChains() []string {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	return r.ReplicatedChains
}

// GetStateSyncConfig returns the required configuration for the state-sync
// service. The trusted period may be empty
// GetStateSyncConfig implements ChainRegistry.
func (r *singletonChainRegistry) GetStateSyncConfig(
	chainID string,
) (*config.StateSyncConfig, error) {
	// Contains the default minimal state-sync config
	// conf := config.DefaultStateSyncConfig()

	// if _, ok := r.SyncConfig[chainID]; !ok {
	// 	return conf, fmt.Errorf("could not find state-sync config for ChainID %s", chainID)
	// }

	// return r.SyncConfig[chainID], nil
	return config.DefaultStateSyncConfig(), nil
}

// GetSeeds returns a comma-separated list of seed nodes using the format `id@host:port`
// or returns an empty string and an error if no seeds can be found.
// GetSeeds implements ChainRegistry.
func (r *singletonChainRegistry) GetSeeds(
	chainID string,
) (string, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if _, ok := r.ChainSeeds[chainID]; ok {
		return r.ChainSeeds[chainID], nil
	}

	return "", fmt.Errorf("could not find seed nodes for ChainID %s", chainID)
}

// GetAddress returns the user address attached to a chainId or returns
// an empty string and an error if the ChainID cannot be found.
// GetAddress implements ChainRegistry.
func (r *singletonChainRegistry) GetAddress(
	chainID string,
) (string, error) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	if !slices.Contains(r.ReplicatedChains, chainID) {
		return "", fmt.Errorf("could not find a user address for ChainID %s", chainID)
	}

	for address, chains := range r.userChains {
		if slices.Contains(chains, chainID) {
			return address, nil
		}
	}

	return "", fmt.Errorf("could not find a user address for ChainID %s", chainID)
}

// FindChain searches for a ChainID and returns its index or -1.
//
// The *index of a node* generally represents the index of the ChainID as per
// the [GetChains] return value, e.g. given ChainIDs ['A','B','C'], the index
// of a node for the chain 'B', is 1 and the index for the chain 'A', is 0.
//
// Note that the ChainID slice is ordered in ascending alphabetical order.
// FindChain implements ChainRegistry.
func (r *singletonChainRegistry) FindChain(chainID string) (int, error) {
	chainIds := r.GetChains()
	for i, cid := range chainIds {
		if cid == chainID {
			return i, nil
		}
	}

	return -1, fmt.Errorf("could not find ChainID %s in replicated chains", chainID)
}

// ----------------------------------------------------------------------------
// Providers

// ChainRegistryProvider defines an interface to provide with an implementation
// that satisfies the ChainRegistry interface.
type ChainRegistryProvider func(*config.MultiplexConfig) (ChainRegistry, error)

// NewChainRegistry creates a chain registry using a [config.MultiplexConfig]
// configuration. It uses the UserChains field to create an [ExtendedChainID]
// per each pair of user address and ChainID.
//
// Note that [GetSyncConfigExtension] and [GetSeedConfigExtension] may be
// changed in `callbacks.go` to use a different configuration extension.
//
// It is safe to call the [client.InjectSyncConfig] extension and also
// the [client.InjectChainSeeds] extension because the caller is still
// preparing a [ChainRegistry] instance, as such, even if the extensions
// implement blocking processes, they won't impact *runtime* afterwards.
//
// This method implementation supports concurrent calls.
// NewChainRegistry implements ChainRegistryProvider.
func NewChainRegistry(
	conf *config.MultiplexConfig,
	genesisDocSetFile string,
) (ChainRegistry, error) {
	if conf.Strategy == DisableReplicationStrategy() {
		return &singletonChainRegistry{mtx: new(sync.Mutex)}, nil
	}

	// This type is used to return an error alongside the ChainRegistry if
	// necessary. Needed because `sync.OnceValue` must get a single value.
	type returnInstanceWithError struct {
		instance *singletonChainRegistry
		err      error
	}

	// This implementation is cacheable in that the execution is guaranteed
	// to be done only once. The returned ChainRegistry instance is the result
	// of parsing the [config.MultiplexConfig] configuration object.
	cacheableChainRegistry := sync.OnceValue(func() returnInstanceWithError {
		registry := &singletonChainRegistry{
			mtx: new(sync.Mutex),

			ChainSeeds:       map[string]string{},
			ReplicatedChains: []string{},

			userChains: map[string][]string{},
		}

		// Copy seed nodes and map to ChainID
		for chainID, seedNodes := range conf.ChainSeeds {
			registry.ChainSeeds[chainID] = seedNodes
		}

		// Copy user addresses and ChainIDs from config
		for userAddress, chainIds := range conf.UserChains {
			registry.ReplicatedChains = append(registry.ReplicatedChains, chainIds...)
			registry.userChains[userAddress] = make([]string, len(chainIds))

			copy(registry.userChains[userAddress], chainIds)

			// Sort user-indexed chains alphabetically by ChainID
			sort.Strings(registry.userChains[userAddress])
			registry.userChains[userAddress] = slices.Compact(registry.userChains[userAddress])
		}

		// Copy user addresses and ChainIDs from genesisDocs
		if len(genesisDocSetFile) > 0 && cmtos.FileExists(genesisDocSetFile) {
			chainsFromGenesis, err := LoadChainsFromGenesisFile(genesisDocSetFile)
			if err != nil {
				return returnInstanceWithError{instance: nil, err: err}
			}

			for userAddress, chainIds := range chainsFromGenesis {
				registry.ReplicatedChains = append(registry.ReplicatedChains, chainIds...)
				registry.userChains[userAddress] = make([]string, len(chainIds))

				copy(registry.userChains[userAddress], chainIds)

				// Sort user-indexed chains alphabetically by ChainID
				sort.Strings(registry.userChains[userAddress])
				registry.userChains[userAddress] = slices.Compact(registry.userChains[userAddress])
			}
		}

		// Sort ReplicatedChains alphabetically by ChainID and compact
		sort.Strings(registry.ReplicatedChains)
		registry.ReplicatedChains = slices.Compact(registry.ReplicatedChains)
		return returnInstanceWithError{instance: registry, err: nil}
	})

	// Create the chain registry or get its pointer
	regWithErr := cacheableChainRegistry()
	cRegistry := regWithErr.instance
	if regWithErr.err != nil {
		return &singletonChainRegistry{mtx: new(sync.Mutex)}, regWithErr.err
	}

	return cRegistry, nil
}

// LoadSeedsFromFile read a seeds.json configuration file and populates
// a mapping of seed nodes by unique ChainID.
//
// This method may be used to parse the content of a `seeds.json` configuration
// file as required to configure the state-sync partners of a replicated chain.
//
// The list of seed nodes is comma-separated and uses the format: id@host:port.
func LoadSeedsFromFile(file string) (map[string]string, error) {
	if len(file) == 0 || !cmtos.FileExists(file) {
		return map[string]string{}, fmt.Errorf("could not find seeds file: %s", file)
	}

	seedsBytes, err := os.ReadFile(file)
	if err != nil {
		return map[string]string{}, err
	}

	chainSeeds := map[string]string{}
	err = json.Unmarshal(seedsBytes, &chainSeeds)
	if err != nil {
		return map[string]string{}, err
	}

	// Drop empty seeds lists
	for chainID, seeds := range chainSeeds {
		if len(seeds) == 0 {
			delete(chainSeeds, chainID)
		}
	}

	return chainSeeds, nil
}

// LoadChainsFromGenesisFile reads a genesis.json configuration file and
// populates a mapping of ChainID values by user address.
//
// This method may be used to fill [ChainRegistry.userChains].
//
// CAUTION: Using this method, it is assumed that the `genesis.json` file
// contains a *set of genesis docs* as defined with [GenesisDocSet].
//
// IMPORTANT: This method requires the ChainID field to contain a user address
// of 20 bytes in hexadecimal format and an arbitrary fingerprint of 8 bytes.
// e.g.: `mx-chain-FF080888BE0F48DE88927C3F49215B96548273AB-3E547E3280313019`.
func LoadChainsFromGenesisFile(genFile string) (map[string][]string, error) {
	type returnEmptyMap map[string][]string

	if !cmtos.FileExists(genFile) {
		return returnEmptyMap{}, fmt.Errorf("genesis file does not exist: %s", genFile)
	}

	// CAUTION: The genesis file is expected to contain a set of genesis docs
	genesisDocSet, err := GenesisDocSetFromFile(genFile)
	if err != nil {
		return returnEmptyMap{}, fmt.Errorf("error unmarshalling GenesisDocSet: %s", err.Error())
	}

	numReplicatedChains := len(genesisDocSet)

	// Read the replicated chains genesis docs to find a list of ChainID by user address.
	userChains := make(map[string][]string, numReplicatedChains)
	for _, userGenDoc := range genesisDocSet {
		extChainID, err := NewExtendedChainIDFromLegacy(userGenDoc.ChainID)
		if err != nil {
			return returnEmptyMap{}, fmt.Errorf("error parsing ChainID fields: %s", err.Error())
		}

		// Extracts user address from ChainID
		userAddress := extChainID.GetUserAddress()

		if _, ok := userChains[userAddress]; !ok {
			userChains[userAddress] = []string{}
		}

		// Stores user-indexed ChainID
		userChains[userAddress] = append(userChains[userAddress], userGenDoc.ChainID)
	}

	return userChains, nil
}
