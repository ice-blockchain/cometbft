package multiplex

import (
	"fmt"

	"github.com/cometbft/cometbft/internal/evidence"
	mempl "github.com/cometbft/cometbft/mempool"
)

type MultiplexServiceRegistry struct {
	WaitStateSync MultiplexFlag
	WaitBlockSync MultiplexFlag

	// stores all multiplex resources in memory
	// TODO: benchmark and verify memory footprint
	PrivValidators  MultiplexPrivValidator
	AddressBooks    MultiplexAddressBook
	ChainStates     MultiplexState
	ConsensusStates MultiplexConsensus
	StateStores     MultiplexStateStore
	BlockStores     MultiplexBlockStore
	IndexerServices MultiplexIndexerService
	AppConns        MultiplexAppConn
	EventBuses      MultiplexEventBus
	EventSwitches   MultiplexSwitch
	Pruners         MultiplexPruner

	Transports      MultiplexP2PTransport
	PeerFilters     MultiplexPeerFilterFunc
	ListenAddresses MultiplexServiceAddress
}

func (mx *MultiplexServiceRegistry) getServices(userScopeHash string) (*NodeServices, error) {

	// This long conditions block validates that the service registry
	// contains all the *required* multiplex entries corresponding to
	// the userScopeHash that is requested.
	//
	// TODO:
	// This can be extracted to a function `Validate()`

	if _, ok := mx.PrivValidators[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find priv validator with scope hash %s", userScopeHash)
	}

	if _, ok := mx.AddressBooks[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find address book with scope hash %s", userScopeHash)
	}

	if _, ok := mx.ChainStates[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state with scope hash %s", userScopeHash)
	}

	if _, ok := mx.ConsensusStates[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find consensus state with scope hash %s", userScopeHash)
	}

	if _, ok := mx.StateStores[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state store with scope hash %s", userScopeHash)
	}

	if _, ok := mx.BlockStores[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find state store with scope hash %s", userScopeHash)
	}

	if _, ok := mx.IndexerServices[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find indexer services with scope hash %s", userScopeHash)
	}

	if _, ok := mx.AppConns[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find proxy app conns with scope hash %s", userScopeHash)
	}

	if _, ok := mx.EventBuses[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find event bus with scope hash %s", userScopeHash)
	}

	if _, ok := mx.EventSwitches[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find event switch with scope hash %s", userScopeHash)
	}

	if _, ok := mx.Pruners[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find pruner with scope hash %s", userScopeHash)
	}

	if _, ok := mx.Transports[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find p2p transport with scope hash %s", userScopeHash)
	}

	if _, ok := mx.PeerFilters[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find peer filter func with scope hash %s", userScopeHash)
	}

	if _, ok := mx.WaitStateSync[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find statesync status with scope hash %s", userScopeHash)
	}

	if _, ok := mx.WaitBlockSync[userScopeHash]; !ok {
		return nil, fmt.Errorf("could not find blocksync status with scope hash %s", userScopeHash)
	}

	// We'll use the event switch to get reactor pointers
	sw := mx.EventSwitches[userScopeHash]

	privValidator := mx.PrivValidators[userScopeHash]
	addrBook := mx.AddressBooks[userScopeHash]
	state := mx.ChainStates[userScopeHash]
	consensusState := mx.ConsensusStates[userScopeHash]
	stateStore := mx.StateStores[userScopeHash]
	blockStore := mx.BlockStores[userScopeHash]
	indexerService := mx.IndexerServices[userScopeHash]
	proxyApp := mx.AppConns[userScopeHash]
	eventBus := mx.EventBuses[userScopeHash]
	pruner := mx.Pruners[userScopeHash]
	transport := mx.Transports[userScopeHash]
	peerFilters := mx.PeerFilters[userScopeHash]

	// boolean flags
	stateSync := mx.WaitStateSync[userScopeHash]
	blockSync := mx.WaitBlockSync[userScopeHash]

	// Mempool and evidence pool read from reactors
	mempool := (sw.Reactor("MEMPOOL").(*mempl.Reactor)).GetMempoolPtr()
	evpool := (sw.Reactor("EVIDENCE").(*evidence.Reactor)).GetPoolPtr()

	return &NodeServices{
		sw:             sw.Switch,
		privValidator:  privValidator,
		addrBook:       addrBook,
		state:          &state.State,
		consensusState: consensusState,
		stateStore:     stateStore,
		blockStore:     blockStore.BlockStore,
		indexerService: indexerService,
		proxyApp:       proxyApp,
		eventBus:       eventBus,
		mempool:        mempool,
		evidencePool:   evpool,
		pruner:         pruner,
		transport:      transport,
		peerFilters:    peerFilters,
		stateSync:      stateSync,
		blockSync:      blockSync,
	}, nil
}
