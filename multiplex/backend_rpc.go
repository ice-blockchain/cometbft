package multiplex

import (
	"errors"
	"fmt"

	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	"github.com/ice-blockchain/cometbft/node"
	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
)

// ----------------------------------------------------------------------------
// mxrpc.Backend API implementation

// GetRelayID returns the node ID assigned in the reactor.
func (b *MultiplexBackend) GetRelayID() p2p.ID {
	return b.reactor.GetNodeKey().ID()
}

// GetListenAddress should return the relay's listen address.
func (b *MultiplexBackend) GetListenAddress() string {
	return b.relayAddr.String()
}

// GetDiscoveryPort returns the port used for discovery,
// i.e. it should map to the relay's discovery port.
func (b *MultiplexBackend) GetDiscoveryPort() uint16 {
	return b.relayAddr.Port
}

// GetNetworks should return a slice of supported ChainID values.
func (b *MultiplexBackend) GetNetworks() []string {
	return b.runtimeRegistry.Networks()
}

// GetValidatorPubs returns all validator pubkeys available per ChainID.
func (b *MultiplexBackend) GetValidatorPubs() map[string]string {
	b.mtx.Lock()
	defer b.mtx.Unlock()

	validators := b.runtimeRegistry.Validators()
	validatorPubs := make(map[string]string, len(validators))
	for chainID, privValidator := range validators {
		// Make sure we can access the priv validator
		privValPubKey, err := privValidator.GetPubKey()
		if err != nil {
			b.logger.Error("failed to read public key from validator", "err", err)
			continue
		}

		validatorPubs[chainID] = pubKeyToHex(privValPubKey)
	}
	return validatorPubs
}

// EnableNewRuntimeRPC adds RPC routes for networks in a running
// http request multiplexer.
func (b *MultiplexBackend) EnableNewRuntimeRPC(networks []string) error {
	b.mtx.Lock()
	rpcMultiplexer := b.rpcMultiplexer
	b.mtx.Unlock()
	if rpcMultiplexer == nil {
		return errors.New(
			"failed to enable RPC runtime, missing multiplexer")
	}

	// We configure one RPC environment per running network,
	// i.e. contains reactors, stores and genesis.
	chainRoutes := map[string]rpccore.RoutesMap{}
	for _, chainID := range networks {
		nodeRuntime, ok := b.resourceMgr.Get(chainID,
			types.ServiceKeyNodeRuntime,
		).(*node.Node)
		if !ok {
			return fmt.Errorf(
				"could not get node runtime in EnableNewRuntimeRPC with ChainID %s", chainID)
		}

		env, err := nodeRuntime.ConfigureRPC()
		if err != nil {
			return fmt.Errorf(
				"could not create RPC environment with ChainID %s: %w", chainID, err)
		}

		nodeRoutes := env.GetRoutes()
		if nodeCfg.RPC.Unsafe {
			env.AddUnsafeRoutes(nodeRoutes)
		}

		chainRoutes[chainID] = nodeRoutes
	}

	// Each network's ChainID is appended to the route name.
	// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
	newRoutes := rpccore.RoutesMap{}
	for chainID, nodeRoutes := range chainRoutes {
		for route, rpcFunc := range nodeRoutes {
			routePath := route + "/" + chainID

			b.mtx.Lock()
			_, hasRoute := b.knownRPCRoutes[routePath]
			b.mtx.Unlock()

			if hasRoute {
				continue
			}

			newRoutes[routePath] = rpcFunc

			b.mtx.Lock()
			reactor.knownRPCRoutes[routePath] = true
			b.mtx.Unlock()
		}
	}

	b.mtx.Lock()
	defer b.mtx.Unlock()

	rpcserver.RegisterAddedRPCFuncs(
		rpcMultiplexer,
		newRoutes,
		b.logger.With("module", "rpc-server"),
	)

	return nil
}
