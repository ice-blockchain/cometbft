package multiplex

import (
	"context"
	"errors"
	"fmt"
	"time"

	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcclient "github.com/ice-blockchain/cometbft/rpc/jsonrpc/client"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	mxrpc "github.com/ice-blockchain/cometbft/multiplex/rpc"
	"github.com/ice-blockchain/cometbft/multiplex/types"
	"github.com/ice-blockchain/cometbft/node"
)

// ----------------------------------------------------------------------------
// mxrpc.Backend API implementation

// GetRelayID returns the node ID assigned in the reactor.
func (b *MultiplexBackend) GetRelayID() p2p.ID {
	return b.nodeKey.ID()
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

// ----------------------------------------------------------------------------
// mxrpc.Client API implementation

// GetRemoteValidatorsInfo connects to relayAddress using a JSONRPC client,
// and calls the InitValidators (/validators) remote procedure to retrieve
// public keys.
//
// The relayAddress parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
func (b *MultiplexBackend) GetRemoteValidatorsInfo(
	clientCtx context.Context,
	relayAddress *helpers.RelayAddress,
	requiredNetworks []string,
) (*mxrpc.RPCResultInitValidators, error) {
	jsonrpc, err := b.connectToRemoteRelayInfoRPC(relayAddress)
	if err != nil {
		return nil, err
	}

	// Setup deadline using DefaultRequestTimeout and new context.
	deadline := time.Now().Add(DefaultRequestTimeout)
	timeoutCtx, cancelFn := context.WithDeadline(context.Background(), deadline)
	defer cancelFn()

	// Calls /validators RPC with networks parameter.
	result := &mxrpc.RPCResultInitValidators{}
	params := map[string]any{
		"networks": requiredNetworks,
	}
	_, callErr := jsonrpc.Call(timeoutCtx, "validators", params, result)

	// As the above call is synchronous, we may interpret the context
	// expirations to find out if request was cancelled or timed out.
	select {
	// cancelled by caller
	case <-clientCtx.Done():
		cancelledErr := fmt.Errorf(
			"GetRemoteValidatorsInfo cancelled for %s", relayAddress.AddressForRelayInfo())
		b.logger.Error(cancelledErr.Error())
		return nil, cancelledErr
	// context timeout (request took too long)
	case <-timeoutCtx.Done():
		timeoutErr := fmt.Errorf(
			"GetRemoteValidatorsInfo timed out for %s", relayAddress.AddressForRelayInfo())
		b.logger.Error(timeoutErr.Error())
		return nil, timeoutErr
	default:
	}

	if callErr != nil {
		return nil, callErr
	}

	b.mtx.Lock()
	b.httpClients = append(b.httpClients, httpClient)
	b.mtx.Unlock()

	return result, nil
}

// GetRemoteRelayInfo connects to relayAddress using a JSONRPC client,
// and calls the GetRelayInfo (/info) remote procedure to retrieve the
// Relay ID, the supported networks and the listen address for this relay.
//
// The relayAddress parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
func (b *MultiplexBackend) GetRemoteRelayInfo(
	clientCtx context.Context,
	relayAddress *helpers.RelayAddress,
) (*mxrpc.RPCResultRelayInfo, error) {
	jsonrpc, err := b.connectToRemoteRelayInfoRPC(relayAddress)
	if err != nil {
		return nil, err
	}

	// Setup deadline using DefaultRequestTimeout and new context.
	deadline := time.Now().Add(DefaultRequestTimeout)
	timeoutCtx, cancelFn := context.WithDeadline(context.Background(), deadline)
	defer cancelFn()

	// Calls /info RPC with empty parameters.
	result := &mxrpc.RPCResultRelayInfo{}
	params := map[string]any{}
	_, callErr := jsonrpc.Call(timeoutCtx, "info", params, result)

	// As the above call is synchronous, we may interpret the context
	// expirations to find out if request was cancelled or timed out.
	select {
	// cancelled by caller
	case <-clientCtx.Done():
		cancelledErr := fmt.Errorf(
			"RelayInfo cancelled for %s", relayAddress.AddressForRelayInfo())
		b.logger.Error(cancelledErr.Error())
		return nil, cancelledErr
	// context timeout (request took too long)
	case <-timeoutCtx.Done():
		timeoutErr := fmt.Errorf(
			"RelayInfo timed out for %s", relayAddress.AddressForRelayInfo())
		b.logger.Error(timeoutErr.Error())
		return nil, timeoutErr
	default:
	}

	if callErr != nil {
		return nil, callErr
	}

	b.mtx.Lock()
	b.httpClients = append(b.httpClients, httpClient)
	b.mtx.Unlock()

	return result, nil
}

// GetRemoteDiscoveryAddress calls the RelayInfo remote procedure for sourcePeer
// to determine its' discovery address and networks information.
func (b *MultiplexBackend) GetRemoteDiscoveryAddress(
	clientCtx context.Context,
	relayAddress *helpers.RelayAddress,
) (*helpers.RelayAddress, error) {
	// Note that publicAddr may contain a secret connection port and must
	// not be used as the DiscoveryPort to determine CometBFT ports.
	publicAddr := relayAddress.NetAddress()

	// CAUTION: do not use as `DiscoveryPort`, may contain secret conn port.
	sourceAddr, err := helpers.NewRelayAddress(publicAddr.String())
	if err != nil {
		return nil, fmt.Errorf(
			"invalid relay address %s: %w", publicAddr.String(), err)
	}

	// Check if we have a RelayInfo and already know this peer by ID.
	b.mtx.Lock()
	partnerRelayInfo, hasRelayInfo := b.knownRelayInfo[string(sourceAddr.ID())]
	b.mtx.Unlock()

	var discoveryPort uint16

	// We may first need to call the RelayInfo RPC, to find DiscoveryPort.
	if !hasRelayInfo {
		relayInfoStr := sourceAddr.AddressForRelayInfo()
		rpcAddr, _ := helpers.NewRelayAddress(relayInfoStr) // DiscoveryPort-1
		relayInfo, infoErr := b.GetRemoteRelayInfo(
			context.TODO(),
			rpcAddr,
		)
		if infoErr != nil {
			return nil, infoErr
		}

		b.mtx.Lock()
		b.knownRelayInfo[string(sourceAddr.ID())] = relayInfo
		b.mtx.Unlock()

		discoveryPort = relayInfo.DiscoveryPort
	} else {
		discoveryPort = partnerRelayInfo.DiscoveryPort
	}

	// Now we know which port is the discovery port on this relay.
	sourceAddr.SetPort(discoveryPort)

	// We can now safely use sourceAddr as it contains `DiscoveryPort` of the relay.
	discoveryAddr, err := helpers.NewRelayAddress(sourceAddr.String())
	if err != nil {
		return nil, fmt.Errorf(
			"invalid discovery relay address %s: %w", publicAddr.String(), err)
	}

	return discoveryAddr, nil
}

// ----------------------------------------------------------------------------

// connectToRemoteRelayInfoRPC connects to a relayAddress' address for RelayInfo,
// if a JSONRPC client is already available for this address, we re-use it.
func (b *MultiplexBackend) connectToRemoteRelayInfoRPC(
	relayAddress *helpers.RelayAddress,
) (*rpcclient.Client, error) {
	rpcAddress := relayAddress.AddressForRelayInfo()
	if c, ok := b.jsonRpcClients[rpcAddress]; ok {
		return c, nil
	}

	c, err := rpcclient.New(relayAddress.AddressForRelayInfo())
	if err != nil {
		return nil, err
	}

	b.mtx.Lock()
	b.jsonRpcClients[rpcAddress] = c
	b.mtx.Unlock()

	return c, nil
}
