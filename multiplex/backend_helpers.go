package multiplex

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	mxrpc "github.com/ice-blockchain/cometbft/multiplex/rpc"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ----------------------------------------------------------------------------
// types.RelayHelpers API implementation

// GetLocalNetworkHeights returns a list of networks that must be created.
// That list is always a subset of the list of required networks.
func (b *MultiplexBackend) GetLocalNetworkHeights(
	userAddress string,
	transactions ...client.Transaction,
) (requiredNetworks []string, mustCreateNetworks []string) {
	requiredNetworks = []string{}
	mustCreateNetworks = []string{}

	uniqueNetworks := map[string]bool{}
	unknownNetworks := map[string]bool{}
	for _, tx := range transactions {
		chainID := client.GetChainID(userAddress, tx.Fingerprint)
		uniqueNetworks[chainID] = true

		// If we don't know this network, we either need a background-sync
		// or we must create a new network if other relays also don't know it.
		if !b.HasNetwork(chainID) {
			unknownNetworks[chainID] = true
		}
	}

	// Returns as a slice of unique ChainIDs
	for unknownChainID := range unknownNetworks {
		mustCreateNetworks = append(mustCreateNetworks, unknownChainID)
	}

	for requiredChainID := range uniqueNetworks {
		requiredNetworks = append(requiredNetworks, requiredChainID)
	}

	return requiredNetworks, mustCreateNetworks
}

// GetLocalNetworkValidators initializes validators for networks and returns a map
// of public keys per ChainID.
func (b *MultiplexBackend) GetLocalNetworkValidators(
	networks []string,
) (pubKeysPerChainID map[string]string, err error) {
	myValidatorPubKeys := b.GetValidatorPubs()
	pubKeysPerChainID = make(map[string]string, len(networks))
	for _, chainID := range networks {
		// If GetValidatorPubs() already has this ChainID
		if pubKey, ok := myValidatorPubKeys[chainID]; ok {
			pubKeysPerChainID[chainID] = pubKey
			continue
		}

		// Otherwise, pre-allocates priv validator instance.
		if err = b.runtimeRegistry.InitRuntime(chainID, []string{}); err != nil {
			b.logger.Error("Failed to allocate new priv validator",
				// TODO(midas): requestId not available here.
				"chainId", chainID,
				"err", err,
			)
			return
		}

		// Retrieve pre-allocated resources for priv validator
		privValidator := b.runtimeRegistry.Composer().Validator(chainID)

		// Make sure we can access the priv validator
		var privValPubKey crypto.PubKey
		if privValPubKey, err = privValidator.GetPubKey(); err != nil {
			b.logger.Error("Failed to read public key from validator", "err", err)
			return
		}

		pubKeysPerChainID[chainID] = fmt.Sprintf("%X", privValPubKey.Bytes())
	}

	return // pubKeysPerChainID, nil
}

// GetValidatorsByNetwork maps each supported network to a slice of validator
// public keys in string (hex) format.
//
// This method uses [GetRemoteValidatorsInfo] to orchestrate the InitValidators
// call if necessary. Given a correct response, we fill a map where keys contain
// ChainID and values are slices of validator public keys.
//
// The relayAddresses parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
func (b *MultiplexBackend) GetValidatorsByNetwork(
	clientCtx context.Context,
	relayAddresses []*helpers.RelayAddress,
	requiredNetworks []string,
) (validatorsByChain map[string][]string, err error) {
	validatorsByChain = make(map[string][]string, len(requiredNetworks))

	// This method should block until it processed all relays' validators.
	var valsWg sync.WaitGroup
	valsWg.Add(len(relayAddresses))

	validatorsCh := make(chan struct {
		start  time.Time
		result *mxrpc.RPCResultInitValidators
		addr   *helpers.RelayAddress
	}, len(relayAddresses))

	// Connect to all other relays using RPC (discovery server) to find
	// out their validator public key for requiredNetworks.
	for _, relAddr := range relayAddresses {
		// Open ephemeral goroutines to request RelayInfo RPC from all relays.
		go func(relayAddr *helpers.RelayAddress) {
			defer valsWg.Done()
			startTz := time.Now()
			addrRPC := relayAddr.AddressForRelayInfo()

			// Discover this relay's validator public key.
			// This executes a RPC request for InitValidators.
			result, valsErr := b.GetRemoteValidatorsInfo(clientCtx,
				relayAddr, // expects DiscoveryPort
				requiredNetworks,
			)
			if valsErr != nil {
				b.logger.Error("Error discovering relay validator public keys",
					"relay", addrRPC,
					"err", valsErr,
				)
				return
			}

			validatorsCh <- struct {
				start  time.Time
				result *mxrpc.RPCResultInitValidators
				addr   *helpers.RelayAddress
			}{result: result, addr: relayAddr, start: startTz}
		}(relAddr)
	}

	// Block this process until all relays have responded or timed out.
	valsWg.Wait()
	close(validatorsCh) // No more responses/timeouts expected.

	for result := range validatorsCh {
		durationMs := time.Since(result.start).Milliseconds()
		relayAddr := result.addr

		// Every relay may return one validator public key per requiredNetworks.
		for chainID, validatorPubKey := range result.result.ValidatorPubs {
			if _, ok := validatorsByChain[chainID]; !ok {
				validatorsByChain[chainID] = make([]string, 0, len(relayAddresses))
			}

			validatorsByChain[chainID] = append(validatorsByChain[chainID], validatorPubKey)
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Retrieved validators information from relay",
			"relay", relayAddr,
			"validators", result.result.ValidatorPubs,
			"time", strconv.Itoa(int(durationMs))+"ms",
		)
	}

	return // validatorsByChain, nil
}

// GetRelaysByNetwork maps each supported network to a slice of relay addresses
// and it also returns a slice of relays that produced errors, e.g. network error.
//
// This method uses [GetRemoteRelayInfo] to find the relay's ID. Given a correct
// response, we use the [RPCResultRelayInfo] to fill the [RelayAddress#ID] and
// the full address (with ID) is added to healthyRelays.
//
// The relayAddresses parameter should use `DiscoveryPort` as this method
// will map it to its corresponding RelayInfo port (`DiscoveryPort - 1`).
func (b *MultiplexBackend) GetRelaysByNetwork(
	clientCtx context.Context,
	relayAddresses []*helpers.RelayAddress,
) (
	healthyRelays []*helpers.RelayAddress,
	chainRelays map[string][]*helpers.RelayAddress,
	errorRelays []string,
) {
	healthyRelays = make([]*helpers.RelayAddress, 0, len(relayAddresses))
	chainRelays = map[string][]*helpers.RelayAddress{}
	errorRelays = []string{}

	relaysWithFailure := map[string]bool{}

	// This method should block until it processed all relays' responses.
	var wg sync.WaitGroup
	wg.Add(len(relayAddresses))

	// We will call RelayInfo concurrently on every relay. A nil result
	// means that the relay did not respond (in time) and is unhealthy.
	relayInfoCh := make(chan struct {
		start  time.Time
		result *mxrpc.RPCResultRelayInfo
		addr   *helpers.RelayAddress
	}, len(relayAddresses))

	// Connect to all other relays using RPC (discovery server) to find
	// out their relay ID (CometBFT Node ID) before we can connect with P2P.
	for _, relAddr := range relayAddresses {
		// Open ephemeral goroutines to request RelayInfo RPC from all relays.
		go func(relayAddr *helpers.RelayAddress) {
			defer wg.Done()
			startTz := time.Now()
			addrRPC := relayAddr.AddressForRelayInfo()

			// Discover this relay's ID (CometBFT Node ID).
			// This executes a RPC request for RelayInfo.
			result, err := b.GetRemoteRelayInfo(clientCtx, relayAddr) // expects DiscoveryPort
			if err != nil {
				b.logger.Error("Error discovering relay information",
					"relay", addrRPC,
					"err", err,
				)
				relayInfoCh <- struct {
					start  time.Time
					result *mxrpc.RPCResultRelayInfo
					addr   *helpers.RelayAddress
				}{result: nil, addr: relayAddr, start: startTz}
				return
			}

			relayInfoCh <- struct {
				start  time.Time
				result *mxrpc.RPCResultRelayInfo
				addr   *helpers.RelayAddress
			}{result: result, addr: relayAddr, start: startTz}
		}(relAddr)
	}

	// Block this process until all relays have responded or timed out.
	wg.Wait()
	close(relayInfoCh) // No more responses/timeouts expected.

	// Now process responses from relays and extend RelayAddress instances
	// to contain the CometBFT Node ID when the relay responded correctly.
	for result := range relayInfoCh {
		relayAddr := result.addr
		if result.result == nil {
			// This error case is logged in above for-loop.
			relaysWithFailure[relayAddr.String()] = true
			continue
		}
		durationMs := time.Since(result.start).Milliseconds()

		// TODO(midas): remove debug logs
		b.logger.Debug("Retrieved networks information from relay",
			"relay", relayAddr,
			"node_id", result.result.DefaultNodeID,
			"networks", result.result.Networks,
			"laddr", result.result.ListenAddress,
			"dport", strconv.FormatUint(uint64(result.result.DiscoveryPort), 10),
			"time", strconv.Itoa(int(durationMs))+"ms",
		)

		// Fill the CometBFT Node ID
		relayAddr.SetID(result.result.DefaultNodeID)
		healthyRelays = append(healthyRelays, relayAddr)

		// Save the RelayInfo result as we need it for dialing.
		b.reactor.SaveRelayInfo(result.result)

		// Also, populate a map of relay addresses by ChainID.
		for _, chainID := range result.result.Networks {
			if _, ok := chainRelays[chainID]; !ok {
				chainRelays[chainID] = []*helpers.RelayAddress{}
			}

			chainRelays[chainID] = append(chainRelays[chainID], relayAddr)
		}
	}

	// Populate a slice of unique relay addresses which produced errors
	for errRelay, _ := range relaysWithFailure {
		if !slices.Contains(errorRelays, errRelay) {
			errorRelays = append(errorRelays, errRelay)
		}
	}

	return healthyRelays, chainRelays, errorRelays
}

// CheckDialCompatibleRelay dials the relay to perform a handshake and
// determine whether relayAddress is compatible.
//
// CheckDialCompatibleRelay implements [types.Backend].
func (b *MultiplexBackend) CheckDialCompatibleRelay(
	_ context.Context,
	relayAddr *helpers.RelayAddress,
) error {
	// If this is us, nothing to do.
	if relayAddr.ID() == b.nodeKey.ID() {
		return nil
	}

	// (1)
	// Dial the relay to find out whether it is compatible (handshake).

	// TODO(midas): remove debug logs
	b.logger.Debug("Process now dialing remote relay (discovery)",
		"relay", relayAddr.String(),
	)

	netAddress := relayAddr.NetAddress()
	_, err := b.discoveryPool.Connector().Dial(netAddress)
	return err
}

// ----------------------------------------------------------------------------
// types.BroadcastHelpers API implementation

// ApplyFilterAckTransactionRelayIds is a RelayID filter function which
// fills a relevantRelays slice that contains only relay IDs that must
// be waited for during the AckTransaction process. In case a relay ID does not
// appear in the resulting slice, it means that they must first handle a
// replication and we should not wait for their tx acknowledgement.
//
// TODO(midas): TBI if more than 2/3 relays must replicate. Transactions won't
// broadcast because of dependency on successfull remote replications by too
// many relays. Note that this is an edge case and the current response of this
// implementation to those conditions is to *deny the transaction broadcast*,
// because too many healthy (required) relays are failing (not syncd).
func (b *MultiplexBackend) ApplyFilterAckTransactionRelayIds(
	chainRelays map[string][]*helpers.RelayAddress,
	catchupRelays map[string][]*helpers.RelayAddress,
) []string {
	relevantRelays := []string{}
	// Any healthy relay should be waited for initially.
	for _, relaysForChain := range chainRelays {
		healthyRelayIds := func() (relayIds []string) {
			relayIds = make([]string, 0, len(relaysForChain))
			for _, relayAddr := range relaysForChain {
				relayIds = append(relayIds, string(relayAddr.ID()))
			}
			return relayIds
		}()
		relevantRelays = slices.DeleteFunc(healthyRelayIds, func(relayId string) bool {
			return len(relayId) == 0 || relayId == string(b.GetRelayID())
		})
	}

	// ... but make sure that if only some of them have to replicate, and others
	// don't have to replicate, we won't wait for the relays that need replication.
	for _, catchupForChain := range catchupRelays {
		catchupRelayIds := func() (relayIds []string) {
			relayIds = make([]string, 0, len(catchupForChain))
			for _, relayAddr := range catchupForChain {
				relayIds = append(relayIds, string(relayAddr.ID()))
			}
			return relayIds
		}()
		if len(relevantRelays) > 0 {
			// Some have chain, some don't. The ones that are missing it
			// will replicate, but we shouldn't be waiting for them.
			relevantRelays = slices.DeleteFunc(relevantRelays, func(relayId string) bool {
				return slices.Contains(catchupRelayIds, relayId)
			})
		}
	}

	return removeDuplicates(relevantRelays)
}

// ApplyFilterReplRequestRelays filters relays and returns a map of relays
// by ChainID which contains only relays that need to catchup, i.e. it returns
// relays that receive a chain replication request.
func (b *MultiplexBackend) ApplyFilterReplRequestRelays(
	requiredNetworks []string,
	relays []*helpers.RelayAddress,
	chainRelays map[string][]*helpers.RelayAddress,
) map[string][]*helpers.RelayAddress {
	// Makes sure to avoid mistakenly including self.
	relaysWithoutSelf := []*helpers.RelayAddress{}
	for _, relayAddr := range relays {
		if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
			relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
		}
	}

	catchupRelays := map[string][]*helpers.RelayAddress{}
	for chainID, relaysByChain := range chainRelays {
		// Did all relays report to know this ChainID?
		if len(relaysByChain) >= len(relaysWithoutSelf) {
			catchupRelays[chainID] = nil
			continue
		}

		// Build a (searchable) slice of relay IDs
		relayIdsByChain := []string{}
		for _, relayAddr := range relaysByChain {
			if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
				relayIdsByChain = append(relayIdsByChain, string(relayAddr.ID()))
			}
		}

		// Find out which relays are missing for this chain.
		// Those are relays that need to catchup with the chain.
		for _, relayAddr := range relaysWithoutSelf {
			if !slices.Contains(relayIdsByChain, string(relayAddr.ID())) {
				catchupRelays[chainID] = append(catchupRelays[chainID], relayAddr)
			}
		}
	}

	// Handling case when chainRelays is empty (0 networks on remote relays).
	for _, chainID := range requiredNetworks {
		if _, has := catchupRelays[chainID]; !has {
			catchupRelays[chainID] = append(catchupRelays[chainID], relaysWithoutSelf...)
		}

		// Reset the sent requests cache for required networks
		// TODO(midas): It is preferrable to move this registry over to the Reactor.
		b.replRequestsMtx.Lock()
		b.replRequestsSent[chainID] = []string{}
		b.replRequestsMtx.Unlock()
	}

	return catchupRelays
}


WaitForRelaysAckChainReplications(
	ctx context.Context,
	catchupRelays map[string][]*helpers.RelayAddress,
	transactions ...client.Transaction,
) (
	relaysPerChain map[string][]string,
	numExpected int,
	numReceived int,
	err error,
)
