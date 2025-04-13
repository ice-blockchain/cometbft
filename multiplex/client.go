package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"
	"strings"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

// MultiplexClient implements a multiplex client by providing an implementation
// for methods as defined in [client.Client].
//
// Note, an [client.Acceptor] implementation may be provided in [NewClient].
type MultiplexClient struct {
	// A backend adapter instance is used to delegate communication
	// with other relays and to execute local operations, e.g. persist data.
	backend server.Backend
}

// Assert that our implementation satisfy the Client interface.
var _ client.Client = (*MultiplexClient)(nil)

// WithBackend is an option helper to overwrite the default adapter instance.
func WithBackend(a server.Backend) func(*MultiplexClient) {
	return func(c *MultiplexClient) {
		c.backend = a
	}
}

// NewClient initializes a new [MultiplexClient].
func NewClient(
	options ...func(*MultiplexClient),
) *MultiplexClient {
	cli := &MultiplexClient{}

	// Enable overwrite of optional properties
	for _, option := range options {
		option(cli)
	}

	return cli
}

// GetBackend returns the backend [server.Backend] implementation.
func (c MultiplexClient) GetBackend() server.Backend {
	return c.backend
}

// SetBackend overwrite the backend [server.Backend] instance.
func (c MultiplexClient) SetBackend(a server.Backend) {
	c.backend = a
}

// BroadcastTx sends an error to a notifier if any of the transactions
// fails basic verification, or if we fail to get a majority approval
// for the broadcast operation from healthy relays.
//
// IMPORTANT:
// It accepts a slice of relays which should be in the format `host:port`.
// The relays should contain the port associated with the P2P discovery.
// i.e. with MultiplexConfig.DiscoveryPort=1000, it should contain `:1000`.
//
// The following steps define a complete broadcast process, in this order:
//
// - Basic verifications and opening relay discovery connections.
// - New networks must be initialized explicitly.
// - Wait for networks to be ready before adding to mempool.
// - Send replication requests for new chains to other relays.
// - Add transactions to mempool, trigger broadcast to relays.
// - Wait for remote (other relays) transaction acceptance.
// - Transactions are now broadcast and accepted by all relays,
//
// i.e. The transaction broadcast happens only when consensus succeeded.
func (c MultiplexClient) BroadcastTx(
	ctx context.Context,
	userAddress string,
	relays []string,
	notifyCh chan<- client.BroadcastStatus,
	transactions ...client.Transaction,
) {
	// Can't broadcast without a multiplex backend
	if c.GetBackend() == nil {
		client.Error(notifyCh, errors.New(
			"could not find a running multiplex backend"))
		return // STOP here
	}

	// Format/parse relay addresses to validate each and permit
	// working with the static multi-port convention for multiplex.
	//
	// IMPORTANT: addresses contains the port associated with P2P discovery.
	addresses := make([]*server.RelayAddress, len(relays))
	for i, relay := range relays {
		relayAddr, err := server.NewRelayAddress(relay)
		if err != nil {
			notifyCh <- client.BroadcastStatus{Error: err}
			return // STOP here
		}

		addresses[i] = relayAddr
	}

	currentBroadcastStep := uint16(1)
	acceptedTxHashes := make([][]byte, 0, len(transactions))
	minHealthyRelays := (len(relays) / 2) + 1
	maxFailingRelays := len(relays) - minHealthyRelays - 1

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting consensus instance",
		"address", userAddress,
		"num_relays", len(relays),
		"min_healthy", minHealthyRelays,
		"max_failing", maxFailingRelays,
		"num_txes", len(transactions),
	)

	// ------------------------------------------------------------------------
	// Step 1: Basic verifications and opening relay connections

	// Determine necessary ChainIDs and current heights using state machines.
	// The second return value is a subset of the first return value.
	networksLocalHeights,
		mustCreateNetworks := c.GetBackend().GetLocalNetworkHeights(
		userAddress,
		transactions...,
	)

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Found network heights locally",
		"num_local", len(networksLocalHeights),
		"num_unknown", len(mustCreateNetworks))

	// Build a slice of unique ChainID values.
	requiredNetworks := []string{}
	for chainID := range networksLocalHeights {
		requiredNetworks = append(requiredNetworks, chainID)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Fetching exact relay addresses",
		"num_relays", len(addresses))

	// Determine relay IDs (CometBFT Node ID) and supported networks of each
	// of the relays and identify potential unhealthy relays.
	chainRelays, errorRelays := c.GetBackend().GetRelaysByNetwork(addresses)

	relaysWithoutSelf := []*server.RelayAddress{}
	for _, relayAddr := range addresses {
		if relayAddr.ID() != c.GetBackend().GetRelayID() {
			relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
		}
	}

	numHealthyRelays := len(relaysWithoutSelf) - len(errorRelays)

	// Next, we dial remote relays to find out about any incompatibility
	// before counting the number of failing relays.
	for _, relayAddr := range relaysWithoutSelf {
		if slices.Contains(errorRelays, string(relayAddr.ID())) {
			continue
		}

		// Uses the local P2P switch to dial a remote peer.
		if err := c.GetBackend().CheckDialCompatibleRelay(relayAddr); err != nil {
			errorRelays = append(errorRelays, relayAddr.String())
		}
	}

	// We must have at least 50%+1 healthy relays, otherwise discard the batch.
	if len(errorRelays) > maxFailingRelays {
		client.Error(notifyCh, fmt.Errorf(
			"CONSENSUS FAILURE: not enough healthy relays; expected %d, got %d",
			minHealthyRelays,
			numHealthyRelays,
		))
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 2: New networks must be initialized explicitly

	// Move status update to next step (step=2)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting networks creation routine",
		"num_networks", len(mustCreateNetworks))

	// Do we have any unknown networks?
	if len(mustCreateNetworks) > 0 {
		// Find networks or create new networks in background
		routineNetworksCreator := c.GetBackend().GetRoutines().NetworksCreator
		go routineNetworksCreator(ctx,
			chainRelays,
			mustCreateNetworks,
			notifyCh,
			c.backend.GetNewChainReadyCh(),
		)
	}

	// ------------------------------------------------------------------------
	// Step 3: Wait for networks to be ready before add to mempool

	// Move status update to next step (step=3)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for networks to be fully created",
		"num_networks", len(mustCreateNetworks))

	// Select a limited number of listeners message updates from
	// the newChainReadyCh channel. This loop forbids excess networks.
	//
	// Waits for routineNetworksCreator to push on newChainReadyCh.
	for i := 0; i < len(mustCreateNetworks); i++ {
		// The routineNetworksCreator communicates the ChainID on a channel
		// to tell this broadcaster about the readiness of a network state.
		chainID,
			waitErr := c.GetBackend().WaitForNextAvailableNetwork(ctx)
		if waitErr != nil {
			client.Error(notifyCh, waitErr)
			return // STOP here
		}

		// Inform about the readiness of this chain
		c.backend.GetLogger().Info("Network is now available", "chain_id", chainID)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Updating events switch for CometBFT",
		"num_networks", len(requiredNetworks))

	// Updates the supported ChainIDs of NodeInfo and p2p.Switch.
	// This enables internal P2P channels for Discovery and CometBFT.
	c.GetBackend().UpdateAvailableNetworks(requiredNetworks)

	// ------------------------------------------------------------------------
	// Step 4.1: Ask relays to replicate chain if necessary

	// Move status update to next step (step=4)
	currentBroadcastStep++

	// As some relays may not know of all networks, we must ask
	// to replicate required networks if they did not report some.
	catchupRelays := c.GetBackend().ApplyFilterReplRequestRelays(
		requiredNetworks,
		relaysWithoutSelf,
		chainRelays,
	)

	numCatchupRelays := 0
	for _, addrs := range catchupRelays {
		numCatchupRelays += len(addrs)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Requesting chain replication from relays",
		"num_peers", numCatchupRelays)

	// Ask the relays to catch-up with the chain by replicating it.
	for chainID, chainCatchupRelays := range catchupRelays {
		if len(chainCatchupRelays) == 0 {
			continue
		}

		numCatchupRelays := len(chainCatchupRelays)
		routineNodeReplRequest := c.GetBackend().GetRoutines().NodeReplRequest
		go routineNodeReplRequest(ctx,
			chainCatchupRelays,
			chainID,
			notifyCh,
		)

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Debug("Waiting for chain replication responses",
			"chain_id", chainID,
			"num_relays", numCatchupRelays)

		// The recipient sends the relay ID in a ChainReplicationResponse.
		// Waits internally until this ChainID has been acknowledged by relays.
		responseRelayIds,
			replErr := c.GetBackend().WaitForRelaysReplResponse(ctx, numCatchupRelays)
		if replErr != nil {
			client.Error(notifyCh, replErr)
			return // STOP here
		}

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Debug("Relays acknowledged chain replication",
			"chain_id", chainID,
			"num_relays", len(responseRelayIds))
	}

	// ------------------------------------------------------------------------
	// Step 4.2: We can start the node services after full ACK of replication
	// and after re-start of services in case the nodes are not yet running.
	//
	// Starting the reactors here fixes a race condition between the multiplex
	// discovery and CometBFT consensus reactors.
	for _, chainID := range requiredNetworks {
		if err := c.backend.StartConsensusInstance(ctx, chainID); err != nil {
			client.Error(notifyCh, err)
			return // STOP here
		}
	}

	// ------------------------------------------------------------------------
	// Step 5: Add transactions to mempool, trigger broadcast to relays

	// Move status update to next step (step=5)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Adding transaction batch to local mempool",
		"num_txes", len(transactions))

	// Add each transaction to the local mempool, an error stops the process.
	if err := c.GetBackend().AddTransactions(userAddress, transactions...); err != nil {
		client.Error(notifyCh, fmt.Errorf(
			"error adding txes to mempool: %w", err))
		return // STOP here
	}

	// IMPORTANT:
	// The transactions have been accepted locally, we may broadcast now.
	//
	// If any of the healthy relays fails to accept the transactions,
	// a rollback will happen because we added the transactions to mempool.

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Broadcasting transaction batch to relays",
		"num_txes", len(transactions), "num_relays", len(relays))

	// Pushes the transaction to other relays mempool to trigger
	// the call to client.AcceptBroadcastTx by the other relays.
	// Uses the filtered list of relays (without self).
	routineRelaysBroadcast := c.GetBackend().GetRoutines().RelaysBroadcast
	go routineRelaysBroadcast(ctx,
		chainRelays,
		catchupRelays,
		userAddress,
		transactions,
		notifyCh,
	)

	// ------------------------------------------------------------------------
	// Step 6: Wait for remote (other relays) transaction acceptance

	// Move status update to next step (step=6)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for remote transaction acceptance",
		"num_relays", numHealthyRelays,
		"num_txes", len(transactions),
		"tx", func() string {
			h := make([]string, 0, len(transactions))
			for _, t := range transactions {
				h = append(h, cmtlog.NewLazySprintf("%X", client.TransactionToRawTx(t).Hash()).String())
			}
			return strings.Join(h, ", ")
		}(),
	)

	// The RelaysBroadcast routine communicates the tx hash on a
	// channel to tell this broadcaster about the acceptance of the
	// transaction by our own mempool.
	relaysPerTx,
		totalAckReceived,
		acceptErr := c.GetBackend().WaitForRelaysAckTransactionBatch(ctx,
		numHealthyRelays,
		len(transactions),
	)
	if acceptErr != nil {
		client.Error(notifyCh, fmt.Errorf(
			"error waiting for relays acceptance: %w", acceptErr))
		return // STOP here
	}

	if totalAckReceived >= numHealthyRelays*len(transactions) {
		// Inform about the readiness of transaction acceptance
		c.backend.GetLogger().Info("Relays accepted transactions",
			"num_relays", numHealthyRelays,
			"num_txes", len(transactions))
	} else {
		// Just log for now, inform will be more precise in loop
		c.backend.GetLogger().Error("Relays did not accept transactions",
			"num_acks", totalAckReceived,
			"num_relays", numHealthyRelays,
			"num_txes", len(transactions))
	}

	for txHash, ackedRelays := range relaysPerTx {
		if len(ackedRelays) < numHealthyRelays {
			client.Error(notifyCh, fmt.Errorf(
				"missing relays acceptance for %s, expected %d, got %d",
				txHash, numHealthyRelays, len(ackedRelays)))
			return // STOP here
		}

		// Will be added to BroadcastStatus.TxHashes in case of success.
		if hashbz, err := hex.DecodeString(txHash); err == nil {
			acceptedTxHashes = append(acceptedTxHashes, hashbz)
		}
	}

	if len(acceptedTxHashes) < len(transactions) {
		client.Error(notifyCh, fmt.Errorf(
			"missing accepted transaction hashes, expected %d, got %d",
			len(transactions), len(acceptedTxHashes)))
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 7: Transactions are now broadcast and accepted by all relays,
	// i.e. consensus succeeded.

	// Move status update to next step (step=7)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Transaction batch was successfully broadcast",
		"num_accepted", len(acceptedTxHashes),
		"num_relays", numHealthyRelays)

	// Done, notify about succeeded broadcast (nil error)
	client.Success(notifyCh, acceptedTxHashes)
}

// BroadcastTxRemoval sends an error to a notifier if any of the removal
// operations fail verification, or if we fail to get a majority approval
// for the broadcast operation from healthy relays.
func (c MultiplexClient) BroadcastTxRemoval(
	ctx context.Context,
	userAddress string,
	relays []string,
	notifier chan<- client.BroadcastStatus,
	transactions ...client.Transaction,
) {
	// 1. verifications for pubkey, relays
	//   1.1. is there any relay we must connect to?
	// 2. determine necessary ChainIDs and current height - delete_ chains!
	//   2.1. does this relay need background sync?
	//   2.2. or, does this relay create a network?
	//   2.3. otherwise, we move on to the next step
	// 2. execute BroadcastTxRemovalExtension
	// 3. broadcast request to other relays
	//   3.1. local mempool addition (no client.AcceptBroadcastTxRemoval)
	//   3.2. mempool addition is broadcast to other relays
	//   3.3. other relays' mempool.PreCheck calls client.AcceptBroadcastTxRemoval
	// 4. wait for all relays to report success
	// 5. commit transactions data to networks
	// 6. remove storage/resources of networks
}
