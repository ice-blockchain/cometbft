package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

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
	// IMPORTANT: relayAddresses contains the port associated with P2P discovery.
	relayAddresses := make([]*server.RelayAddress, len(relays))
	for i, relay := range relays {
		relayAddr, err := server.NewRelayAddress(relay)
		if err != nil {
			notifyCh <- client.BroadcastStatus{Error: err}
			return // STOP here
		}

		relayAddresses[i] = relayAddr
	}

	currentBroadcastStep := uint16(1)
	acceptedTxHashes := make([][]byte, 0, len(transactions))
	transactionHashes := func() []string {
		txHashes := []string{}
		for _, tx := range transactions {
			txHashes = append(txHashes, fmt.Sprintf("%X", tx.Hash()))
		}
		return txHashes
	}()

	numConsensusRelays := len(relayAddresses)
	minHealthyRelays := (numConsensusRelays / 2) + 1
	maxFailingRelays := numConsensusRelays - minHealthyRelays

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting consensus instance",
		"address", userAddress,
		"num_relays", numConsensusRelays,
		"min_healthy", minHealthyRelays,
		"max_failing", maxFailingRelays,
		"num_txes", len(transactions),
		"tx_hashes", transactionHashes,
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
		"num_unknown", len(mustCreateNetworks),
		"tx_hashes", transactionHashes)

	// Build a slice of unique ChainID values.
	requiredNetworks := []string{}
	for chainID := range networksLocalHeights {
		requiredNetworks = append(requiredNetworks, chainID)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Fetching networks information from relays",
		"num_relays", len(relayAddresses),
		"tx_hashes", transactionHashes)

	// Determine relay IDs (CometBFT Node ID) and supported networks of each
	// of the relays and identify potential unhealthy relays.
	startRelaysByNetwork := time.Now()
	chainRelays, errorRelays := c.GetBackend().GetRelaysByNetwork(ctx, relayAddresses)
	durationRelaysByNetwork := time.Since(startRelaysByNetwork).Milliseconds()

	// Now we know how many (remote) relays are actually healthy.
	numHealthyRelays := len(relayAddresses) - len(errorRelays)
	numHealthyRemote := numHealthyRelays

	// Exclude self, should not be used for dialing/broadcast operations.
	relaysWithoutSelf := []*server.RelayAddress{}
	relaysContainSelf := false
	for _, relayAddr := range relayAddresses {
		if !relayAddr.HasID() {
			continue // GetRemoteRelayInfo did not respond.
		}

		if relayAddr.ID() == c.GetBackend().GetRelayID() {
			numHealthyRemote = numHealthyRemote - 1
			relaysContainSelf = true
			continue
		}

		relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
	}

	// When "self" is not present in relays, we set it healthy and we
	// update this consensus instance to count "self" in required relays.
	if !relaysContainSelf && len(relaysWithoutSelf) > 0 {
		numHealthyRelays = numHealthyRelays + 1
		numConsensusRelays = numConsensusRelays + 1

		minHealthyRelays = (numConsensusRelays / 2) + 1
		maxFailingRelays = numConsensusRelays - minHealthyRelays
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Networks information retrieved from relays",
		"num_relays", numConsensusRelays,
		"num_remote", len(relaysWithoutSelf),
		"num_healthy", numHealthyRelays,
		"min_healthy", minHealthyRelays,
		"max_failing", maxFailingRelays,
		"err_relays", len(errorRelays),
		"time", strconv.Itoa(int(durationRelaysByNetwork))+"ms",
		"tx_hashes", transactionHashes,
	)

	// We should work only with healthy relays for the next operations.
	healthyRemoteRelays := make([]*server.RelayAddress, 0, len(relaysWithoutSelf))
	for _, relayAddr := range relaysWithoutSelf {
		if !slices.Contains(errorRelays, string(relayAddr.String())) &&
			!slices.Contains(errorRelays, string(relayAddr.StringWithoutId())) {
			healthyRemoteRelays = append(healthyRemoteRelays, relayAddr)
		}
	}

	// We must have at least 50%+1 healthy relays, otherwise discard the batch.
	if numHealthyRelays < minHealthyRelays {
		client.Error(notifyCh, fmt.Errorf(
			"CONSENSUS FAILURE: not enough healthy relays; expected %d, got %d",
			minHealthyRelays,
			numHealthyRelays,
		))
		return // STOP here
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Dialing healthy remote relays",
		"num_relays", len(healthyRemoteRelays),
		"tx_hashes", transactionHashes)

	// Next, we dial remote relays to find out about any incompatibility
	// before counting the number of failing relays.
	for _, relayAddr := range healthyRemoteRelays {
		// Uses the local P2P switch to dial a remote peer.
		if err := c.GetBackend().CheckDialCompatibleRelay(ctx, relayAddr); err != nil {
			errorRelays = append(errorRelays, relayAddr.String())
		}
	}

	// Remove any healthy remote relay which errored during dialing process.
	healthyRemoteRelays = slices.DeleteFunc(healthyRemoteRelays, func(a *server.RelayAddress) bool {
		return slices.Contains(errorRelays, string(a.String()))
	})

	// As relays may own ChainID that we are not interested in, here we remove
	// any undesired ChainID to avoid dialing relays that we are not interested in.
	maps.DeleteFunc(chainRelays, func(network string, addresses []*server.RelayAddress) bool {
		_, networkContainedByBroadcast := networksLocalHeights[network]
		return !networkContainedByBroadcast
	})

	// Make sure dialing did not error for too many of the healthy relays.
	if len(errorRelays) > maxFailingRelays {
		client.Error(notifyCh, fmt.Errorf(
			"CONSENSUS FAILURE: got errors from too many relays; expected %d, got %d",
			maxFailingRelays,
			len(errorRelays),
		))
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 2: New networks must be initialized explicitly

	// Move status update to next step (step=2)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting networks creation routine",
		"num_networks", len(mustCreateNetworks),
		"tx_hashes", transactionHashes)

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
		"num_networks", len(mustCreateNetworks),
		"tx_hashes", transactionHashes)

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
		"num_networks", len(requiredNetworks),
		"tx_hashes", transactionHashes)

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
		healthyRemoteRelays,
		chainRelays,
	)

	numCatchupRelays := 0
	for _, addrs := range catchupRelays {
		numCatchupRelays += len(addrs)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Requesting chain replication from relays",
		"num_peers", numCatchupRelays,
		"tx_hashes", transactionHashes)

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
			"num_relays", numCatchupRelays,
			"tx_hashes", transactionHashes)

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
			"num_relays", len(responseRelayIds),
			"tx_hashes", transactionHashes)
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

	// IMPORTANT:
	// The transactions have been accepted locally, we may broadcast now.
	//
	// If any of the healthy relays fails to accept the transactions,
	// a rollback will happen because we added the transactions to mempool.

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Broadcasting transaction batch to relays",
		"num_txes", len(transactions), "num_relays", numHealthyRemote,
		"tx_hashes", transactionHashes)

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
		"num_relays", numHealthyRemote,
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
	ackedRelaysPerTx,
		numExpectedAcks,
		totalAckReceived,
		acceptErr := c.GetBackend().WaitForRelaysAckTransactionBatch(ctx,
		chainRelays,
		catchupRelays,
		transactions,
	)
	if acceptErr != nil {
		// Otherwise broadcast a rollback operation if some of the healthy
		// relays does not accept this transaction.
		c.backend.CancelBroadcastOperation(ctx,
			userAddress,
			transactions...,
		)

		client.Error(notifyCh, fmt.Errorf(
			"error waiting for relays acceptance: %w", acceptErr))
		return // STOP here
	}

	if totalAckReceived >= numExpectedAcks {
		// Inform about the readiness of transaction acceptance
		c.backend.GetLogger().Info("Relays accepted transactions",
			"num_relays", numHealthyRemote,
			"num_acks", totalAckReceived,
			"num_txes", len(transactions),
			"tx_hashes", transactionHashes)
	} else {
		// Just log for now, report will be more precise
		c.backend.GetLogger().Error("Relays did not accept transactions",
			"num_relays", numHealthyRemote,
			"num_acks", totalAckReceived,
			"num_txes", len(transactions),
			"tx_hashes", transactionHashes)
	}

	// In single-node network we don't expect acks from remotes, meaning the
	// transactions should get accepted when this condition matches.
	if numExpectedAcks == 0 {
		for _, tx := range transactions {
			acceptedTxHashes = append(acceptedTxHashes, tx.Hash())
		}
	}

	for txHash, ackedRelays := range ackedRelaysPerTx {
		// Filters relay IDs such that relays that are still replicating are
		// not expected to respond with a transaction ack, because their mempool
		// is not ready to include a transaction - these relays will get the
		// transaction by blocksync instead.
		txExpectedAckRelays := c.GetBackend().ApplyFilterAckTransactionRelayIds(
			chainRelays,
			catchupRelays,
		)

		if len(ackedRelays) < len(txExpectedAckRelays) {
			// Not all healthy relays acked this transaction.
			// Broadcast a rollback operation because some of the healthy relays
			// may have included (some) transactions in their mempool already.
			c.backend.CancelBroadcastOperation(ctx,
				userAddress,
				transactions...,
			)

			client.Error(notifyCh, fmt.Errorf(
				"missing relays acceptance for %s, expected %d, got %d",
				txHash, len(txExpectedAckRelays), len(ackedRelays)))
			return // STOP here
		}

		// Will be added to BroadcastStatus.TxHashes in case of success.
		if hashbz, err := hex.DecodeString(txHash); err == nil {
			acceptedTxHashes = append(acceptedTxHashes, hashbz)
		}
	}

	if len(acceptedTxHashes) < len(transactions) {
		// Relays did not accept *all* transactions.
		// Broadcast a rollback operation because some of the healthy relays
		// may have included transactions in their mempool already.
		c.backend.CancelBroadcastOperation(ctx,
			userAddress,
			transactions...,
		)

		client.Error(notifyCh, fmt.Errorf(
			"missing accepted transaction hashes, expected %d, got %d",
			len(transactions), len(acceptedTxHashes)))
		return // STOP here
	}
	// TODO: move upper before broadcast to remote as we fix validators
	// ------------------------------------------------------------------------
	// Step 5: Add transactions to mempool, trigger broadcast to relays

	// Move status update to next step (step=5)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Adding transaction batch to local mempool",
		"num_txes", len(transactions),
		"tx_hashes", transactionHashes)

	// Add each transaction to the local mempool, an error stops the process.
	if err := c.GetBackend().AddTransactions(userAddress, transactions...); err != nil {
		client.Error(notifyCh, fmt.Errorf(
			"error adding txes to mempool: %w", err))
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
		"num_relays", numHealthyRelays,
		"tx_hashes", transactionHashes)

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
