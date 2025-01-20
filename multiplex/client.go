package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"

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

	// A client notifier implementation, e.g. [StatusNotifier].
	notifier client.Notifier
}

// Assert that our implementation satisfy the Client interface.
var _ client.Client = (*MultiplexClient)(nil)

// WithBackend is an option helper to overwrite the default adapter instance.
func WithBackend(a server.Backend) func(*MultiplexClient) {
	return func(c *MultiplexClient) {
		c.backend = a
	}
}

// WithNotifier is an option helper to overwrite the default client notifier.
func WithNotifier(n client.Notifier) func(*MultiplexClient) {
	return func(c *MultiplexClient) {
		c.notifier = n
	}
}

// NewClient initializes a new [MultiplexClient].
func NewClient(
	options ...func(*MultiplexClient),
) *MultiplexClient {
	cli := &MultiplexClient{
		notifier: &client.StatusNotifier{},
	}

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
// It accepts a slice of relays which should be in the format `id@host`.
// The relays should not contain port numbers as the port number shall be
// discovered upon confirming that the relay is up and running.
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
		notifyCh <- client.BroadcastStatus{
			Error: errors.New(
				"could not find a running multiplex backend"),
		}
		return // STOP here
	}

	currentBroadcastStep := uint16(1)
	acceptedTxHashes := make([][]byte, 0, len(transactions))
	minHealthyRelays := (len(relays) / 2) + 1
	maxFailingRelays := len(relays) - minHealthyRelays

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting consensus instance",
		"address", userAddress,
		"num_relays", len(relays),
		"min_healthy", minHealthyRelays,
		"max_failing", maxFailingRelays,
		"num_txes", len(transactions),
	)

	c.notifier.SetChannel(notifyCh)

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
		"num_relays", len(relays))

	// Determine port numbers and correct listen addresses per network.
	// Note that this also checks for the relays to be up and running.
	chainRelays, errorRelays := c.GetBackend().FetchRelayAddresses(relays)
	numHealthyRelays := len(relays) - len(errorRelays)

	// We must have at least 50%+1 healthy relays, otherwise discard the batch.
	if len(errorRelays) > maxFailingRelays {
		c.notifier.Error(fmt.Errorf(
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
		// Uses the filtered list of relays (without self).
		routineNetworksCreator := c.GetBackend().GetRoutines().NetworksCreator
		go routineNetworksCreator(ctx,
			chainRelays,
			mustCreateNetworks,
			c.notifier,
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
	// Waits for createNetworksRoutine to push on newChainReadyCh.
	for i := 0; i < len(mustCreateNetworks); i++ {
		// The createNetworksRoutine communicates the ChainID on a channel
		// to tell this broadcaster about the readiness of a network state.
		chainID,
			waitErr := c.GetBackend().WaitForNextAvailableNetwork(ctx)
		if waitErr != nil {
			c.notifier.Error(waitErr)
			return // STOP here
		}

		// Inform about the readiness of this chain
		c.backend.GetLogger().Info("Network is now available", "chain_id", chainID)
	}

	// ------------------------------------------------------------------------
	// Step 4: Ask relays to replicate chain if necessary

	// Move status update to next step (step=4)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Requesting chain replication from relays",
		"num_peers", len(chainRelays))

	// As some relays may not know of all networks, we must ask
	// to replicate required networks if they did not report some.
	catchupRelays := c.GetBackend().ApplyFilterReplRequestRelays(
		requiredNetworks,
		relays,
		chainRelays,
	)

	// Ask the relays to catch-up with the chain by replicating it.
	for chainID, chainCatchupRelays := range catchupRelays {
		if len(chainCatchupRelays) == 0 {
			continue
		}

		routineNodeReplRequest := c.GetBackend().GetRoutines().NodeReplRequest
		go routineNodeReplRequest(ctx,
			chainCatchupRelays,
			chainID,
			c.notifier,
		)
	}

	// XXX relayID, waitErr := c.GetBackend().WaitForRelayReplResponse(ctx)

	// ------------------------------------------------------------------------
	// Step 5: Add transactions to mempool, trigger broadcast to relays

	// Move status update to next step (step=5)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Adding transaction batch to local mempool",
		"num_txes", len(transactions))

	// Add each transaction to the local mempool, an error stops the process.
	if err := c.GetBackend().AddTransactions(userAddress, transactions...); err != nil {
		c.notifier.Error(fmt.Errorf(
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
		"num_relays", len(relays))

	// Pushes the transaction to other relays mempool to trigger
	// the call to client.AcceptBroadcastTx by the other relays.
	// Uses the filtered list of relays (without self).
	routineRelaysBroadcast := c.GetBackend().GetRoutines().RelaysBroadcast
	go routineRelaysBroadcast(ctx,
		chainRelays,
		userAddress,
		transactions,
		c.notifier,
		c.backend.GetRelayAcceptTxCh(),
	)

	// ------------------------------------------------------------------------
	// Step 6: Wait for remote (other relays) transaction acceptance

	// Move status update to next step (step=6)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for remote transaction acceptance",
		"num_txes", len(transactions))

	// Select a limited number of listeners message updates from
	// the relayAcceptTxCh channel. This loop forbids excess transactions.
	//
	// Waits for RelaysBroadcast routine to push on relayAcceptTxCh.
	for i := 0; i < len(transactions); i++ {
		// The RelaysBroadcast routine communicates the tx hash on a
		// channel to tell this broadcaster about the acceptance of the
		// transaction by our own mempool.
		acceptedTxHash,
			acceptErr := c.GetBackend().WaitForRelayTxAcceptance(ctx)
		if acceptErr != nil {
			c.notifier.Error(acceptErr)
			return // STOP here
		}

		// Decode to bytes slice makes sure we have a transaction hash
		txHashBytes, err := hex.DecodeString(acceptedTxHash)
		if err != nil || len(txHashBytes) != tmhash.Size {
			acceptErr = fmt.Errorf(
				"invalid transaction hash %s: %w", acceptedTxHash, err)
			c.backend.GetLogger().Error(acceptErr.Error())
			break
		}

		// Inform about the readiness of transaction acceptance
		c.backend.GetLogger().Info("Relays accepted transaction", "hash", acceptedTxHash)

		// Will be added to BroadcastStatus.TxHashes in case of success.
		acceptedTxHashes = append(acceptedTxHashes, txHashBytes)
	}

	// ------------------------------------------------------------------------
	// Step 7: Transactions are now broadcast and accepted by all relays,
	// i.e. consensus succeeded.

	// Move status update to next step (step=7)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Transaction batch was successfully broadcast",
		"num_accepted", len(acceptedTxHashes))

	// Done, notify about succeeded broadcast (nil error)
	c.notifier.Success(acceptedTxHashes)
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
