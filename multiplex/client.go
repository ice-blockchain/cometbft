package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"slices"

	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

// ClientNotifier defines the contract for client status notifiers as they
// are used during broadcast operations to asynchronously notify the caller
// about exact broadcast status updates and errors.
type ClientNotifier interface {
	SetChannel(ch chan<- client.BroadcastStatus)
	GetChannel() chan<- client.BroadcastStatus

	Error(err error)
	Success(txHashes [][]byte)
}

// MultiplexClient implements a multiplex client by providing an implementation
// for methods as defined in [client.Client].
//
// Note, an [client.Acceptor] implementation may be provided in [NewClient].
type MultiplexClient struct {
	// A backend adapter instance is used to delegate communication
	// with other relays and to execute local operations, e.g. persist data.
	backend Adapter

	// A client notifier implementation, e.g. [StatusNotifier].
	notifier ClientNotifier

	// This channel is used to wait when new networks must be created.
	newChainReadyCh chan string

	// This channel is used to communicate the tx hash of a transaction
	// that has been accepted by our own mempool AND by the relays' mempools.
	relayAcceptTxCh chan string
}

// Assert that our implementation satisfy the Client interface.
var _ client.Client = (*MultiplexClient)(nil)

// WithBackend is an option helper to overwrite the default adapter instance.
func WithBackend(a Adapter) func(*MultiplexClient) {
	return func(c *MultiplexClient) {
		c.backend = a
	}
}

// WithNotifier is an option helper to overwrite the default client notifier.
func WithNotifier(n ClientNotifier) func(*MultiplexClient) {
	return func(c *MultiplexClient) {
		c.notifier = n
	}
}

// NewClient initializes a new [MultiplexClient].
func NewClient(
	options ...func(*MultiplexClient),
) *MultiplexClient {
	cli := &MultiplexClient{
		newChainReadyCh: make(chan string),
		relayAcceptTxCh: make(chan string),

		notifier: &StatusNotifier{},
	}

	// Enable overwrite of optional properties
	for _, option := range options {
		option(cli)
	}

	return cli
}

// GetBackend returns the backend [Adapter] implementation.
func (c MultiplexClient) GetBackend() Adapter {
	return c.backend
}

// SetBackend overwrite the backend [Adapter] instance.
func (c MultiplexClient) SetBackend(a Adapter) {
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
// - Basic verifications and opening relay connections.
// - New networks must be initialized explicitly.
// - Wait for networks to be ready before adding to mempool.
// - Add transactions to mempool, trigger broadcast to relays.
// - Wait for remote (other relays) transaction acceptance.
// - Transactions are now broadcast and accepted by all relays,
//
// i.e. The transaction broadcast happens only when consensus succeeded.
func (c MultiplexClient) BroadcastTx(
	ctx context.Context,
	userAddress string,
	relays []string,
	notifier chan<- client.BroadcastStatus,
	transactions ...client.Transaction,
) {
	// Can't broadcast without a multiplex backend
	if c.GetBackend() == nil {
		notifier <- client.BroadcastStatus{
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

	c.notifier.SetChannel(notifier)

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

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting to dial unknown relays",
		"num_peers", len(chainRelays))

	// Connect to any relays that we are not yet connected to.
	// Note, the AddrBook is updated in p2p.Switch#dialPeersAsync.
	routineNodeRelayDialer := c.GetBackend().GetRoutines().NodeRelayDialer
	go routineNodeRelayDialer(ctx,
		chainRelays,
		c.notifier,
	)

	// As some relays may not know of all networks, we must ask
	// to replicate required networks if they did not report some.
	catchupRelays := map[string][]string{}
	for chainID, relaysByChain := range chainRelays {
		// Did all relays report to know this ChainID?
		if len(relaysByChain) == len(relays) {
			catchupRelays[chainID] = nil
			continue
		}

		// Find out which relays are missing for this chain.
		// Those are relays that need to catchup with the chain.
		for _, relay := range relays {
			if !slices.Contains(relaysByChain, relay) {
				catchupRelays[chainID] = append(catchupRelays[chainID], relay)
			}
		}
	}

	// Handling case when chainRelays is empty (0 networks on remote relays).
	for _, chainID := range requiredNetworks {
		if _, has := catchupRelays[chainID]; !has {
			catchupRelays[chainID] = append(catchupRelays[chainID], relays...)
		}
	}

	// Ask the relays to catch-up with the chain by replicating it.
	for chainID, chainCatchupRelays := range catchupRelays {
		if len(chainCatchupRelays) == 0 {
			continue
		}
		// Ask the relays to catch-up with the chain by replicating it.
		routineNodeReplRequest := c.GetBackend().GetRoutines().NodeReplRequest
		go routineNodeReplRequest(ctx,
			chainCatchupRelays,
			chainID,
			c.notifier,
		)
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
			c.newChainReadyCh,
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
		chainID := <-c.newChainReadyCh

		// Inform about the readiness of this chain
		c.backend.GetLogger().Info("Network is now available", "chain_id", chainID)

		// TODO(midas): Network can now be connected to by other relays.
	}

	// ------------------------------------------------------------------------
	// Step 4: Add transactions to mempool, trigger broadcast to relays

	// Move status update to next step (step=4)
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
		c.relayAcceptTxCh,
	)

	// ------------------------------------------------------------------------
	// Step 5: Wait for remote (other relays) transaction acceptance

	// Move status update to next step (step=5)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for remote transaction acceptance",
		"num_txes", len(transactions))

	// Select a limited number of listeners message updates from
	// the relayAcceptTxCh channel. This loop forbids excess transactions.
	//
	// Waits for broadcastTransactionRoutine to push on relayAcceptTxCh.
	var acceptErr error
	for i := 0; i < len(transactions); i++ {
		select {
		// The broadcastTransactionRoutine communicates the tx hash on a
		// channel to tell this broadcaster about the acceptance of the
		// transaction by our own mempool AND by the relays' mempools.
		case acceptedTxHash := <-c.relayAcceptTxCh:
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

		// Handle potential expiration of context and consider as an error.
		case <-ctx.Done():
			acceptErr = fmt.Errorf(
				"context expired for BroadcastTx at step %d", currentBroadcastStep)
			c.backend.GetLogger().Error(acceptErr.Error())
		}

		// Stop waiting if we have encountered errors
		if acceptErr != nil {
			break
		}
	}

	// If we found at least one error, we shall reject the transactions batch.
	if acceptErr != nil {
		c.notifier.Error(acceptErr)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 6: Transactions are now broadcast and accepted by all relays,
	// i.e. consensus succeeded.

	// Move status update to next step (step=6)
	currentBroadcastStep++

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Transaction batch was successfully broadcast",
		"num_accepted", len(acceptedTxHashes))

	// Done, notify about succeeded broadcast (nil error)
	c.notifier.Success(acceptedTxHashes)

	// We may cleanup, CometBFT will proceed to create proposal.
	close(c.newChainReadyCh)
	close(c.relayAcceptTxCh)
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
