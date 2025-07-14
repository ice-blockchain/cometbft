package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"math"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
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
func (c *MultiplexClient) GetBackend() server.Backend {
	return c.backend
}

// SetBackend overwrite the backend [server.Backend] instance.
func (c *MultiplexClient) SetBackend(a server.Backend) {
	c.backend = a
}

// GetRuntimeRegistry returns the reactor's [runtime.RuntimeRegistry] implementation.
func (c *MultiplexClient) GetRuntimeRegistry() *runtime.RuntimeRegistry {
	return c.backend.GetRuntimeRegistry().(*runtime.RuntimeRegistry)
}

// BroadcastTx sends an error to a notifier if any of the transactions
// fails basic verification, or if we fail to get a majority approval
// for the broadcast operation from healthy relays.
//
// It accepts a slice of relays which should be in the format `host:port`.
// The relays should contain the port associated with the P2P discovery.
// i.e. with MultiplexConfig.DiscoveryPort=1000, it should contain `:1000`.
//
// The following steps define a complete broadcast process, in this order:
//
// - Basic verifications and opening relay discovery connections.
// - Orchestration of PrivValidator instances remotely.
// - New networks must be initialized explicitly.
// - Wait for networks to be fully ready, i.e. genesis created.
// - Send replication requests for required chains to relays.
// - Add transactions to mempool, trigger broadcast to relays.
// - Wait for remote (other relays) transaction acceptance (ACK).
// - Transactions are now broadcast and accepted by all relays,
//
// i.e. The transaction broadcast happens only when consensus succeeded.
//
// Also, for every consensus instance, we track active runtimes using the
// runtime registry and upon completion (or error), we mark the runtimes
// as completed with [runtime.RuntimeRegistry#OnComplete].
// Note that if there are any chain replications happening on one of the
// remote relays, we will keep alive the active runtimes for these chains
// and shall mark them as complete only when the replications are done.
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
			"CLIENT ERROR: Failed to find a running multiplex backend"))
		return // STOP here
	}

	// Format/parse relay addresses to validate each and permit
	// working with the static multi-port convention for multiplex.
	relayAddresses := make([]*server.RelayAddress, len(relays))
	for i, relay := range relays {
		relayAddr, err := server.NewRelayAddress(relay)
		if err != nil {
			notifyCh <- client.BroadcastStatus{Error: err}
			return // STOP here
		}

		relayAddresses[i] = relayAddr
	}

	acceptedTxHashes := make([][]byte, 0, len(transactions))
	acceptedTxHashesStr := make([]string, 0, len(transactions))
	transactionHashes := txHashesToHex(transactions...)
	relevantChainIds := chainIdsFromTransactions(userAddress, transactions...)
	broadcastID := client.GetBroadcastID(transactions...)

	// IMPORTANT:
	// For every consensus instance, we track active runtimes using the
	// runtime registry and upon completion (or error), we mark the runtimes
	// as completed.
	for _, activeChainID := range relevantChainIds {
		// Marks the runtime active
		c.GetRuntimeRegistry().OnActivate(activeChainID)
	}

	// Find out how many relays will be necessary to reach consensus
	// before evaluating the presence of "self", and we will update
	// numConsensusRelays later, in case self is not present.
	numConsensusRelays := len(relayAddresses)
	minHealthyRelays := numConsensusRelays*2/3 + 1
	maxFailingRelays := numConsensusRelays - minHealthyRelays

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting consensus instance",
		"address", userAddress,
		"requestId", broadcastID,
		"numRelays", numConsensusRelays,
		"minHealthy", minHealthyRelays,
		"maxFailing", maxFailingRelays,
		"numTxes", len(transactions),
		"txBatch", transactionHashes,
		"chainIds", relevantChainIds,
		"relays", relayAddresses,
	)

	// ------------------------------------------------------------------------
	// Step 1: Basic verifications and requesting networks information.
	//
	// relayAddresses contains addresses with *or* without CometBFT Node ID,
	// and relays present in this slice may be unhealthy.
	//
	// healthyRelays contains addresses with CometBFT Node ID.
	// chainRelays maps addresses with CometBFT Node ID by required ChainIDs.
	// errorRelays contains addresses that did not respond to RelayInfo RPC.
	//
	// relaysWithoutSelf excludes self from healthyRelays if present.
	//
	// The broadcast process will be terminated at this step only if we have
	// less than 2/3+1 of relays being considered healthy.
	// ------------------------------------------------------------------------

	// Determine required ChainIDs and unknown ChainIDs.
	// The second return value is a subset of the first return value.
	requiredNetworks,
		mustCreateNetworks := c.GetBackend().GetLocalNetworkHeights(
		userAddress,
		transactions...,
	)

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Found network heights locally",
		"requestId", broadcastID,
		"numRequired", len(requiredNetworks),
		"numUnknowns", len(mustCreateNetworks),
		"txBatch", transactionHashes)

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Fetching networks information from relays",
		"requestId", broadcastID,
		"numRelays", len(relayAddresses),
		"relays", relayAddresses,
		"txBatch", transactionHashes)

	// Determine relay IDs (CometBFT Node ID) and supported networks of each
	// of the relays and identify potential unhealthy relays.
	startRelaysByNetwork := time.Now()
	healthyRelays,
		chainRelays,
		errorRelays := c.GetBackend().GetRelaysByNetwork(ctx, relayAddresses)
	durationRelaysByNetwork := time.Since(startRelaysByNetwork).Milliseconds()

	// Now we know how many relays are actually healthy.
	// healthyRelays contains addresses including a CometBFT Node ID.
	numHealthyRelays := len(healthyRelays)
	numHealthyRemote := numHealthyRelays

	// Exclude undesired ChainID from chainRelays.
	maps.DeleteFunc(chainRelays, func(network string, addresses []*server.RelayAddress) bool {
		return !slices.Contains(requiredNetworks, network)
	})

	// Exclude self for upcoming remote operations (dial + broadcast).
	relaysContainSelf := false
	relaysWithoutSelf := slices.DeleteFunc(healthyRelays, func(a *server.RelayAddress) bool {
		if a.ID() == c.GetBackend().GetRelayID() {
			numHealthyRemote = numHealthyRemote - 1
			relaysContainSelf = true
		}

		return a.ID() == c.GetBackend().GetRelayID()
	})

	// When "self" is not present in relays, we set it healthy and we
	// update this consensus instance to count "self" in numConsensusRelays.
	if !relaysContainSelf && len(relaysWithoutSelf) > 0 {
		numHealthyRelays = numHealthyRelays + 1     // count self as healthy
		numConsensusRelays = numConsensusRelays + 1 // add as required relay

		minHealthyRelays = numConsensusRelays*2/3 + 1
		maxFailingRelays = numConsensusRelays - minHealthyRelays
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Networks information retrieved from relays",
		"requestId", broadcastID,
		"numRelays", numConsensusRelays,
		"numRemote", len(relaysWithoutSelf),
		"numHealthy", numHealthyRelays,
		"minHealthy", minHealthyRelays,
		"maxFailing", maxFailingRelays,
		"errRelays", len(errorRelays),
		"time", strconv.Itoa(int(durationRelaysByNetwork))+"ms",
		"txBatch", transactionHashes,
	)

	// We must have at least 2/3+1 healthy relays, otherwise discard the batch.
	if numHealthyRelays < minHealthyRelays {
		err := fmt.Errorf(
			"CLIENT ERROR: not enough healthy relays; expected %d, got %d",
			minHealthyRelays,
			numHealthyRelays,
		)

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 2: Orchestration of PrivValidator instances remotely.
	//
	// validatorsByChain maps a ChainID to a public keys slice (validators).
	//
	// Note that the local PrivValidator instance will be added to validators.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Orchestrating remote validators instances",
		"requestId", broadcastID,
		"numRelays", len(relaysWithoutSelf),
		"requiredNetworks", requiredNetworks,
		"txBatch", transactionHashes)

	startValidatorsInfo := time.Now()
	validatorsByChain, _ := c.GetBackend().GetValidatorsByNetwork(ctx,
		relaysWithoutSelf,
		requiredNetworks,
	)
	durationValidatorsByNetwork := time.Since(startValidatorsInfo).Milliseconds()

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Validators information retrieved from relays",
		"requestId", broadcastID,
		"numRemote", len(relaysWithoutSelf),
		"numNetworks", len(requiredNetworks),
		"time", strconv.Itoa(int(durationValidatorsByNetwork))+"ms",
		"txBatch", transactionHashes,
	)

	// ------------------------------------------------------------------------
	// Step 3: Dialing healthy relays for chain discovery (ReplicationChannel).
	//
	// relaysWithoutSelf contains addresses that have been dialed successfully.
	//
	// The broadcast process will be terminated at this step if we have more
	// than maxFailingRelays of relays which could not be dialed.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Dialing healthy remote relays",
		"requestId", broadcastID,
		"numRelays", len(relaysWithoutSelf),
		"relays", relaysWithoutSelf,
		"txBatch", transactionHashes)

	discoveryWg := new(sync.WaitGroup)
	discoveryWg.Add(len(relaysWithoutSelf))

	// Track completion and failures individually for dialing process.
	errorRelaysCh := make(chan server.RelayDialError, len(relaysWithoutSelf))

	// Open connections to healthy relays to enable ReplicationChannel.
	routineDiscoveryDialer := c.GetBackend().GetRoutines().DiscoveryDialer
	go routineDiscoveryDialer(ctx,
		relaysWithoutSelf,
		discoveryWg,
		errorRelaysCh,
		c.backend.GetLogger().With(
			"requestId", broadcastID,
			"txBatch", transactionHashes,
		),
	)

	// Blocks the broadcast thread until discovery is available for all relays.
	discoveryWg.Wait()
	close(errorRelaysCh) // No more errors expected.

	for dialRelayErr := range errorRelaysCh {
		errRelayAddr := dialRelayErr.Addr

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Error("Failed to validate relay compatibility",
			"requestId", broadcastID,
			"relay", errRelayAddr.String(),
			"err", dialRelayErr.Error,
		)

		if !slices.Contains(errorRelays, errRelayAddr.String()) {
			errorRelays = append(errorRelays, errRelayAddr.String())
		}
	}

	// We do not allow more than maxFailingRelays to be failing here.
	if len(errorRelays) > maxFailingRelays {
		err := fmt.Errorf(
			"CLIENT ERROR: dialing discovery got errors from too many relays; expected %d, got %d",
			maxFailingRelays,
			len(errorRelays),
		)

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 4: New networks must be initialized explicitly (create GenesisDoc).
	//
	// mustCreateNetworks contains ChainID of networks that must be created.
	//
	// The broadcast process will be terminated at this step if we encountered
	// any error while creating the networks' genesis block.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Starting networks creation routine",
		"requestId", broadcastID,
		"numNetworks", len(requiredNetworks),
		"numCreate", len(mustCreateNetworks),
		"networks", mustCreateNetworks,
		"txBatch", transactionHashes)

	// Track failures individually for ChainID network genesis.
	errorGenesisCh := make(chan error, 1)

	// Do we have any unknown networks?
	if len(mustCreateNetworks) > 0 {
		genesisWg := new(sync.WaitGroup)
		genesisWg.Add(len(mustCreateNetworks))

		// Find networks or create new networks in background
		routineNetworksCreator := c.GetBackend().GetRoutines().NetworksCreator
		go func() {
			if err := routineNetworksCreator(ctx,
				chainRelays,
				mustCreateNetworks,
				validatorsByChain,
				genesisWg,
				c.backend.GetLogger().With(
					"requestId", broadcastID,
					"txBatch", transactionHashes,
				),
			); err != nil {
				errorGenesisCh <- err
			}
		}()

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Debug("Waiting for networks to be fully created",
			"requestId", broadcastID,
			"numNetworks", len(mustCreateNetworks),
			"networks", mustCreateNetworks,
			"txBatch", transactionHashes)

		// Blocks the broadcast thread until all networks are created.
		genesisWg.Wait()
	}
	close(errorGenesisCh)

	for err := range errorGenesisCh {
		genesisErr := fmt.Errorf(
			"CLIENT ERROR: failed to create required networks locally: %w", err,
		)

		c.backend.OnBroadcastError(genesisErr, userAddress, transactions...)
		client.Error(notifyCh, genesisErr)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 5: Ask relays to replicate chains.
	//
	// catchupRelays contains relay addresses that will receive a message
	// with a `ChainReplicationRequest` on [server.ReplicationChannel].
	//
	// We should wait for all replication requests to be completed. Notably,
	// the catchupRelays are expected to send a `ChainReplicationResponse`.
	// ------------------------------------------------------------------------

	// As some relays may not know of all networks, we must ask
	// to replicate required networks if they did not report some.
	catchupRelays := c.GetBackend().ApplyFilterReplRequestRelays(
		requiredNetworks,
		relaysWithoutSelf,
		chainRelays,
	)

	totalNumReplRequests := 0
	for _, addrs := range catchupRelays {
		totalNumReplRequests += len(addrs)
	}

	// We shall concurrently assess the replication of networks but shall
	// wait for completion of *all* replication responses before we proceed.
	chainCatchupsWg := new(sync.WaitGroup)
	chainCatchupsWg.Add(totalNumReplRequests)

	// Ask the relays to catch-up with the chain by replicating it.
	for chainID, chainCatchupRelays := range catchupRelays {
		if len(chainCatchupRelays) == 0 {
			continue
		}

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Debug("Requesting chain replication from relays",
			"requestId", broadcastID,
			"numRequests", len(chainCatchupRelays),
			"chainId", chainID,
			"txBatch", transactionHashes)

		routineNodeReplRequest := c.GetBackend().GetRoutines().NodeReplRequest
		go routineNodeReplRequest(ctx,
			chainCatchupRelays,
			chainID,
			notifyCh,
			c.backend.GetLogger().With(
				"requestId", broadcastID,
				"txBatch", transactionHashes,
			),
		)
	}

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for chain replication responses",
		"requestId", broadcastID,
		"numNetworks", len(catchupRelays),
		"totalRequests", totalNumReplRequests,
		"txBatch", transactionHashes)

	// The recipients send the relay ID in a ChainReplicationResponse.
	// Blocks the broadcast thread all networks have been acknowledged by relays.
	replRelaysPerChainID,
		numExpectedResponses,
		numReceivedResponses,
		replErr := c.GetBackend().WaitForRelaysAckChainReplications(ctx, catchupRelays, transactions...)
	if replErr != nil {
		err := fmt.Errorf(
			"CLIENT ERROR: failed to receive replication responses: %w", replErr)

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	if numReceivedResponses >= numExpectedResponses {
		// Inform about the readiness of replication acceptance
		c.backend.GetLogger().Info("Relays accepted chain replication",
			"requestId", broadcastID,
			"numRcvd", numReceivedResponses,
			"txBatch", transactionHashes)
	} else {
		// Just log for now, report will be more precise
		c.backend.GetLogger().Error("Relays did not accept chain replication",
			"requestId", broadcastID,
			"numRcvd", numReceivedResponses,
			"numExpect", numExpectedResponses,
			"txBatch", transactionHashes)
	}

	for chainID, replRelays := range replRelaysPerChainID {
		// Make sure to have a minimum of healthy relays to accept.
		minHealthyReplicators := int(math.Max(
			float64(minHealthyRelays-1), // -self
			float64(len(catchupRelays[chainID])),
		))
		minAcceptReplication := minHealthyReplicators * 2 / 3 // no + 1, because self doesn't ACK

		// We must receive replication responses from at least minHealthyRelays.
		if len(replRelays) < minAcceptReplication {
			// Not all healthy relays replicated this ChainID.
			err := fmt.Errorf(
				"CLIENT ERROR: missing relays replication for %s, expected %d, got %d",
				chainID, minAcceptReplication, len(replRelays))

			c.backend.OnBroadcastError(err, userAddress, transactions...)
			client.Error(notifyCh, err)
			return // STOP here
		}
	}

	// ------------------------------------------------------------------------
	// Step 6: We can start the node services after full ACK of replication
	// and after re-start of services in case the nodes are not yet running.
	//
	// Starting the reactors here fixes a race condition between the multiplex
	// discovery and CometBFT consensus reactors.
	// ------------------------------------------------------------------------

	for _, chainID := range requiredNetworks {
		// No-op in case the reactors are already running, i.e. this method
		// calls [mempool.Reactor#IsRunning] before starting.
		if err := c.backend.StartConsensusInstance(chainID); err != nil {
			reactErr := fmt.Errorf(
				"CLIENT ERROR: failed to start reactors for %s: %w", chainID, err)

			c.backend.OnBroadcastError(reactErr, userAddress, transactions...)
			client.Error(notifyCh, reactErr)
			return // STOP here
		}
	}

	// ------------------------------------------------------------------------
	// Step 7: Dialing healthy relays for CometBFT reactors.
	//
	// relaysWithoutSelf contains addresses that have been dialed successfully.
	//
	// The broadcast process will be terminated at this step if we have more
	// than maxFailingRelays of relays which could not be dialed.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Dialing healthy remote relays for CometBFT",
		"requestId", broadcastID,
		"numRelays", len(relaysWithoutSelf),
		"relaysDiscovery", relaysWithoutSelf,
		"txBatch", transactionHashes)

	dialingWg := new(sync.WaitGroup)
	dialingWg.Add(len(relaysWithoutSelf) * len(relevantChainIds)) // each relay is dialed for each ChainID

	// Track completion and failures individually for dialing process.
	errorPeersCh := make(chan server.RelayDialError, len(relaysWithoutSelf))

	// Open connections to healthy relays to enable CometBFT messages.
	routineCometBFTDialer := c.GetBackend().GetRoutines().CometBFTDialer
	go routineCometBFTDialer(ctx,
		relaysWithoutSelf,
		relevantChainIds,
		dialingWg,
		errorPeersCh,
		c.backend.GetLogger().With(
			"requestId", broadcastID,
			"txBatch", transactionHashes,
		),
	)

	// Blocks the broadcast thread until discovery is available for all relays.
	dialingWg.Wait()
	close(errorPeersCh) // No more errors expected.

	for dialRelayErr := range errorPeersCh {
		errRelayAddr := dialRelayErr.Addr

		// TODO(midas): remove debug logs
		c.backend.GetLogger().Error("Failed to validate relay compatibility for CometBFT",
			"requestId", broadcastID,
			"relay", errRelayAddr.String(),
			"err", dialRelayErr.Error,
		)

		if !slices.Contains(errorRelays, errRelayAddr.String()) {
			errorRelays = append(errorRelays, errRelayAddr.String())
		}
	}

	// We do not allow more than maxFailingRelays to be failing here.
	if len(errorRelays) > maxFailingRelays {
		err := fmt.Errorf(
			"CLIENT ERROR: dialing CometBFT peers got errors from too many relays; expected %d, got %d",
			maxFailingRelays,
			len(errorRelays),
		)

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 8: Add transactions to local mempool.
	//
	// We shall store the transaction as accepted in the local mempool.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Adding transaction batch to local mempool",
		"requestId", broadcastID,
		"numTxes", len(transactions),
		"txBatch", transactionHashes)

	// Add each transaction to the local mempool, an error stops the process.
	if err := c.GetBackend().AddTransactions(userAddress, transactions...); err != nil {
		memplErr := fmt.Errorf(
			"CLIENT ERROR: error adding txes to mempool: %w", err)

		c.backend.OnBroadcastError(memplErr, userAddress, transactions...)
		client.Error(notifyCh, memplErr)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 9: Broadcast transactions to relays.
	//
	// If any of the healthy relays fails to accept the transactions, a rollback
	// will happen because we added the transactions to our local mempool.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Broadcasting transaction batch to relays",
		"requestId", broadcastID,
		"numTxes", len(transactions),
		"numRelays", numHealthyRemote,
		"txBatch", transactionHashes)

	broadcastWg := new(sync.WaitGroup)
	broadcastWg.Add(1) // Wait for the full broadcast operation.

	// Pushes the transaction to other relays mempool to trigger
	// the call to client.AcceptBroadcastTx by the other relays.
	// Uses the filtered list of relays (without self).
	routineRelaysBroadcast := c.GetBackend().GetRoutines().RelaysBroadcast
	go routineRelaysBroadcast(ctx,
		chainRelays,
		catchupRelays,
		userAddress,
		transactions,
		broadcastWg,
		c.backend.GetLogger().With(
			"requestId", broadcastID,
			"txBatch", transactionHashes,
		),
	)

	// Blocks the broadcast thread until we have sent txes to all relays.
	broadcastWg.Wait()

	// ------------------------------------------------------------------------
	// Step 10: Wait for remote transaction acceptance (ACK).
	//
	// Healthy relays are expected to send us back a message which contains
	// a `AckTransactionBroadcast` on [server.AckBroadcastChannel].
	//
	// If any of the healthy relays fails to ACK the transactions, a rollback
	// will happen because we added the transactions to our local mempool.
	// ------------------------------------------------------------------------

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Waiting for remote transactions ACK",
		"requestId", broadcastID,
		"numRelays", numHealthyRemote,
		"numTxes", len(transactions),
		"txBatch", transactionHashes,
	)

	// acceptErr will NOT be set for individual ACK errors because we may
	// be able to reach consensus without ALL relays sending ACK responses.
	_, ackedRelaysPerTx,
		numExpectedAcks,
		totalAckReceived,
		acceptErr := c.GetBackend().WaitForRelaysAckTransactionBatch(ctx,
		userAddress,
		chainRelays,
		catchupRelays,
		transactions...,
	)

	// TODO(midas): Refactor cancel broadcast and evaluate all conditions at once.
	if acceptErr != nil {
		// Otherwise broadcast a rollback operation if some of the healthy
		// relays does not accept this transaction.
		c.backend.CancelBroadcastOperation(ctx,
			userAddress,
			transactions...,
		)

		err := fmt.Errorf(
			"CLIENT ERROR: error waiting for remote transactions ACK: %w", acceptErr)

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	if totalAckReceived >= numExpectedAcks {
		// Inform about the readiness of transaction acceptance
		c.backend.GetLogger().Info("Relays accepted transactions",
			"requestId", broadcastID,
			"numRelays", numHealthyRemote,
			"numAcks", totalAckReceived,
			"numTxes", len(transactions),
			"txBatch", transactionHashes)
	} else {
		// Just log for now, report will be more precise
		c.backend.GetLogger().Error("Relays did not accept transactions",
			"requestId", broadcastID,
			"numRelays", numHealthyRemote,
			"numAcks", totalAckReceived,
			"numTxes", len(transactions),
			"txBatch", transactionHashes)
	}

	// In single-node network we don't expect acks from remotes, meaning the
	// transactions should get accepted when this condition matches.
	if numExpectedAcks == 0 {
		for _, tx := range transactions {
			acceptedTxHashes = append(acceptedTxHashes, tx.Hash())
		}
	}

	for txHash, ackedRelays := range ackedRelaysPerTx {
		// Make sure to have a minimum of 2/3 of relays to ACK the tx.
		minAcceptTransaction := (numConsensusRelays - 1) * 2 / 3 // -1 for self
		if numExpectedAcks == 0 {
			minAcceptTransaction = 0
		}

		if len(ackedRelays) < minAcceptTransaction {
			// Not all healthy relays acked this transaction.
			// Broadcast a rollback operation because some of the healthy relays
			// may have included (some) transactions in their mempool already.
			c.backend.CancelBroadcastOperation(ctx,
				userAddress,
				transactions...,
			)

			// Just log for now, report will be more precise
			c.backend.GetLogger().Error("Failed to receive required transactions ACK",
				"requestId", broadcastID,
				"numReceived", len(ackedRelays),
				"numExpected", minAcceptTransaction,
				"relayAcks", ackedRelays,
				"txHash", txHash)

			err := fmt.Errorf(
				"CLIENT ERROR: missing transaction ACK for %s, expected %d, got %d",
				txHash, minAcceptTransaction, len(ackedRelays))

			c.backend.OnBroadcastError(err, userAddress, transactions...)
			client.Error(notifyCh, err)
			return // STOP here
		}

		// Will be added to BroadcastStatus.TxHashes in case of success.
		if hashbz, err := hex.DecodeString(txHash); err == nil {
			acceptedTxHashes = append(acceptedTxHashes, hashbz)
			acceptedTxHashesStr = append(acceptedTxHashesStr, txHash)
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

		// Just log for now, report will be more precise
		c.backend.GetLogger().Error("Failed to accept required transactions in batch",
			"requestId", broadcastID,
			"numAccepted", len(acceptedTxHashes),
			"numExpected", len(transactions),
			"txsAccepted", acceptedTxHashesStr,
			"txBatch", transactionHashes)

		err := fmt.Errorf(
			"CLIENT ERROR: missing accepted transaction hashes, expected %d, got %d",
			len(transactions), len(acceptedTxHashes))

		c.backend.OnBroadcastError(err, userAddress, transactions...)
		client.Error(notifyCh, err)
		return // STOP here
	}

	// ------------------------------------------------------------------------
	// Step 11: Transactions are now broadcast and accepted by all relays,
	// i.e. consensus succeeded.

	// TODO(midas): remove debug logs
	c.backend.GetLogger().Debug("Transaction batch was successfully broadcast",
		"requestId", broadcastID,
		"numAccepted", len(acceptedTxHashes),
		"numRelays", numHealthyRelays,
		"txBatch", transactionHashes)

	// Done, notify about succeeded broadcast (nil error)
	client.Success(notifyCh, acceptedTxHashes)

	// NOTE(midas): The OnComplete callback must be executed only if
	// all relays have completed the broadcast operation (+ sync).
	defer c.backend.OnBroadcastComplete(context.Background(),
		userAddress,
		relaysWithoutSelf,
		transactions...,
	)
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
