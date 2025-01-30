package multiplex

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"

	memp2p "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

// GetRoutines returns an injected implementation of [server.Jobs] methods
// or the default implementations as defined with MultiplexBackend.
//
// This is mainly used to overwrite routines for testing purposes.
// GetRoutines implements [Adapter].
func (b *MultiplexBackend) GetRoutines() *server.Jobs {
	if b.routines == nil {
		b.routines = &server.Jobs{
			NodeReplRequest: b.DefaultNodeReplRequestRoutine(),
			NetworksCreator: b.DefaultNetworksCreatorRoutine(),
			RelaysBroadcast: b.DefaultRelaysBroadcastRoutine(),
			CancelBroadcast: b.DefaultCancelBroadcastRoutine(),
		}
	}

	return b.routines
}

// ----------------------------------------------------------------------------
// Default routines implementation for a MultiplexBackend

// DefaultNodeReplRequestRoutine asks relays to replicate a network by
// attaching the corresponding ChainParams.
//
// This method broadcasts a [mxp2p.ChainReplicationRequest] message to
// relays, to ask them to replicate a chain using the ChainParams.
func (b *MultiplexBackend) DefaultNodeReplRequestRoutine() server.NodeReplRequestFn {
	return func(
		_ context.Context,
		relays []*server.RelayAddress,
		chainID string,
		notifierImpl client.Notifier,
	) {
		replRequestPeers := []string{}
		knownPeers := []string{}
		for _, relayAddr := range relays {
			if relayAddr.HasID() {
				knownPeers = append(knownPeers, string(relayAddr.ID()))
			}
		}

		b.logger.Debug("preparing to send ChainReplicationRequest",
			"chain_id", chainID,
			"relays", relays,
			"ids", knownPeers,
		)

		// Retrieve the GenesisDoc for this chain
		genDocProvider := b.reactor.GetGenesisProvider()
		genesisDoc := genDocProvider(chainID)

		// Build a transportable ChainParams protobuf message
		chainParams, err := GenesisDocToChainParams(*genesisDoc)
		if err != nil {
			notifierImpl.Error(err)
			return // terminates the process
		}

		// Broadcast the ChainReplicationRequest.
		eventsSwitch := b.EventSwitch()
		eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
			// Send only to relays we are interested in.
			peerID := string(peer.ID())
			if !slices.Contains(knownPeers, peerID) {
				return
			}

			b.logger.Debug("now sending ChainReplicationRequest",
				"chain_id", chainID,
				"peer_id", peerID,
			)

			peer.Send(p2p.Envelope{
				ChannelID: server.ReplicationChannel,
				Message: &mxp2p.Message{
					Sum: &mxp2p.Message_ChainReplicationRequest{
						ChainReplicationRequest: &mxp2p.ChainReplicationRequest{
							ChainID:     chainID,
							ChainParams: chainParams,
						},
					},
				},
			})

			replRequestPeers = append(replRequestPeers, peerID)

			// TODO(midas): wait for replication ACK with b.reactor.ackReplResCh
		})

		// Keep track of node IDs
		b.replRequestsSent[chainID] = replRequestPeers
	}
}

// DefaultNetworksCreatorRoutine communicates with relays about
// missing networks as described by chainIDs. If the relays return an empty
// response, it means that we must create a new network.
//
// This method creates a new network genesis using [Reactor#MustCreateNetwork],
// then injects a node runtime using [Reactor#MustInjectNodeRuntime].
func (b *MultiplexBackend) DefaultNetworksCreatorRoutine() server.NetworksCreatorFn {
	return func(
		ctx context.Context,
		relaysByChain map[string][]*server.RelayAddress,
		missingChains []string,
		notifierImpl client.Notifier,
		newChainReadyCh chan<- string,
	) {
		// Find out if any of the relays told us about some missing networks,
		// in this case, this is NOT a new network and our relay needs sync.
		unknownNetworks := []string{}
		for _, missingChainID := range missingChains {
			if _, ok := relaysByChain[missingChainID]; !ok {
				unknownNetworks = append(unknownNetworks, missingChainID)
				continue
			}

			// This is NOT a new network (existing on some relay)
			newChainReadyCh <- missingChainID
		}

		// If possible, notify success and terminate here.
		if len(unknownNetworks) == 0 {
			notifierImpl.Success([][]byte{})
			return
		}

		// We must create at least one NEW network.
		for _, newChainID := range unknownNetworks {
			err := func() (err error) {
				// Recover from potential panic in below block due to inability to create
				// a new network. This recovery ensures that the client implementation is
				// able to react to errors happening in the process, any errors here must
				// terminate the broadcast process as it is effectively invalidated here.
				defer func() {
					if errRecovered := recover(); errRecovered != nil {
						// Error happened in MustCreateNetwork process.
						err = errRecovered.(error)

						// The reactor will have pushed on createErr already.
						b.logger.Error(fmt.Errorf(
							"CLIENT PANIC encountered with MustCreateNetwork: %w",
							errRecovered.(error),
						).Error())
					}
				}()

				// Create the network genesis, state machine, etc.
				err = b.reactor.InjectNewNetwork(newChainID)
				if err != nil {
					return err
				}

				// Inject a *running* node.Node for the new network.
				// TODO(midas): currently not passing any node options.
				return b.reactor.InjectNewRuntime(ctx, newChainID)
			}()
			if err != nil {
				// Terminates the upper broadcast process
				notifierImpl.Error(fmt.Errorf(
					"could not create required networks: %w", err))
				return
			}

			// We may now proceed with the transaction broadcast, and other
			// relays will be able to join the newly created network.
			newChainReadyCh <- newChainID
		}

		// We are not done with the entire broadcast process,
		// the transaction must not be considered accepted.
		notifierImpl.Success([][]byte{})
	}
}

// DefaultRelaysBroadcastRoutine broadcasts all transactions to relays
// and verifies their respective acceptance of the transaction batch.
//
// If any broadcast to other relays produces an error, the complete
// transaction batch will be discarded, and a rollback message will
// be broadcast to other relay's mempool reactors.
func (b *MultiplexBackend) DefaultRelaysBroadcastRoutine() server.RelaysBroadcastFn {
	return func(
		ctx context.Context,
		relaysByChain map[string][]*server.RelayAddress,
		userAddress string,
		transactions []client.Transaction,
		notifierImpl client.Notifier,
		relayAcceptTxCh chan<- string,
	) {
		// switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)
		broadcastTxHashes := make([][]byte, 0, len(transactions))

		// Reset the sent requests cache
		b.poolRequestsSent = map[string][]string{}

		// Iterate through transaction and broadcast each of them to other relays
		eventsSwitch := b.reactor.GetEventSwitch()
		for i, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			// eventsSwitch := switchProvider(chainID).(*p2p.Switch)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			// Broadcast must happen only if there is at least one healthy relay.
			// For NEW networks, we don't need to broadcast to other relays.
			if _, ok := relaysByChain[chainID]; !ok {
				relayAcceptTxCh <- txHash
				continue // Do not broadcast to relays
			}

			// Force the execution of mempool broadcast to *all* healthy relays.
			relaysAccepted := 0
			minHealthyRelays := len(relaysByChain[chainID])
			chainHealthyPeers := make([]string, len(relaysByChain[chainID]))
			for i, relayAddr := range relaysByChain[chainID] {
				chainHealthyPeers[i] = string(relayAddr.ID())
			}

			// Broadcast the transaction to all healthy relays.
			poolRequestPeers := []string{}
			eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
				// Send only to relays we are interested in (healthy relays).
				// Skip unhealthy relays because they would produce an error.
				peerID := string(peer.ID())
				if !slices.Contains(chainHealthyPeers, peerID) {
					return
				}

				// Send transaction to relay mempool, after checks the mempool
				// reactor shall send a AckTransactionBroadcast back to us which
				// sends a AckTransactionBroadcast object on ackTxAcceptCh
				if success := peer.Send(p2p.Envelope{
					ChannelID: mempl.MempoolChannel,
					Message:   &memp2p.Txs{Txs: [][]byte{rawTx}},
				}); success {
					relaysAccepted++
				}

				poolRequestPeers = append(poolRequestPeers, peerID)
			})

			b.poolRequestsSent[chainID] = poolRequestPeers

			// TODO(midas): update flow here to WaitForRelayAckTransaction() for each relay
			// TODO(midas): relayID, waitErr := c.GetBackend().WaitForRelayAckTransaction(ctx)

			// We require healthy relays to accept this broadcast.
			if relaysAccepted >= minHealthyRelays {
				relayAcceptTxCh <- txHash
			} else {
				// Otherwise broadcast a rollback operation if some of the healthy
				// relays already added this transaction to their mempool.
				// The mempool calls [Acceptor#RollbackTx] before removing txes.
				routineCancelBroadcast := b.GetRoutines().CancelBroadcast
				go routineCancelBroadcast(ctx, userAddress, transactions)

				// Also call RollbackTx extension locally and remove from mempool.
				if err := b.acceptor.RollbackTx(ctx, userAddress, transactions...); err == nil {
					// Remove the transactions from local mempool.
					b.RemoveTransactions(userAddress, transactions...)
				}

				// We are missing some relays' acceptance, fail here.
				notifierImpl.Error(fmt.Errorf(
					"other relays failed to accept transaction %s", txHash))
				return // terminates the process
			}

			copy(broadcastTxHashes[i], rawTx.Hash())
		}

		// Done, notify about succeeded broadcast (nil error)
		notifierImpl.Success(broadcastTxHashes)
	}
}

// DefaultCancelBroadcastRoutine broadcasts a rollback message to healthy relays
// in case any of the relays has already included the transactions in their
// mempool. The mempool should call [Acceptor#RollbackTx] upon receiving this
// message.
func (b *MultiplexBackend) DefaultCancelBroadcastRoutine() server.CancelBroadcastFn {
	return func(
		ctx context.Context,
		userAddress string,
		transactions []client.Transaction,
	) {
		// switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)
		eventsSwitch := b.reactor.GetEventSwitch()

		// Iterate through transactions and broadcast rollback operations
		// for each of them to all other relays.
		for _, transaction := range transactions {
			// chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			// eventsSwitch := switchProvider(chainID).(*p2p.Switch)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)

			// Broadcast the rollback message for this transaction to all relays.
			eventsSwitch.Broadcast(p2p.Envelope{
				ChannelID: mempl.MempoolChannel,
				Message:   &memp2p.RollbackTxs{Txs: [][]byte{rawTx}},
			})
		}
	}
}
