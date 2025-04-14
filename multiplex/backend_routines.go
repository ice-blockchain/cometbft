package multiplex

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
	"sync"

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
		notifyCh chan<- client.BroadcastStatus,
	) {
		replRequestPeers := []string{}
		knownPeers := []string{}
		for _, relayAddr := range relays {
			if relayAddr.HasID() {
				knownPeers = append(knownPeers, string(relayAddr.ID()))
			}
		}

		// TODO(midas): remove debug logs
		b.logger.Debug("Preparing to send ChainReplicationRequest",
			"chain_id", chainID,
			"relays", relays,
			"ids", knownPeers,
		)

		// Retrieve the GenesisDoc for this chain
		genesisDocProvider := b.reactor.GetGenesisProvider()
		genesisDoc, err := genesisDocProvider(chainID)
		if err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"could not load genesis doc in NodeReplRequest: %w", err))
			return // terminates the process
		}

		// Build a transportable ChainParams protobuf message
		chainParams, err := GenesisDocToChainParams(*genesisDoc)
		if err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"could not load genesis doc in NodeReplRequest: %w", err))
			return // terminates the process
		}

		// Broadcast the ChainReplicationRequest.
		// Note that this events switch uses `DiscoveryPort`.
		eventsSwitch := b.CreateOrLoadDiscoveryEventSwitch()
		eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
			// Send only to relays we are interested in.
			peerID := string(peer.ID())
			if !slices.Contains(knownPeers, peerID) {
				return
			}

			// TODO(midas): remove debug logs
			b.logger.Debug("Now sending ChainReplicationRequest",
				"chain_id", chainID,
				"peer_id", peerID,
			)

			peer.Send(chainID, p2p.Envelope{
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
		})

		// Keep track of node IDs
		b.replRequestsMtx.Lock()
		b.replRequestsSent[chainID] = replRequestPeers
		b.replRequestsMtx.Unlock()
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
		notifyCh chan<- client.BroadcastStatus,
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

		// If possible,  terminate here.
		if len(unknownNetworks) == 0 {
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
				client.Error(notifyCh, fmt.Errorf(
					"could not create required networks: %w", err))
				return
			}

			// We may now proceed with the transaction broadcast, and other
			// relays will be able to join the newly created network.
			newChainReadyCh <- newChainID
		}
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
		replReqRelays map[string][]*server.RelayAddress,
		userAddress string,
		transactions []client.Transaction,
		notifyCh chan<- client.BroadcastStatus,
	) {
		broadcastTxHashes := make([][]byte, len(transactions))

		// (1)
		// First dial the CometBFT P2P addresses to make sure
		// communication with this relay is possible using mempool.
		minRelaysByChain := make(map[string]int, len(relaysByChain))
		for chainID, relays := range relaysByChain {
			relaysWithoutSelf := []*server.RelayAddress{}
			for _, relayAddr := range relays {
				if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
					relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
				}
			}

			minRelaysByChain[chainID] = len(relaysWithoutSelf)

			// Excludes "self" and replication partners (already dialed).
			relaysToDial := slices.DeleteFunc(relaysWithoutSelf, func(address *server.RelayAddress) bool {
				return slices.Contains(replReqRelays[chainID], address)
			})
			for _, relayAddr := range relaysToDial {
				peerAddr, err := relayAddr.NetAddressForCometBFT()
				if err != nil {
					client.Error(notifyCh, fmt.Errorf(
						"invalid cometbft relay address %s: %w", relayAddr.AddressForCometBFT(), err))
					return
				}

				// Note that this events switch uses `DiscoveryPort+1`.
				sw := b.reactor.GetEventSwitchForCometBFT()
				if err := sw.DialPeerWithAddress(peerAddr); err != nil {
					if _, ok := err.(p2p.ErrCurrentlyDialingOrExistingAddress); !ok {
						client.Error(notifyCh, fmt.Errorf(
							"could not dial relay %s: %w", relayAddr.AddressForCometBFT(), err))
					}
				}
			}
		}

		// (2)
		// Iterate through transaction and broadcast each of them to other relays
		// Note that this events switch uses `DiscoveryPort+1`.
		eventsSwitch := b.reactor.GetEventSwitchForCometBFT()
		for i, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			// Broadcast must happen only if there is at least one healthy relay.
			// For NEW networks, we don't need to broadcast to other relays,
			// instead a ChainReplicationRequest will be sent to all of them.
			if _, ok := relaysByChain[chainID]; !ok {
				peers := []string{}
				for _, relayAddr := range replReqRelays[chainID] {
					peers = append(peers, string(relayAddr.ID()))
				}

				// Considers the relays as "ack'd", given no need to broadcast.
				b.reactor.poolRequestsMtx.Lock()
				b.reactor.poolRequestsSent[txHash] = peers
				b.reactor.poolRequestsMtx.Unlock()

				continue // Do not broadcast to relays
			}

			// Force the execution of mempool broadcast to *all* healthy relays.
			minHealthyRelays := minRelaysByChain[chainID]
			chainHealthyPeers := []string{}
			for _, relayAddr := range relaysByChain[chainID] {
				chainHealthyPeers = append(chainHealthyPeers, string(relayAddr.ID()))
			}

			// TODO(midas): remove debug logs
			b.logger.Debug("Keeping only healthy relays for broadcast",
				"chain_id", chainID,
				"tx_hash", txHash,
				"num_relays", len(chainHealthyPeers),
				"num_peers", eventsSwitch.Peers().Size(),
			)

			// Waits to make sure we reached enough of the healthy relays.
			sentWg := sync.WaitGroup{}
			sentWg.Add(eventsSwitch.Peers().Size())

			// Reset the sent requests cache for this txHash
			b.reactor.poolRequestsMtx.Lock()
			if _, ok := b.reactor.poolRequestsSent[txHash]; ok {
				b.reactor.poolRequestsSent[txHash] = []string{}
			}
			b.reactor.poolRequestsMtx.Unlock()

			// Broadcast the transaction to all healthy relays.
			poolRequestPeers := []string{}
			relaysAccepted := 0
			eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
				defer sentWg.Done()

				// Send only to relays we are interested in (healthy relays).
				// Skip unhealthy relays because they would produce an error.
				peerID := string(peer.ID())
				if !slices.Contains(chainHealthyPeers, peerID) {
					return
				}

				// TODO(midas): remove debug logs
				b.logger.Debug("Sending transaction to remote mempool",
					"chain_id", chainID,
					"tx_hash", txHash,
					"peer", peerID,
				)

				// Send transaction to relay mempool, after checks the mempool
				// reactor shall send a AckTransactionBroadcast back to us which
				// sends a AckTransactionBroadcast object on ackTxAcceptCh
				if success := peer.Send(chainID, p2p.Envelope{
					ChannelID: mempl.MempoolChannel,
					Message:   &memp2p.Txs{Txs: [][]byte{rawTx}},
				}); !success {
					b.logger.Error("could not send message on mempool channel",
						"chain_id", chainID,
						"tx_hash", txHash,
						"peer", peerID,
					)

					// Note: we do not push an error on the notifyCh channel
					// because a failure in sending to one relay must not
					// prevent the transaction broadcast operation.

					return
				}

				poolRequestPeers = append(poolRequestPeers, peerID)
				relaysAccepted++
			})

			// Waits to process all healthy relays' acknowledgments.
			sentWg.Wait()

			b.reactor.poolRequestsMtx.Lock()
			b.reactor.poolRequestsSent[txHash] = poolRequestPeers
			b.reactor.poolRequestsMtx.Unlock()

			// We require healthy relays to accept this broadcast.
			if relaysAccepted >= minHealthyRelays {
				// TODO(midas): remove debug logs
				b.logger.Debug("Done broadcasting to remote mempools",
					"num_relays", relaysAccepted,
					"tx_hash", txHash,
				)

				rawTxHashBytes := rawTx.Hash()
				broadcastTxHashes[i] = make([]byte, len(rawTxHashBytes))
				copy(broadcastTxHashes[i], rawTxHashBytes)
				continue
			} else {
				// Otherwise broadcast a rollback operation if some of the healthy
				// relays already added this transaction to their mempool.
				// The mempool calls [Acceptor#RollbackTx] before removing txes.

				// TODO(midas): remove debug logs
				b.logger.Debug("Cancelling broadcast request",
					"chain_id", chainID,
					"tx_hash", txHash,
				)

				routineCancelBroadcast := b.GetRoutines().CancelBroadcast
				go routineCancelBroadcast(ctx, userAddress, transactions)

				// Also call RollbackTx extension locally and remove from mempool.
				if err := b.acceptor.RollbackTx(ctx, userAddress, transactions...); err == nil {
					// Remove the transactions from local mempool.
					b.RemoveTransactions(userAddress, transactions...)
				}

				// We are missing some relays' acceptance, fail here.
				client.Error(notifyCh, fmt.Errorf(
					"other relays failed to accept transaction %s", txHash))
				return // terminates the process
			}
		}
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
		eventsSwitch := b.reactor.GetEventSwitchForCometBFT()

		// Iterate through transactions and broadcast rollback operations
		// for each of them to all other relays.
		for _, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)

			// Broadcast the rollback message for this transaction to all relays.
			eventsSwitch.Broadcast(chainID, p2p.Envelope{
				ChannelID: mempl.MempoolChannel,
				Message: &memp2p.Message{
					Sum: &memp2p.Message_RollbackTxs{
						RollbackTxs: &memp2p.RollbackTxs{
							Txs: [][]byte{rawTx},
						},
					},
				},
			})
		}
	}
}
