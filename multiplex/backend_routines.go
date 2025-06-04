package multiplex

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	memp2p "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
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
			DiscoveryDialer: b.DefaultDiscoveryDialerRoutine(),
			CometBFTDialer:  b.DefaultCometBFTDialerRoutine(),
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

// DefaultDiscoveryDialerRoutine dials relays to enable discovery messages
// on [server.ReplicationChannel].
//
// This method checks for compatibility of relays by executing a connection
// handshake as defined with [p2p.Switch#DialPeerWithAddress]. The discovery
// switch is updated to accept [mxp2p.ChainReplicationRequest] messages.
func (b *MultiplexBackend) DefaultDiscoveryDialerRoutine() server.DiscoveryDialerFn {
	return func(
		ctx context.Context,
		relays []*server.RelayAddress,
		waitGroup *sync.WaitGroup,
		errorsCh chan<- server.RelayDialError,
		logger cmtlog.Logger,
	) {
		// We dial using discovery, init'd in [MultiplexBackend#MustStart].
		discoverySwitch := b.reactor.GetEventSwitchForDiscovery()

		// Concurrently dial relays to enable ReplicationChannel messages.
		// CheckDialCompatibleRelay opens connection for `DiscoveryPort`.
		for _, relayAddr := range relays {
			go func(sw *p2p.Switch, addr *server.RelayAddress) {
				defer waitGroup.Done()
				startTz := time.Now()

				// Uses the local P2P switch to dial a remote peer.
				if err := b.CheckDialCompatibleRelay(ctx, sw, addr); err != nil {
					errorsCh <- server.RelayDialError{
						Addr:  addr,
						Error: err,
					}
					return
				}
				durationMs := time.Since(startTz).Milliseconds()

				// TODO(midas): remove debug logs
				logger.Debug("Successfully dialed relay for discovery",
					"relay", addr.String(),
					"time", strconv.Itoa(int(durationMs))+"ms",
				)
			}(discoverySwitch, relayAddr)
		}
	}
}

// DefaultCometBFTDialerRoutine dials relays to enable CometBFT messages.
//
// This method checks for compatibility of relays by executing a connection
// handshake as defined with [p2p.Switch#DialPeerWithAddress]. The CometBFT
// switch is updated to accept blocksync, consensus and mempool messages.
func (b *MultiplexBackend) DefaultCometBFTDialerRoutine() server.CometBFTDialerFn {
	return func(
		ctx context.Context,
		relays []*server.RelayAddress,
		relevantChainIds []string,
		waitGroup *sync.WaitGroup,
		errorsCh chan<- server.RelayDialError,
		logger cmtlog.Logger,
	) {
		// We dial using CometBFT, init'd in [MultiplexBackend#MustStart].
		cometbftSwitch := b.reactor.GetEventSwitchForCometBFT()

		// Concurrently dial relays to enable CometBFT messages.
		// Opens peer connections for `DiscoveryPort+1`.
		for _, relayAddr := range relays {
			cometbftAddr, _ := server.NewRelayAddress(relayAddr.AddressForCometBFT())
			for _, relevantChainID := range relevantChainIds {
				// TODO(midas): remove debug logs
				logger.Debug("Now dialing relay for CometBFT",
					"relay", cometbftAddr.String(),
					"chain_id", relevantChainID,
				)

				go func(sw *p2p.Switch, addr *server.RelayAddress, chainID string) {
					defer waitGroup.Done()
					startTz := time.Now()

					if err := b.reactor.DialRelayForScope(sw, addr, chainID); err != nil {
						errorsCh <- server.RelayDialError{
							Addr:  addr,
							Error: err,
						}
						return
					}
					durationMs := time.Since(startTz).Milliseconds()

					// TODO(midas): remove debug logs
					logger.Debug("Successfully dialed relay for CometBFT",
						"relay", addr.String(),
						"time", strconv.Itoa(int(durationMs))+"ms",
					)
				}(cometbftSwitch, cometbftAddr, relevantChainID)
			}
		}
	}
}

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
		logger cmtlog.Logger,
	) {
		// ReplicationChannel is listened on discovery.
		discoverySwitch := b.reactor.GetEventSwitchForDiscovery()

		requestSentPeerIds := []string{}
		replRequestPeerIds := []string{}
		for _, relayAddr := range relays {
			if relayAddr.HasID() {
				replRequestPeerIds = append(replRequestPeerIds, string(relayAddr.ID()))
			}
		}

		// Retrieve the GenesisDoc for this chain
		genesisDocProvider := b.reactor.GetGenesisProvider()
		genesisDoc, err := genesisDocProvider(chainID)
		if err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"failed to load genesis doc in NodeReplRequest: %w", err))
			return // terminates the process
		}

		// Build a transportable ChainParams protobuf message
		chainParams, err := GenesisDocToChainParams(*genesisDoc)
		if err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"failed to format genesis doc in NodeReplRequest: %w", err))
			return // terminates the process
		}

		// Note that this events switch uses `DiscoveryPort`.
		discoveryPeers := discoverySwitch.Peers(p2p.ScopeForDiscovery)

		// Send only to relays we are interested in.
		peersAvailable := discoveryPeers.Copy()
		peersForRequests := slices.DeleteFunc(peersAvailable, func(p *p2p.PeerImpl) bool {
			return !slices.Contains(replRequestPeerIds, string(p.ID())) ||
				(!p.IsOutbound() && discoveryPeers.HasOutbound(p.ID()))
		})

		// TODO(midas): remove debug logs
		logger.Debug("Preparing to send ChainReplicationRequest",
			"chain_id", chainID,
			"relays", relays,
			"num_validators", len(genesisDoc.Validators),
			"num_requests", len(peersForRequests),
			"num_peers", discoveryPeers.Size(),
		)

		requestsWg := sync.WaitGroup{}
		requestsWg.Add(len(peersForRequests))

		// Broadcast the ChainReplicationRequest.
		for _, p := range peersForRequests {
			func(peer *p2p.PeerImpl) {
				defer requestsWg.Done()

				peerID := string(peer.ID())

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

				requestSentPeerIds = append(requestSentPeerIds, peerID)
			}(p)
		}

		// Waits to have sent all ChainReplicationRequest.
		requestsWg.Wait()

		// Keep track of node IDs
		b.replRequestsMtx.Lock()
		b.replRequestsSent[chainID] = requestSentPeerIds
		b.replRequestsMtx.Unlock()

		// TODO(midas): remove debug logs
		logger.Debug("Done sending ChainReplicationRequest to peers",
			"chain_id", chainID,
			"num_sent", len(requestSentPeerIds),
			"relay_ids", requestSentPeerIds,
		)
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
		genesisWg *sync.WaitGroup,
		logger cmtlog.Logger,
	) error {
		// Find out if any of the relays told us about some missing networks,
		// in this case, this is NOT a new network and our relay needs sync.
		unknownNetworks := []string{}
		for _, missingChainID := range missingChains {
			if _, ok := relaysByChain[missingChainID]; !ok {
				unknownNetworks = append(unknownNetworks, missingChainID)
				continue
			}

			// This is NOT a new network (existing on some relay)
			genesisWg.Done()
		}

		// If possible, terminate here.
		if len(unknownNetworks) == 0 {
			return nil
		}

		// We must create at least one NEW network.
		// TODO(midas): TBD on concurrently creating the networks.
		for _, newChainID := range unknownNetworks {
			err := func() (err error) {
				// Recover from potential panic in below block due to inability to create
				// a new network. This recovery ensures that the client implementation is
				// able to react to errors happening in the process, any errors here must
				// terminate the broadcast process as it is effectively invalidated here.
				defer func() {
					defer genesisWg.Done()

					if errRecovered := recover(); errRecovered != nil {
						// Error happened in MustCreateNetwork process.
						err = errRecovered.(error)

						// The reactor will have pushed on createErr already.
						logger.Error(fmt.Errorf(
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
				return fmt.Errorf(
					"could not create required networks: %w", err)
			}
		}

		return nil
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
		waitGroup *sync.WaitGroup,
		errorsCh chan<- error,
		logger cmtlog.Logger,
	) {
		broadcastTxHashes := make([][]byte, len(transactions))

		// (1)
		// First dial the CometBFT P2P addresses to make sure
		// communication with this relay is possible using mempool.
		numExpectedAcksByChain := make(map[string]int, len(relaysByChain))
		for chainID, relays := range relaysByChain {
			relaysWithoutSelf := []*server.RelayAddress{}
			for _, relayAddr := range relays {
				if relayAddr.ID() != b.reactor.GetNodeKey().ID() {
					relaysWithoutSelf = append(relaysWithoutSelf, relayAddr)
				}
			}

			numExpectedAcksByChain[chainID] = len(relaysWithoutSelf)
		}

		// If we error, or the broadcast is done for all txes and all relays,
		// then we may unlock the broadcast process from caller.
		defer waitGroup.Done()

		// (2)
		// Iterate through transaction and broadcast each of them to other relays
		for i, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			poolRequestPeers := []string{}
			numBroadcastDone := 0

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			// Reset the sent requests cache for this txHash
			b.reactor.poolRequestsMtx.Lock()
			if _, ok := b.reactor.poolRequestsSent[txHash]; ok {
				b.reactor.poolRequestsSent[txHash] = []string{}
			}
			b.reactor.poolRequestsMtx.Unlock()

			// Broadcast must happen only if there is at least one healthy relay.
			// For NEW networks, we don't need to wait for acknowledgments.
			if _, ok := relaysByChain[chainID]; !ok {
				peers := []string{}
				for _, relayAddr := range replReqRelays[chainID] {
					peers = append(peers, string(relayAddr.ID()))
				}

				// Considers the relays as "ack'd", given no need to broadcast.
				b.reactor.poolRequestsMtx.Lock()
				b.reactor.poolRequestsSent[txHash] = peers
				b.reactor.poolRequestsMtx.Unlock()

				// We count as ACK'd *but* we must broadcast the tx to the relay,
				// otherwise the relay won't activate "us" (inbound for "them")
				// in consensus and blocksync reactors.
				numExpectedAcksByChain[chainID] = 0
			}

			minNumBroadcastByChain := numExpectedAcksByChain[chainID]

			// Force the execution of mempool broadcast to *all* healthy relays.
			// chainHealthyRelays is used to filter relevant peer IDs.
			chainHealthyPeers := []string{}
			chainReplPartners := []string{}
			for _, relayAddr := range relaysByChain[chainID] {
				if relayAddr.ID() != b.GetRelayID() {
					chainHealthyPeers = append(chainHealthyPeers, string(relayAddr.ID()))
				}
			}
			// Add replication partners to the healthy relays.
			for _, relayAddr := range replReqRelays[chainID] {
				chainHealthyPeers = append(chainHealthyPeers, string(relayAddr.ID()))
				chainReplPartners = append(chainReplPartners, string(relayAddr.ID()))

				// If only some of the relays must replicate, we can't wait
				// for them acknowledge the transaction now - they will get
				// the transaction by the blocksync process during replication.
				if minNumBroadcastByChain > 0 {
					minNumBroadcastByChain--
				}
			}

			cometbftSwitch := b.reactor.GetEventSwitchForCometBFT()
			chainPeerSet := cometbftSwitch.Peers(chainID)

			// Send only to relays we are interested in.
			peersAvailable := chainPeerSet.Copy()
			peersForMempool := slices.DeleteFunc(peersAvailable, func(p *p2p.PeerImpl) bool {
				return !slices.Contains(chainHealthyPeers, string(p.ID())) ||
					(!p.IsOutbound() && chainPeerSet.HasOutbound(p.ID()))
			})

			if minNumBroadcastByChain > len(peersForMempool) {
				logger.Error("Not enough peers to satisfy remote acceptance",
					"chain_id", chainID,
					"tx_hash", txHash,
					"min_accept", minNumBroadcastByChain,
					"num_peers", len(peersForMempool),
				)

				errorsCh <- fmt.Errorf(
					"not enough peers to accept transaction %s", txHash)
				return // terminates the process
			}

			sentWg := sync.WaitGroup{}
			sentWg.Add(len(peersForMempool))

			// TODO(midas): remove debug logs
			logger.Debug("Preparing to send mempool.Tx",
				"chain_id", chainID,
				"tx_hash", txHash,
				"min_accept", minNumBroadcastByChain,
				"num_broadcast", len(peersForMempool),
				"num_peers_chain", chainPeerSet.Size(),
			)

			// TODO(midas): Send inside goroutine for max concurrency.

			// Broadcast the transaction to all healthy relays.
			for _, p := range peersForMempool {
				func(peer *p2p.PeerImpl) {
					defer sentWg.Done()

					mempoolPartnerPeerID := string(peer.ID())
					isReplicationPartner := slices.Contains(chainReplPartners, mempoolPartnerPeerID)

					// Expect an ACK from any remote relays which are not
					// handling a replication request.
					if !isReplicationPartner {
						poolRequestPeers = append(poolRequestPeers, mempoolPartnerPeerID)
					}

					// TODO(midas): remove debug logs
					logger.Debug("Sending transaction to remote mempool",
						"chain_id", chainID,
						"tx_hash", txHash,
						"peer", peer,
						"is_outbound", peer.IsOutbound(),
						"is_running", peer.IsRunning(),
					)

					// Send transaction to relay mempool, after checks the mempool
					// reactor shall send a AckTransactionBroadcast back to us.
					if success := peer.Send(chainID, p2p.Envelope{
						ChannelID: mempl.MempoolChannel,
						Message:   &memp2p.Txs{Txs: [][]byte{rawTx}},
					}); !success {
						logger.Error("failed to send transaction to remote mempool",
							"chain_id", chainID,
							"tx_hash", txHash,
							"peer", peer,
							"is_outbound", peer.IsOutbound(),
							"is_running", peer.IsRunning(),
						)

						// Note: we an error on the errorsCh channel but we don't
						// terminate the process because a failure in sending to
						// one relay must not prevent the transaction broadcast.
						errorsCh <- fmt.Errorf(
							"could not send message on mempool channel for tx %s with ChainID %s", txHash, chainID)
						return
					}

					// Counts healthy ACK partners (not replicating).
					if !isReplicationPartner {
						numBroadcastDone++
					}
				}(p)
			}

			// Waits until we have sent to all required peers
			sentWg.Wait()

			b.reactor.poolRequestsMtx.Lock()
			b.reactor.poolRequestsSent[txHash] = poolRequestPeers
			b.reactor.poolRequestsMtx.Unlock()

			// We require healthy relays to accept this broadcast as a whole,
			// thus fails if any transaction can't get ACK'd by enough relays.
			if numBroadcastDone >= minNumBroadcastByChain {
				// TODO(midas): remove debug logs
				logger.Debug("Done broadcasting to remote mempools",
					"chain_id", chainID,
					"tx_hash", txHash,
					"min_accept", minNumBroadcastByChain,
					"num_accept", numBroadcastDone,
					"total_sent", len(peersForMempool),
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
				logger.Error("Cancelling broadcast request",
					"chain_id", chainID,
					"tx_hash", txHash,
					"min_accept", minNumBroadcastByChain,
					"num_accept", numBroadcastDone,
				)

				errorsCh <- ErrBroadcastCancelled{
					TxHash: txHash,
				}
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
		logger cmtlog.Logger,
	) {
		eventsSwitch := b.reactor.GetEventSwitchForCometBFT()
		if eventsSwitch == nil {
			// TODO(midas): remove debug logs
			logger.Error("Failed to send RollbackTxs messages - CometBFT switch is not ready",
				"reactor_up", b.reactor.IsRunning(),
			)
			return
		}

		// Iterate through transactions and broadcast rollback operations
		// for each of them to all other relays.
		for _, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			chainPeerSet := eventsSwitch.Peers(chainID)
			if chainPeerSet.Size() == 0 {
				// TODO(midas): remove debug logs
				logger.Error("Failed to send RollbackTxs message for transaction - empty peerset",
					"chain_id", chainID,
					"tx_hash", txHash,
				)
				continue
			}

			// TODO(midas): remove debug logs
			logger.Debug("Sending RollbackTxs message to remote mempools",
				"chain_id", chainID,
				"tx_hash", txHash,
				"num_peers", chainPeerSet.Size(),
			)

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
