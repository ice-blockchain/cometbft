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
					"chainId", relevantChainID,
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
			"chainId", chainID,
			"relays", relays,
			"numValidators", len(genesisDoc.Validators),
			"numRequests", len(peersForRequests),
			"numPeers", discoveryPeers.Size(),
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
					"chainId", chainID,
					"peerId", peerID,
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
			"chainId", chainID,
			"numSent", len(requestSentPeerIds),
			"relayIds", requestSentPeerIds,
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
		clientCtx context.Context,
		relaysByChain map[string][]*server.RelayAddress,
		missingChains []string,
		validatorsByChain map[string][]string,
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
							"CLIENT PANIC encountered with InjectNewNetwork: %w",
							errRecovered.(error),
						).Error())
					}
				}()

				var otherValPubKeys []string
				if valPubKeys, ok := validatorsByChain[newChainID]; ok {
					otherValPubKeys = valPubKeys[:]
				} else {
					otherValPubKeys = []string{}
				}

				// TODO(midas): remove debug logs
				logger.Debug("Injecting new ChainID with validators",
					"chainId", newChainID,
					"numVals", len(otherValPubKeys)+1,
				)

				// AllocateNetwork is NOT part of InjectNewNetwork anymore.
				if err = b.reactor.AllocateNetwork(newChainID); err != nil {
					return err
				}

				// Create the network genesis, state machine, etc.
				err = b.reactor.InjectNewNetwork(newChainID, otherValPubKeys)
				if err != nil {
					return err
				}

				// Inject a *running* node.Node for the new network.
				// TODO(midas): currently not passing any node options.
				return b.reactor.InjectNewRuntime(b.Context(), newChainID)
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
		logger cmtlog.Logger,
	) {
		// If we error, or the broadcast is done for all txes and all relays,
		// then we may unlock the broadcast process from caller.
		defer waitGroup.Done()

		// (1)
		// Iterate through transaction and broadcast each of them to other relays
		for _, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			poolRequestPeers := []string{}

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			// Reset the sent requests cache for this txHash
			b.reactor.poolRequestsMtx.Lock()
			if _, ok := b.reactor.poolRequestsSent[txHash]; ok {
				b.reactor.poolRequestsSent[txHash] = []string{}
			}
			b.reactor.poolRequestsMtx.Unlock()

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
			}

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
			}

			cometbftSwitch := b.reactor.GetEventSwitchForCometBFT()
			chainPeerSet := cometbftSwitch.Peers(chainID)

			// Send only to relays we are interested in.
			peersForMempool := chainPeerSet.Copy()

			sentWg := sync.WaitGroup{}
			sentWg.Add(len(peersForMempool))

			// TODO(midas): remove debug logs
			logger.Debug("Preparing to send mempool.Tx",
				"chainId", chainID,
				"txHash", txHash,
				"numBroadcast", len(peersForMempool),
				"numPeersChain", chainPeerSet.Size(),
			)

			// TODO(midas): Send inside goroutine for max concurrency.

			// Broadcast the transaction to all healthy relays.
			for _, p := range peersForMempool {
				func(peer *p2p.PeerImpl) {
					defer sentWg.Done()

					mempoolPartnerPeerID := string(peer.ID())
					isReplicationPartner := slices.Contains(chainReplPartners, mempoolPartnerPeerID)
					hasSentToPeerID := slices.Contains(poolRequestPeers, mempoolPartnerPeerID)

					// Expect an ACK from any remote relays which are not
					// handling a replication request.
					if !isReplicationPartner && !hasSentToPeerID {
						poolRequestPeers = append(poolRequestPeers, mempoolPartnerPeerID)
					} else if hasSentToPeerID || !peer.IsRunning() {
						return
					}

					// TODO(midas): remove debug logs
					logger.Debug("Sending transaction to remote mempool",
						"chainId", chainID,
						"txHash", txHash,
						"peer", peer,
						"isOutbound", peer.IsOutbound(),
						"isRunning", peer.IsRunning(),
					)

					// Send transaction to relay mempool, after checks the mempool
					// reactor shall send a AckTransactionBroadcast back to us.
					if success := peer.Send(chainID, p2p.Envelope{
						ChainID:   chainID,
						ChannelID: mempl.MempoolChannel,
						Message:   &memp2p.Txs{Txs: [][]byte{rawTx}},
					}); !success {
						logger.Error("failed to send transaction to remote mempool",
							"chainId", chainID,
							"txHash", txHash,
							"peer", peer,
							"isOutbound", peer.IsOutbound(),
							"isRunning", peer.IsRunning(),
						)
						return
					}
				}(p)
			}

			// Waits until we have sent to all required peers
			sentWg.Wait()

			b.reactor.poolRequestsMtx.Lock()
			b.reactor.poolRequestsSent[txHash] = poolRequestPeers
			b.reactor.poolRequestsMtx.Unlock()
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
					"chainId", chainID,
					"txHash", txHash,
				)
				continue
			}

			// TODO(midas): remove debug logs
			logger.Debug("Sending RollbackTxs message to remote mempools",
				"chainId", chainID,
				"txHash", txHash,
				"numPeers", chainPeerSet.Size(),
			)

			// Broadcast the rollback message for this transaction to all relays.
			eventsSwitch.Broadcast(chainID, p2p.Envelope{
				ChainID:   chainID,
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
