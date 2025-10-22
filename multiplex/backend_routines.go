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
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// Routines returns an implementation of [types.Jobs] methods with
// the default methods implemented in [MultiplexBackend].
func (b *MultiplexBackend) Routines() *types.Jobs {
	if b.routines == nil {
		b.routines = &types.Jobs{
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
// on [types.ReplicationChannel].
func (b *MultiplexBackend) DefaultDiscoveryDialerRoutine() types.DiscoveryDialerFn {
	return func(
		ctx context.Context,
		relays []*helpers.RelayAddress,
		waitGroup *sync.WaitGroup,
		errorsCh chan<- types.RelayDialError,
		logger cmtlog.Logger,
	) {
		// Concurrently dial relays to enable ReplicationChannel messages.
		// CheckDialCompatibleRelay opens connection for `DiscoveryPort`.
		for _, relayAddr := range relays {
			go func(addr *helpers.RelayAddress) {
				defer waitGroup.Done()
				startTz := time.Now()

				// Uses the discovery pool to dial a remote peer.
				if err := b.CheckDialCompatibleRelay(ctx, addr); err != nil {
					errorsCh <- types.RelayDialError{
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
			}(relayAddr)
		}
	}
}

// DefaultCometBFTDialerRoutine dials relays to enable CometBFT messages.
func (b *MultiplexBackend) DefaultCometBFTDialerRoutine() types.CometBFTDialerFn {
	return func(
		ctx context.Context,
		relays []*helpers.RelayAddress,
		relevantChainIds []string,
		waitGroup *sync.WaitGroup,
		errorsCh chan<- types.RelayDialError,
		logger cmtlog.Logger,
	) {
		// Concurrently dial relays to enable CometBFT messages.
		// Opens peer connections for `DiscoveryPort+1`.
		for _, relayAddr := range relays {
			cometbftAddr, _ := helpers.NewRelayAddress(relayAddr.AddressForCometBFT())
			for _, relevantChainID := range relevantChainIds {
				// TODO(midas): remove debug logs
				logger.Debug("Now dialing relay for CometBFT",
					"relay", cometbftAddr.String(),
					"chainId", relevantChainID,
				)

				go func(addr *helpers.RelayAddress, chainID string) {
					defer waitGroup.Done()
					startTz := time.Now()

					netAddress := addr.NetAddress()
					if _, err := b.cometbftPool.Connector().Dial(netAddress); err != nil {
						errorsCh <- types.RelayDialError{
							Addr:  addr,
							Error: err,
						}
						return
					}
					b.cometbftPool.SetPeerForChainID(addr.ID(), relevantChainID)

					durationMs := time.Since(startTz).Milliseconds()

					// TODO(midas): remove debug logs
					logger.Debug("Successfully dialed relay for CometBFT",
						"relay", addr.String(),
						"time", strconv.Itoa(int(durationMs))+"ms",
					)
				}(cometbftAddr, relevantChainID)
			}
		}
	}
}

// DefaultNodeReplRequestRoutine asks relays to replicate a network by
// attaching the corresponding ChainParams.
//
// This method broadcasts a [mxp2p.ChainReplicationRequest] message to
// catchupRelays, to ask them to replicate a chain using the ChainParams.
//
// remoteRelays should contain a list of all the relays' discovery addresses,
// including "self" - i.e. the sender relay.
// catchupRelays should contain a list of the relays that shall receive
// a chain replication request - i.e. these relays must "catch-up".
func (b *MultiplexBackend) DefaultNodeReplRequestRoutine() types.NodeReplRequestFn {
	return func(
		_ context.Context,
		remoteRelays []*helpers.RelayAddress,
		catchupRelays []*helpers.RelayAddress,
		chainID string,
		notifyCh chan<- client.BroadcastStatus,
		logger cmtlog.Logger,
	) {
		requestSentPeerIds := []string{}
		replRequestPeerIds := []string{}
		for _, relayAddr := range catchupRelays {
			if relayAddr.HasID() {
				replRequestPeerIds = append(replRequestPeerIds, string(relayAddr.ID()))
			}
		}
		healthyRemoteRelays := make([]*helpers.RelayAddress, 0, len(remoteRelays))
		for _, relayAddr := range remoteRelays {
			if relayAddr != nil && relayAddr.HasID() {
				healthyRemoteRelays = append(healthyRemoteRelays, relayAddr)
			}
		}

		genesisDoc := b.runtimeRegistry.Composer().GenesisDoc(chainID)

		// Build a transportable ChainParams protobuf message
		chainParams, err := helpers.GenesisDocToChainParams(genesisDoc)
		if err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"failed to format genesis doc in NodeReplRequest: %w", err))
			return // terminates the process
		}

		// For each ChainID that requires replication of at least one relay,
		// we initialize the replication manager to evaluate with correct relays.
		if err = b.replicationMgr.Init(chainID, catchupRelays); err != nil {
			client.Error(notifyCh, fmt.Errorf(
				"failed to initialize replication in NodeReplRequest: %w", err))
			return // terminates the process
		}

		discoveryPeers := b.discoveryPool.Peers()
		replicationReq := &mxp2p.ChainReplicationRequest{
			ChainID:     chainID,
			ChainParams: chainParams,
		}

		// TODO(midas): remove debug logs
		logger.Debug("Preparing to send ChainReplicationRequest",
			"chainId", chainID,
			"relays", catchupRelays,
			"numValidators", len(genesisDoc.Validators),
			"numPeers", discoveryPeers.Size(),
			"dialRelays", healthyRemoteRelays,
		)

		requestsWg := sync.WaitGroup{}
		requestsWg.Add(len(catchupRelays))

		// Broadcast the ChainReplicationRequest.
		for _, relay := range catchupRelays {
			func(addr *helpers.RelayAddress) {
				defer requestsWg.Done()

				peer := discoveryPeers.Get(addr.ID())
				peerID := string(addr.ID())

				// The receiving end (peerID) will have to dial all other
				// CometBFT peers that are involved in this broadcast to permit
				// faster consensus build-up and reaching consensus faster.
				cometbftPeers := make([]string, 0, len(healthyRemoteRelays))
				for _, relayDiscoveryAddr := range healthyRemoteRelays {
					if relayDiscoveryAddr.ID() != addr.ID() {
						cometbftPeers = append(cometbftPeers, relayDiscoveryAddr.AddressForCometBFT())
					}
				}
				replicationReq.Relays = cometbftPeers

				// TODO(midas): remove debug logs
				b.logger.Debug("Now sending ChainReplicationRequest",
					"chainId", chainID,
					"peerId", peerID,
					"relays", cometbftPeers,
				)

				e := cmtp2p.Envelope{
					ChainID:   chainID,
					ChannelID: types.ReplicationChannel,
					Message: &mxp2p.Message{
						Sum: &mxp2p.Message_ChainReplicationRequest{
							ChainReplicationRequest: replicationReq,
						},
					},
				}
				peer.Send(chainID, e)
				b.replicationMgr.Process(addr.ID(), e)

				requestSentPeerIds = append(requestSentPeerIds, peerID)
			}(relay)
		}

		// Waits to have sent all ChainReplicationRequest.
		requestsWg.Wait()

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
func (b *MultiplexBackend) DefaultNetworksCreatorRoutine() types.NetworksCreatorFn {
	return func(
		clientCtx context.Context,
		relaysByChain map[string][]*helpers.RelayAddress,
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

				// InitRuntime orchestrates newChainID using [types.RuntimeComposer].
				if err = b.runtimeRegistry.InitRuntime(
					newChainID,
					otherValPubKeys,
					true, // createNetworkGenesis
				); err != nil {
					return err
				}

				return nil
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
func (b *MultiplexBackend) DefaultRelaysBroadcastRoutine() types.RelaysBroadcastFn {
	return func(
		ctx context.Context,
		relaysByChain map[string][]*helpers.RelayAddress,
		replReqRelays map[string][]*helpers.RelayAddress,
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

			// For NEW networks, we don't need to wait for acknowledgments.
			if _, ok := relaysByChain[chainID]; !ok {
				// We won't wait for AckTransactionBroadcast.
				b.broadcastMgr.Init(chainID, txHash, []*helpers.RelayAddress{})
			} else {
				// For each TxHash that requires ACK of at least one relay,
				// we initialize the broadcast manager to evaluate with correct relays.
				b.broadcastMgr.Init(chainID, txHash, relaysByChain[chainID])
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

			// Send only to relays we are interested in.
			chainPeerSet := b.cometbftPool.Peers(chainID)
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
				go func(peer *p2p.PeerImpl) {
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
		}
	}
}

// DefaultCancelBroadcastRoutine broadcasts a rollback message to healthy relays
// in case any of the relays has already included the transactions in their
// mempool. The mempool should call [Acceptor#RollbackTx] upon receiving this
// message.
func (b *MultiplexBackend) DefaultCancelBroadcastRoutine() types.CancelBroadcastFn {
	return func(
		ctx context.Context,
		userAddress string,
		transactions []client.Transaction,
		logger cmtlog.Logger,
	) {
		// Iterate through transactions and broadcast rollback operations
		// for each of them to all other relays.
		for _, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			chainPeerSet := b.cometbftPool.Peers(chainID)
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
			b.cometbftPool.Broadcast(cmtp2p.Envelope{
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
