package multiplex

import (
	"context"
	"fmt"
	"strings"

	protomem "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/p2p"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// -----------------------------------------------------------------------------
// Reactor

// The Reactor implementation processes multiplex messages, about
// chain replications and/or transaction broadcast operations.
//
// i.e. the [Reactor#Receive] method reacts to messages of types:
// - [mxp2p.ChainReplicationStatus]
// - [mxp2p.ChainReplicationRequest]
// - [mxp2p.ChainReplicationResponse]
// - [mxp2p.AckTransactionBroadcast]
type Reactor struct {
	cmtp2p.BaseReactor // BaseService + p2p.Switch

	// Resources
	nodeKey *cmtp2p.NodeKey
	nodeCfg *config.Config

	// Services
	resourceMgr    types.ResourceManager
	replicationMgr types.ReplicationManager
	broadcastMgr   types.BroadcastManager
	runtimeMgr     *runtime.Registry
	discoveryPool  *p2p.ConnectionPool
	cometbftPool   *p2p.ConnectionPool

	// Internal
	logger cmtlog.Logger

	// Precalculated max message sizes.
	broadcastRecvMessageCapacity int
	runtimeRecvMessageCapacity   int
	recvMempoolTxMessageCapacity int
}

type ReactorOption func(*Reactor)

func NewReactor(
	ctx context.Context,
	nodeKey *cmtp2p.NodeKey,
	nodeCfg *config.Config,
	resourceMgr types.ResourceManager,
	replicationMgr types.ReplicationManager,
	broadcastMgr types.BroadcastManager,
	logger cmtlog.Logger,
	options ...ReactorOption,
) *Reactor {
	reactor := &Reactor{
		resourceMgr: resourceMgr,
		nodeKey:     nodeKey,
		logger:      logger,
	}

	// Enable overwrite of some optional properties.
	for _, option := range options {
		option(reactor)
	}

	reactor.BaseReactor = *cmtp2p.NewBaseReactor(ctx, "Multiplex", reactor)

	// Estimates max lengths for message payloads
	reactor.computePayloadSizes()

	return reactor
}

// ----------------------------------------------------------------------------

func (reactor *Reactor) SetOptions(options ...ReactorOption) {
	for _, option := range options {
		option(reactor)
	}
}

// GetLogger returns a [cmtlog.Logger] instance.
func (reactor *Reactor) GetLogger() cmtlog.Logger {
	return reactor.logger
}

// SetLogger sets a custom [cmtlog.Logger] instance.
func (reactor *Reactor) SetLogger(logger cmtlog.Logger) {
	reactor.logger = logger
}

// SetRuntimeManager sets a custom [*runtime.Registry] instance.
func (reactor *Reactor) SetRuntimeManager(reg *runtime.Registry) {
	reactor.runtimeMgr = reg
}

// SetDiscoveryPool sets a custom [*p2p.ConnectionPool] instance.
func (reactor *Reactor) SetDiscoveryPool(pool *p2p.ConnectionPool) {
	reactor.discoveryPool = pool
}

// SetRuntimePool sets a custom [*p2p.ConnectionPool] instance.
func (reactor *Reactor) SetRuntimePool(pool *p2p.ConnectionPool) {
	reactor.cometbftPool = pool
}

func (reactor *Reactor) IdleManager() types.IdleManager {
	return reactor.runtimeMgr
}

// ----------------------------------------------------------------------------
// Reactor implements cmtp2p.Reactor

// GetChannels implements cmtp2p.Reactor.
func (reactor *Reactor) GetChannels() []*cmtp2p.ChannelDescriptor {
	return []*cmtp2p.ChannelDescriptor{
		{
			ID: types.ReplicationChannel,
			// Lower priority than blocksync, evidence, mempool & consensus
			// i.e. This channel has priority to be gossiped on.
			Priority:    3,
			MessageType: &mxp2p.Message{},
		},
		{
			ID:          types.AckBroadcastChannel,
			Priority:    2,
			MessageType: &mxp2p.Receipt{},
			//RecvMessageCapacity: reactor.broadcastRecvMessageCapacity,
		},
		{
			ID:          types.RuntimeChannel,
			Priority:    10, // This channel does not have priority.
			MessageType: &mxp2p.Message{},
			//RecvMessageCapacity: reactor.runtimeRecvMessageCapacity,
		},
		{
			ID:                  mempl.MempoolChannel,
			Priority:            5,
			RecvMessageCapacity: reactor.recvMempoolTxMessageCapacity,
			MessageType:         &protomem.Message{},
		},
	}
}

// AddPeer implements cmtp2p.Reactor.
func (*Reactor) AddPeer(peer *cmtp2p.PeerImpl) {}

// RemovePeer implements cmtp2p.Reactor.
func (*Reactor) RemovePeer(peer *cmtp2p.PeerImpl, _ any) {}

// Receive implements cmtp2p.Reactor.
func (reactor *Reactor) Receive(e cmtp2p.Envelope) {
	reactor.logger.Debug("Receive", "src", e.Src, "chId", e.ChannelID, "chainID", e.ChainID)

	// CAUTION:
	//
	// Due to the MempoolChannel also being added to multiplex Reactor,
	// we must make sure that those messages are forwarded to running mempool.
	//
	// TODO(midas): refactor this with BaseReactor.ForwardMessage("MEMPOOL", e).
	if e.ChannelID == mempl.MempoolChannel && len(e.ChainID) > 0 {
		mempoolReactor, ok := reactor.resourceMgr.Get(
			e.ChainID,
			types.ServiceKeyMempoolReactor,
		).(*mempl.Reactor)
		if !ok || !mempoolReactor.IsRunning() {
			reactor.runtimeMgr.InitRuntime(e.ChainID, []string{})
			reactor.runtimeMgr.StartRuntime(e.ChainID)
		}

		// IMPORTANT:
		//
		// Forwards this message for processing to mempool.Reactor.
		// servicesProvider := r.GetServicesProvider()
		reactor.logger.Info("Forwarding Tx",
			"memR", mempoolReactor,
			"running", mempoolReactor.IsRunning(),
			"msg", e.Message)
		mempoolReactor.Receive(e)
		return // Forwarded
	}

	// Determine public source address from secret connection.
	sourcePeer := e.Src
	sourceAddr, err := sourcePeer.NodeInfo().NetAddress()
	if err != nil {
		reactor.logger.Error("ignoring message - failed to parse source address from message",
			"msg", e, "err", err)
		return
	}

	reactor.logger.Debug("Received from", "src", sourceAddr.String())

	switch extMsg := e.Message.(type) {
	// TODO(midas): ChainReplicationStatus
	// ChainReplicationRequest
	// ChainReplicationResponse
	// ChainReplicationComplete
	case *mxp2p.Message:
		// We shall track completeness of replications using a manager.
		reactor.replicationMgr.Process(e)

		msg := extMsg.GetSum()
		switch msg.(type) {
		// ChainReplicationRequest
		// Received a request to replicate a (new) chain.
		case *mxp2p.Message_ChainReplicationRequest:
			replRequest := extMsg.GetChainReplicationRequest()
			reactor.logger.Debug("Received ChainReplicationRequest", "msg", replRequest)

			// Process the chain replication request.
			if err := reactor.handleChainReplicationRequest(e); err != nil {
				reactor.logger.Error(
					"failed to process ChainReplicationRequest: error handling replication",
					"chainId", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// Dial the peer for CometBFT to permit faster consensus building.
			if _, err := reactor.cometbftPool.Connector().Dial(sourceAddr); err != nil {
				reactor.logger.Error(
					"failed to process ChainReplicationRequest: error dialing source peer",
					"chainId", replRequest.ChainID,
					"err", err,
				)
			}

			// Start consensus reactors for newly injected runtime.
			if err := reactor.runtimeMgr.StartRuntime(
				replRequest.ChainID,
			); err != nil {
				reactor.logger.Error(
					"failed to process ChainReplicationRequest: error starting consensus reactors",
					"chainId", replRequest.ChainID,
					"err", err,
				)
				return
			}

			// Activate this runtime in our runtime registry.
			//
			// In case of conR.WaitSync, OnComplete is called by conR.SwitchToConsensus,
			// otherwise OnComplete is called by memR.sendChainReplicationComplete when
			// transactions are successfully processed with memR.processTxs.
			reactor.IdleManager().OnActivate(replRequest.ChainID)

			// Dial the peer for Discovery as we will be sending a ChainReplicationResponse.
			if _, err := reactor.discoveryPool.Connector().Dial(sourceAddr); err != nil {
				reactor.logger.Error(
					"failed to process ChainReplicationRequest: error dialing source peer",
					"chainId", replRequest.ChainID,
					"err", err,
				)
			}

			// Now respond with a [ChainReplicationResponse].
			// This serves as a receipt for a chain replication request.
			if err = reactor.sendChainReplicationResponse(e.Src, replRequest.ChainID); err != nil {
				reactor.logger.Error("failed to send ChainReplicationResponse",
					"chainId", replRequest.ChainID,
					"from", reactor.nodeKey.ID(),
					"to", e.Src.ID(),
					"err", err,
				)
			}

			return

		// ChainReplicationResponse
		// Received a receipt of replication from one of the relays.
		case *mxp2p.Message_ChainReplicationResponse:
			replResponse := extMsg.GetChainReplicationResponse()
			reactor.logger.Debug("Received ChainReplicationResponse", "msg", replResponse)

			// Dial the peer for CometBFT to permit faster consensus building.
			if _, err := reactor.cometbftPool.Connector().Dial(sourceAddr); err != nil {
				reactor.logger.Error(
					"failed to process ChainReplicationRequest: error dialing source peer",
					"chainId", replResponse.ChainID,
					"err", err,
				)
			}

			return

		// ChainReplicationComplete
		// Received a receipt of replication completeness from a peer.
		case *mxp2p.Message_ChainReplicationComplete:
			replComplete := extMsg.GetChainReplicationComplete()
			reactor.logger.Debug("Received ChainReplicationComplete", "msg", replComplete)

			// NOTE: Don't dial back replication partner here, since we may
			// approach runtime idling due to completion of the replication.
			return

		default:
			reactor.logger.Error(
				"Unknown internal message type",
				"src", e.Src,
				"chainId", e.ChainID,
				"chId", e.ChannelID,
				"msg", e.Message,
			)
			return
		}

	case *mxp2p.Receipt:
		// We shall track completeness of broadcast operations using a manager.
		reactor.broadcastMgr.Process(e)

		msg := extMsg.GetSum()
		switch msg.(type) {
		// AckTransactionBroadcast
		// Received a receipt of relay mempool inclusion for a transaction hash.
		case *mxp2p.Receipt_AckTransactionBroadcast:
			ackTxBroadcast := extMsg.GetAckTransactionBroadcast()
			reactor.logger.Debug("Received AckTransactionBroadcast", "msg", ackTxBroadcast)

			return

		default:
			reactor.logger.Error(
				"Unknown internal receipt type",
				"src", e.Src,
				"chainId", e.ChainID,
				"chId", e.ChannelID,
				"msg", e.Message,
			)
			return
		}

	default:
		reactor.logger.Error(
			"Unknown message type",
			"src", e.Src,
			"chainId", e.ChainID,
			"chId", e.ChannelID,
			"msg", e.Message,
		)
		return
	}
}

// ----------------------------------------------------------------------------
// Reactor implements [cmtlibs.Service]

// OnStart starts the multiplex reactor.
func (reactor *Reactor) OnStart(ctx context.Context) error {
	// TODO(midas): remove debug logs
	reactor.logger.Debug("Starting multiplex reactor",
		"nodeId", reactor.nodeKey.ID(),
	)

	return nil
}

// OnStop stops the multiplex reactor.
func (reactor *Reactor) OnStop() {
	// TODO(midas): remove debug logs
	reactor.logger.Debug("Stopping multiplex reactor",
		"nodeId", reactor.nodeKey.ID(),
	)
}

// OnReset resets the multiplex reactor.
func (reactor *Reactor) OnReset(ctx context.Context) error {
	// TODO(midas): remove debug logs
	reactor.logger.Debug("Reset multiplex reactor",
		"nodeId", reactor.nodeKey.ID(),
	)
	return nil
}

// ----------------------------------------------------------------------------

// computePayloadSizes computes the max payload lengths for messages received
// with this reactor implementation.
func (reactor *Reactor) computePayloadSizes() {
	allocNodeId := make([]byte, cmtp2p.IDByteLength)
	allocChainID := strings.Join([]string{
		helpers.DefaultMultiplexPrefix,
		helpers.MakeAddress().String(),
		helpers.MakeFingerprint(""),
	}, "-")

	// Pre-allocate an example AckTransactionBroadcast message to realistically
	// estimate the message capacity needed for AckBroadcastChannel.
	{
		allocTxHash := make([]byte, tmhash.Size)
		ackTxMsg := mxp2p.Receipt{
			Sum: &mxp2p.Receipt_AckTransactionBroadcast{
				AckTransactionBroadcast: &mxp2p.AckTransactionBroadcast{
					ChainID: allocChainID,
					NodeId:  string(allocNodeId),
					TxHash:  allocTxHash,
				},
			},
		}
		reactor.broadcastRecvMessageCapacity = ackTxMsg.Size()
	}

	// Pre-allocate an example ChainReplicationComplete message to realistically
	// estimate the message capacity needed for RuntimeChannel.
	{
		runtimeUpdateMsg := mxp2p.Message{
			Sum: &mxp2p.Message_ChainReplicationComplete{
				ChainReplicationComplete: &mxp2p.ChainReplicationComplete{
					ChainID: allocChainID,
					NodeId:  string(allocNodeId),
				},
			},
		}
		reactor.runtimeRecvMessageCapacity = runtimeUpdateMsg.Size()
	}

	{
		largestTx := make([]byte, reactor.nodeCfg.Mempool.MaxTxBytes)
		batchMsg := protomem.Message{
			Sum: &protomem.Message_Txs{
				Txs: &protomem.Txs{Txs: [][]byte{largestTx}},
			},
		}
		reactor.recvMempoolTxMessageCapacity = batchMsg.Size()
	}
}

// sendChainReplicationResponse sends a ChainReplicationResponse.
// This response object may be used to determine that a relay acknowledges
// the replication of a chain it doesn't know yet.
//
// IMPORTANT: sourcePeerOut must be an outbound peer, otherwise errors.
func (reactor *Reactor) sendChainReplicationResponse(
	sourcePeerOut *cmtp2p.PeerImpl,
	chainID string,
) error {
	myPeerID := reactor.nodeKey.ID()

	// sendResponseToPeer is an internal helper to send a message on
	// ReplicationChannel with body ChainReplicationResponse.
	sendResponseToPeer := func(fromID cmtp2p.ID, toPeer *cmtp2p.PeerImpl, withChainID string) error {
		if success := toPeer.Send(withChainID, cmtp2p.Envelope{
			ChannelID: types.ReplicationChannel,
			Message: &mxp2p.Message{
				Sum: &mxp2p.Message_ChainReplicationResponse{
					ChainReplicationResponse: &mxp2p.ChainReplicationResponse{
						ChainID: withChainID,
						NodeId:  string(fromID),
					},
				},
			},
		}); !success {
			return fmt.Errorf(
				"could not send message to peer, sender: %s, recipient: %s",
				string(fromID), string(toPeer.ID()))
		}
		return nil
	}

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Sending ChainReplicationResponse to peer",
		"fromId", myPeerID,
		"toPeer", sourcePeerOut,
		"chainId", chainID,
	)

	// Note that dialing the peer *must* have happened before.
	if err := sendResponseToPeer(myPeerID, sourcePeerOut, chainID); err != nil {
		return err
	}

	return nil
}

// handleChainReplicationRequest processes a ChainReplicationRequest.
func (reactor *Reactor) handleChainReplicationRequest(
	e cmtp2p.Envelope,
) error {
	var replRequest *mxp2p.ChainReplicationRequest
	switch extMsg := e.Message.(type) {
	case *mxp2p.Message:
		msg := extMsg.GetSum()
		switch msg.(type) {
		case *mxp2p.Message_ChainReplicationRequest:
			replRequest = extMsg.GetChainReplicationRequest()
		default:
			return fmt.Errorf(
				"invalid message, expected ChainReplicationRequest, got %v", extMsg)
		}
	default:
		return fmt.Errorf(
			"invalid message, expected ChainReplicationRequest, got %v", extMsg)
	}

	// Parse the network genesis parameters from request.
	genesisDoc, err := helpers.GenesisDocFromChainParams(replRequest.GetChainParams())
	if err != nil {
		return fmt.Errorf(
			"invalid genesis parameters: %w", err)
	}

	// Inject genesis doc for runtime initialization.
	reactor.runtimeMgr.AddRuntime(replRequest.ChainID, genesisDoc)

	// Extract validator public keys.
	validatorPubKeys := make([]string, 0, len(genesisDoc.Validators))
	for _, validator := range genesisDoc.Validators {
		validatorPubKeys = append(validatorPubKeys, pubKeyToHex(validator.PubKey))
	}

	// Initializes the runtime services (not starting).
	reactor.runtimeMgr.InitRuntime(replRequest.ChainID, validatorPubKeys)

	return nil
}
