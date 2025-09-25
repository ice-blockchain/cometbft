package runtime

import (
	"context"
	"slices"
	"sync"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtp2p "github.com/ice-blockchain/cometbft/p2p"

	"github.com/ice-blockchain/cometbft/multiplex/helpers"
	"github.com/ice-blockchain/cometbft/multiplex/types"
)

// ReplicationPool defines a chain replication pool.
type ReplicationPool struct {
	service.BaseService
	mtx *sync.Mutex

	// A message pool is used to store incoming/outgoing messages
	// by type. This pool handles messages of types:
	// - mxp2p.ChainReplicationRequest
	// - mxp2p.ChainReplicationResponse
	// - mxp2p.ChainReplicationComplete
	pool *MessagePool

	acceptedChs map[string]chan struct{}
	completeChs map[string]chan struct{}

	relays   map[string][]*helpers.RelayAddress
	partners map[string][]cmtp2p.ID

	// Options
	logger cmtlog.Logger
}

// Ensure that our implementation satisfies interface.
var _ types.ReplicationManager = (*ReplicationPool)(nil)

type ReplicationPoolOption func(*ReplicationPool)

// NewReplicationManager creates a new database service.
func NewReplicationManager(
	ctx context.Context,
	logger cmtlog.Logger,
	options ...ReplicationPoolOption,
) types.ReplicationManager {
	mgr := &ReplicationPool{
		mtx:  new(sync.Mutex),
		pool: NewMessageManager(ctx, logger),

		acceptedChs: map[string]chan struct{}{},
		completeChs: map[string]chan struct{}{},

		relays:   map[string][]*helpers.RelayAddress{},
		partners: map[string][]cmtp2p.ID{},

		// Options
		logger: logger,
	}

	// Use option helpers
	mgr.SetOptions(options...)

	mgr.BaseService = *service.NewBaseService(ctx, logger, "ReplicationPool", mgr)
	return mgr
}

// ReplicationPoolWithLogger injects a custom logger instance.
func ReplicationPoolWithLogger(logger cmtlog.Logger) ReplicationPoolOption {
	return func(mgr *ReplicationPool) {
		mgr.logger = logger
	}
}

// ----------------------------------------------------------------------------
// ReplicationPool implements [service.Service]

// OnStart implements [service.Service] by opening a database.
func (mgr *ReplicationPool) OnStart(ctx context.Context) (err error) {
	// TODO(midas): persistence of replication status in database
	return nil
}

// OnStop implements [service.Service] by closing the database.
func (mgr *ReplicationPool) OnStop() {
	// TODO(midas): persistence of replication status in database
}

// OnReset implements [service.Service] by resetting the service.
func (mgr *ReplicationPool) OnReset(ctx context.Context) error {
	return nil
}

// ----------------------------------------------------------------------------
// ReplicationManager API implementation

// Init initializes a replication processor for chainID with relays.
func (mgr *ReplicationPool) Init(
	chainID string,
	relays []*helpers.RelayAddress,
) error {
	// TODO(midas): remove debug logs
	mgr.logger.Debug("ReplicationPool#Init",
		"chainId", chainID,
		"numRelays", len(relays),
	)

	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	prev, has := mgr.relays[chainID]
	if !has {
		prev = []*helpers.RelayAddress{}
	}

	prev = append(prev, relays...)
	mgr.relays[chainID] = prev
	return nil
}

// Process processes a received message e to the replication pool.
func (mgr *ReplicationPool) Process(peerID cmtp2p.ID, e cmtp2p.Envelope) error {
	// TODO(midas): remove debug logs
	mgr.logger.Debug("ReplicationPool#Process",
		"chainId", e.ChainID,
		"peerID", string(peerID),
		"msg", e.Message,
	)

	if e.Src != nil {
		// Adds incoming message to message pool.
		mgr.pool.AddIncoming(e)
	} else {
		// Adds outgoing message to message pool.
		mgr.pool.AddOutgoing(peerID, e)
	}

	// Also store the peer ID as a partner.
	mgr.mtx.Lock()
	mgr.addPartner(e.ChainID, peerID)
	mgr.mtx.Unlock()

	switch extMsg := e.Message.(type) {
	case *mxp2p.Message:
		msg := extMsg.GetSum()
		switch msg.(type) {
		// - ChainReplicationRequest
		// Nothing to do about ChainReplicationRequest.

		// - ChainReplicationResponse
		// We evaluate a potential acceptance super majority.
		case *mxp2p.Message_ChainReplicationResponse:
			replResponse := extMsg.GetChainReplicationResponse()

			mgr.mtx.Lock()
			isAccepted := mgr.evaluateAcceptanceMajority(replResponse.ChainID)
			mgr.mtx.Unlock()

			// Close the "Accepted" channel when we have 2/3+1 responses.
			if isAccepted {
				ch := mgr.Accepted(replResponse.ChainID)
				close(ch) // DONE!
			}

		case *mxp2p.Message_ChainReplicationComplete:
			replComplete := extMsg.GetChainReplicationComplete()

			mgr.mtx.Lock()
			hasCompleted := mgr.evaluateCompletionMajority(replComplete.ChainID)
			mgr.mtx.Unlock()

			// Close the "Complete" channel when we have 2/3+1 completions.
			if hasCompleted {
				ch := mgr.Completed(replComplete.ChainID)
				close(ch) // DONE!
			}
		}
	}

	return nil
}

// Partners returns a list of relay ID from replication partners.
func (mgr *ReplicationPool) Partners(chainID string) []cmtp2p.ID {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if p, ok := mgr.partners[chainID]; ok {
		return p
	}

	return []cmtp2p.ID{}
}

// Requests returns the outgoing ChainReplicationRequest by chainID.
func (mgr *ReplicationPool) Requests(chainID string) []*mxp2p.ChainReplicationRequest {
	responses := []*mxp2p.ChainReplicationRequest{}

	// Collect all chain replication requests.
	envelopes := mgr.pool.OutgoingByType("ChainReplicationRequest")
	if len(envelopes) == 0 {
		return responses
	}

	// Keep only relevant ones for this chainID.
	for _, envelope := range envelopes {
		if envelope.ChainID != chainID {
			continue
		}

		switch extMsg := envelope.Message.(type) {
		case *mxp2p.Message:
			msg := extMsg.GetSum()
			switch msg.(type) {
			case *mxp2p.Message_ChainReplicationRequest:
				replReq := extMsg.GetChainReplicationRequest()
				responses = append(responses, replReq)
			default:
				continue
			}
		default:
			continue
		}
	}

	return responses
}

// Responses returns the incoming ChainReplicationResponse by chainID.
func (mgr *ReplicationPool) Responses(chainID string) []*mxp2p.ChainReplicationResponse {
	responses := []*mxp2p.ChainReplicationResponse{}

	// Collect all chain replication responses.
	envelopes := mgr.pool.IncomingByType("ChainReplicationResponse")
	if len(envelopes) == 0 {
		return responses
	}

	// Keep only relevant ones for this chainID.
	for _, envelope := range envelopes {
		if envelope.ChainID != chainID {
			continue
		}

		switch extMsg := envelope.Message.(type) {
		case *mxp2p.Message:
			msg := extMsg.GetSum()
			switch msg.(type) {
			case *mxp2p.Message_ChainReplicationResponse:
				replResp := extMsg.GetChainReplicationResponse()
				responses = append(responses, replResp)
			default:
				continue
			}
		default:
			continue
		}
	}

	return responses
}

// Completions returns a list of incoming ChainReplicationComplete for chainID.
func (mgr *ReplicationPool) Completions(chainID string) []*mxp2p.ChainReplicationComplete {
	responses := []*mxp2p.ChainReplicationComplete{}

	// Collect all chain replication responses.
	envelopes := mgr.pool.IncomingByType("ChainReplicationComplete")
	if len(envelopes) == 0 {
		return responses
	}

	// Keep only relevant ones for this chainID.
	for _, envelope := range envelopes {
		if envelope.ChainID != chainID {
			continue
		}

		switch extMsg := envelope.Message.(type) {
		case *mxp2p.Message:
			msg := extMsg.GetSum()
			switch msg.(type) {
			case *mxp2p.Message_ChainReplicationComplete:
				replComp := extMsg.GetChainReplicationComplete()
				responses = append(responses, replComp)
			default:
				continue
			}
		default:
			continue
		}
	}

	return responses
}

// Accepted returns a channel, which is closed when chainID has 2/3+1 responses.
func (mgr *ReplicationPool) Accepted(chainID string) chan struct{} {
	mgr.mtx.Lock()
	acceptedCh, hasChannel := mgr.acceptedChs[chainID]
	mgr.mtx.Unlock()

	if !hasChannel {
		acceptedCh = make(chan struct{}, 1) // buffered

		mgr.mtx.Lock()
		mgr.acceptedChs[chainID] = acceptedCh
		mgr.mtx.Unlock()
	}

	return acceptedCh
}

// Completed returns a channel, which is closed when chainID has 2/3+1 completions.
func (mgr *ReplicationPool) Completed(chainID string) chan struct{} {
	mgr.mtx.Lock()
	completeCh, hasChannel := mgr.completeChs[chainID]
	mgr.mtx.Unlock()

	if !hasChannel {
		completeCh = make(chan struct{}, 1) // buffered

		mgr.mtx.Lock()
		mgr.completeChs[chainID] = completeCh
		mgr.mtx.Unlock()
	}

	return completeCh
}

// WaitAccepted blocks a thread until chainID has 2/3+1 responses.
func (mgr *ReplicationPool) WaitAccepted(chainID string) bool {
	ch := mgr.Accepted(chainID)

	for mgr.Context().Err() == nil {
		select {
		case <-ch:
			return true
		case <-mgr.Context().Done():
			return false
		case <-mgr.Quit():
			return false
		}
	}

	return false
}

// WaitCompleted blocks a thread until chainID has 2/3+1 completions.
func (mgr *ReplicationPool) WaitCompleted(chainID string) bool {
	ch := mgr.Completed(chainID)

	for mgr.Context().Err() == nil {
		select {
		case <-ch:
			return true
		case <-mgr.Context().Done():
			return false
		case <-mgr.Quit():
			return false
		}
	}

	return false
}

// ----------------------------------------------------------------------------

// SetOptions uses custom option helpers.
func (mgr *ReplicationPool) SetOptions(options ...ReplicationPoolOption) {
	for _, option := range options {
		option(mgr)
	}
}

// Logger returns the logger instance.
func (mgr *ReplicationPool) Logger() cmtlog.Logger {
	return mgr.logger
}

// ----------------------------------------------------------------------------

// addPartner adds a peer ID as a partner for replication of chainID in the pool.
func (mgr *ReplicationPool) addPartner(chainID string, peerID cmtp2p.ID) {
	prev, has := mgr.partners[chainID]
	if !has {
		prev = []cmtp2p.ID{}
	}

	if -1 == slices.IndexFunc(prev, func(p cmtp2p.ID) bool {
		return p == peerID
	}) {
		prev = append(prev, peerID)
	}

	mgr.partners[chainID] = prev
}

// evaluateAcceptanceMajority counts the received ChainReplicationResponse
// messages and evaluates whether we have a majority of responses (2/3).
//
// Note that we don't evaluate a super-majority here because there is always
// the one relay which sends ChainReplicationRequest messages ("self").
func (mgr *ReplicationPool) evaluateAcceptanceMajority(
	chainID string,
) bool {
	if _, ok := mgr.relays[chainID]; !ok {
		return true
	} else if len(mgr.relays[chainID]) == 0 {
		return true
	}

	// Keep only relevant ones for this acceptance evaluation.
	responses := mgr.Responses(chainID)

	// The number of received ChainReplicationResponse messages
	numRequired := len(mgr.relays[chainID]) * 2 / 3
	return len(responses) >= numRequired
}

// evaluateCompletionMajority counts the received ChainReplicationComplete
// messages and evaluates whether we have a majority of completion (2/3).
//
// Note that we don't evaluate a super-majority here because there is always
// the one relay which expects ChainReplicationComplete messages ("self").
func (mgr *ReplicationPool) evaluateCompletionMajority(
	chainID string,
) bool {
	if _, ok := mgr.relays[chainID]; !ok {
		return false
	} else if len(mgr.relays[chainID]) == 0 {
		return true
	}

	// Keep only relevant ones for this acceptance evaluation.
	completes := mgr.Completions(chainID)

	// The number of received ChainReplicationComplete messages
	numRequired := len(mgr.relays[chainID]) * 2 / 3
	return len(completes) >= numRequired
}
