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

	pool *MessagePool

	acceptedChs map[string]chan struct{}
	completeChs map[string]chan struct{}

	relays    map[string][]*helpers.RelayAddress
	partners  map[string][]cmtp2p.ID
	requests  map[string][]*mxp2p.ChainReplicationRequest
	responses map[string][]*mxp2p.ChainReplicationResponse
	completes map[string][]*mxp2p.ChainReplicationComplete

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

		relays:    map[string][]*helpers.RelayAddress{},
		partners:  map[string][]cmtp2p.ID{},
		requests:  map[string][]*mxp2p.ChainReplicationRequest{},
		responses: map[string][]*mxp2p.ChainReplicationResponse{},
		completes: map[string][]*mxp2p.ChainReplicationComplete{},

		// Options
		logger: logger,
	}

	// Use option helpers
	mgr.SetOptions(options...)

	mgr.BaseService = *service.NewBaseService(ctx, nil, "ReplicationPool", mgr)
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
		case *mxp2p.Message_ChainReplicationRequest:
			replRequest := extMsg.GetChainReplicationRequest()

			mgr.mtx.Lock()
			mgr.addRequest(replRequest)
			mgr.mtx.Unlock()

			// Nothing more to do about ChainReplicationRequest.

		case *mxp2p.Message_ChainReplicationResponse:
			replResponse := extMsg.GetChainReplicationResponse()

			mgr.mtx.Lock()
			mgr.addResponse(replResponse)
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
			mgr.addComplete(replComplete)
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

// Status returns the status of a chain replication.
func (mgr *ReplicationPool) Status(chainID string) *mxp2p.ChainReplicationStatus {
	return nil
}

// Requests returns the stored replication requests for chainID.
func (mgr *ReplicationPool) Requests(chainID string) []*mxp2p.ChainReplicationRequest {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if r, ok := mgr.requests[chainID]; ok {
		return r
	}

	return []*mxp2p.ChainReplicationRequest{}
}

// Responses returns the stored replication responses for chainID.
func (mgr *ReplicationPool) Responses(chainID string) []*mxp2p.ChainReplicationResponse {
	mgr.mtx.Lock()
	defer mgr.mtx.Unlock()

	if r, ok := mgr.responses[chainID]; ok {
		return r
	}

	return []*mxp2p.ChainReplicationResponse{}
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

// addRequest adds a ChainReplicationRequest to the pool.
func (mgr *ReplicationPool) addRequest(req *mxp2p.ChainReplicationRequest) {
	prev, has := mgr.requests[req.ChainID]
	if !has {
		prev = []*mxp2p.ChainReplicationRequest{}
	}

	prev = append(prev, req)
	mgr.requests[req.ChainID] = prev
}

// addResponse adds a ChainReplicationResponse to the pool.
func (mgr *ReplicationPool) addResponse(res *mxp2p.ChainReplicationResponse) {
	prev, has := mgr.responses[res.ChainID]
	if !has {
		prev = []*mxp2p.ChainReplicationResponse{}
	}

	prev = append(prev, res)
	mgr.responses[res.ChainID] = prev
}

// addComplete adds a ChainReplicationComplete to the pool.
func (mgr *ReplicationPool) addComplete(res *mxp2p.ChainReplicationComplete) {
	prev, has := mgr.completes[res.ChainID]
	if !has {
		prev = []*mxp2p.ChainReplicationComplete{}
	}

	prev = append(prev, res)
	mgr.completes[res.ChainID] = prev
}

// evaluateAcceptanceMajority counts the received ChainReplicationResponse
// messages and evaluates whether we have a super majority (2/3+1).
func (mgr *ReplicationPool) evaluateAcceptanceMajority(
	chainID string,
) bool {
	if _, ok := mgr.relays[chainID]; !ok {
		return true
	} else if len(mgr.relays[chainID]) == 0 {
		return true
	}
	if _, ok := mgr.responses[chainID]; !ok {
		return false
	}

	// The number of received ChainReplicationResponse messages
	responses := mgr.responses[chainID]

	numRequired := len(mgr.relays[chainID])*2/3 + 1
	return len(responses) >= numRequired
}

// evaluateCompletionMajority counts the received ChainReplicationComplete
// messages and evaluates whether we have a super majority (2/3+1).
func (mgr *ReplicationPool) evaluateCompletionMajority(
	chainID string,
) bool {
	if _, ok := mgr.relays[chainID]; !ok {
		return false
	} else if len(mgr.relays[chainID]) == 0 {
		return true
	}
	if _, ok := mgr.completes[chainID]; !ok {
		return false
	}

	// The number of received ChainReplicationComplete messages
	completes := mgr.completes[chainID]

	numRequired := len(mgr.relays[chainID])*2/3 + 1
	return len(completes) >= numRequired
}
