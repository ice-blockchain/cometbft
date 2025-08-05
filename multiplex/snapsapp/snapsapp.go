package snapsapp

import (
	"sync"

	abcitypes "github.com/ice-blockchain/cometbft/abci/types"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	"github.com/ice-blockchain/cometbft/multiplex/client"
)

const (
	// The AppVersion constant determines the current state machine version,
	// it can be increased to mark an upgrade in the State storage format.
	AppVersion = 1

	// The snapsappVersion constant determines the current compiled version
	// of this ABCI application.
	snapsappVersion = "snapsapp/v1"
)

// ----------------------------------------------------------------------------
// SnapsApp
//
// SnapsApp defines an ABCI application around a multiplex backend, and a
// default or custom client implementation.
//
// Note that *only one instance* of the SnapsApp application must be created
// for node multiplexes. The SnapsApp application must keep thread-safety.
type SnapsApp struct {
	mtx *sync.RWMutex

	// A multiplex runtime manager as described with [Backend].
	backend Backend
	// Inject custom transaction verification with an acceptor implementation.
	txAcceptor client.Acceptor

	// The initial heights as used for state-sync of replicated chains.
	initialHeights map[string]int64
	// The last block height by ChainID.
	lastBlockHeights map[string]int64
	// The current heights being worked on by ChainID.
	workingHeights map[string]int64
	// The finalized block heights consist of working block heights.
	finalizeBlockHeights map[string]int64
	// The transaction batches that have been confirmed by tx hash.
	committedTxHashes map[string]bool

	// Options
	logger     cmtlog.Logger
	useMempool bool
}

var _ abcitypes.Application = (*SnapsApp)(nil)

// NewSnapsApplication creates a [SnapsApp] ABCI application instance and
// initializes a [snapshots.Manager] for every replicated chain.
func NewSnapsApplication(
	backend Backend,
	logger cmtlog.Logger,
	options ...func(*SnapsApp),
) *SnapsApp {
	app := &SnapsApp{
		mtx:        new(sync.RWMutex),
		backend:    backend,
		logger:     logger,
		useMempool: true,
	}

	// Apply all options before anything else
	for _, option := range options {
		option(app)
	}

	app.mtx.Lock()
	{
		// Use the backend to retrieve chains of interest
		chainIds := backend.GetNetworks()

		app.initialHeights = make(map[string]int64, len(chainIds))
		app.lastBlockHeights = make(map[string]int64, len(chainIds))
		app.workingHeights = make(map[string]int64, len(chainIds))
		app.finalizeBlockHeights = make(map[string]int64, len(chainIds))
		app.committedTxHashes = map[string]bool{}
	}
	app.mtx.Unlock()

	return app
}

// WithAcceptor is an option helper to inject a custom acceptor implementation
// which accepts an acceptor implementation.
func WithAcceptor(
	acceptor client.Acceptor,
) func(*SnapsApp) {
	return func(a *SnapsApp) {
		a.SetAcceptor(acceptor)
	}
}

func WithUseMempool(
	useMempool bool,
) func(*SnapsApp) {
	return func(a *SnapsApp) {
		a.useMempool = useMempool
	}
}

// Acceptor returns the injected acceptor implementation.
func (app *SnapsApp) Acceptor() client.Acceptor {
	return app.txAcceptor
}

// SetAcceptor sets a custom acceptor implementation.
func (app *SnapsApp) SetAcceptor(impl client.Acceptor) {
	app.txAcceptor = impl
}

// InitialHeight returns the initial block height for a chainID or 0.
func (app *SnapsApp) InitialHeight(chainID string) int64 {
	app.mtx.RLock()
	defer app.mtx.RUnlock()

	if h, ok := app.initialHeights[chainID]; ok {
		return h
	}

	return int64(0)
}

// LastBlockHeight returns the last block height processed for a chainID.
func (app *SnapsApp) LastBlockHeight(chainID string) int64 {
	app.mtx.RLock()
	defer app.mtx.RUnlock()

	if h, ok := app.lastBlockHeights[chainID]; ok {
		return h
	}

	return int64(0)
}

// FinalizeBlockHeight returns the latest finalizeBlock height.
func (app *SnapsApp) FinalizeBlockHeight(chainID string) int64 {
	app.mtx.RLock()
	defer app.mtx.RUnlock()

	if h, ok := app.finalizeBlockHeights[chainID]; ok {
		return h
	}

	return int64(0)
}

// setFinalizeBlockHeight sets the last finalized block height internally.
func (app *SnapsApp) setFinalizeBlockHeight(chainID string, reqHeight int64) error {
	app.mtx.Lock()
	defer app.mtx.Unlock()

	app.finalizeBlockHeights[chainID] = reqHeight
	return nil
}
