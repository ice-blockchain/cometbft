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
// SnapsApp defines an ABCI application around a multiplex reactor, and a
// default or custom client implementation.
//
// Read-write mutexes are created to track initial heights on concurrent
// threads, as well as for the currently working height in the process of
// finalizing and committing blocks.
//
// Note that *only one instance* of the SnapsApp application must be created
// for node multiplexes. The SnapsApp application must keep thread-safety.
type SnapsApp struct {
	// A logger instance to report asynchronous ABCI messages.
	logger cmtlog.Logger

	// A multiplex reactor as described with [Reactor].
	reactor Reactor

	// Inject custom transaction verification with an acceptor implementation.
	txAcceptor client.Acceptor

	// The initial heights as used for state-sync of replicated chains.
	ihMutex        *sync.RWMutex
	initialHeights map[string]int64

	// The last block height by ChainID.
	lbMutex          *sync.RWMutex
	lastBlockHeights map[string]int64

	// The current heights being worked on by ChainID.
	whMutex        *sync.RWMutex
	workingHeights map[string]int64

	// The finalized block heights consist of working block heights.
	fbMutex              *sync.RWMutex
	finalizeBlockHeights map[string]int64
}

var _ abcitypes.Application = (*SnapsApp)(nil)

// NewSnapsApplication creates a [SnapsApp] ABCI application instance and
// initializes a [snapshots.Manager] for every replicated chain.
func NewSnapsApplication(
	reactor Reactor,
	logger cmtlog.Logger,
	options ...func(*SnapsApp),
) *SnapsApp {
	app := &SnapsApp{
		reactor: reactor,
		logger:  logger,
		lbMutex: new(sync.RWMutex),
		whMutex: new(sync.RWMutex),
		ihMutex: new(sync.RWMutex),
		fbMutex: new(sync.RWMutex),
	}

	// Apply all options before anything else
	for _, option := range options {
		option(app)
	}

	// Use the reactor to retrieve chains of interest
	replicatedChains := reactor.GetNetworks()

	// initial heights are thread-safe
	app.ihMutex.Lock()
	app.initialHeights = make(map[string]int64, len(replicatedChains))
	app.ihMutex.Unlock()

	// last block heights are thread-safe
	app.lbMutex.Lock()
	app.lastBlockHeights = make(map[string]int64, len(replicatedChains))
	app.lbMutex.Unlock()

	// working heights are thread-safe
	app.whMutex.Lock()
	app.workingHeights = make(map[string]int64, len(replicatedChains))
	app.whMutex.Unlock()

	// finalizeBlock heights must be thread-safe
	app.fbMutex.Lock()
	app.finalizeBlockHeights = make(map[string]int64, len(replicatedChains))
	app.fbMutex.Unlock()

	return app
}

// WithAcceptor is an option helper to inject a custom acceptor implementation
// which accepts an acceptor implementation.
func WithAcceptor(
	acceptor client.Acceptor,
) func(*SnapsApp) {
	return func(a *SnapsApp) {
		a.txAcceptor = acceptor
	}
}

// GetAcceptor returns the attached acceptor implementation.
func (app *SnapsApp) GetAcceptor() client.Acceptor {
	return app.txAcceptor
}

// InitialHeight returns the initial block height for a chainID.
func (app *SnapsApp) InitialHeight(chainID string) int64 {
	app.ihMutex.RLock()
	defer app.ihMutex.RUnlock()

	return app.initialHeights[chainID]
}

// LastBlockHeight returns the last block height processed for a chainID.
func (app *SnapsApp) LastBlockHeight(chainID string) int64 {
	app.lbMutex.RLock()
	defer app.lbMutex.RUnlock()

	return app.lastBlockHeights[chainID]
}

// FinalizeBlockHeight returns the latest finalizeBlock height.
func (app *SnapsApp) FinalizeBlockHeight(chainID string) int64 {
	app.fbMutex.RLock()
	defer app.fbMutex.RUnlock()

	return app.finalizeBlockHeights[chainID]
}

func (app *SnapsApp) setFinalizeBlockHeight(chainID string, reqHeight int64) error {
	app.fbMutex.Lock()
	defer app.fbMutex.Unlock()

	app.finalizeBlockHeights[chainID] = reqHeight
	return nil
}
