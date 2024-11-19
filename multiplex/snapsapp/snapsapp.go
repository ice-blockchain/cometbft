package snapsapp

import (
	"fmt"
	"path/filepath"
	"sync"

	abcitypes "github.com/ice-blockchain/cometbft/abci/types"
	"github.com/ice-blockchain/cometbft/config"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots"
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
// SnapsApp defines an ABCI application around a multiplex chain registry, and
// which delegates snapshotting to a [snapshots.Manager] implementation.
//
// This application creates snapshots of full state machines, without filtering
// any of the included properties: ChainID, ConsensusParams, Validators, etc.
//
// Read-write mutexes are created to track initial heights on concurrent
// threads, as well as for the currently working height in the process of
// finalizing and committing blocks.
//
// Note that *only one instance* of the SnapsApp application must be created
// for node multiplexes. The SnapsApp application must be thread-safe and uses
// one [snapshots.Manager] instance per replicated chain.
type SnapsApp struct {
	// A logger instance to report asynchronous ABCI messages.
	logger cmtlog.Logger

	// A multiplex reactor as described with [Reactor].
	reactor Reactor

	// A map of [snapshots.Manager] instances mapped to ChainID values.
	snapshotManagers map[string]*snapshots.Manager

	// The current heights being worked on for replicated chains.
	chMutex        *sync.RWMutex
	currentHeights map[string]int64

	// The initial heights as used for state-sync of replicated chains.
	ihMutex        *sync.RWMutex
	initialHeights map[string]int64

	// The finalized block heights consist of working block heights.
	fbMutex              *sync.RWMutex
	finalizeBlockHeights map[string]int64

	// Extensions / Hooks
	checkTxExtension         client.CheckTxExtensionFn
	prepareProposalExtension client.PrepareProposalExtensionFn
	processProposalExtension client.ProcessProposalExtensionFn
	finalizeBlockExtension   client.FinalizeBlockExtensionFn
	commitExtension          client.CommitExtensionFn
}

var _ abcitypes.Application = (*SnapsApp)(nil)

// NewSnapsApplication creates a [SnapsApp] ABCI application instance and
// initializes a [snapshots.Manager] for every replicated chain.
func NewSnapsApplication(
	reactor Reactor,
	snapshotOptions config.SnapshotOptions,
	logger cmtlog.Logger,
	options ...func(*SnapsApp),
) *SnapsApp {
	app := &SnapsApp{
		reactor: reactor,
		logger:  logger,
		chMutex: new(sync.RWMutex),
		ihMutex: new(sync.RWMutex),
		fbMutex: new(sync.RWMutex),
	}

	// Apply all options before anything else
	for _, option := range options {
		option(app)
	}

	// Use the chain registry to determine which chains are of interest
	replicatedChains := reactor.GetNetworks()
	storagePaths := reactor.GetStoragePaths()

	// initial heights are thread-safe
	app.ihMutex.Lock()
	app.initialHeights = make(map[string]int64, len(replicatedChains))
	app.ihMutex.Unlock()

	// working heights are thread-safe
	app.chMutex.Lock()
	app.currentHeights = make(map[string]int64, len(replicatedChains))
	app.chMutex.Unlock()

	// finalizeBlock heights must be thread-safe
	app.fbMutex.Lock()
	app.finalizeBlockHeights = make(map[string]int64, len(replicatedChains))
	app.fbMutex.Unlock()

	// Each replicated chain creates its own snapshot manager instance
	app.snapshotManagers = make(map[string]*snapshots.Manager, len(replicatedChains))
	for _, chainID := range replicatedChains {
		// Snapshots are stored in a different subfolder per chain
		// i.e.: %rootDir%/data/%address%/%ChainID%/snapshots/...
		chainDataFolder := storagePaths[chainID]
		snapshotsFolder := filepath.Join(chainDataFolder, "snapshots")

		// A snapshots store creates a `metadata.db` file and folders per-height
		snapshotStore, err := snapshots.NewStore(snapshotsFolder)
		if err != nil {
			panic(fmt.Errorf("could not create snapshots store: %w", err))
		}

		// Retrieve a particular chain's state machine store
		chainStore := reactor.GetStateStore(chainID)

		// The chain state machine implementation is passed as a commitment
		// snapshotter - which executes after a block is committed.
		// Snapshot() and Restore() are implemented in [ChainHistoryStore].
		manager := snapshots.NewManager(
			chainID,
			snapshotStore,
			snapshotOptions,
			chainStore,
			logger,
		)

		app.snapshotManagers[chainID] = manager
	}

	return app
}

// InitialHeight returns the initial block height for a chainID.
func (app *SnapsApp) InitialHeight(chainID string) int64 {
	app.ihMutex.RLock()
	defer app.ihMutex.RUnlock()

	return app.initialHeights[chainID]
}

// LastBlockHeight returns the last block height processed for a chainID.
func (app *SnapsApp) LastBlockHeight(chainID string) int64 {
	app.chMutex.RLock()
	defer app.chMutex.RUnlock()

	return app.currentHeights[chainID]
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

// ----------------------------------------------------------------------------
// SnapsApp option helpers

// WithCheckTxExtension is an option helper that allows you to
// overwrite the default (nil-returning) CheckTx extension.
func WithCheckTxExtension(
	extensionFn client.CheckTxExtensionFn,
) func(*SnapsApp) {
	return func(app *SnapsApp) {
		app.checkTxExtension = extensionFn
	}
}

// WithPrepareProposalExtension is an option helper that allows you to
// overwrite the default (deep-copying) PrepareProposal extension.
func WithPrepareProposalExtension(
	extensionFn client.PrepareProposalExtensionFn,
) func(*SnapsApp) {
	return func(app *SnapsApp) {
		app.prepareProposalExtension = extensionFn
	}
}

// WithProcessProposalExtension is an option helper that allows you to
// overwrite the default (deep-copying) ProcessProposal extension.
func WithProcessProposalExtension(
	extensionFn client.ProcessProposalExtensionFn,
) func(*SnapsApp) {
	return func(app *SnapsApp) {
		app.processProposalExtension = extensionFn
	}
}

// WithFinalizeBlockExtension is an option helper that allows you to
// overwrite the default (deep-copying) FinalizeBlock extension.
func WithFinalizeBlockExtension(
	extensionFn client.FinalizeBlockExtensionFn,
) func(*SnapsApp) {
	return func(app *SnapsApp) {
		app.finalizeBlockExtension = extensionFn
	}
}

// WithCommitExtension is an option helper that allows you to
// overwrite the default (nil-returning) Commit extension.
func WithCommitExtension(
	extensionFn client.CommitExtensionFn,
) func(*SnapsApp) {
	return func(app *SnapsApp) {
		app.commitExtension = extensionFn
	}
}
