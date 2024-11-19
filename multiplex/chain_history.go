package multiplex

import (
	"errors"
	"fmt"
	"io"
	"time"

	protoio "github.com/cosmos/gogoproto/io"
	"github.com/cosmos/gogoproto/proto"

	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapshots"
	snapshottypes "github.com/ice-blockchain/cometbft/multiplex/snapshots/types"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
)

// database keys.
var (
	// IMPORTANT: Do not modify this key because it is used to store the
	// current state machine payload (bytes). see state/state.go.
	//
	// We reuse this same key to be able to reload an entire state machine.
	stateKey = []byte("stateKey")

	// Note: we use a separate key space for the genesis doc and doc hashes
	// to prevent mixing both storages as mxGenesisDocHash holds hash of
	// each GenesisDoc in the GenesisDocSet.
	genesisDocHashKey = []byte("mxGenesisDocHash")
)

// ----------------------------------------------------------------------------
// HistoricalState
//
// HistoricalState embeds a [sm.State] state machine instance and adds an
// arbitrary binary data payload to it to allow storing relevant historical
// state data alongside a state machine instance.
type HistoricalState struct {
	// Embeds a [sm.State], i.e. a state machine instance which also contains
	// a validators set, latest block information and consensus parameters.
	//
	// This embedding contains the actual block height at which a historical
	// data snapshot is taken.
	*sm.State

	// Adds a binary data payload which may contain any relevant historical
	// state data, e.g. a total number of transactions since inception.
	//
	// This payload is provided as a `[]byte` slice to maximize flexibility.
	// Note, it is recommended to store custom data payloads in JSON format.
	Data []byte
}

// NewHistoricalState creates a new state machine and attaches data to it.
func NewHistoricalState(chainID string, data []byte) *HistoricalState {
	return &HistoricalState{
		State: &sm.State{
			ChainID: chainID,
		},
		Data: data,
	}
}

// ----------------------------------------------------------------------------
// ChainHistoryStore
//
// ChainHistoryStore embeds a [sm.DBStore] pointer and adds a ChainID. This store
// implementation is compatible with [snapshots.StateSnapshotter] such that
// the full state instance may be *restored* (state-sync) from snapshot files.
//
// Note that this structure embeds a [sm.DBStore] which provides methods for
// the implementation of an interface [sm.Store]. This structure overwrites the
// Load method to load the historical state from a database instance.
type ChainHistoryStore struct {
	ChainID     string
	*sm.DBStore // embeds an implementation of [sm.Store]

	// Used for user address discovery by ChainID
	chainRegistry ChainRegistry

	// Extensions / Hooks
	snapshotMutationHook    client.SnapshotMutationExtensionFn
	checkMutationResultHook client.CheckMutationResultExtensionFn
	snapshotRestoreHook     client.SnapshotRestoreExtensionFn
}

// MultiplexArchiveStore maps ChainIDs to history store instances.
type MultiplexArchiveStore map[string]*ChainHistoryStore

// State machine compatibility check.
var (
	_ sm.Store                   = (*ChainHistoryStore)(nil)
	_ snapshots.StateSnapshotter = (*ChainHistoryStore)(nil)
)

func NewChainHistoryStore(
	chainDB *ChainDB,
	dbKeyLayoutVersion string,
) *ChainHistoryStore {
	return &ChainHistoryStore{
		ChainID: chainDB.ChainID,
		DBStore: sm.NewDBStore(chainDB, sm.StoreOptions{
			DiscardABCIResponses: false,
			DBKeyLayout:          dbKeyLayoutVersion,
		}).(*sm.DBStore),
	}
}

// SetChainRegistry registers a [ChainRegistry] which is used to find
// user addresses by ChainID.
func (store *ChainHistoryStore) SetChainRegistry(reg ChainRegistry) {
	store.chainRegistry = reg
}

// Save persists the State, the ValidatorsInfo, and the ConsensusParamsInfo to the database.
// This flushes the writes (e.g. calls SetSync).
//
// CAUTION: This method is called in the consensus handshake method.
// See also: internal/consensus/replay.go.
func (store ChainHistoryStore) Save(state sm.State) error {
	// First intend to store state machine
	err := store.DBStore.Save(state)
	if err != nil {
		return err
	}

	// If the above worked, we may re-wrap to HistoricalState
	stateMachine := state.Copy()
	archiveState := &HistoricalState{
		State: &stateMachine,
		Data:  []byte{},
	}

	// .. and update the database entry
	return store.GetDatabase().SetSync(stateKey, archiveState.Bytes())
}

// LoadFromDBOrGenesisDoc loads the most recent state from the database,
// or creates a new one from the given genesisDoc.
func (store ChainHistoryStore) LoadFromDBOrGenesisDoc(
	genesisDoc *types.GenesisDoc,
) (sm.State, error) {
	state, err := store.LoadArchive(stateKey)
	if err != nil {
		return sm.State{}, err
	}

	if state.State.IsEmpty() {
		var err error
		stateMachine, err := sm.MakeGenesisState(genesisDoc)
		if err != nil {
			return sm.State{}, err
		}

		state = new(HistoricalState)
		state.State = &stateMachine
		state.Data = []byte{} // TODO(midas): use AppState or add data field to genesis?
	}

	defer addTimeSampleNow(store.DBStore.StoreOptions.Metrics.StoreAccessDurationSeconds.With("method", "load"))()
	return state.State.Copy(), nil
}

// Load loads the HistoricalState from the database using the stateKey.
//
// Load implements [sm.Store].
func (store ChainHistoryStore) Load() (sm.State, error) {
	archive, err := store.LoadArchive(stateKey)
	if err != nil {
		return sm.State{}, err
	}

	return archive.State.Copy(), err
}

// LoadArchive loads the [HistoricalState] from the database given a key.
func (store ChainHistoryStore) LoadArchive(key []byte) (*HistoricalState, error) {
	start := time.Now()
	buf, err := store.GetDatabase().Get(key)
	if err != nil {
		return NewHistoricalState(store.ChainID, []byte{}), err
	}

	defer addTimeSample(store.DBStore.StoreOptions.Metrics.StoreAccessDurationSeconds.With("method", "load"), start)()
	return store.loadArchiveFromPayload(buf)
}

// GetUserAddress returns a hexadecimal representation of the user address
// corresponding to the current ChainID, given it exists. Otherwise it will
// return an empty string.
func (store ChainHistoryStore) GetUserAddress() string {
	if store.chainRegistry == nil {
		return ""
	}

	userAddress, err := store.chainRegistry.GetAddress(store.ChainID)
	if err != nil {
		return ""
	}

	return userAddress
}

// GetChainID returns the current ChainID as a string.
func (store ChainHistoryStore) GetChainID() string {
	return store.ChainID
}

// Commit updates the state instance in the database.
//
// This method is called as part of the process of restoration of snapshots
// which is issued in a separate goroutine. The state machine instance in
// the database is updated to contain the snapshot's state instance.
func (store ChainHistoryStore) Commit(state HistoricalState) error {
	// (1): Retrieves the current state from DB
	oldState, err := store.GetStateMachine()
	if err != nil {
		return err
	}

	// (2): Validate compatibility of new State
	if oldState.ChainID != state.ChainID {
		return fmt.Errorf(
			"cannot commit state from other chain; expected %v, got %v",
			state.ChainID, oldState.ChainID,
		)
	}

	// (3): Commit the new HistoricalState to DB
	if err := store.GetDatabase().Set(stateKey, state.Bytes()); err != nil {
		return err
	}

	return nil
}

// loadArchiveFromPayload takes a bytes payload and unmarshals it to a [HistoricalState].
func (store ChainHistoryStore) loadArchiveFromPayload(
	payload []byte,
) (*HistoricalState, error) {
	if len(payload) == 0 {
		return NewHistoricalState(store.ChainID, []byte{}), nil
	}

	sp := new(mxp2p.HistoricalState)

	err := proto.Unmarshal(payload, sp)
	if err != nil {
		// DATA HAS BEEN CORRUPTED OR THE SPEC HAS CHANGED
		cmtos.Exit(fmt.Sprintf(`LoadArchive: Data has been corrupted or its spec has changed:
		%v\n`, err))
	}

	historicalState, err := FromProto(sp)
	if err != nil {
		return NewHistoricalState(store.ChainID, []byte{}), err
	}
	return historicalState, nil
}

// ----------------------------------------------------------------------------
// ChainHistoryStore implements snapshottypes.Snapshotter

// GetStateMachine returns the latest application state (state machine).
//
// GetStateMachine implements [snapshottypes.StateSnapshotter].
func (store ChainHistoryStore) GetStateMachine() (sm.State, error) {
	return store.Load()
}

// AppHash returns the application hash as available from the state machine.
//
// AppHash implements [snapshottypes.StateSnapshotter].
func (store ChainHistoryStore) AppHash() []byte {
	// Load the state machine
	sm, err := store.GetStateMachine()
	if err != nil {
		// TODO(midas): TBI the occurrence of this error.
		cmtos.Exit(fmt.Sprintf(`could not load state machine for ChainID %s: %v\n`,
			store.ChainID,
			err.Error(),
		))
	}

	return sm.AppHash
}

// The snapshot output for a given format must be identical across nodes such
// that chunks from different sources fit together. If the output for a given
// format changes (at the byte level), the snapshot format must be bumped.
//
// State is serialized as a stream of SnapshotItem Protobuf messages which
// contain a bytes payload of the state store. As such, each SnapshotItem
// represents a state instance and snapshots contain only one item.
//
// Snapshot implements [snapshottypes.StateSnapshotter].
func (store ChainHistoryStore) Snapshot(
	height uint64,
	protoWriter protoio.Writer,
) error {
	// Verify snapshot height (logic errors)
	if height == 0 {
		return errors.New("cannot snapshot height 0")
	}

	// Load latest state for further verifications
	// LoadArchive returns a HistoricalState instance.
	latestState, err := store.LoadArchive(stateKey)
	if err != nil {
		return fmt.Errorf("could not load state to create snapshot: %w", err)
	}

	// Can only snapshot "now" / latest state
	if height > uint64(latestState.LastBlockHeight) {
		return fmt.Errorf("cannot snapshot future height %v", height)
	}

	// Prepare the contract for state mutation extensions
	var mutatedStateBytes []byte

	// Executes snapshot mutation extension and catches potential panics to
	// ensure data consistency, also making a compromise on availability.
	//
	// Catch recovered errors from snapshot mutation extension and stop.
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	mutatedStateBytes, err = store.runSnapshotMutationHook(latestState)
	if err != nil {
		return fmt.Errorf(
			"CLIENT PANIC: failing snapshot mutation extension: %w", err)
	}

	// Reaching the following block means that the snapshot mutation extension
	// mutated the snapshot state correctly and that no errors happened during
	// the mutation process.

	// State is serialized as a stream of SnapshotItem Protobuf
	// messages which contain a bytes payload of the state store.
	// As such, each SnapshotItem represents a full state instance.
	err = func() error {
		err = protoWriter.WriteMsg(&snapshottypes.SnapshotItem{
			Item: &snapshottypes.SnapshotItem_Store{
				Store: &snapshottypes.SnapshotStoreItem{
					Payload: mutatedStateBytes,
				},
			},
		})
		if err != nil {
			return err
		}

		return nil
	}()
	if err != nil {
		return err
	}

	return nil
}

// Restore restores [snapshottypes.SnapshotItem] instances into a bytes payload
// and commits the new state if available.
//
// Restore implements [snapshottypes.StateSnapshotter].
func (store ChainHistoryStore) Restore(
	height uint64,
	format uint32,
	protoReader protoio.Reader,
) (snapshottypes.SnapshotItem, error) {
	// We restore as many items as there are for a specific snapshot
	var snapshotItem snapshottypes.SnapshotItem
	var restoredStateBytes []byte

	// Reads the next snapshot item until it finds EOF or an error occurs
RESTORE_LOOP:
	for {
		snapshotItem = snapshottypes.SnapshotItem{}
		err := protoReader.ReadMsg(&snapshotItem)

		// If we reached EOF, we are done restoring
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"could not read snapshot item message: %w", err)
		}

		switch item := snapshotItem.Item.(type) {
		case *snapshottypes.SnapshotItem_Store:
			restoredStateBytes = item.Store.Payload
			if len(restoredStateBytes) == 0 {
				return snapshottypes.SnapshotItem{}, errors.New(
					"found empty snapshot item payload")
			}

			// store.logger.Debug("restoring snapshot",
			// 	"chain_id", store.ChainID,
			// 	"height", height,
			// 	"size", len(restoredStateBytes),
			// )

		default:
			break RESTORE_LOOP
		}
	}

	if restoredStateBytes != nil {
		// Executes snapshot restoration extension and catches potential panics to
		// ensure data consistency, also making a compromise on availability.
		//
		// Catch recovered errors from snapshot restore extension and stop.
		// When a snapshot restore extension fails, the data cannot be restored
		// because the extension determines the format of the mutated state machine.
		restoredStateBytes, err := store.runSnapshotRestoreHook(restoredStateBytes)
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"CLIENT PANIC: failing snapshot restoration extension: %w", err)
		}

		// Create a [HistoricalState] instance from snapshot item data
		newState, err := store.loadArchiveFromPayload(restoredStateBytes)
		if err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"could not load snapshot data: %w", err)
		}

		// Commit the restoration process, this updates the database
		if err = store.Commit(*newState); err != nil {
			return snapshottypes.SnapshotItem{}, fmt.Errorf(
				"could not commit restored state: %w", err)
		}
	}

	// Re-Load the newly committed/restored state instance
	_, err := store.Load()
	return snapshotItem, err
}

// ----------------------------------------------------------------------------
// HistoricalState rewrites [sm.State] methods

// Bytes serializes the HistoricalState using protobuf.
// It panics if either casting to protobuf or serialization fails.
func (state HistoricalState) Bytes() []byte {
	hsm, err := state.ToProto()
	if err != nil {
		panic(err)
	}
	bz, err := proto.Marshal(hsm)
	if err != nil {
		panic(err)
	}
	return bz
}

// ToProto takes the local state type and returns the equivalent protobuf message.
func (state *HistoricalState) ToProto() (*mxp2p.HistoricalState, error) {
	if state == nil {
		return nil, errors.New("historicalState is nil")
	}

	hsm := new(mxp2p.HistoricalState)

	// Delegates extracting the state machine to state module
	smProto, err := state.State.ToProto()
	if err != nil {
		return nil, fmt.Errorf(
			"could not create protobuf for historical state: %w", err)
	}

	hsm.State = *smProto
	hsm.Data = state.Data

	return hsm, nil
}

// FromProto takes a historical state proto message & returns the local state type.
func FromProto(pb *mxp2p.HistoricalState) (*HistoricalState, error) { //nolint:golint
	if pb == nil {
		return nil, errors.New("nil HistoricalState")
	}

	historicalState := new(HistoricalState)

	// Delegates loading the state machine to state module
	stateMachine, err := sm.FromProto(&pb.State)
	if err != nil {
		return nil, fmt.Errorf(
			"could not load state machine from protobuf: %w", err)
	}

	historicalState.State = stateMachine
	historicalState.Data = pb.Data
	return historicalState, nil
}

// ----------------------------------------------------------------------------
// ChainHistoryStore option helpers

// WithChainRegistry is an option helper that allows you to
// overwrite the internal chainRegistry instance.
func WithChainRegistry(
	chainRegistry ChainRegistry,
) func(*ChainHistoryStore) {
	return func(store *ChainHistoryStore) {
		store.chainRegistry = chainRegistry
	}
}

// WithSnapshotMutationExtension is an option helper that allows you to
// overwrite the default (deep-copying) snapshot mutation extension.
func WithSnapshotMutationExtension(
	extensionFn client.SnapshotMutationExtensionFn,
) func(*ChainHistoryStore) {
	return func(store *ChainHistoryStore) {
		store.snapshotMutationHook = extensionFn
	}
}

// WithCheckMutationResultExtension is an option helper that allows you to
// overwrite the default (nil-returning) mutation result audit extension.
func WithCheckMutationResultExtension(
	extensionFn client.CheckMutationResultExtensionFn,
) func(*ChainHistoryStore) {
	return func(store *ChainHistoryStore) {
		store.checkMutationResultHook = extensionFn
	}
}

// WithSnapshotRestoreExtension is an option helper that allows you to
// overwrite the default (deep-copying) snapshot restoration extension.
func WithSnapshotRestoreExtension(
	extensionFn client.SnapshotRestoreExtensionFn,
) func(*ChainHistoryStore) {
	return func(store *ChainHistoryStore) {
		store.snapshotRestoreHook = extensionFn
	}
}

// ----------------------------------------------------------------------------
// ChainHistoryStore internal extension hooks

// runSnapshotMutationHook executes a snapshot mutation extension. This method
// uses the snapshotMutationHook on the instance if available, or otherwise it
// uses the extension configured as the *default*
//
// Executes snapshot mutation extension and catches potential panics to ensure
// data consistency, also making a compromise on availability.
// When a snapshot mutation extension fails, the data cannot be snapshotted
// because the extension determines the format of the mutated state machine.
//
// See also: [GetSnapshotMutationExtension], [WithSnapshotMutationExtension].
func (store ChainHistoryStore) runSnapshotMutationHook(
	latestState *HistoricalState,
) ([]byte, error) {
	// Prepare the contract for state mutation extensions
	var mutatedStateBytes []byte
	var snapshotMutationHook client.SnapshotMutationExtensionFn

	// Uses the default extension or the one configured
	snapshotMutationHook = GetSnapshotMutationExtension()
	if store.snapshotMutationHook != nil {
		snapshotMutationHook = store.snapshotMutationHook
	}

	// Executes snapshot mutation extension and catch potential panics to
	// ensure data consistency, also making a compromise on availability.
	//
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a state mutation extension. This recovery ensures *data consistency*
		// in case of failing snapshot mutation extensions, and thus breaks the
		// snapshotting process until the state mutation extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in state mutation extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. deep-copy the sm.State
		// See `multiplex/client.go` to use a custom state mutation extension.
		// See also [WithSnapshotMutationExtension].
		mutatedStateBytes = client.InjectSnapshotMutation(
			store.GetChainID(),
			latestState.Bytes(),
			snapshotMutationHook,
		)

		// XXX return client.AuditMutationResult(...)

		return err
	}()
	// Catch recovered errors from snapshot mutation extension and stop.
	// When a snapshot mutation extension fails, the data cannot be snapshotted
	// because the extension determines the format of the mutated state machine.
	if err != nil {
		return []byte{}, err
	}

	// No errors happened, we can safely use the mutated state instance
	return mutatedStateBytes, nil
}

// runSnapshotRestoreHook executes a snapshot restoration extension. This
// method uses the snapshotRestoreHook on the instance if available, or
// otherwise it uses the extension configured as the *default*.
//
// Executes snapshot restoration extension and catches potential panics
// to ensure data consistency, also making a compromise on availability.
// When a snapshot restore extension fails, the data cannot be restored
// because the extension determines the format of the restored state machine.
//
// See also: [GetSnapshotRestoreExtension], [WithSnapshotRestoreExtension].
func (store ChainHistoryStore) runSnapshotRestoreHook(
	stateBytes []byte,
) ([]byte, error) {
	// Prepare the contract for state restoration extensions
	var restoredStateBytes []byte
	var snapshotRestoreHook client.SnapshotRestoreExtensionFn

	// Uses the default extension or the one configured
	snapshotRestoreHook = GetSnapshotRestoreExtension()
	if store.snapshotRestoreHook != nil {
		snapshotRestoreHook = store.snapshotRestoreHook
	}

	// Executes snapshot restoration extension and catch potential panics
	// to ensure data consistency, making a compromise on availability.
	//
	// When a snapshot restore extension fails, the data cannot be restored
	// because the extension determines the format of the mutated state machine.
	err := func() (err error) {
		// Recover from potential panic in below block due to inability to inject
		// a state restoration extension. This recovery ensures *data consistency*
		// in case of failing snapshot restoration extensions, and thus breaks the
		// snapshotting process until the state restoration extension is fine.
		defer func() {
			if errRecovered := recover(); errRecovered != nil {
				// Error happened in state restoration extension
				err = errRecovered.(error)
			}
		}()

		// Uses the default extension implementation, i.e. deep-copy the bytes slice
		// see `multiplex/client.go` to use a custom state restoration extension.
		// See also [WithSnapshotRestoreExtension].
		restoredStateBytes = client.InjectSnapshotRestore(
			store.GetChainID(),
			stateBytes,
			snapshotRestoreHook,
		)

		return err
	}()
	// Catch recovered errors from snapshot restoration extension and stop.
	// When a snapshot restoration extension fails, the data cannot be restored
	// because the extension determines the format of the restored state machine.
	if err != nil {
		return []byte{}, err
	}

	// No errors happened, we can safely use the restored bytes slice
	return restoredStateBytes, nil
}
