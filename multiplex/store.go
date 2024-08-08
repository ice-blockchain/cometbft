package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/cosmos/gogoproto/proto"

	cmtstate "github.com/cometbft/cometbft/api/cometbft/state/v1"
	cmtos "github.com/cometbft/cometbft/internal/os"
	sm "github.com/cometbft/cometbft/state"
	store "github.com/cometbft/cometbft/store"
)

// database keys.
var (
	stateKey = []byte("stateKey")
)

// ScopedStateStore embeds a DBStore and adds a scope hash
type ScopedStateStore struct {
	ScopeHash string
	*sm.DBStore
}

// ScopedBlockStore embeds a BlockStore and adds a scope hash
type ScopedBlockStore struct {
	ScopeHash string
	*store.BlockStore
}

// LoadState loads the State from the database.
func (store ScopedStateStore) Load() (sm.State, error) {
	return store.loadState(stateKey)
}

func (store ScopedStateStore) loadState(key []byte) (state sm.State, err error) {
	start := time.Now()
	buf, err := store.GetDatabase().Get(key)
	if err != nil {
		return state, err
	}

	addTimeSample(store.StoreOptions.Metrics.StoreAccessDurationSeconds.With("method", "load"), start)()

	if len(buf) == 0 {
		return state, nil
	}

	sp := new(cmtstate.State)

	err = proto.Unmarshal(buf, sp)
	if err != nil {
		// DATA HAS BEEN CORRUPTED OR THE SPEC HAS CHANGED
		cmtos.Exit(fmt.Sprintf(`LoadState: Data has been corrupted or its spec has changed:
		%v\n`, err))
	}

	sm, err := sm.FromProto(sp)
	if err != nil {
		return state, err
	}
	return *sm, nil
}

// ----------------------------------------------------------------------------
// Multiplex providers

// MultiplexStateStoreProvider creates multiple ScopedStateStore using
// the database multiplex dbm.DB instances and the state machine DBStore.
func MultiplexStateStoreProvider(
	multiplex MultiplexDB,
	options sm.StoreOptions,
) (mStore MultiplexStateStore) {
	mStore = make(MultiplexStateStore, len(multiplex))
	for _, db := range multiplex {
		mStore[db.ScopeHash] = &ScopedStateStore{
			ScopeHash: db.ScopeHash,
			DBStore:   sm.NewDBStore(db, options).(*sm.DBStore),
		}
	}

	return mStore
}

// MultiplexBlockStoreProvider creates multiple ScopedBlockStore using
// the database multiplex dbm.DB instances and the BlockStore struct.
// Additionally, the block store is initialized to the last height that
// was committed to the corresponding DB.
func MultiplexBlockStoreProvider(
	multiplex MultiplexDB,
	options ...store.BlockStoreOption,
) (mbStore MultiplexBlockStore) {
	mbStore = make(MultiplexBlockStore, len(multiplex))
	for _, db := range multiplex {
		bs := store.NewBlockStore(db, options...)

		mbStore[db.ScopeHash] = &ScopedBlockStore{
			ScopeHash:  db.ScopeHash,
			BlockStore: bs,
		}
	}

	return mbStore
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedStateStore tries to find a state store in the store multiplex using its scope hash
func GetScopedStateStore(
	multiplex MultiplexStateStore,
	userScopeHash string,
) (*ScopedStateStore, error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for state store multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedStateStore, ok := multiplex[userScopeHash]; ok {
		return scopedStateStore, nil
	}

	return nil, fmt.Errorf("could not find state store in multiplex using scope hash %s", scopeHash)
}

// GetScopedBlockStore tries to find a block store in the store multiplex using its scope hash
func GetScopedBlockStore(
	multiplex MultiplexBlockStore,
	userScopeHash string,
) (*ScopedBlockStore, error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for block store multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedBlockStore, ok := multiplex[userScopeHash]; ok {
		return scopedBlockStore, nil
	}

	return nil, fmt.Errorf("could not find block store in multiplex using scope hash %s", scopeHash)
}
