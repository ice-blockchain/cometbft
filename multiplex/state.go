package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	sm "github.com/cometbft/cometbft/state"
)

// StateServices describes scoped services that are used to
// manage a replicated state machine and contains instances
// of [ScopedDB], [ScopedState], [ScopedStateStore], [ScopedBlockStore].
type StateServices struct {
	DB           *ScopedDB
	StateMachine *ScopedState
	StateStore   *ScopedStateStore
	BlockStore   *ScopedBlockStore
}

// ScopedState embeds a [sm.State] instance and adds a scope hash
type ScopedState struct {
	ScopeHash string
	sm.State
}

// GetState returns a copy of the state machine
func (s ScopedState) GetState() sm.State {
	return s.Copy()
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedState tries to find a state instance in the state multiplex using its scope hash
func GetScopedState(
	multiplex MultiplexState,
	userScopeHash string,
) (*ScopedState, error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for state multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedState, ok := multiplex[userScopeHash]; ok {
		return scopedState, nil
	}

	return nil, fmt.Errorf("could not find state in multiplex using scope hash %s", scopeHash)
}
