package multiplex

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	auto "github.com/cometbft/cometbft/internal/autofile"
	"github.com/cometbft/cometbft/libs/service"
)

// ScopedGroup embeds a [auto.Group] pointer and adds a scope hash
type ScopedGroup struct {
	ScopeHash string
	*auto.Group
}

// ----------------------------------------------------------------------------
// Scoped instance getters

// GetScopedGroup tries to find a group instance in the group multiplex using its scope hash
func GetScopedGroup(
	multiplex MultiplexGroup,
	userScopeHash string,
) (*ScopedGroup, error) {
	scopeHash, err := hex.DecodeString(userScopeHash)
	if err != nil || len(scopeHash) != sha256.Size {
		return nil, fmt.Errorf("incorrect scope hash for autofile group multiplex, got %v bytes, expected %v bytes", len(scopeHash), sha256.Size)
	}

	if scopedGroup, ok := multiplex[userScopeHash]; ok {
		return scopedGroup, nil
	}

	return nil, fmt.Errorf("could not find autofile group in multiplex using scope hash %s", scopeHash)
}

// OpenGroup creates a new scoped Group with head at headPath. It returns an error if
// it fails to open head file.
func OpenScopedGroup(
	scopeHash string,
	headPath string,
	groupOptions ...func(*auto.Group),
) (*ScopedGroup, error) {
	baseGroup, err := auto.OpenGroup(headPath, groupOptions...)
	if err != nil {
		return nil, fmt.Errorf("could not open scoped autofile group with scope hash %s: %w", scopeHash, err)
	}

	g := &ScopedGroup{
		ScopeHash: scopeHash,
		Group:     baseGroup,
	}

	for _, option := range groupOptions {
		option(g.Group)
	}

	g.BaseService = *service.NewBaseService(nil, "ScopedGroup", g)
	return g, nil
}
