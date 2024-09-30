package multiplex

import (
	"fmt"
	"path/filepath"
	"time"

	auto "github.com/cometbft/cometbft/internal/autofile"
	cs "github.com/cometbft/cometbft/internal/consensus"
	cmtos "github.com/cometbft/cometbft/internal/os"
	"github.com/cometbft/cometbft/libs/service"
)

const (
	// how often the WAL should be sync'd during period sync'ing.
	mxWalDefaultFlushInterval = 10 * 2 * time.Second
)

// ScopedWAL embeds a [cs.BaseWAL] pointer and adds a scope hash
type ScopedWAL struct {
	ScopeHash string
	*cs.BaseWAL
}

var _ cs.WAL = &ScopedWAL{}

// NewScopedWAL returns a new write-ahead logger based on `baseWAL`, which implements
// WAL. It's flushed and synced to disk every 20s and once when stopped.
func NewScopedWAL(walFiles map[string]string, groupOptions ...func(*auto.Group)) (*MultiplexWAL, error) {
	multiplexWAL := make(MultiplexWAL, len(walFiles))
	for scopeHash, walFile := range walFiles {
		err := cmtos.EnsureDir(filepath.Dir(walFile), 0o700)
		if err != nil {
			return nil, fmt.Errorf("failed to ensure WAL directory is in place: %w", err)
		}

		// FIXME(midas): head file is opened, i.e. bottleneck with > 10k files.
		group, err := OpenScopedGroup(scopeHash, walFile, groupOptions...)
		if err != nil {
			return nil, err
		}
		wal := &ScopedWAL{
			ScopeHash: scopeHash,
			BaseWAL:   cs.NewWALWithParams(group.Group, cs.NewWALEncoder(group), mxWalDefaultFlushInterval),
		}

		wal.BaseService = *service.NewBaseService(nil, "baseWAL", wal)
		multiplexWAL[scopeHash] = wal
	}

	return &multiplexWAL, nil
}
