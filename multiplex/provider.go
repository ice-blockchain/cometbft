package multiplex

import (
	cmtlibs "github.com/cometbft/cometbft/libs/service"
	"github.com/cometbft/cometbft/proxy"
	"github.com/cometbft/cometbft/types"
)

// ScopedDBProvider should provide a [ScopedDB] by table name and scope hash.
type ScopedDBProvider func(string, string) *ScopedDB

// DBMultiplexProvider should provide a [MultiplexDB] by table name.
type DBMultiplexProvider func(string) MultiplexDB

// ScopedSeedNodesProvider should provide a comma-separated string containing p2p addresses.
type ScopedSeedNodesProvider func(string) string

// ScopedGenesisProvider should provide a [GenesisDoc] by scope hash.
type ScopedGenesisProvider func(string) (*types.GenesisDoc, error)

// ServiceProvider should provide a [cmtlibs.Service] by scope hash and name.
type ServiceProvider func(string, string) cmtlibs.Service

// ScopedStateProvider should provide a [ScopedState] by scope hash.
type ScopedStateProvider func(string) *ScopedState

// ScopedStateStoreProvider should provide a [ScopedStateStore] by scope hash.
type ScopedStateStoreProvider func(string) *ScopedStateStore

// ScopedBlockStoreProvider should provide a [ScopedBlockStore] by scope hash.
type ScopedBlockStoreProvider func(string) *ScopedBlockStore

// ProxyAppProvider should provide a [proxy.AppConns] by scope hash.
type ProxyAppProvider func(string) (proxy.AppConns, error)

// PrivValidatorProvider should provide a [types.PrivValidator] by scope hash.
type PrivValidatorProvider func(string) types.PrivValidator
