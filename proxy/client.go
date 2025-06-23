package proxy

import (
	"context"
	"fmt"

	abcicli "github.com/ice-blockchain/cometbft/abci/client"
	"github.com/ice-blockchain/cometbft/abci/example/kvstore"
	"github.com/ice-blockchain/cometbft/abci/types"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
	e2e "github.com/ice-blockchain/cometbft/test/e2e/app"
)

//go:generate ../scripts/mockery_generate.sh ClientCreator

// ClientCreator creates new ABCI clients based on the intended use of the client.
type ClientCreator interface {
	// NewABCIConsensusClient creates an ABCI client for handling
	// consensus-related queries.
	NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error)
	// NewABCIMempoolClient creates an ABCI client for handling mempool-related
	// queries.
	NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error)
	// NewABCIQueryClient creates an ABCI client for handling
	// query/info-related queries.
	NewABCIQueryClient(ctx context.Context) (abcicli.Client, error)
	// NewABCISnapshotClient creates an ABCI client for handling
	// snapshot-related queries.
	NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error)
}

// ----------------------------------------------------
// local proxy uses a mutex on an in-proc app

type localClientCreator struct {
	mtx *cmtsync.Mutex
	app types.Application
}

// NewLocalClientCreator returns a [ClientCreator] for the given app, which
// will be running locally.
//
// Maintains a single mutex over all new clients created with NewABCIClient.
func NewLocalClientCreator(app types.Application) ClientCreator {
	return &localClientCreator{
		mtx: new(cmtsync.Mutex),
		app: app,
	}
}

// NewABCIConsensusClient implements ClientCreator.
func (l *localClientCreator) NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error) {
	return l.newABCIClient(ctx)
}

// NewABCIMempoolClient implements ClientCreator.
func (l *localClientCreator) NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error) {
	return l.newABCIClient(ctx)
}

// NewABCIQueryClient implements ClientCreator.
func (l *localClientCreator) NewABCIQueryClient(ctx context.Context) (abcicli.Client, error) {
	return l.newABCIClient(ctx)
}

// NewABCISnapshotClient implements ClientCreator.
func (l *localClientCreator) NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error) {
	return l.newABCIClient(ctx)
}

func (l *localClientCreator) newABCIClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewLocalClient(ctx, l.mtx, l.app), nil
}

// -------------------------------------------------------------------------
// connection-synchronized local client uses a mutex per "connection" on an
// in-process app

type connSyncLocalClientCreator struct {
	app types.Application
}

// NewConnSyncLocalClientCreator returns a local [ClientCreator] for the given
// app.
//
// Unlike [NewLocalClientCreator], this is a "connection-synchronized" local
// client creator, meaning each call to NewABCIClient returns an ABCI client
// that maintains its own mutex over the application (i.e. it is
// per-"connection" synchronized).
func NewConnSyncLocalClientCreator(app types.Application) ClientCreator {
	return &connSyncLocalClientCreator{
		app: app,
	}
}

// NewABCIConsensusClient implements ClientCreator.
func (c *connSyncLocalClientCreator) NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error) {
	return c.newABCIClient(ctx)
}

// NewABCIMempoolClient implements ClientCreator.
func (c *connSyncLocalClientCreator) NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error) {
	return c.newABCIClient(ctx)
}

// NewABCIQueryClient implements ClientCreator.
func (c *connSyncLocalClientCreator) NewABCIQueryClient(ctx context.Context) (abcicli.Client, error) {
	return c.newABCIClient(ctx)
}

// NewABCISnapshotClient implements ClientCreator.
func (c *connSyncLocalClientCreator) NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error) {
	return c.newABCIClient(ctx)
}

func (c *connSyncLocalClientCreator) newABCIClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewLocalClient(ctx, nil, c.app), nil
}

// -----------------------------------------------------------------------------
// advanced local client creator with a more complex concurrency model than the
// other local client creators

type consensusSyncLocalClientCreator struct {
	app types.Application
}

// NewConsensusSyncLocalClientCreator returns a [ClientCreator] with a more
// advanced concurrency model than that provided by [NewLocalClientCreator] or
// [NewConnSyncLocalClientCreator].
//
// In this model (a "consensus-synchronized" model), only the consensus client
// has a mutex over it to serialize consensus interactions. With all other
// clients (mempool, query, snapshot), enforcing synchronization is left up to
// the app.
func NewConsensusSyncLocalClientCreator(app types.Application) ClientCreator {
	return &consensusSyncLocalClientCreator{
		app: app,
	}
}

// NewABCIConsensusClient implements ClientCreator.
func (c *consensusSyncLocalClientCreator) NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error) {
	// A mutex is created by the local client and applied across all
	// consensus-related calls.
	return abcicli.NewLocalClient(ctx, nil, c.app), nil
}

// NewABCIMempoolClient implements ClientCreator.
func (c *consensusSyncLocalClientCreator) NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error) {
	// It is up to the ABCI app to manage its concurrency when handling
	// mempool-related calls.
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// NewABCIQueryClient implements ClientCreator.
func (c *consensusSyncLocalClientCreator) NewABCIQueryClient(ctx context.Context) (abcicli.Client, error) {
	// It is up to the ABCI app to manage its concurrency when handling
	// query-related calls.
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// NewABCISnapshotClient implements ClientCreator.
func (c *consensusSyncLocalClientCreator) NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error) {
	// It is up to the ABCI app to manage its concurrency when handling
	// snapshot-related calls.
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// -----------------------------------------------------------------------------
// most advanced local client creator with a more complex concurrency model
// than the other local client creators - all concurrency is assumed to be
// handled by the application

type unsyncLocalClientCreator struct {
	app types.Application
}

// NewUnsyncLocalClientCreator returns a [ClientCreator] that is fully
// unsynchronized, meaning that all synchronization must be handled by the
// application. This is an advanced type of client creator, and requires
// special care on the application side to ensure that consensus concurrency is
// not violated.
func NewUnsyncLocalClientCreator(ctx context.Context, app types.Application) ClientCreator {
	return &unsyncLocalClientCreator{
		app: app,
	}
}

// NewABCIConsensusClient implements ClientCreator.
func (c *unsyncLocalClientCreator) NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// NewABCIMempoolClient implements ClientCreator.
func (c *unsyncLocalClientCreator) NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// NewABCIQueryClient implements ClientCreator.
func (c *unsyncLocalClientCreator) NewABCIQueryClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// NewABCISnapshotClient implements ClientCreator.
func (c *unsyncLocalClientCreator) NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error) {
	return abcicli.NewUnsyncLocalClient(ctx, c.app), nil
}

// ---------------------------------------------------------------
// remote proxy opens new connections to an external app process

type remoteClientCreator struct {
	addr        string
	transport   string
	mustConnect bool
}

// NewRemoteClientCreator returns a ClientCreator for the given address (e.g.
// "192.168.0.1") and transport (e.g. "tcp"). Set mustConnect to true if you
// want the client to connect before reporting success.
func NewRemoteClientCreator(addr, transport string, mustConnect bool) ClientCreator {
	return &remoteClientCreator{
		addr:        addr,
		transport:   transport,
		mustConnect: mustConnect,
	}
}

// NewABCIConsensusClient implements ClientCreator.
func (r *remoteClientCreator) NewABCIConsensusClient(ctx context.Context) (abcicli.Client, error) {
	return r.newABCIClient(ctx)
}

// NewABCIMempoolClient implements ClientCreator.
func (r *remoteClientCreator) NewABCIMempoolClient(ctx context.Context) (abcicli.Client, error) {
	return r.newABCIClient(ctx)
}

// NewABCIQueryClient implements ClientCreator.
func (r *remoteClientCreator) NewABCIQueryClient(ctx context.Context) (abcicli.Client, error) {
	return r.newABCIClient(ctx)
}

// NewABCISnapshotClient implements ClientCreator.
func (r *remoteClientCreator) NewABCISnapshotClient(ctx context.Context) (abcicli.Client, error) {
	return r.newABCIClient(ctx)
}

func (r *remoteClientCreator) newABCIClient(ctx context.Context) (abcicli.Client, error) {
	remoteApp, err := abcicli.NewClient(ctx, r.addr, r.transport, r.mustConnect)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to proxy: %w", err)
	}

	return remoteApp, nil
}

// DefaultClientCreator returns a default [ClientCreator], which will create a
// local client if addr is one of "kvstore", "persistent_kvstore", "e2e",
// "noop".
//
// Otherwise a remote client will be created.
//
// Each of "kvstore", "persistent_kvstore" and "e2e" also currently have an
// "_connsync" variant (i.e. "kvstore_connsync", etc.), which attempts to
// replicate the same concurrency model as the remote client.
func DefaultClientCreator(ctx context.Context, addr, transport, dbDir string) ClientCreator {
	switch addr {
	case "kvstore":
		return NewLocalClientCreator(kvstore.NewInMemoryApplication())
	case "kvstore_connsync":
		return NewConnSyncLocalClientCreator(kvstore.NewInMemoryApplication())
	case "kvstore_unsync":
		return NewUnsyncLocalClientCreator(ctx, kvstore.NewInMemoryApplication())
	case "persistent_kvstore":
		return NewLocalClientCreator(kvstore.NewPersistentApplication(dbDir))
	case "persistent_kvstore_connsync":
		return NewConnSyncLocalClientCreator(kvstore.NewPersistentApplication(dbDir))
	case "persistent_kvstore_unsync":
		return NewUnsyncLocalClientCreator(ctx, kvstore.NewPersistentApplication(dbDir))
	case "e2e":
		app, err := e2e.NewApplication(e2e.DefaultConfig(dbDir))
		if err != nil {
			panic(err)
		}
		return NewLocalClientCreator(app)
	case "e2e_connsync":
		app, err := e2e.NewApplication(e2e.DefaultConfig(dbDir))
		if err != nil {
			panic(err)
		}
		return NewConnSyncLocalClientCreator(app)
	case "e2e_unsync":
		app, err := e2e.NewApplication(e2e.DefaultConfig(dbDir))
		if err != nil {
			panic(err)
		}
		return NewUnsyncLocalClientCreator(ctx, app)
	case "noop":
		return NewLocalClientCreator(types.NewBaseApplication())
	default:
		mustConnect := false // loop retrying
		return NewRemoteClientCreator(addr, transport, mustConnect)
	}
}
