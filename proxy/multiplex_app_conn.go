package proxy

import (
	"fmt"
	"slices"

	abcicli "github.com/ice-blockchain/cometbft/abci/client"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/libs/service"
	cmtsync "github.com/ice-blockchain/cometbft/libs/sync"
)

// -----------------------------------------------------------------------------------------
// multiplexAppConn

// sharedConnClients contains shared ABCI client instances.
type sharedConnClients struct {
	mtx *cmtsync.Mutex

	consensus abcicli.Client
	mempool   abcicli.Client
	query     abcicli.Client
	snapshot  abcicli.Client
}

// multiplexAppConn implements ChainConns.
type multiplexAppConn struct {
	service.BaseService

	// Thread-safe slice of ChainIDs
	chainMtx *cmtsync.RWMutex
	chainIds []string

	// Implements ChainConns interface
	connsMutex     *cmtsync.RWMutex
	consensusConns map[string]AppConnConsensus
	mempoolConns   map[string]AppConnMempool
	queryConns     map[string]AppConnQuery
	snapshotConns  map[string]AppConnSnapshot

	// Bridges to legacy AppConns interface
	proxyMutex    *cmtsync.RWMutex
	proxyAppConns *multiAppConn

	// ABCI Client using the above conns
	sharedClients *sharedConnClients
	clientCreator ClientCreator
	metrics       *Metrics
}

var _ ChainConns = (*multiplexAppConn)(nil)

// NewMultiplexAppConn makes all necessary abci connections to the application
// for a slice of replicated chains by ChainID.
func NewMultiplexAppConn(
	chainIds []string,
	clientCreator ClientCreator,
	metrics *Metrics,
) ChainConns {
	mac := &multiplexAppConn{
		chainMtx: new(cmtsync.RWMutex),
		chainIds: chainIds,

		connsMutex:     new(cmtsync.RWMutex),
		consensusConns: map[string]AppConnConsensus{},
		mempoolConns:   map[string]AppConnMempool{},
		queryConns:     map[string]AppConnQuery{},
		snapshotConns:  map[string]AppConnSnapshot{},

		proxyMutex:    new(cmtsync.RWMutex),
		proxyAppConns: nil,

		clientCreator: clientCreator,
		sharedClients: &sharedConnClients{
			mtx: new(cmtsync.Mutex),
		},
		metrics: metrics,
	}
	mac.BaseService = *service.NewBaseService(nil, "multiplexAppConn", mac)
	return mac
}

// ChainIds locks the chainMtx for read.
func (conn *multiplexAppConn) ChainIds() []string {
	conn.chainMtx.RLock()
	defer conn.chainMtx.RUnlock()
	return conn.chainIds
}

// AddNetwork implements [ChainConns].
func (conn *multiplexAppConn) AddNetwork(chainID string) {
	chainIds := conn.ChainIds()
	if slices.Contains(chainIds, chainID) {
		return
	}

	cli := conn.sharedClients

	conn.chainMtx.Lock()
	conn.chainIds = append(conn.chainIds, chainID)
	conn.chainMtx.Unlock()

	conn.connsMutex.Lock()
	defer conn.connsMutex.Unlock()

	conn.queryConns[chainID] = NewChainConnQuery(chainID, cli.query, conn.metrics)
	conn.snapshotConns[chainID] = NewChainConnSnapshot(chainID, cli.snapshot, conn.metrics)
	conn.mempoolConns[chainID] = NewChainConnMempool(chainID, cli.mempool, conn.metrics)
	conn.consensusConns[chainID] = NewChainConnConsensus(chainID, cli.consensus, conn.metrics)
}

// Mempool implements [ChainConns].
func (conn *multiplexAppConn) Mempool(chainID string) AppConnMempool {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.mempoolConns[chainID]
}

// Consensus implements [ChainConns].
func (conn *multiplexAppConn) Consensus(chainID string) AppConnConsensus {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.consensusConns[chainID]
}

// Query implements [ChainConns].
func (conn *multiplexAppConn) Query(chainID string) AppConnQuery {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.queryConns[chainID]
}

// Snapshot implements [ChainConns].
func (conn *multiplexAppConn) Snapshot(chainID string) AppConnSnapshot {
	conn.connsMutex.RLock()
	defer conn.connsMutex.RUnlock()
	return conn.snapshotConns[chainID]
}

// ToAppConns converts the instance to be AppConns compatible
// Note that this method uses multiAppConn, not multiplexAppConn.
//
// ToAppConns implements [ChainConns].
func (conn *multiplexAppConn) ToAppConns(chainID string) AppConns {
	conn.proxyMutex.Lock()
	defer conn.proxyMutex.Unlock()

	// Note: this instance uses the legacy AppConns implementation
	// Mutex connsMutex is locked for read through method calls.
	conn.proxyAppConns = &multiAppConn{
		metrics:       conn.metrics,
		consensusConn: conn.Consensus(chainID),
		mempoolConn:   conn.Mempool(chainID),
		queryConn:     conn.Query(chainID),
		snapshotConn:  conn.Snapshot(chainID),

		consensusConnClient: conn.sharedClients.consensus,
		mempoolConnClient:   conn.sharedClients.mempool,
		queryConnClient:     conn.sharedClients.query,
		snapshotConnClient:  conn.sharedClients.snapshot,

		clientCreator: conn.clientCreator,
	}

	return conn.proxyAppConns
}

// OnStart implements [service.Service].
func (conn *multiplexAppConn) OnStart() error {
	if err := conn.startQueryClient(); err != nil {
		return err
	}
	if err := conn.startSnapshotClient(); err != nil {
		conn.stopAllClients()
		return err
	}
	if err := conn.startMempoolClient(); err != nil {
		conn.stopAllClients()
		return err
	}
	if err := conn.startConsensusClient(); err != nil {
		conn.stopAllClients()
		return err
	}

	// Kill CometBFT if the ABCI application crashes.
	go conn.killTMOnClientError()

	return nil
}

func (conn *multiplexAppConn) startQueryClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.query == nil {
		c, err := conn.clientCreator.NewABCIQueryClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (query client): %w", err)
		}
		conn.sharedClients.query = c
		shouldStart = true
	}

	chainIds := conn.ChainIds()

	// .. But we create x connections with the client, one per replicated chain
	conn.connsMutex.Lock()
	for _, chainID := range chainIds {
		conn.queryConns[chainID] = NewChainConnQuery(chainID, conn.sharedClients.query, conn.metrics)
	}
	conn.connsMutex.Unlock()

	// Start only on thread that creates the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.query, "query")
	}

	return nil
}

func (conn *multiplexAppConn) startSnapshotClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.snapshot == nil {
		c, err := conn.clientCreator.NewABCISnapshotClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (snapshot client): %w", err)
		}
		conn.sharedClients.snapshot = c
		shouldStart = true
	}

	chainIds := conn.ChainIds()

	// .. But we create x connections with the client, one per replicated chain
	conn.connsMutex.Lock()
	for _, chainID := range chainIds {
		conn.snapshotConns[chainID] = NewChainConnSnapshot(chainID, conn.sharedClients.snapshot, conn.metrics)
	}
	conn.connsMutex.Unlock()

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.snapshot, "snapshot")
	}

	return nil
}

func (conn *multiplexAppConn) startMempoolClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.mempool == nil {
		c, err := conn.clientCreator.NewABCIMempoolClient()
		if err != nil {
			return fmt.Errorf("error creating ABCI client (mempool client): %w", err)
		}
		conn.sharedClients.mempool = c
		shouldStart = true
	}

	chainIds := conn.ChainIds()

	// .. But we create x connections with the client, one per replicated chain
	conn.connsMutex.Lock()
	for _, chainID := range chainIds {
		conn.mempoolConns[chainID] = NewChainConnMempool(chainID, conn.sharedClients.mempool, conn.metrics)
	}
	conn.connsMutex.Unlock()

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.mempool, "mempool")
	}

	return nil
}

func (conn *multiplexAppConn) startConsensusClient() error {
	// Tracks whether a new client is created
	shouldStart := false

	// One shared ABCI client used by all replicated chains
	if conn.sharedClients.consensus == nil {
		c, err := conn.clientCreator.NewABCIConsensusClient()
		if err != nil {
			conn.stopAllClients()
			return fmt.Errorf("error creating ABCI client (consensus client): %w", err)
		}
		conn.sharedClients.consensus = c
		shouldStart = true
	}

	chainIds := conn.ChainIds()

	// .. But we create x connections with the client, one per replicated chain
	conn.connsMutex.Lock()
	for _, chainID := range chainIds {
		conn.consensusConns[chainID] = NewChainConnConsensus(chainID, conn.sharedClients.consensus, conn.metrics)
	}
	conn.connsMutex.Unlock()

	// Start only on thread that created the ABCI client
	if shouldStart {
		return conn.startClient(conn.sharedClients.consensus, "consensus")
	}

	return nil
}

func (conn *multiplexAppConn) startClient(c abcicli.Client, addr string) error {
	c.SetLogger(conn.Logger.With("module", "abci-client", "connection", addr))
	if err := c.Start(); err != nil {
		return fmt.Errorf("error starting ABCI client (%s client): %w", addr, err)
	}
	return nil
}

// OnStop implements [service.Service].
func (conn *multiplexAppConn) OnStop() {
	conn.stopAllClients()
}

func (conn *multiplexAppConn) killTMOnClientError() {
	killFn := func(conn string, err error, logger cmtlog.Logger) {
		logger.Error(
			conn+" connection terminated. Did the application crash? Please restart CometBFT",
			"err", err)
		killErr := cmtos.Kill()
		if killErr != nil {
			logger.Error("Failed to kill this process - please do so manually", "err", killErr)
		}
	}

	for {
		select {
		// NOTE(midas): stop selecting as we are handling a shutdown.
		case <-conn.Quit():
			return
		case <-conn.sharedClients.consensus.Quit():
			if err := conn.sharedClients.consensus.Error(); err != nil {
				killFn(connConsensus, err, conn.Logger)
			}
		case <-conn.sharedClients.mempool.Quit():
			if err := conn.sharedClients.mempool.Error(); err != nil {
				killFn(connMempool, err, conn.Logger)
			}
		case <-conn.sharedClients.query.Quit():
			if err := conn.sharedClients.query.Error(); err != nil {
				killFn(connQuery, err, conn.Logger)
			}
		case <-conn.sharedClients.snapshot.Quit():
			if err := conn.sharedClients.snapshot.Error(); err != nil {
				killFn(connSnapshot, err, conn.Logger)
			}
		default: // come back later.
		}
	}
}

func (conn *multiplexAppConn) stopAllClients() {
	if conn.sharedClients.consensus != nil {
		if err := conn.sharedClients.consensus.Stop(); err != nil {
			conn.Logger.Error("error while stopping consensus client", "error", err)
		}
	}
	if conn.sharedClients.mempool != nil {
		if err := conn.sharedClients.mempool.Stop(); err != nil {
			conn.Logger.Error("error while stopping mempool client", "error", err)
		}
	}
	if conn.sharedClients.query != nil {
		if err := conn.sharedClients.query.Stop(); err != nil {
			conn.Logger.Error("error while stopping query client", "error", err)
		}
	}
	if conn.sharedClients.snapshot != nil {
		if err := conn.sharedClients.snapshot.Stop(); err != nil {
			conn.Logger.Error("error while stopping snapshot client", "error", err)
		}
	}
}
