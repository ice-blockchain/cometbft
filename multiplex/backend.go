package multiplex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/log"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	rpccore "github.com/ice-blockchain/cometbft/rpc/core"
	rpcclient "github.com/ice-blockchain/cometbft/rpc/jsonrpc/client"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
	sm "github.com/ice-blockchain/cometbft/state"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
	"github.com/rs/cors"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
)

const (
	// Collecting metrics every 10 seconds, this may need to be adapted
	// to equal the Prometheus scrape interval (1s) for better granularity.
	metricsTickerDuration = 10 * time.Second

	// Prometheus timeout configuration
	readHeaderTimeout = 10 * time.Second
)

// Assert that our implementation satisfies the [server.Backend] interface.
var _ server.Backend = (*MultiplexBackend)(nil)

// ----------------------------------------------------------------------------
// MultiplexBackend defines a multiplex backend adapter implementation
//
// MultiplexBackend implements the [server.Backend] interface for a multiplex.
// This implementation makes use of an internal [Reactor] instance to read
// local networks heights and uses instances of [p2p.Switch] to communicate
// with relays about chain replications and transaction broadcasts.
//
// Additionally, an internal [client.Acceptor] instance may be used to further
// extend the broadcast process, e.g. to call RollbackTx.
type MultiplexBackend struct {
	// A mutex is locked for reactor and eventSwitch updates.
	relayMtx sync.Mutex

	// The multiplex reactor is used to find ChainID, last block heights,
	// and to retrieve the AddrBook and connect to unknown relays.
	reactor      *Reactor
	eventSwitch  *p2p.Switch
	rpcListeners []net.Listener
	httpServers  []*http.Server

	multiNodeInfo   *MultiNetworkNodeInfo
	broadcastAddr   *p2p.NetAddress // P2P Discovery (:dp)
	discoveryAddr   *p2p.NetAddress // RPC Discovery (:dp-1)
	cometbftP2PAddr *p2p.NetAddress // CometBFT P2P  (:dp+1)
	cometbftRPCAddr *p2p.NetAddress // CometBFT RPC  (:dp+2)
	prometheusAddr  *p2p.NetAddress // Prometheus (:dp+3)

	// An acceptor implementation to which transactions will be forwarded.
	acceptor client.Acceptor

	// Routines may be extended directly using the [server.Jobs] struct.
	routines *server.Jobs

	// Stores the node IDs of relays to whom we sent P2P messages.
	replRequestsSent map[string][]string
	poolRequestsSent map[string][]string

	// This channel is used to wait when new networks must be created.
	newChainReadyCh chan string

	// This channel is used to communicate the tx hash of a transaction
	// that has been accepted by our own mempool AND by a relays' mempool.
	relayAcceptTxCh chan string

	// Internals
	logger   cmtlog.Logger
	errorsCh chan error
	metrics  *Metrics
}

// WithRoutines is an option helper to overwrite the [server.Jobs] instance.
func WithRoutines(jobs *server.Jobs) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.routines = jobs
	}
}

// WithMetrics is an option helper to overwrite the [Metrics] instance.
func WithMetrics(metrics *Metrics) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.metrics = metrics
	}
}

// NewServer initializes a new [MultiplexBackend] around an empty multiplex
// configuration and prepares the node backend by starting the reactor and
// configuring the necessary node services, i.e. event bus, mempool, etc.
//
// The internal [Reactor] instance will be started when calling this method,
// and individual [node.Node] instances can be retrieved using the reactor's
// services registry: [Reactor#GetServiceProvider].
//
// See also: [NewNodesMultiplex]
func NewServer(
	impl client.Acceptor,
	nodeConfig *config.Config,
	nodeLogger cmtlog.Logger,
	options ...func(*MultiplexBackend),
) (*MultiplexBackend, error) {
	// Force to create blocks only if there is transactions.
	nodeConfig.Consensus.CreateEmptyBlocks = false

	initTime := time.Now()
	_, reactor, err := NewNodesMultiplex(
		context.Background(),
		impl,
		nodeConfig,
		nodeLogger,
		node.NodeWithStartRPC(false),     // delegates to MustStart()
		node.NodeWithStartP2P(false),     // delegates to MustStart()
		node.NodeWithStartMonitor(false), // delegates to MustStart()
	)
	if err != nil {
		return nil, fmt.Errorf(
			"SERVER PANIC: could not initialize node backend: %w", err)
	}

	server := &MultiplexBackend{
		reactor:      reactor,
		acceptor:     impl,
		rpcListeners: []net.Listener{},
		httpServers:  []*http.Server{},

		logger:   nodeLogger,
		errorsCh: make(chan error),
	}

	// Enable overwrite of optional properties
	for _, option := range options {
		option(server)
	}

	if server.metrics != nil {
		defer addTimeSample(server.metrics.InitDurationSeconds, initTime)()
	}

	return server, nil
}

// GetLogger returns the [cmtlog.Logger] property.
func (b *MultiplexBackend) GetLogger() cmtlog.Logger {
	return b.logger
}

// GetAcceptor returns the injected [client.Acceptor] implementation.
//
// GetAcceptor implements [server.Server]
func (b *MultiplexBackend) GetAcceptor() client.Acceptor {
	if b.acceptor == nil {
		return &client.DefaultAcceptor{}
	}

	return b.acceptor
}

// GetReactor returns the [Reactor] instance.
func (b *MultiplexBackend) GetReactor() *Reactor {
	return b.reactor
}

// GetNewChainReadyCh returns a channel used to communicate ChainID values.
func (b *MultiplexBackend) GetNewChainReadyCh() chan<- string {
	return b.newChainReadyCh
}

// GetRelayAcceptTxCh returns a channel used to communicate transaction hashes.
func (b *MultiplexBackend) GetRelayAcceptTxCh() chan<- string {
	return b.relayAcceptTxCh
}

// GetRelayID returns the node ID assigned in the reactor.
func (b *MultiplexBackend) GetRelayID() p2p.ID {
	return b.reactor.nodeKey.ID()
}

// GetListenAddress should return the relay's listen address.
func (b *MultiplexBackend) GetListenAddress() string {
	return b.broadcastAddr.String()
}

// GetNetworks should return a slice of supported ChainID values.
func (b *MultiplexBackend) GetNetworks() []string {
	if b.reactor == nil {
		return []string{}
	}

	return b.reactor.GetNetworks()
}

// GetReplRequestPeers returns a list of node IDs to whom we have previously
// sent a ChainReplicationRequest.
// See also: [DefaultNodeReplRequestRoutine]
func (b *MultiplexBackend) GetReplRequestPeers(chainID string) []string {
	if peers, ok := b.replRequestsSent[chainID]; ok {
		return peers
	}

	return []string{}
}

// GetPoolRequestPeers returns a list of node IDs to whom we have previously
// sent a transaction through the Mempool.
// See also: [DefaultRelaysBroadcastRoutine]
func (b *MultiplexBackend) GetPoolRequestPeers(chainID string) []string {
	if peers, ok := b.poolRequestsSent[chainID]; ok {
		return peers
	}

	return []string{}
}

// EventSwitch creates a local [p2p.Switch] instance which is used
// to determine the required channels and connection information.
//
// The relayMtx is expected to be locked by the caller.
func (b *MultiplexBackend) EventSwitch() *p2p.Switch {
	if b.reactor.discoverySwitch != nil {
		return b.reactor.discoverySwitch
	}

	b.logger.Debug("Creating switch for P2P discovery",
		"addr", b.broadcastAddr.String(),
	)

	// In-place mutation of the listen address so that it always uses
	// the configured broadcast address.
	b.multiNodeInfo = NewMultiNetworkNodeInfo(
		b.reactor.nodeConfig,
		b.reactor.nodeKey,
		b.broadcastAddr,
	)

	mConnConfig := p2p.MConnConfig(b.reactor.nodeConfig.P2P)
	localTransport := p2p.NewMultiplexTransportWithCustomHandshake(
		b.multiNodeInfo, // local nodeInfo
		*b.reactor.nodeKey,
		mConnConfig,
		MultiplexTransportHandshake,
	)

	sw := p2p.NewSwitch(
		b.reactor.nodeConfig.P2P,
		localTransport,
	)
	sw.SetLogger(b.reactor.logger.With("module", "p2p"))
	sw.SetNodeInfo(b.multiNodeInfo)
	sw.SetNodeKey(b.reactor.nodeKey)

	// Make sure we listen to ChainReplicationRequest messages
	sw.AddReactor(conn.SharedChannelsNamespace, "MULTIPLEX", b.reactor)

	b.reactor.discoverySwitch = sw
	return sw
}

// UpdateAvailableNetworks updates the NodeInfo pointer and event switch
// to permit communications related to networks.
//
// TODO(midas): TBI whether p2p.conn channels must be opened manually.
func (b *MultiplexBackend) UpdateAvailableNetworks(networks []string) []string {
	if b.reactor.discoverySwitch == nil {
		return b.multiNodeInfo.Networks
	}

	for _, chainID := range networks {
		if !slices.Contains(b.multiNodeInfo.Networks, chainID) {
			b.multiNodeInfo.Networks = append(b.multiNodeInfo.Networks, chainID)
			b.multiNodeInfo.ProtocolVersions = append(b.multiNodeInfo.ProtocolVersions,
				NewChainProtocolVersion(
					chainID,
					DefaultProtocolVersion,
				),
			)
		}
	}

	b.reactor.discoverySwitch.SetNodeInfo(b.multiNodeInfo)
	return b.multiNodeInfo.Networks
}

// MustStart starts a replication backend basically selecting void
// and running forever.
//
// This method also opens a custom P2P "broadcast" port such that
// the relay may be communicated to, even without hosting any
// replicated chain.
//
// MustStart implements [server.Server]
func (b *MultiplexBackend) MustStart() {
	startTime := time.Now()

	if b.metrics != nil {
		defer addTimeSample(b.metrics.StartDurationSeconds, startTime)()
	}

	b.logger.Debug("Process now starting a node backend",
		"id", b.reactor.nodeKey.ID(),
	)

	// We use a wait group to block the process until the transport
	// and p2p switches are created and until we start listening.
	var wg sync.WaitGroup
	wg.Add(1)

	// Since we'll modify the reactor and eventSwitch internals,
	// we lock the mutex to ensure that initialization completes.
	b.relayMtx.Lock()
	defer b.relayMtx.Unlock() // happens after wg.Wait()

	// Starting a node backend starts internal channels
	b.newChainReadyCh = make(chan string)
	b.relayAcceptTxCh = make(chan string)
	b.replRequestsSent = map[string][]string{}

	go b.metricsReporter()

	// Here we should wait forever, until the internal Reactor instance
	// is told to replicate a new chain using the server.ReplicationChannel.
	go func() {
		// CAUTION:
		// We open a discovery port which is required such that the relay may
		// be communicated to, even without hosting any replicated chain.
		//
		// Additionally, a RPC server is started which permits to read
		// node information such as the node ID.

		if _, err := b.StartP2PServerDiscovery(
			b.reactor.nodeConfig,
			b.reactor.nodeKey,
		); err != nil {
			wg.Done()
			b.logger.Error("error with P2P server", "err", err)
			return
		}

		if _, err := b.StartRPCServerDiscovery(
			b.reactor.nodeConfig,
			b.reactor.nodeKey,
		); err != nil {
			wg.Done()
			b.logger.Error("error with RPC server", "err", err)
			return
		}

		// Start the Prometheus server, if enabled.
		if err := b.StartPrometheusServer(); err != nil {
			b.logger.Error("error with Prometheus server", "err", err)
			return
		}

		b.logger.Info("Process idle, waiting to replicate chains...",
			"time", cmttime.Now(),
			"id", b.reactor.nodeKey.ID(),
			"p2p", b.broadcastAddr.DialString(),
			"rpc", b.discoveryAddr.DialString(),
			"mon", b.prometheusAddr.DialString(),
			"info", b.reactor.discoverySwitch.NodeInfo(),
		)

		// Start the RPC server before the P2P server
		// so we can eg. receive txs for the first block
		if err := b.StartRPCServerCometBFT(); err != nil {
			b.logger.Error("error with CometBFT RPC server", "err", err)
			return
		}

		// Then start the P2P server
		if err := b.StartP2PServerCometBFT(); err != nil {
			b.logger.Error("error with CometBFT P2P server", "err", err)
			return
		}

		b.logger.Info("CometBFT RPC and P2P listening",
			"time", cmttime.Now(),
			"id", b.reactor.nodeKey.ID(),
			"p2p", b.cometbftP2PAddr.DialString(),
			"rpc", b.cometbftRPCAddr.DialString(),
			"info", b.reactor.cometbftSwitch.NodeInfo(),
		)

		if len(b.reactor.GetNetworks()) > 0 {
			if err := b.StartNodeInstances(); err != nil {
				b.errorsCh <- err
			}
		}

		// This relay can now be used to communicate P2P messages.
		wg.Done()

		if b.metrics != nil {
			addTimeSample(b.metrics.StartDurationSeconds, startTime)()
		}

		for {
			select {
			case <-b.reactor.Quit():
				return

			case err := <-b.errorsCh:
				b.logger.Error("error with multiplex backend", "err", err)
				return
			}
		}
	}()

	// IMPORTANT:
	// Block until we successfully setup the p2p server.
	wg.Wait()
}

// Close implements io.Closer
func (b *MultiplexBackend) Close() error {
	// Lock the mutex to complete shutdown gracefully
	b.relayMtx.Lock()
	defer b.relayMtx.Unlock()

	b.logger.Debug("Shutting down node backend",
		"id", b.reactor.nodeKey.ID(),
	)

	// Stop any running node runtime
	if len(b.reactor.GetNetworks()) > 0 {
		if err := b.StopNodeInstances(); err != nil {
			return err
		}
	}

	if b.reactor != nil {
		// Must stop the node backend
		b.reactor.Stop()
		b.reactor.Reset()
	}

	// We may close channels now.
	if b.newChainReadyCh != nil {
		close(b.newChainReadyCh)
	}

	if b.relayAcceptTxCh != nil {
		close(b.relayAcceptTxCh)
	}

	// Stop RelayInfo RPC and CometBFT RPC
	for _, rpcListener := range b.rpcListeners {
		rpcListener.Close()
	}

	// Stop any custom HTTP servers (e.g. prometheus)
	for _, httpServer := range b.httpServers {
		httpServer.Close()
	}

	return nil
}

// WaitForNextAvailableNetwork waits for a chain replication using
// the internal newChainReadyCh channel and returns a ChainID.
// WaitForNextAvailableNetwork implements [server.Backend].
func (b *MultiplexBackend) WaitForNextAvailableNetwork(
	ctx context.Context,
) (string, error) {
	select {
	case chainID := <-b.newChainReadyCh:
		return chainID, nil

	case <-ctx.Done():
		return "", errors.New(
			"process timed out waiting for network availability")
	}
}

// WaitForRelayReplResponse waits for a relay replication response using
// the internal ackReplResCh channel and returns a relay ID.
// WaitForRelayReplResponse implements [server.Backend].
func (b *MultiplexBackend) WaitForRelayReplResponse(
	ctx context.Context,
) (string, error) {
	select {
	case res := <-b.reactor.ackReplResCh:
		return res.NodeId, nil

	case <-ctx.Done():
		return "", errors.New(
			"process timed out waiting for relay replication")
	}
}

// WaitForRelayAckTransaction waits for a *remote* relay to acknowledge
// a transaction broadcast operation and reads a message from the internal
// ackTxAcceptCh channel.
// Use this method to wait for a transaction to be accepted *remotely*.
// WaitForRelayAckTransaction implements [server.Backend].
func (b *MultiplexBackend) WaitForRelayAckTransaction(
	ctx context.Context,
) error {
	// Blocks until a AckTransactionBroadcast was received.
	select {
	case ackResponse := <-b.reactor.ackTxAcceptCh:
		for _, txHash := range ackResponse.TxHashes {
			b.relayAcceptTxCh <- fmt.Sprintf("%x", txHash)
		}

	case <-ctx.Done():
		return errors.New(
			"process timed out waiting for relay replication")
	}

	return nil
}

// WaitForRelayTxAcceptance waits for a relay transaction acceptance using
// the internal relayAcceptTxCh channel and returns a transaction hash.
// WaitForRelayTxAcceptance implements [server.Backend].
func (b *MultiplexBackend) WaitForRelayTxAcceptance(
	ctx context.Context,
) (string, error) {
	select {
	case txHash := <-b.relayAcceptTxCh:
		return txHash, nil

	case <-ctx.Done():
		return "", errors.New(
			"process timed out waiting for relay acceptance")
	}
}

// getLocalNetworkHeights finds out about the last block height and determines
// a list of networks that must be created. The list of networks that must be
// created will also be present in the list of required networks.
// GetLocalNetworkHeights implements [server.Backend].
func (b *MultiplexBackend) GetLocalNetworkHeights(
	userAddress string,
	transactions ...client.Transaction,
) (map[string]int64, []string) {
	requiredNetworks := map[string]int64{}
	unknownNetworks := map[string]bool{}
	for _, tx := range transactions {
		chainID := client.GetChainID(userAddress, tx.Fingerprint)
		stateProvider := b.reactor.GetInstanceProvider(InstanceKeyState)

		// If we don't know this network, we either need a background-sync
		// or we must create a new network if other relays also don't know it.
		localBlockHeight := int64(1)
		if !b.reactor.HasNetwork(chainID) {
			unknownNetworks[chainID] = true
		} else {
			stateMachine := stateProvider(chainID).(sm.State)
			localBlockHeight = stateMachine.LastBlockHeight
		}

		requiredNetworks[chainID] = localBlockHeight
	}

	// Returns as a slice of unique ChainIDs
	mustCreateNetworks := []string{}
	for unknownChainID := range unknownNetworks {
		mustCreateNetworks = append(mustCreateNetworks, unknownChainID)
	}

	return requiredNetworks, mustCreateNetworks
}

// GetRemoteRelayInfo connects to relayAddress using a JSONRPC client,
// and calls the GetRelayInfo remote procedure to retrieve the Relay ID,
// the supported networks and the listen address for the remote relay.
//
// GetRemoteRelayInfo implements [server.Backend].
func (b *MultiplexBackend) GetRemoteRelayInfo(
	relayAddress *server.RelayAddress,
) (*server.RPCResultRelayInfo, error) {
	c, connectErr := rpcclient.New(relayAddress.AddressForRelayInfo())
	if connectErr != nil {
		return nil, connectErr
	}

	result := &server.RPCResultRelayInfo{}
	params := map[string]any{}
	_, callErr := c.Call(context.TODO(), "info", params, result)
	if callErr != nil {
		return nil, callErr
	}

	return result, nil
}

// GetRelaysByNetwork maps each supported network to a slice of relay addresses
// and it also returns a slice of relays that produced errors,
// e.g. network error.
//
// This method uses [GetRemoteRelayInfo] to find the relay's ID.
// GetRelaysByNetwork implements [server.Backend].
func (b *MultiplexBackend) GetRelaysByNetwork(
	relayAddresses []*server.RelayAddress,
) (
	chainRelays map[string][]*server.RelayAddress,
	errorRelays []string,
) {
	relaysWithFailure := map[string]bool{}

	// Connect to all other relays using RPC (discovery server) to find
	// out their relay ID (CometBFT Node ID) before we can connect with P2P.
	chainRelays = map[string][]*server.RelayAddress{}
	errorRelays = []string{}
	for _, relayAddr := range relayAddresses {
		// Discover this relay's ID (CometBFT Node ID).
		// This executes a RPC request for RelayInfo.
		result, err := b.GetRemoteRelayInfo(relayAddr)
		if err != nil {
			b.logger.Error("Error discovering relay information",
				"relay", relayAddr,
				"err", err,
			)
			relaysWithFailure[relayAddr.String()] = true
			continue
		}

		b.logger.Debug("Retrieved networks information from relay",
			"relay", relayAddr.String(),
			"networks", result.Networks,
		)

		relayAddr.SetID(result.DefaultNodeID)

		// Populate a map of relay addresses by ChainID.
		for _, chainID := range result.Networks {
			if _, ok := chainRelays[chainID]; !ok {
				chainRelays[chainID] = []*server.RelayAddress{}
			}

			chainRelays[chainID] = append(chainRelays[chainID], relayAddr)
		}
	}

	// Populate a slice of relay addresses which produced errors
	for errRelay, _ := range relaysWithFailure {
		if !slices.Contains(errorRelays, errRelay) {
			errorRelays = append(errorRelays, errRelay)
		}
	}

	return chainRelays, errorRelays
}

// CheckDialCompatibleRelay dials the relay using a local [p2p.Switch] instance
// to perform a handshake and determine whether relayAddress is compatible.
//
// CheckDialCompatibleRelay implements [server.Backend].
func (b *MultiplexBackend) CheckDialCompatibleRelay(
	relayAddr *server.RelayAddress,
) error {
	// If this is us, nothing to do.
	if relayAddr.ID() == b.reactor.nodeKey.ID() {
		return nil
	}

	b.logger.Debug("Process now dialing remote relay",
		"relay", relayAddr.String(),
	)

	relayNetAddr, err := relayAddr.NetAddress()
	if err != nil {
		return fmt.Errorf(
			"invalid relay address %s: %w", relayAddr.String(), err)
	}

	// Dial the relay to find out whether it is compatible (handshake).
	// Using the switch here affects the internal AddrBook.
	localSwitch := b.EventSwitch()
	if err := localSwitch.DialPeerWithAddress(relayNetAddr); err != nil {
		// Ignore existing addresses here in case of long-living process
		// broadcasting more transactions, when peer is already dialed.
		if _, ok := err.(p2p.ErrCurrentlyDialingOrExistingAddress); ok {
			return nil
		}

		return fmt.Errorf(
			"could not dial relay %s: %w", relayAddr.String(), err)
	}

	return nil
}

// ApplyFilterReplRequestRelays filters relays and returns a map of relays
// by ChainID which contains only relays that need to catchup, i.e. it returns
// relays that will receive a chain replication request.
func (b *MultiplexBackend) ApplyFilterReplRequestRelays(
	requiredNetworks []string,
	relays []*server.RelayAddress,
	chainRelays map[string][]*server.RelayAddress,
) map[string][]*server.RelayAddress {
	catchupRelays := map[string][]*server.RelayAddress{}
	for chainID, relaysByChain := range chainRelays {
		// Did all relays report to know this ChainID?
		if len(relaysByChain) == len(relays) {
			catchupRelays[chainID] = nil
			continue
		}

		// Build a (searchable) slice of relay IDs
		relayIdsByChain := []string{}
		for _, relayAddr := range relaysByChain {
			relayIdsByChain = append(relayIdsByChain, string(relayAddr.ID()))
		}

		// Find out which relays are missing for this chain.
		// Those are relays that need to catchup with the chain.
		for _, relayAddr := range relays {
			if !slices.Contains(relayIdsByChain, string(relayAddr.ID())) {
				catchupRelays[chainID] = append(catchupRelays[chainID], relayAddr)
			}
		}
	}

	// Handling case when chainRelays is empty (0 networks on remote relays).
	for _, chainID := range requiredNetworks {
		if _, has := catchupRelays[chainID]; !has {
			catchupRelays[chainID] = append(catchupRelays[chainID], relays...)
		}
	}

	// Also reset the sent requests cache
	b.replRequestsSent = map[string][]string{}

	return catchupRelays
}

// AddTransactions executes the CheckTx call to add individual
// transactions to the mempool by ChainID.
//
// This method is called by [BroadcastTx] when the transaction is ready
// to be broadcast to all other relays. Adding the transaction to the
// mempool effectively marks the transaction as locally accepted.
// AddTransactions implements [server.Backend].
func (b *MultiplexBackend) AddTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With("chain_id", chainID)

		reactorsProvider := b.reactor.GetServicesProvider()
		memplReactor := reactorsProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		chainMempool := memplReactor.GetMempoolPtr()

		checkTxRes, err := chainMempool.CheckTx(
			client.TransactionToRawTx(transaction),
			b.reactor.GetNodeKey().ID(),
		)
		if err != nil {
			return err
		}

		// Inform about local mempool addition result
		clogger.Info("Received CheckTx response", "res", checkTxRes)
	}

	return nil
}

// RemoveTransactions remove a transaction from the local mempool
// if it has been added already, e.g. using addTransactionToMempool.
//
// This method is called by [BroadcastTx] when a transaction rollback must
// be executed due to some of the healthy relays not accepting a batch.
// RemoveTransactions implements [server.Backend].
func (b *MultiplexBackend) RemoveTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		clogger := b.logger.With("chain_id", chainID)

		reactorsProvider := b.reactor.GetServicesProvider()
		memplReactor := reactorsProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		chainMempool := memplReactor.GetMempoolPtr()

		memTx := client.TransactionToRawTx(transaction)
		if err := chainMempool.RemoveTxByKey(memTx.Key()); err != nil {
			clogger.Debug("Rollback transaction not in local mempool (not an error)",
				"tx", log.NewLazySprintf("%X", memTx.Hash()),
				"error", err.Error())
		}
	}

	return nil
}

// ----------------------------------------------------------------------------
// Servers

// StartP2PServerDiscovery creates a [p2p.Switch] instance that may be used
// to transport [ChainReplicationRequest] messages to nodes that do not have
// network ports open yet (due to not replicating any chain).
// Creates a transport listening on DiscoveryPort.
func (b *MultiplexBackend) StartP2PServerDiscovery(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	p2pListenAddr := overwriteListenPort(
		nodeCfg.P2P.ListenAddress,
		int(nodeCfg.DiscoveryPort),
	)

	relayAddr, err := server.NewRelayAddress(p2pListenAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"could not create relay address for P2P: %w", err)
	}

	relayAddr.SetID(nodeKey.ID())
	if b.broadcastAddr, err = relayAddr.NetAddress(); err != nil {
		return nil, fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	b.logger.Info("Process is now setting up P2P discovery",
		"addr", relayAddr.String(),
	)

	// Initializes the local p2p.Switch
	// Creates a global P2P switch to respond even without chain info.
	eventSwitch := b.EventSwitch()

	// And start the switch (the P2P server).
	err = eventSwitch.Start()
	if err != nil {
		return nil, fmt.Errorf(
			"could not start p2p switch: %w", err)
	}

	// Open the broadcast port for listening continuously
	if listenTransport := eventSwitch.Transport(); listenTransport != nil {
		netAddress := b.broadcastAddr
		if err := listenTransport.Listen(*netAddress); err != nil {
			return nil, fmt.Errorf(
				"could not start listening on %s: %w", netAddress.DialString(), err)
		}

		b.logger.Debug("Process is now listening on broadcast port",
			"addr", netAddress.DialString(),
		)
	}

	return b.broadcastAddr, nil
}

// StartRPCServerDiscovery starts a RPC server with a RelayInfo function that
// may be used to retrieve node information, including the node ID.
// This method sets the listen address in discoveryAddr.
// Creates a transport listening on DiscoveryPort-1.
func (b *MultiplexBackend) StartRPCServerDiscovery(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	// RPC Discovery Port is always: `discovery_port-1`
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.DiscoveryPort-1), // always DiscoveryPort-1
	)

	relayAddr, err := server.NewRelayAddress(rpcListenAddr)
	if err != nil {
		return nil, fmt.Errorf(
			"could not create relay address for RPC: %w", err)
	}

	relayAddr.SetID(nodeKey.ID())
	if b.discoveryAddr, err = relayAddr.NetAddress(); err != nil {
		return nil, fmt.Errorf(
			"could not create rpc listen address: %w", err)
	}

	b.logger.Info("Process is now setting up RPC discovery",
		"addr", relayAddr.StringWithoutId(),
	)

	// Initializes a local RPC server
	nodeRpc := b.reactor.nodeConfig.RPC
	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = nodeRpc.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = nodeRpc.MaxBodyBytes
	rpcConf.MaxHeaderBytes = nodeRpc.MaxHeaderBytes
	rpcConf.MaxOpenConnections = nodeRpc.MaxOpenConnections

	mux := http.NewServeMux()
	rpcLogger := b.reactor.logger.With("module", "rpc-server")

	// Enabled procedures:
	// - "info": POST /info to retrieve RelayInfo.

	infoImpl := server.NewRelayInfoServer(b)
	rpcserver.RegisterRPCFuncs(mux, map[string]*rpcserver.RPCFunc{
		"info": rpcserver.NewRPCFunc(infoImpl.GetRelayInfo, ""),
	}, rpcLogger)

	rpcListener, err := rpcserver.Listen(
		relayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return nil, err
	}

	var rootHandler http.Handler = mux
	go func() {
		if err := rpcserver.Serve(
			rpcListener,
			rootHandler,
			rpcLogger,
			rpcConf,
		); err != nil && !errors.Is(err, net.ErrClosed) {
			b.logger.Error("Error serving RPC discovery server", "err", err)
		}
	}()

	b.rpcListeners = append(b.rpcListeners, rpcListener)

	return b.discoveryAddr, nil
}

// StartP2PServerCometBFT creates the CometBFT P2P Server that may be used
// to interact directly with a CometBFT node runtime, e.g to broadcast a
// transaction.
// Creates a transport listening on DiscoveryPort+1.
// This method sets the listen address in cometbftP2PAddr.
func (b *MultiplexBackend) StartP2PServerCometBFT() error {
	// P2P CometBFT Port is always: `discovery_port+1`
	p2pListenAddr := overwriteListenPort(
		b.reactor.nodeConfig.P2P.ListenAddress,
		int(b.reactor.nodeConfig.DiscoveryPort+1), // always DiscoveryPort+1
	)

	sw := b.reactor.cometbftSwitch // uses DiscoveryPort+1

	// Start the transport.
	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(
		b.reactor.nodeKey.ID(),
		p2pListenAddr,
	))
	if err != nil {
		return err
	}
	if err := sw.Transport().Listen(*addr); err != nil {
		return err
	}

	// Start the switch (the P2P server).
	err = sw.Start()
	if err != nil {
		return err
	}

	b.cometbftP2PAddr = addr

	// Always connect to chain seed nodes, if any available
	if len(b.reactor.nodeConfig.P2P.Seeds) > 0 {
		knownSeeds := splitAndTrimEmpty(b.reactor.nodeConfig.P2P.Seeds, ",", " ")
		seedIds := []string{}
		for _, seedNodeAddr := range knownSeeds {
			seedAddr, err := server.NewRelayAddress(seedNodeAddr)
			if err != nil {
				return fmt.Errorf(
					"could not create seed node address for P2P: %w", err)
			}

			seedIds = append(seedIds, string(seedAddr.ID()))
		}

		sw.AddUnconditionalPeerIDs(seedIds)

		if err := sw.DialPeersAsync(knownSeeds); err != nil {
			return fmt.Errorf("could not dial peers from seeds field: %w", err)
		}
	}

	return nil
}

// StartRPCServerCometBFT starts a CometBFT RPC server that may be used
// to interact directly with a CometBFT node runtime, e.g to request the
// status of a node.
//
// CAUTION:
// Each network's ChainID is appended to RPC route names,
// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
// This permits us to use legacy RPC function implementation without
// modification apart from the route paths.
//
// Creates a transport listening on DiscoveryPort+2.
// This method sets the listen address in cometbftRPCAddr.
func (b *MultiplexBackend) StartRPCServerCometBFT() error {
	nodeCfg := b.reactor.nodeConfig

	// RPC CometBFT Port is always: `discovery_port+2`
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.DiscoveryPort+2), // always DiscoveryPort+2
	)

	// We configure one RPC environment per running network,
	// i.e. contains reactors, stores and genesis.
	nodesProvider := b.reactor.GetServicesProvider()
	chainRoutes := map[string]rpccore.RoutesMap{}
	for _, chainID := range b.GetNetworks() {
		nodeRuntime := nodesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		env, err := nodeRuntime.ConfigureRPC()
		if err != nil {
			return fmt.Errorf(
				"could not create RPC environment with ChainID %s: %w", chainID, err)
		}

		nodeRoutes := env.GetRoutes()
		if nodeCfg.RPC.Unsafe {
			env.AddUnsafeRoutes(nodeRoutes)
		}

		chainRoutes[chainID] = nodeRoutes
	}

	// Each network's ChainID is appended to the route name.
	// i.e. `/broadcast_tx_commit/%CHAIN_ID%`.
	routes := rpccore.RoutesMap{}
	for chainID, nodeRoutes := range chainRoutes {
		for route, rpcFunc := range nodeRoutes {
			routeKey := route + "/" + chainID
			routes[routeKey] = rpcFunc
		}
	}

	relayAddr, err := server.NewRelayAddress(rpcListenAddr)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for RPC: %w", err)
	}

	relayAddr.SetID(b.reactor.nodeKey.ID())
	if b.cometbftRPCAddr, err = relayAddr.NetAddress(); err != nil {
		return fmt.Errorf(
			"could not create rpc listen address: %w", err)
	}

	b.logger.Info("Process is now setting up CometBFT RPC",
		"addr", relayAddr.StringWithoutId(),
	)

	rpcConf := rpcserver.DefaultConfig()
	rpcConf.MaxRequestBatchSize = nodeCfg.RPC.MaxRequestBatchSize
	rpcConf.MaxBodyBytes = nodeCfg.RPC.MaxBodyBytes
	rpcConf.MaxHeaderBytes = nodeCfg.RPC.MaxHeaderBytes
	rpcConf.MaxOpenConnections = nodeCfg.RPC.MaxOpenConnections
	if rpcConf.WriteTimeout <= nodeCfg.RPC.TimeoutBroadcastTxCommit {
		rpcConf.WriteTimeout = nodeCfg.RPC.TimeoutBroadcastTxCommit + 1*time.Second
	}

	rpcMultiplexer := http.NewServeMux()
	rpcLogger := b.logger.With("module", "rpc-server")
	wmLogger := rpcLogger.With("protocol", "websocket")
	wm := rpcserver.NewWebsocketManager(routes,
		// TODO(midas): many instances of eventBus exist
		// rpcserver.OnDisconnect(func(remoteAddr string) {
		// 	err := n.eventBus.UnsubscribeAll(context.Background(), remoteAddr)
		// 	if err != nil && err != cmtpubsub.ErrSubscriptionNotFound {
		// 		wmLogger.Error("Failed to unsubscribe addr from events", "addr", remoteAddr, "err", err)
		// 	}
		// }),
		rpcserver.ReadLimit(rpcConf.MaxBodyBytes),
		rpcserver.WriteChanCapacity(nodeCfg.RPC.WebSocketWriteBufferSize),
	)
	wm.SetLogger(wmLogger)
	rpcMultiplexer.HandleFunc("/websocket", wm.WebsocketHandler)
	rpcMultiplexer.HandleFunc("/v1/websocket", wm.WebsocketHandler)
	rpcserver.RegisterRPCFuncs(rpcMultiplexer, routes, rpcLogger)
	rpcListener, err := rpcserver.Listen(
		relayAddr.StringWithoutId(),
		rpcConf.MaxOpenConnections,
	)
	if err != nil {
		return err
	}

	var rootHandler http.Handler = rpcMultiplexer
	if nodeCfg.RPC.IsCorsEnabled() {
		corsMiddleware := cors.New(cors.Options{
			AllowedOrigins: nodeCfg.RPC.CORSAllowedOrigins,
			AllowedMethods: nodeCfg.RPC.CORSAllowedMethods,
			AllowedHeaders: nodeCfg.RPC.CORSAllowedHeaders,
		})
		rootHandler = corsMiddleware.Handler(rpcMultiplexer)
	}
	if nodeCfg.RPC.IsTLSEnabled() {
		go func() {
			if err := rpcserver.ServeTLS(
				rpcListener,
				rootHandler,
				nodeCfg.RPC.CertFile(),
				nodeCfg.RPC.KeyFile(),
				rpcLogger,
				rpcConf,
			); err != nil && !errors.Is(err, net.ErrClosed) {
				b.logger.Error("Error serving server with TLS", "err", err)
			}
		}()
	} else {
		go func() {
			if err := rpcserver.Serve(
				rpcListener,
				rootHandler,
				rpcLogger,
				rpcConf,
			); err != nil && !errors.Is(err, net.ErrClosed) {
				b.logger.Error("Error serving server", "err", err)
			}
		}()
	}

	b.reactor.SetRPCMultiplexer(rpcMultiplexer)

	b.rpcListeners = append(b.rpcListeners, rpcListener)
	return nil
}

// StartPrometheusServer starts a Prometheus HTTP server, listening for metrics
// collectors on addr.
// Creates a transport listening on DiscoveryPort+3.
// This method sets the listen address in prometheusAddr.
func (b *MultiplexBackend) StartPrometheusServer() error {
	nodeCfg := b.reactor.nodeConfig
	prometheusCfg := nodeCfg.Instrumentation

	// Allows disabling prometheus through legacy config.
	if !prometheusCfg.Prometheus {
		return nil
	}

	// RPC Discovery Port is always: `discovery_port+3`
	monListenAddr := overwriteListenPort(
		prometheusCfg.PrometheusListenAddr,
		int(nodeCfg.DiscoveryPort+3), // always DiscoveryPort+3
	)

	relayAddr, err := server.NewRelayAddress(monListenAddr)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for Prometheus: %w", err)
	}

	relayAddr.SetID(b.reactor.nodeKey.ID())
	if b.prometheusAddr, err = relayAddr.NetAddress(); err != nil {
		return fmt.Errorf(
			"could not create Prometheus listen address: %w", err)
	}

	b.logger.Info("Process is now setting up Prometheus HTTP",
		"addr", relayAddr.StringHostname(),
	)

	srv := &http.Server{
		Addr: relayAddr.StringHostname(),
		Handler: promhttp.InstrumentMetricHandler(
			prometheus.DefaultRegisterer, promhttp.HandlerFor(
				prometheus.DefaultGatherer,
				promhttp.HandlerOpts{MaxRequestsInFlight: prometheusCfg.MaxOpenConnections},
			),
		),
		ReadHeaderTimeout: readHeaderTimeout,
	}
	go func() {
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			// Error starting or closing listener:
			b.logger.Error("Error serving Prometheus HTTP server", "err", err)
		}
	}()

	b.httpServers = append(b.httpServers, srv)
	return nil
}

// StartNodeInstances calls the Start method of [node.Node] instances that
// are registered in the services multiplex map of the reactor.
func (b *MultiplexBackend) StartNodeInstances() error {
	if len(b.GetNetworks()) == 0 {
		return nil
	}

	servicesProvider := b.reactor.GetServicesProvider()
	for _, chainID := range b.GetNetworks() {
		// Type-assertion makes sure we have a [*node.Node]
		runNode := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)

		// TODO(midas): relax some resources at i % 1000 == 0

		// Calls the Start method on the node.Node instance.
		// This goroutine produces a panic in case of errors.
		go func(network string, n *node.Node) {
			b.reactor.logger.Info("Starting new node", "chain_id", network)
			b.reactor.logger.Info("Using custom listen addresses",
				"p2p", n.Config().P2P.ListenAddress,
				"rpc", n.Config().RPC.ListenAddress,
			)

			if err := n.Start(); err != nil {
				panic(fmt.Errorf("failed to start node: %w", err))
			}

			b.reactor.logger.Info("Started node",
				"chain_id", network,
				"nodeInfo", n.Switch().NodeInfo(),
			)
		}(chainID, runNode)
	}

	return nil
}

// StopNodeInstances calls the Stop method of [node.Node] instances that
// are registered in the services multiplex map of the reactor.
func (b *MultiplexBackend) StopNodeInstances() error {
	if len(b.GetNetworks()) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(b.GetNetworks()))

	servicesProvider := b.reactor.GetServicesProvider()
	for _, chainID := range b.GetNetworks() {
		// Type-assertion makes sure we have a [*node.Node]
		runNode := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		if !runNode.IsRunning() {
			wg.Done()
			continue
		}

		// Calls the Stop method on the node.Node instance.
		// This goroutine produces a panic in case of errors.
		go func(network string, n *node.Node) {
			b.reactor.logger.Info("Stopping node runtime", "chain_id", network)

			if n.IsRunning() {
				if err := n.Stop(); err != nil {
					panic(fmt.Errorf("failed to stop node: %w", err))
				}
			}

			wg.Done()
			b.reactor.logger.Info("Stopped node runtime", "chain_id", network)
		}(chainID, runNode)
	}

	// Wait for all nodes to be stopped.
	wg.Wait()

	return nil
}

// metricsReporter runs a reporter every metricsTickerDuration until
// the backend is stopped.
//
// - Report ProcessorUsage as the current CPU load percentage.
// - Report MemoryUsage as the total allocated bytes across all networks.
// - Report TotalNetworkBytes as the total of bytes received and sent across all networks.
// - Report TotalBlocks as the total number of blocks across all networks.
// - Report TotalTxs as the total number of transactions across all networks.
// - Report Errors as the total number of failed transactions across all networks.
// - Report TotalBlocksPerUser as the total number of blocks by each user.
// - Report TotalTxsPerUser as the total number of transactions by each user.
// - Report ErrorsPerUser as the total number of failed transactions by each user.
func (b *MultiplexBackend) metricsReporter() {
	metricsTicker := time.NewTicker(metricsTickerDuration)
	defer metricsTicker.Stop()

	for {
		select {
		case <-metricsTicker.C:
			if b.metrics == nil {
				return
			}

			prometheusCfg := b.reactor.GetNodeConfig().Instrumentation
			relayMetricsPrefix := prometheusCfg.Namespace + "_" + string(b.reactor.nodeKey.ID())

			// Resources sampling for CPU and RAM
			collectSampleCPU(b.metrics.ProcessorUsage)()
			collectSampleRAM(b.metrics.MemoryUsage)()

			// Bandwidth usage sampling
			collectSampleP2P(relayMetricsPrefix, b.metrics, []string{
				"message_receive_bytes_total",
				"message_send_bytes_total",
			})()

			// For each network, collect CometBFT metrics (blocks, txes)
			for _, chainID := range b.GetNetworks() {
				// See also: multiplex/consensus.go
				chainMetricsPrefix := prometheusCfg.Namespace + "_" + strings.ReplaceAll(chainID, "-", "_")

				collectSampleCometBFT(
					// string(b.reactor.nodeKey.ID()),
					// chainID,
					chainMetricsPrefix,
					b.metrics,
				)()
			}

		case <-b.reactor.Quit():
			return
		}
	}
}
