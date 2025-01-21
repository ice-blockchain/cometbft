package multiplex

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"slices"
	"strings"
	"sync"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/log"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	rpcclient "github.com/ice-blockchain/cometbft/rpc/jsonrpc/client"
	rpcserver "github.com/ice-blockchain/cometbft/rpc/jsonrpc/server"
	sm "github.com/ice-blockchain/cometbft/state"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/server"
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
	reactor       *Reactor
	eventSwitch   *p2p.Switch
	broadcastAddr *p2p.NetAddress // P2P Discovery
	discoveryAddr *p2p.NetAddress // RPC Discovery
	rpcListener   net.Listener

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
	errorsCh <-chan error
}

// WithRoutines is an option helper to overwrite the [server.Jobs] instance.
func WithRoutines(jobs *server.Jobs) func(*MultiplexBackend) {
	return func(b *MultiplexBackend) {
		b.routines = jobs
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
	_, reactor, err := NewNodesMultiplex(
		context.Background(),
		impl,
		nodeConfig,
		nodeLogger,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"SERVER PANIC: could not initialize node backend: %w", err)
	}

	server := &MultiplexBackend{
		reactor:  reactor,
		acceptor: impl,

		logger:   nodeLogger,
		errorsCh: make(chan error),
	}

	// Enable overwrite of optional properties
	for _, option := range options {
		option(server)
	}

	return server, nil
}

// Close implements io.Closer
func (b *MultiplexBackend) Close() error {
	// Lock the mutex to complete shutdown gracefully
	b.relayMtx.Lock()
	defer b.relayMtx.Unlock()

	b.logger.Debug("Shutting down node backend",
		"id", b.reactor.nodeKey.ID(),
	)

	if b.eventSwitch != nil && b.eventSwitch.IsRunning() {
		// Must stop listening for P2P messages on broadcast port
		if ts := b.eventSwitch.Transport(); ts != nil {
			ts.Close()
		}

		// Must stop reactors and listener channels
		b.eventSwitch.Stop()
		b.eventSwitch = nil
	}

	if b.rpcListener != nil {
		// Must stop listening for RPC discovery messages
		b.rpcListener.Close()
	}

	if b.reactor != nil {
		// Must stop the node backend (switch is not yet running)
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

	return nil
}

// GetRelayID returns the node ID assigned in the reactor.
func (b *MultiplexBackend) GetRelayID() p2p.ID {
	return b.reactor.nodeKey.ID()
}

// GetLogger returns the [cmtlog.Logger] property.
func (b *MultiplexBackend) GetLogger() cmtlog.Logger {
	return b.logger
}

// GetAcceptor returns the injected [client.Acceptor] implementation.
//
// GetAcceptor implements [client.Server]
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
	if b.eventSwitch != nil {
		return b.eventSwitch
	}

	// In-place mutation of the listen address so that it always uses
	// the configured broadcast address.
	laddrModifier := NodeInfoWithListenAddress(b.broadcastAddr)
	laddrModifier(b.reactor.nodeInfo)
	localNodeInfo := b.reactor.nodeInfo

	mConnConfig := p2p.MConnConfig(b.reactor.nodeConfig.P2P)
	localTransport := p2p.NewMultiplexTransportWithCustomHandshake(
		localNodeInfo, // local nodeInfo
		*b.reactor.nodeKey,
		mConnConfig,
		MultiplexTransportHandshake,
	)

	sw := p2p.NewSwitch(
		b.reactor.nodeConfig.P2P,
		localTransport,
	)
	sw.SetLogger(b.reactor.logger.With("module", "p2p"))
	sw.SetNodeInfo(localNodeInfo)
	sw.SetNodeKey(b.reactor.nodeKey)

	// Make sure we listen to ChainReplicationRequest messages
	sw.AddReactor("MULTIPLEX", b.reactor)
	return sw
}

// MustStart starts a replication backend basically selecting void
// and running forever.
//
// This method also opens a custom P2P "broadcast" port such that
// the relay may be communicated to, even without hosting any
// replicated chain.
//
// MustStart implements [client.Server]
func (b *MultiplexBackend) MustStart() {
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

	// Here we should wait forever, until the internal Reactor instance
	// is told to replicate a new chain using the server.ReplicationChannel.
	go func() {
		// CAUTION:
		// We open a discovery port which is required such that the relay may
		// be communicated to, even without hosting any replicated chain.
		//
		// Additionally, a RPC server is started which permits to read
		// node information such as the node ID.

		p2pAddr,
			err := b.startP2PServer(b.reactor.nodeConfig, b.reactor.nodeKey)
		if err != nil {
			wg.Done()
			b.logger.Error("error with P2P server", "err", err)
			return
		}

		rpcAddr,
			err := b.startRPCServer(b.reactor.nodeConfig, b.reactor.nodeKey)
		if err != nil {
			wg.Done()
			b.logger.Error("error with RPC server", "err", err)
			return
		}

		b.logger.Info("Process idle, waiting to replicate chains...",
			"time", cmttime.Now(),
			"id", b.reactor.nodeKey.ID(),
			"p2p", p2pAddr.DialString(),
			"rpc", rpcAddr.DialString(),
		)

		// This relay can now be used to communicate P2P messages.
		wg.Done()

		// TODO(midas): startNodeInstances

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

// WaitForNextAvailableNetwork waits for a chain replication using
// the internal newChainReadyCh channel and returns a ChainID
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

// WaitForRelayAckTransaction waits for a relay to acknowledge a transaction.
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
// GetLocalNetworkHeights implements [Adapter].
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

// DiscoverRelayID connects to relayWithoutId using a JSONRPC client,
// and calls the GetRelayInfo remote procedure to retrieve the Relay ID.
func (b *MultiplexBackend) DiscoverRelayID(
	relayWithoutId string,
) (p2p.ID, error) {
	relayWithProtocol := relayWithoutId
	if !strings.HasPrefix(relayWithProtocol, "tcp://") {
		relayWithProtocol = "tcp://" + relayWithProtocol
	}

	c, connectErr := rpcclient.New(relayWithProtocol)
	if connectErr != nil {
		return "", connectErr
	}

	result := &server.RPCResultRelayInfo{}
	params := map[string]any{}
	_, callErr := c.Call(context.TODO(), "info", params, result)
	if callErr != nil {
		return "", callErr
	}

	return result.DefaultNodeID, nil
}

// FetchRelayAddresses uses DiscoverRelayNetworks once for each relay to find
// the supported networks and their respective listen addresses.
//
// IMPORTANT:
// This method expects the relay addresses to be complete and to include a
// cometbft node ID, as well as a broadcast port.
// The broadcast port of one relay can be changed with [MultiplexConfig].
//
// FetchRelayAddresses implements [Adapter].
func (b *MultiplexBackend) FetchRelayAddresses(
	relays []string,
) (
	chainRelays map[string][]string,
	errorRelays []string,
) {
	relaysWithFailure := map[string]bool{}

	// Uses a server-local event switch to determine channels
	localP2PSwitch := b.EventSwitch()

	// Connect to all other relays using RPC (discovery server) to find
	// out their relay ID (CometBFT Node ID) before we can connect with P2P.
	regExpRelays := regexp.MustCompile(`(.*)@(.*)(\:\d+)(.*)`)
	relaysWithId := []string{}
	for _, relayWithoutId := range relays {
		if regExpRelays.MatchString(relayWithoutId) {
			relaysWithId = append(relaysWithId, relayWithoutId)
			continue
		}

		relayID, err := b.DiscoverRelayID(relayWithoutId)
		if err != nil {
			b.logger.Error("Error discovering relay information",
				"relay", relayWithoutId,
				"err", err,
			)
		}

		relayWithId := string(relayID) + "@" + relayWithoutId
		relaysWithId = append(relaysWithId, relayWithId)
	}

	// TODO(midas): localP2PSwitch.AddUnconditionalPeerIDs([]string{relayId})

	// Discover supported networks for all relays and build list of
	// P2P listen addresses by ChainID.
	chainRelays = map[string][]string{}
	for _, relayWithIdAndPort := range relaysWithId {
		// Ensure presence of node ID and broadcast port
		re := regexp.MustCompile(`(.*)@(.*)(\:\d+)(.*)`)
		matches := re.FindStringSubmatch(relayWithIdAndPort)

		// Extract the node ID from relay address
		relayNetworkNodeID := matches[1]

		// This will dial each relay once to find out their list of networks.
		networks, laddrs, err := b.DiscoverRelayNetworks(
			localP2PSwitch,
			relayWithIdAndPort,
		)
		if err != nil {
			b.logger.Error("Error discovering relay networks",
				"relay", relayWithIdAndPort,
				"err", err,
			)
			relaysWithFailure[relayWithIdAndPort] = true
			continue
		}

		b.logger.Debug("Retrieved networks information from relay",
			"relay", relayWithIdAndPort,
			"networks", networks,
		)

		// We now have `id@host:port`, i.e. a p2p.NetAddress.
		for i, chainID := range networks {
			if _, ok := chainRelays[chainID]; !ok {
				chainRelays[chainID] = []string{}
			}

			laddr := laddrs[i]
			laddr = relayNetworkNodeID + "@" + laddr

			chainRelays[chainID] = append(chainRelays[chainID], laddr)
		}
	}

	errorRelays = []string{}
	for errRelay, _ := range relaysWithFailure {
		errorRelays = append(errorRelays, errRelay)
	}
	return chainRelays, errorRelays
}

// DiscoverRelayNetworks dials the relay using a local [p2p.Switch] instance
// to perform a handshake and finally to retrieve a [MultiNetworkNodeInfo].
//
// DiscoverRelayNetworks implements [Adapter].
func (b *MultiplexBackend) DiscoverRelayNetworks(
	localSwitch *p2p.Switch,
	relayWithIdAndPort string,
) (
	networks []string,
	listenAddrs []string,
	err error,
) {
	// Find out if relayWithIdAndPort is valid at all?
	relayAddr, err := p2p.NewNetAddressString(relayWithIdAndPort)
	if err != nil {
		return []string{}, []string{}, fmt.Errorf(
			"error with relay %s: %w", relayWithIdAndPort, err)
	}

	// If this is the relay itself, return empty.
	if relayAddr.ID == b.reactor.nodeKey.ID() {
		return []string{}, []string{}, err
	}

	b.logger.Debug("Process now querying networks information",
		"relay", relayWithIdAndPort,
	)

	// Dial the relay to find out all networks it supports
	// Using the switch here affects the internal AddrBook.
	err = localSwitch.DialPeerWithAddress(relayAddr)
	if err != nil {
		return []string{}, []string{}, fmt.Errorf(
			"could not dial relay %s: %w", relayWithIdAndPort, err)
	}

	// Retrieves the multi network information
	peer := localSwitch.Peers().Get(relayAddr.ID)
	if peer == nil {
		return []string{}, []string{}, fmt.Errorf(
			"could not establish connection to peer %s", relayWithIdAndPort)
	}

	mnni := peer.NodeInfo().(MultiNetworkNodeInfo)

	// Copy supported networks to output slice
	networks = make([]string, len(mnni.Networks))
	copy(networks, mnni.Networks)

	// Copy the ChainListenAddr content
	listenAddrs = make([]string, len(mnni.ListenAddrs))
	for i, chainladdr := range mnni.ListenAddrs {
		listenAddrs[i] = chainladdr.ListenAddr
	}

	// Create events switch for each ChainID
	for _, chainID := range networks {
		err := b.reactor.CreateTransportSwitch(
			chainID,
			mnni,
		)
		if err != nil {
			return []string{}, []string{}, fmt.Errorf(
				"could not create p2p.Switch for ChainID %s: %w", chainID, err)
		}
	}

	return networks, listenAddrs, nil
}

// ApplyFilterReplRequestRelays filters relays and returns a map of relays
// by ChainID which contains only relays that need to catchup, i.e. it returns
// relays that will receive a chain replication request.
func (b *MultiplexBackend) ApplyFilterReplRequestRelays(
	requiredNetworks []string,
	relays []string,
	chainRelays map[string][]string,
) map[string][]string {
	catchupRelays := map[string][]string{}
	for chainID, relaysByChain := range chainRelays {
		// Did all relays report to know this ChainID?
		if len(relaysByChain) == len(relays) {
			catchupRelays[chainID] = nil
			continue
		}

		// Find out which relays are missing for this chain.
		// Those are relays that need to catchup with the chain.
		for _, relay := range relays {
			if !slices.Contains(relaysByChain, relay) {
				catchupRelays[chainID] = append(catchupRelays[chainID], relay)
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
// AddTransactions implements [Adapter].
func (b *MultiplexBackend) AddTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
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
		b.logger.Info("Received CheckTx response", "res", checkTxRes)
	}

	return nil
}

// RemoveTransactions remove a transaction from the local mempool
// if it has been added already, e.g. using addTransactionToMempool.
//
// This method is called by [BroadcastTx] when a transaction rollback must
// be executed due to some of the healthy relays not accepting a batch.
// RemoveTransactions implements [Adapter].
func (b *MultiplexBackend) RemoveTransactions(
	userAddress string,
	transactions ...client.Transaction,
) error {
	for _, transaction := range transactions {
		chainID := client.GetChainID(userAddress, transaction.Fingerprint)
		reactorsProvider := b.reactor.GetServicesProvider()

		memplReactor := reactorsProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor)
		chainMempool := memplReactor.GetMempoolPtr()

		memTx := client.TransactionToRawTx(transaction)
		if err := chainMempool.RemoveTxByKey(memTx.Key()); err != nil {
			b.logger.Debug("Rollback transaction not in local mempool (not an error)",
				"tx", log.NewLazySprintf("%X", memTx.Hash()),
				"error", err.Error())
		}
	}

	return nil
}

// ----------------------------------------------------------------------------

// startP2PServer creates a [p2p.Switch] instance that may be used
// to transport [ChainReplicationRequest] messages to nodes that do not have
// network ports open yet (due to not replicating any chain).
func (b *MultiplexBackend) startP2PServer(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	p2pListenAddr := overwriteListenPort(
		nodeCfg.P2P.ListenAddress,
		int(nodeCfg.BroadcastPort),
	)

	b.logger.Debug("Process is now setting up P2P discovery",
		"id", nodeKey.ID(),
		"p2p", p2pListenAddr,
	)

	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(
		nodeKey.ID(),
		p2pListenAddr,
	))
	if err != nil {
		return nil, fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	// Initializes the local p2p.Switch
	// Creates a global P2P switch to respond even without chain info.
	b.broadcastAddr = addr
	b.eventSwitch = b.EventSwitch()

	// And start the switch (the P2P server).
	err = b.eventSwitch.Start()
	if err != nil {
		return nil, fmt.Errorf(
			"could not start p2p switch: %w", err)
	}

	// "Open" the broadcast port for listening continuously
	if b.eventSwitch.Transport() != nil {
		listenTransport := b.eventSwitch.Transport()
		if err := listenTransport.Listen(*addr); err != nil {
			return nil, fmt.Errorf(
				"could not start listening for %s: %w", addr.DialString(), err)
		}

		b.logger.Info("Process is now listening on broadcast port",
			"addr", addr.DialString(),
		)
	}

	return addr, nil
}

// startRPCServer starts a RPC server with a global [Status] function that
// may be used to retrieve node information, including the node ID.
func (b *MultiplexBackend) startRPCServer(
	nodeCfg *config.Config,
	nodeKey *p2p.NodeKey,
) (
	*p2p.NetAddress, // P2P
	error,
) {
	rpcListenAddr := overwriteListenPort(
		nodeCfg.RPC.ListenAddress,
		int(nodeCfg.BroadcastPort-1), // always BroadcastPort - 1
	)

	b.logger.Debug("Process is now setting up RPC discovery",
		"id", nodeKey.ID(),
		"rpc", rpcListenAddr,
	)

	addr, err := p2p.NewNetAddressString(p2p.IDAddressString(
		nodeKey.ID(),
		rpcListenAddr,
	))
	if err != nil {
		return nil, fmt.Errorf(
			"could not create rpc listen address: %w", err)
	}

	// Initializes a local RPC server
	b.discoveryAddr = addr

	rpcCfg := b.reactor.nodeConfig.RPC
	config := rpcserver.DefaultConfig()
	config.MaxRequestBatchSize = rpcCfg.MaxRequestBatchSize
	config.MaxBodyBytes = rpcCfg.MaxBodyBytes
	config.MaxHeaderBytes = rpcCfg.MaxHeaderBytes
	config.MaxOpenConnections = rpcCfg.MaxOpenConnections

	mux := http.NewServeMux()
	rpcLogger := b.reactor.logger.With("module", "rpc-server")

	infoImpl := server.NewRelayInfoServer(b)
	rpcserver.RegisterRPCFuncs(mux, map[string]*rpcserver.RPCFunc{
		"info": rpcserver.NewRPCFunc(infoImpl.GetRelayInfo, ""),
	}, rpcLogger)

	b.rpcListener, err = rpcserver.Listen(
		rpcListenAddr,
		config.MaxOpenConnections,
	)
	if err != nil {
		return nil, err
	}

	var rootHandler http.Handler = mux
	go func() {
		if err := rpcserver.Serve(
			b.rpcListener,
			rootHandler,
			rpcLogger,
			config,
		); err != nil {
			b.logger.Error("Error serving RPC discovery server", "err", err)
		}
	}()

	return addr, nil
}

// startNodeInstances calls the Start method of [node.Node] instances that
// are registered in the services multiplex map of the reactor.
func (b *MultiplexBackend) startNodeInstances() error {
	if len(b.reactor.GetNetworks()) == 0 {
		return nil
	}

	servicesProvider := b.reactor.GetServicesProvider()
	for _, chainID := range b.reactor.GetNetworks() {
		// Type-assertion makes sure we have a [*node.Node]
		runNode := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)

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
