package multiplex

import (
	"context"
	"encoding/hex"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"sync"

	memp2p "github.com/ice-blockchain/cometbft/api/cometbft/mempool/v1"
	mxp2p "github.com/ice-blockchain/cometbft/api/cometbft/multiplex/v1"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/libs/log"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/p2p"
	sm "github.com/ice-blockchain/cometbft/state"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// Assert that our implementation satisfy the Backend interface.
var _ Adapter = (*MultiplexBackend)(nil)
var _ ClientNotifier = (*StatusNotifier)(nil)

// ----------------------------------------------------------------------------
// Adapter defines a multiplex backend adapter
//
// Adapter is an interface that defines the rules for the implementation of
// a backend adapter as required by [MultiplexClient]. The backend adapter is
// notably responsible for communicating with relays and transporting data.
//
// This interface also embeds a [client.Server] interface.
type Adapter interface {
	client.Server

	// GetLogger should return a [cmtlog.Logger] instance.
	GetLogger() cmtlog.Logger

	// GetRoutines should return an implementation of [BroadcastJobs] methods.
	GetRoutines() *BroadcastJobs

	// GetLocalNetworkHeights should query the last block height and determine
	// a list of required networks. Iff the last block height is 1, the network
	// is considered unknown and may need to be explicitely created.
	GetLocalNetworkHeights(
		userAddress string,
		transactions ...client.Transaction,
	) (map[string]int64, []string)

	// FetchRelayAddresses should find the supported networks which it should
	// map (ChainID) to their respective listen addresses, and it returns a
	// slice of relays that produced errors, e.g. network error.
	FetchRelayAddresses(
		networks []string,
		relays []string,
	) (map[string][]string, []string)

	// DiscoverRelayNetworks should dials all other relays and perform
	// handshakes to retrieve a [MultiNetworkNodeInfo] from each of the relays.
	DiscoverRelayNetworks(
		localSwitch *p2p.Switch,
		relay string,
		withBroadcastPort uint16,
	) ([]string, []string, error)

	// AddTransactions should execute the CheckTx call to add individual
	// transactions to the mempool by ChainID.
	AddTransactions(
		userAddress string,
		transactions ...client.Transaction,
	) error

	// RemoveTransactions should remove transactions from the local mempool
	// if they were added already, e.g. using AddTransactions.
	RemoveTransactions(
		userAddress string,
		transactions ...client.Transaction,
	) error
}

// ----------------------------------------------------------------------------
// BroadcastJobs defines a multiplex background jobs implementation
//
// BroadcastJobs provides routines implementation for the broadcast process.
// This structure encapsulates routines implementation for further extension
// and the adapter instance injects the default implementation if necessary.
type BroadcastJobs struct {
	// Routine extensions/overwrites may be provided here.
	NodeRelayDialer NodeRelayDialerFn
	NodeReplRequest NodeReplRequestFn
	NetworksCreator NetworksCreatorFn
	RelaysBroadcast RelaysBroadcastFn
	CancelBroadcast CancelBroadcastFn
}

// NodeRelayDialerFn describes a function that may be run on a separate
// goroutine and which should open connections to relays if necessary.
//
// A [StatusNotifier] instance contains a channel used to transmit errors.
type NodeRelayDialerFn func(
	context.Context,
	map[string][]string,
	ClientNotifier,
)

// NodeReplRequestFn describes a function that may be run on a separate
// goroutine and which should open connections to relays if necessary.
//
// A [StatusNotifier] instance contains a channel used to transmit errors.
type NodeReplRequestFn func(
	context.Context,
	[]string,
	string,
	ClientNotifier,
)

// NetworksCreatorFn describes a function that may be run on a separate
// goroutine and which should communicate with relays about missing networks.
//
// A [StatusNotifier] instance contains a channel used to transmit errors.
// Also a string channel instance is accepted as newChainReadyCh where ChainIDs
// are pushed when a new network is ready (or is now known through relay).
type NetworksCreatorFn func(
	context.Context,
	map[string][]string,
	[]string,
	ClientNotifier,
	chan<- string, // newChainReadyCh
)

// RelaysBroadcastFn describes a function that may be run on a separate
// goroutine and which should broadcast all transactions to relays.
//
// A [StatusNotifier] instance contains a channel used to transmit errors.
// Also a string channel instance is accepted as relayAcceptTxCh where
// transaction hashes are pushed when a transaction has been accepted by at
// least 50%+1 of the healthy (currently active) relays.
type RelaysBroadcastFn func(
	context.Context,
	map[string][]string,
	string,
	[]client.Transaction,
	ClientNotifier,
	chan<- string, // relayAcceptTxCh
)

// CancelBroadcastFn describes a function that may be run on a separate
// goroutine and which should broadcast a rollback message to healthy relays.
//
// The method ignores error from the mempool as transaction are not found.
type CancelBroadcastFn func(
	context.Context,
	string,
	[]client.Transaction,
)

// ----------------------------------------------------------------------------
// StatusNotifier defines a broadcast status notifier
//
// StatusNotifier implements the [ClientNotifier] interface.
type StatusNotifier struct {
	channel chan<- client.BroadcastStatus
}

// SetChannel registers a receive-only channel for this notifier.
func (n *StatusNotifier) SetChannel(ch chan<- client.BroadcastStatus) {
	n.channel = ch
}

// GetChannel returns a receive-only channel for this notifier.
func (n *StatusNotifier) GetChannel() chan<- client.BroadcastStatus {
	return n.channel
}

// Error pushes a [client.BroadcastStatus] on the notifier and
// attaches the error.
func (n *StatusNotifier) Error(err error) {
	n.GetChannel() <- client.BroadcastStatus{
		Error:    err,
		TxHashes: [][]byte{},
	}
}

// Success pushes a [client.BroadcastStatus] on the notifier and
// attaches a nil-error and accepted transaction hashes.
func (n *StatusNotifier) Success(txHashes [][]byte) {
	n.GetChannel() <- client.BroadcastStatus{
		Error:    nil,
		TxHashes: txHashes,
	}
}

// ----------------------------------------------------------------------------
// MultiplexBackend defines a multiplex backend adapter implementation
//
// MultiplexBackend implements the [Adapter] interface for a multiplex client.
// This implementation makes use of an internal [Reactor] instance to read
// local networks heights and uses instances of [p2p.Switch] to communicate
// with relays about transactions broadcast operations.
//
// Additionally, an internal [Acceptor] instance may be used to further
// extend the broadcast process, e.g. to call RollbackTx.
type MultiplexBackend struct {
	// A mutex is locked for reactor and eventSwitch updates.
	relayMtx sync.Mutex

	// The multiplex reactor is used to find ChainID, last block heights,
	// and to retrieve the AddrBook and connect to unknown relays.
	reactor       *Reactor
	eventSwitch   *p2p.Switch
	broadcastAddr *p2p.NetAddress

	// An acceptor implementation to which transactions will be forwarded.
	acceptor client.Acceptor

	// Routines may be extended directly using the [BroadcastJobs] struct.
	routines *BroadcastJobs

	// Internals
	logger   cmtlog.Logger
	errorsCh <-chan error
}

// WithRoutines is an option helper to overwrite the [BroadcastJobs] instance.
func WithRoutines(jobs *BroadcastJobs) func(*MultiplexBackend) {
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
		b.logger.Debug("Shutting down P2P switch",
			"id", b.reactor.nodeKey.ID(),
		)

		// Must stop listening for P2P messages on broadcast port
		if ts := b.eventSwitch.Transport(); ts != nil {
			b.logger.Debug("Closing P2P transport",
				"id", b.reactor.nodeKey.ID(),
			)

			ts.Close()
		}

		// Must stop reactors and listener channels
		b.eventSwitch.Stop()
		b.eventSwitch = nil
	}

	if b.reactor != nil {
		b.logger.Debug("Shutting down multiplex reactor",
			"id", b.reactor.nodeKey.ID(),
		)

		// Must stop the node backend (switch is not yet running)
		b.reactor.Stop()
		b.reactor.Reset()
	}

	return nil
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

	// Here we should wait forever, until the internal Reactor instance
	// is told to replicate a new chain using the ReplicationChannel.
	go func() {
		// CAUTION:
		// This opens a custom P2P "broadcast" port and will break in case
		// the node backend is told to replicated 9999 separate networks.
		//
		// Opening this broadcast port is required such that the relay may
		// be communicated to, even without hosting any replicated chain.

		listenAddr := b.reactor.nodeConfig.P2P.ListenAddress
		broadcastPort := b.reactor.nodeConfig.BroadcastPort
		p2pListenAddr := overwriteListenPort(listenAddr, int(broadcastPort))

		b.logger.Debug("Process is now setting up discovery",
			"addr", p2pListenAddr,
			"id", b.reactor.nodeKey.ID(),
		)

		addr, err := p2p.NewNetAddressString(p2p.IDAddressString(
			b.reactor.nodeKey.ID(),
			p2pListenAddr,
		))
		if err != nil {
			b.logger.Error("could not create p2p listen address", "err", err)
			wg.Done()
			return
		}

		// Initializes the local p2p.Switch
		// Creates a global P2P switch to respond even without chain info.
		b.broadcastAddr = addr
		b.eventSwitch = b.EventSwitch()

		// And start the switch (the P2P server).
		err = b.eventSwitch.Start()
		if err != nil {
			b.logger.Error("could not start p2p switch", "err", err)
			wg.Done()
			return
		}

		// "Open" the broadcast port for listening continuously
		if b.eventSwitch.Transport() != nil {
			listenTransport := b.eventSwitch.Transport()
			if err := listenTransport.Listen(*addr); err != nil {
				b.logger.Error("error with P2P transport listener",
					"addr", addr.DialString(),
					"err", err,
				)
			}

			b.logger.Info("Process is now listening on broadcast port",
				"addr", addr.DialString(),
			)
		}

		b.logger.Info("Process idle, waiting to replicate chains...",
			"time", cmttime.Now(),
			"id", b.reactor.nodeKey.ID(),
			"addr", addr.DialString(),
		)

		// This relay can now be used to communicate P2P messages.
		wg.Done()

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

// GetRoutines returns an injected implementation of [BroadcastJobs] methods
// or the default implementations as defined with MultiplexBackend.
//
// This is mainly used to overwrite routines for testing purposes.
// GetRoutines implements [Adapter].
func (b *MultiplexBackend) GetRoutines() *BroadcastJobs {
	if b.routines == nil {
		b.routines = &BroadcastJobs{
			NodeRelayDialer: b.DefaultNodeRelayDialerRoutine(),
			NodeReplRequest: b.DefaultNodeReplRequestRoutine(),
			NetworksCreator: b.DefaultNetworksCreatorRoutine(),
			RelaysBroadcast: b.DefaultRelaysBroadcastRoutine(),
			CancelBroadcast: b.DefaultCancelBroadcastRoutine(),
		}
	}

	return b.routines
}

// EventSwitch creates a local [p2p.Switch] instance which is used
// to determine the required channels and connection information.
//
// The relayMtx is expected to be locked by the caller.
func (b *MultiplexBackend) EventSwitch() *p2p.Switch {
	if b.eventSwitch != nil {
		return b.eventSwitch
	}

	localNodeInfo := NewMultiNetworkNodeInfo(
		b.reactor.nodeConfig,
		b.reactor.nodeKey,
		b.broadcastAddr,
	)

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

// FetchRelayAddresses uses DiscoverRelayNetworks once for each relay to find
// the supported networks and their respective listen addresses.
//
// FetchRelayAddresses implements [Adapter].
func (b *MultiplexBackend) FetchRelayAddresses(
	networks []string,
	relays []string,
) (
	chainRelays map[string][]string,
	errorRelays []string,
) {
	relaysWithFailure := map[string]bool{}

	// Uses a server-local event switch to determine channels
	localP2PSwitch := b.EventSwitch()

	// Discover supported networks for all relays and build list of
	// P2P listen addresses by ChainID.
	chainRelays = map[string][]string{}
	for _, relay := range relays {
		// This will dial each relay once to find out their list of networks.
		networks, laddrs, err := b.DiscoverRelayNetworks(localP2PSwitch, relay, 0) // 0 uses config.BroadcastPort
		if err != nil {
			relaysWithFailure[relay] = true
			continue
		}

		b.logger.Debug("Retrieved networks information from relay",
			"relay", relay,
			"len", len(networks),
		)

		// We now have `id@host:port`, i.e. a p2p.NetAddress.
		for i, chainID := range networks {
			if _, ok := chainRelays[chainID]; !ok {
				chainRelays[chainID] = []string{}
			}

			laddr := laddrs[i]
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
	relay string,
	withBroadcastPort uint16,
) (
	networks []string,
	listenAddrs []string,
	err error,
) {
	networks = []string{}
	listenAddrs = []string{}

	// If no broadcast port overwrite is provided, use config
	if withBroadcastPort == 0 {
		withBroadcastPort = b.reactor.nodeConfig.BroadcastPort
	}

	// Use the BroadcastPort to fetch MultiNetworkNodeInfo.
	relayAndPort := relay + ":" + strconv.Itoa(int(withBroadcastPort))

	// Find out if relayAndPort is valid at all?
	relayAddr, err := p2p.NewNetAddressString(relayAndPort)
	if err != nil {
		return []string{}, []string{}, fmt.Errorf(
			"error with relay %s: %w", relayAndPort, err)
	}

	// If this is the relay itself, return empty.
	if relayAddr.ID == b.reactor.nodeKey.ID() {
		return []string{}, []string{}, err
	}

	b.logger.Debug("Process now querying networks information",
		"relay", relayAndPort,
	)

	// Dial the relay to find out all networks it supports
	peer, err := localSwitch.Transport().Dial(*relayAddr, localSwitch.GetPeerConfig())
	if err != nil {
		return []string{}, []string{}, fmt.Errorf(
			"could not dial relay %s: %w", relayAndPort, err)
	}

	// Retrieves the multi network information
	mnni := peer.NodeInfo().(MultiNetworkNodeInfo)

	// Copy supported networks to output slice
	copy(networks, mnni.Networks)

	// Copy the ChainListenAddr content
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
// Default routines implementation for a MultiplexBackend

// DefaultNodeRelayDialerRoutine opens new connections to relays if necessary.
// In case an error happens while trying to connect to relays, an error will
// be pushed on a pre-allocated notifier.
//
// This method will use the local events switch to open a single connection
// with each relays. This conn is used to retrieve a [MultiNetworkNodeInfo].
func (b *MultiplexBackend) DefaultNodeRelayDialerRoutine() NodeRelayDialerFn {
	return func(
		_ context.Context,
		relaysByChain map[string][]string,
		notifierImpl ClientNotifier,
	) {
		// Uses one p2p.Switch instance per network
		switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)

		// Open connection to all relays for all networks.
		for chainID, chainRelays := range relaysByChain {
			eventsSwitch := switchProvider(chainID).(*p2p.Switch)

			// The address book is updated in p2p.Switch#dialPeersAsync.
			if err := eventsSwitch.DialPeersAsync(chainRelays); err != nil {
				// Error happened dialing one of the relays P2P address
				notifierImpl.Error(err)
				return // terminates the process
			}
		}
	}
}

// DefaultNodeReplRequestRoutine asks relays to replicate a network by
// attaching the corresponding ChainParams.
//
// This method broadcasts a [mxp2p.ChainReplicationRequest] message to
// relays, to ask them to replicate a chain using the ChainParams.
func (b *MultiplexBackend) DefaultNodeReplRequestRoutine() NodeReplRequestFn {
	return func(
		_ context.Context,
		relays []string,
		chainID string,
		notifierImpl ClientNotifier,
	) {
		// Retrieve the p2p.Switch and GenesisDoc for this chain
		switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)
		genDocProvider := b.reactor.GetGenesisProvider()

		eventsSwitch := switchProvider(chainID).(*p2p.Switch)
		genesisDoc := genDocProvider(chainID)

		// Build a transportable ChainParams protobuf message
		chainParams, err := GenesisDocToChainParams(*genesisDoc)
		if err != nil {
			notifierImpl.Error(err)
			return // terminates the process
		}

		// Broadcast the ChainReplicationRequest.
		eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
			// Send only to relays we are interesting in.
			relayAddr := peer.SocketAddr().String()
			if !slices.Contains(relays, relayAddr) {
				return
			}

			peer.Send(p2p.Envelope{
				ChannelID: ReplicationChannel,
				Message: &mxp2p.ChainReplicationRequest{
					ChainID:     chainID,
					ChainParams: chainParams,
				},
			})
		})
	}
}

// DefaultNetworksCreatorRoutine communicates with relays about
// missing networks as described by chainIDs. If the relays return an empty
// response, it means that we must create a new network.
//
// This method creates a new network genesis using [Reactor#MustCreateNetwork],
// then injects a node runtime using [Reactor#MustInjectNodeRuntime].
func (b *MultiplexBackend) DefaultNetworksCreatorRoutine() NetworksCreatorFn {
	return func(
		ctx context.Context,
		relaysByChain map[string][]string,
		missingChains []string,
		notifierImpl ClientNotifier,
		newChainReadyCh chan<- string,
	) {
		// Find out if any of the relays told us about some missing networks,
		// in this case, this is NOT a new network and our relay needs sync.
		unknownNetworks := []string{}
		for _, missingChainID := range missingChains {
			if _, ok := relaysByChain[missingChainID]; !ok {
				unknownNetworks = append(unknownNetworks, missingChainID)
				continue
			}

			// This is NOT a new network (existing on some relay)
			newChainReadyCh <- missingChainID
		}

		// If possible, notify success and terminate here.
		if len(unknownNetworks) == 0 {
			notifierImpl.Success([][]byte{})
			return
		}

		// We must create at least one NEW network.
		for _, newChainID := range unknownNetworks {
			err := func() (err error) {
				// Recover from potential panic in below block due to inability to create
				// a new network. This recovery ensures that the client implementation is
				// able to react to errors happening in the process, any errors here must
				// terminate the broadcast process as it is effectively invalidated here.
				defer func() {
					if errRecovered := recover(); errRecovered != nil {
						// Error happened in MustCreateNetwork process.
						err = errRecovered.(error)

						// The reactor will have pushed on createErr already.
						b.logger.Error(fmt.Errorf(
							"CLIENT PANIC encountered with MustCreateNetwork: %w",
							errRecovered.(error),
						).Error())
					}
				}()

				// Create the network genesis, state machine, etc.
				err = b.reactor.InjectNewNetwork(newChainID)
				if err != nil {
					return err
				}

				// Inject a *running* node.Node for the new network.
				// TODO(midas): currently not passing any node options.
				return b.reactor.InjectNewRuntime(ctx, newChainID)
			}()
			if err != nil {
				// Terminates the upper broadcast process
				notifierImpl.Error(fmt.Errorf(
					"could not create required networks: %w", err))
				return
			}

			// We may now proceed with the transaction broadcast, and other
			// relays will be able to join the newly created network.
			newChainReadyCh <- newChainID
		}

		// We are not done with the entire broadcast process,
		// the transaction must not be considered accepted.
		notifierImpl.Success([][]byte{})
	}
}

// DefaultRelaysBroadcastRoutine broadcasts all transactions to relays
// and verifies their respective acceptance of the transaction batch.
//
// If any broadcast to other relays produces an error, the complete
// transaction batch will be discarded, and a rollback message will
// be broadcast to other relay's mempool reactors.
func (b *MultiplexBackend) DefaultRelaysBroadcastRoutine() RelaysBroadcastFn {
	return func(
		ctx context.Context,
		relaysByChain map[string][]string,
		userAddress string,
		transactions []client.Transaction,
		notifierImpl ClientNotifier,
		relayAcceptTxCh chan<- string,
	) {
		switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)
		broadcastTxHashes := make([][]byte, 0, len(transactions))

		// Iterate through transaction and broadcast each of them to other relays
		for i, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			eventsSwitch := switchProvider(chainID).(*p2p.Switch)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)
			txHash := strings.ToUpper(hex.EncodeToString(rawTx.Hash()))

			// Broadcast must happen only if there is at least one healthy relay.
			// For NEW networks, we don't need to broadcast to other relays.
			if _, ok := relaysByChain[chainID]; !ok {
				relayAcceptTxCh <- txHash
				continue // Do not broadcast to relays
			}

			// Force the execution of mempool broadcast to healthy relays
			relaysAccepted := 0
			minHealthyRelays := len(relaysByChain[chainID])

			// Broadcast the transaction to all healthy relays.
			eventsSwitch.Peers().ForEach(func(peer p2p.Peer) {
				// Skip unhealthy relays because they would produce an error.
				relayAddr := peer.SocketAddr().String()
				if !slices.Contains(relaysByChain[chainID], relayAddr) {
					return
				}

				if success := peer.Send(p2p.Envelope{
					ChannelID: mempl.MempoolChannel,
					Message:   &memp2p.Txs{Txs: [][]byte{rawTx}},
				}); success {
					relaysAccepted++
				}
			})

			// We require healthy relays to accept this broadcast.
			if relaysAccepted >= minHealthyRelays {
				relayAcceptTxCh <- txHash
			} else {
				// Otherwise broadcast a rollback operation if some of the healthy
				// relays already added this transaction to their mempool.
				// The mempool calls [Acceptor#RollbackTx] before removing txes.
				routineCancelBroadcast := b.GetRoutines().CancelBroadcast
				go routineCancelBroadcast(ctx, userAddress, transactions)

				// Also call RollbackTx extension locally and remove from mempool.
				if err := b.acceptor.RollbackTx(ctx, userAddress, transactions...); err == nil {
					// Remove the transactions from local mempool.
					b.RemoveTransactions(userAddress, transactions...)
				}

				// We are missing some relays' acceptance, fail here.
				notifierImpl.Error(fmt.Errorf(
					"other relays failed to accept transaction %s", txHash))
				return // terminates the process
			}

			copy(broadcastTxHashes[i], rawTx.Hash())
		}

		// Done, notify about succeeded broadcast (nil error)
		notifierImpl.Success(broadcastTxHashes)
	}
}

// broadcastRollbacksRoutine broadcasts a rollback message to healthy relays
// in case any of the relays has already included the transactions in their
// mempool. The mempool should call [Acceptor#RollbackTx] upon receiving this
// message.
func (b *MultiplexBackend) DefaultCancelBroadcastRoutine() CancelBroadcastFn {
	return func(
		ctx context.Context,
		userAddress string,
		transactions []client.Transaction,
	) {
		switchProvider := b.reactor.GetInstanceProvider(InstanceKeyP2PSwitch)

		// Iterate through transactions and broadcast rollback operations
		// for each of them to all other relays.
		for _, transaction := range transactions {
			chainID := client.GetChainID(userAddress, transaction.Fingerprint)
			eventsSwitch := switchProvider(chainID).(*p2p.Switch)

			// Encode and get transaction hash
			rawTx := client.TransactionToRawTx(transaction)

			// Broadcast the rollback message for this transaction to all relays.
			eventsSwitch.Broadcast(p2p.Envelope{
				ChannelID: mempl.MempoolChannel,
				Message:   &memp2p.RollbackTxs{Txs: [][]byte{rawTx}},
			})
		}
	}
}
