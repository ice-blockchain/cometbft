package multiplex

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	bc "github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/ice-blockchain/cometbft/p2p/pex"
	"github.com/ice-blockchain/cometbft/statesync"
	"github.com/ice-blockchain/cometbft/version"
)

func (reactor *Reactor) CreateOrLoadCometBFTEventSwitch(ctx context.Context, addr *p2p.NetAddress) *p2p.Switch {
	cometbftSwitch := reactor.GetEventSwitchForCometBFT()
	if cometbftSwitch != nil {
		return cometbftSwitch
	}

	var (
		nodeInfo *MultiNetworkNodeInfo
		err      error
		nodeKey  *p2p.NodeKey = reactor.GetNodeKey()
	)

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Creating switch for P2P cometbft",
		"addr", addr.String(),
		"id", nodeKey.ID(),
	)

	p2pLogger := reactor.logger.With("module", "p2p")

	if nodeInfo, err = reactor.MakeMultiNetworkNodeInfo(); err != nil {
		// TODO(midas): remove debug logs
		reactor.logger.Error("Failed to create multiplex node information",
			"addr", addr.String(),
			"id", nodeKey.ID(),
			"err", err,
		)
	}

	nodeConfig := reactor.GetNodeConfig()

	p2pMetricsId := strings.Join([]string{
		nodeConfig.Instrumentation.Namespace,
		string(nodeKey.ID()),
	}, "_")
	p2pMetricsProvider := p2p.PrometheusMetrics(p2pMetricsId,
		"node_id", string(nodeKey.ID()),
	)

	// 1) Create the p2p transport
	//
	// We use a legacy structure [p2p.MultiplexTransport], but inject
	// a custom TLS handshake implementation with [MultiplexTransportHandshake].
	mConnConfig := p2p.MConnConfig(nodeConfig.P2P)
	localTransport := p2p.NewMultiplexTransportWithCustomHandshake(
		nodeInfo,
		*nodeKey,
		mConnConfig,
		MultiplexTransportHandshake,
	)
	cometbftSwitch = p2p.NewSwitch(
		ctx,
		nodeConfig.P2P,
		localTransport,
		p2p.WithMetrics(p2pMetricsProvider),
		func(s *p2p.Switch) {
			s.Typ = "cometBFT"
		},
	)
	localTransport.SetSwitch(cometbftSwitch)
	cometbftSwitch.SetLogger(p2pLogger)
	cometbftSwitch.SetNodeInfo(nodeInfo)
	cometbftSwitch.SetNodeKey(nodeKey)

	// Make sure we accept ChainReplicationRequest messages
	cometbftSwitch.AddReactor(conn.SharedChannelsNamespace, "MULTIPLEX", reactor)

	reactor.SetEventSwitchForCometBFT(cometbftSwitch)
	reactor.SetTransportForCometBFT(localTransport)

	return cometbftSwitch
}

// MakeMultiNetworkNodeInfo creates the [MultiNetworkNodeInfo] instance as
// will be attached to the CometBFT [p2p.Switch].
//
// TODO(midas): txIndexer may be disabled but multiplex reports "on".
// txIndexer may be disabled but multiplex always *reports* it as enabled.
// The reason is that the `Other` part of the MultiNetworkNodeInfo is not
// available on a per-network basis. A fix would be to include this in a
// custom [ChainProtocolVersion] as the type is related to node capacities.
func (reactor *Reactor) MakeMultiNetworkNodeInfo() (
	nodeInfo *MultiNetworkNodeInfo,
	err error,
) {
	// Get an ordered list of replicated chains
	knownNetworks := reactor.GetNetworks()
	countNetworks := len(knownNetworks)

	// Fill ProtocolVersions and Networks fields
	protocolVersions := make([]ChainProtocolVersion, countNetworks)
	for i, chainID := range knownNetworks {
		protocolVersions[i] = NewChainProtocolVersion(chainID, p2p.NewProtocolVersion(
			version.P2PProtocol,
			version.BlockProtocol,
			0, // App version may be filled by ABCI Handshake.
		))
	}

	nodeConfig := reactor.GetNodeConfig()
	promoteAddr := nodeConfig.P2P.ExternalAddress
	if promoteAddr == "" {
		promoteAddr = nodeConfig.P2P.ListenAddress
	}
	p2pListenAddr := overwriteListenPort(
		promoteAddr,
		int(nodeConfig.DiscoveryPort)+1, // defaults to 30002
	)

	rpcListenAddr := overwriteListenPort(
		nodeConfig.RPC.ListenAddress,
		int(nodeConfig.DiscoveryPort)+2, // defaults to 30003
	)

	nodeKey := reactor.GetNodeKey()

	txIndexerStatus := "on"
	nodeInfo = &MultiNetworkNodeInfo{
		DefaultNodeID:    nodeKey.ID(),
		Networks:         knownNetworks,
		ProtocolVersions: protocolVersions,
		ListenAddr:       p2pListenAddr,
		Version:          version.CMTSemVer,
		Channels: []byte{
			bc.BlocksyncChannel,
			cs.StateChannel, cs.DataChannel, cs.VoteChannel, cs.VoteSetBitsChannel,
			mempl.MempoolChannel,
			evidence.EvidenceChannel,
			statesync.SnapshotChannel, statesync.ChunkChannel,
			pex.PexChannel,

			// AckBroadcastChannel may be used to send AckTransactionBroadcast messages.
			server.AckBroadcastChannel,
			// RuntimeChannel may be used to send ChainReplicationComplete messages.
			server.RuntimeChannel,
		},
		Moniker: nodeConfig.BaseConfig.Moniker,
		Other: p2p.DefaultNodeInfoOther{
			TxIndex:    txIndexerStatus,
			RPCAddress: rpcListenAddr,
		},
	}

	if err = nodeInfo.Validate(); err == nil {
		reactor.SetNodeInfo(nodeInfo)
	}

	return // nodeInfo, err
}

// CreateTransportSwitches initializes P2P transports using the legacy
// structure [p2p.MultiplexTransport], but injects a *custom TLS handshake*
// implementation with [MultiplexTransportHandshake].
//
// Then, an event switch is initialized with [p2p.Switch] with the transport
// multiplex and an address book. At last, a [pex.Reactor] is also created.
//
// Note that this method must be called after [CreateConsensusInstanceReactors].
//
// This method also registers instances in the multiplexRegistry:
// - `p2p/transport`: the [p2p.MultiplexTransport] instance per chain.
// - `p2p/switch`: the [p2p.Switch] instance with all consensus reactors.
//
// TODO(midas): TBI impact of ABCI query that uses /p2p/filter, discarded here.
// TODO(midas): we must probably divide the max peers by the number of known networks.
func (reactor *Reactor) CreateTransportSwitchesWithReactors(
	ctx context.Context,
	networks []string,
) error {
	// Used for global metrics provider
	globalConfig := reactor.GetNodeConfig()
	p2pLogger := reactor.logger.With("module", "p2p")

	cometbftConfig := deepCopyConfig(globalConfig)
	cometbftConfig.P2P.ListenAddress = overwriteListenPort(
		cometbftConfig.P2P.ListenAddress,
		int(cometbftConfig.DiscoveryPort)+1, // defaults to 30002
	)

	relayAddr, err := server.NewRelayAddress(cometbftConfig.P2P.ListenAddress)
	if err != nil {
		return fmt.Errorf(
			"could not create relay address for P2P: %w", err)
	}

	relayAddr.SetID(reactor.GetNodeKey().ID())
	netAddr, err := relayAddr.NetAddress()
	if err != nil {
		return fmt.Errorf(
			"could not create p2p listen address: %w", err)
	}

	var (
		eventSwitch *p2p.Switch           = reactor.CreateOrLoadCometBFTEventSwitch(ctx, netAddr)
		nodeKey     *p2p.NodeKey          = reactor.GetNodeKey()
		nodeInfo    *MultiNetworkNodeInfo = reactor.GetMultiNetworkNodeInfo()
	)

	// Used to retrieve configuration and state per chain.
	serviceProvider := reactor.GetServicesProvider()
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)

	// We iterate through an ordered list of known networks to add reactors
	// for each of the available ChainID.
	for _, chainID := range networks {
		// The config overwrite notably contains P2P.Seeds overwrite
		cfgOverwrite := configProvider(chainID).(*config.Config)

		var (
			connFilters        = []p2p.ConnFilterFunc{}
			persistentPeers    = splitAndTrimEmpty(cfgOverwrite.P2P.PersistentPeers, ",", " ")
			unconditionalPeers = splitAndTrimEmpty(cfgOverwrite.P2P.UnconditionalPeerIDs, ",", " ")
		)

		if !cfgOverwrite.P2P.AllowDuplicateIP {
			p2pLogger.Info("Disallowing peers with duplicate IP", "ID", nodeKey.ID())
			connFilters = append(connFilters, p2p.ConnDuplicateIPFilter())
		}

		// We should error if consensus reactors for this ChainID are not ready.
		memR := serviceProvider(ServiceKeyMempoolReactor, chainID)
		bsR := serviceProvider(ServiceKeyBlockSyncReactor, chainID)
		conR := serviceProvider(ServiceKeyConsensusReactor, chainID)
		evR := serviceProvider(ServiceKeyEvidenceReactor, chainID)
		if memR == nil || bsR == nil || conR == nil || evR == nil {
			p2pLogger.Error("Failed to load consensus reactors - not available",
				"nodeId", nodeKey.ID(),
				"chainId", chainID,
				"mempool", memR,
				"blocksync", bsR,
				"consensus", conR,
				"evidence", evR,
			)
			return errors.New("failed to load consensus reactors")
		}

		// Feed reactors, created in [CreateConsensusInstanceReactors].
		//
		// The event switch contains a pointer to internal module reactors.
		eventSwitch.AddReactor(chainID, "MEMPOOL", memR.(*mempl.Reactor))
		eventSwitch.AddReactor(chainID, "BLOCKSYNC", bsR.(*blocksync.Reactor))
		eventSwitch.AddReactor(chainID, "CONSENSUS", conR.(*cs.Reactor))
		eventSwitch.AddReactor(chainID, "EVIDENCE", evR.(*evidence.Reactor))

		if len(persistentPeers) > 0 {
			if err := eventSwitch.AddPersistentPeers(persistentPeers); err != nil {
				return fmt.Errorf("failed to add peers from persistent_peers field: %w", err)
			}
		}

		if len(unconditionalPeers) > 0 {
			if err := eventSwitch.AddUnconditionalPeerIDs(unconditionalPeers); err != nil {
				return fmt.Errorf("failed to add peer ids from unconditional_peer_ids field: %w", err)
			}
		}
	}

	p2pLogger.Info("P2P Node ID",
		"nodeId", nodeKey.ID(),
		"file", globalConfig.NodeKeyFile(),
		"info", nodeInfo,
	)

	return nil
}

// CreateAddressBooks validates the existence of a per-network filesystem
// path for configuration files, i.e. %rootDir%/config/%address%/%chain%/.
//
// Then it configures a [pex.AddrBook] instance which is used to create
// the [pex.Reactor], and then added to the event switch. The result is that
// one `addrbook.json` file exists per each replicated chain.
//
// Note that this method must be called after [CreateTransportSwitches].
func (reactor *Reactor) CreateAddressBooks(
	ctx context.Context,
	networks []string,
) error {
	// Used for logging with custom address book
	p2pLogger := reactor.logger.With("module", "p2p")
	nodeConfig := reactor.GetNodeConfig()
	nodeKey := reactor.GetNodeKey()

	// Used to retrieve configuration and state per chain.
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	addressBookPath := filepath.Join(nodeConfig.RootDir, config.DefaultConfigDir)
	addrBookFile := filepath.Join(addressBookPath, config.DefaultAddrBookName)
	if _, err := os.Stat(addressBookPath); err != nil {
		return fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
	}

	addrBook := pex.NewAddrBook(ctx, addrBookFile, false) // routabilityStrict=false
	addrBook.SetLogger(p2pLogger.With("book", addrBookFile))

	cometbftSwitch := reactor.GetEventSwitchForCometBFT()
	for _, chainID := range networks {
		// The config overwrite notably contains P2P.Seeds overwrite
		cfgOverwrite := configProvider(chainID).(*config.Config)
		chainSeedNodes := splitAndTrimEmpty(cfgOverwrite.P2P.Seeds, ",", " ")

		// Add ourselves to addrbook to prevent dialing ourselves
		if cfgOverwrite.P2P.ExternalAddress != "" {
			externalAddress := p2p.IDAddressString(nodeKey.ID(), cfgOverwrite.P2P.ExternalAddress)
			addr, err := p2p.NewNetAddressString(externalAddress)
			if err != nil {
				return fmt.Errorf("p2p.external_address is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}
		if cfgOverwrite.P2P.ListenAddress != "" {
			internalAddress := p2p.IDAddressString(nodeKey.ID(), cfgOverwrite.P2P.ListenAddress)
			addr, err := p2p.NewNetAddressString(internalAddress)
			if err != nil {
				return fmt.Errorf("p2p.laddr is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}

		// 2) Create the PEX reactor
		//
		// Here we feed the P2P.Seeds from the config overwrite.
		pexLogger := reactor.logger.With("module", "pex")
		pexReactor := pex.NewReactor(ctx, addrBook,
			&pex.ReactorConfig{
				Seeds:    chainSeedNodes,
				SeedMode: cfgOverwrite.P2P.SeedMode,
				// See consensus/reactor.go: blocksToContributeToBecomeGoodPeer 10000
				// blocks assuming 10s blocks ~ 28 hours.
				SeedDisconnectWaitPeriod:     28 * time.Hour,
				PersistentPeersMaxDialPeriod: cfgOverwrite.P2P.PersistentPeersMaxDialPeriod,
			}, pex.WithChainID(chainID))
		pexReactor.SetLogger(pexLogger)

		// Set address book and PEX reactor on Switch
		cometbftSwitch.AddReactor(chainID, "PEX", pexReactor)
	}

	cometbftSwitch.SetAddrBook(addrBook)
	return nil
}

// RemoveConnectionChannels closes reactor channels to stop listening.
// Accepts a [p2p.Switch], and a list of scopes which may contain ChainIDs.
func (reactor *Reactor) RemoveConnectionChannels(
	sw *p2p.Switch,
	scopes []string,
) error {
	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()

	removeWg := new(sync.WaitGroup)

	// CAUTION: Updates the MConnection.channelsIdx, removes channels for scopes.
	connCleanupFn := sw.CloseChannelsForScopes(scopes)
	for _, chainOrScope := range scopes {
		peers := sw.Peers(chainOrScope).Copy()
		removeWg.Add(len(peers))
		for _, p := range peers {
			go func(peer *p2p.PeerImpl) {
				defer removeWg.Done()
				connCleanupFn(peer.MConn())
			}(p)
		}
	}
	removeWg.Wait()

	return nil
}

// AddConnectionChannels opens reactor channels for listening to new messages.
// Accepts a [p2p.Switch], and a list of scopes which may contain ChainIDs.
func (reactor *Reactor) AddConnectionChannels(
	sw *p2p.Switch,
	scopes []string,
) error {
	// NOTE(midas): If one of the scopes is a ChainID that is being initialized
	// concurrently and that we have not yet added to the switch, we do so here
	// so that we may proceed with handling messages from unknown ChainIDs.
	serviceProvider := reactor.GetServicesProvider()
	for _, chainID := range scopes {
		// Skip if this is not a ChainID (e.g., discovery scope)
		if chainID == p2p.ScopeForDiscovery {
			continue
		}

		sw.AddActiveRuntime(chainID)

		// Add reactors for the new ChainID if they don't exist
		if sw.Reactor(chainID, "MEMPOOL") == nil {
			sw.AddReactor(chainID, "MEMPOOL",
				serviceProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor))
		}
		if sw.Reactor(chainID, "BLOCKSYNC") == nil {
			sw.AddReactor(chainID, "BLOCKSYNC",
				serviceProvider(ServiceKeyBlockSyncReactor, chainID).(*blocksync.Reactor))
		}
		if sw.Reactor(chainID, "CONSENSUS") == nil {
			sw.AddReactor(chainID, "CONSENSUS",
				serviceProvider(ServiceKeyConsensusReactor, chainID).(*cs.Reactor))
		}
		if sw.Reactor(chainID, "EVIDENCE") == nil {
			sw.AddReactor(chainID, "EVIDENCE",
				serviceProvider(ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor))
		}
	}

	reactor.networkMutex.RLock()
	defer reactor.networkMutex.RUnlock()

	// CAUTION: Updates the MConnection.channelsIdx to contain channels for scopes.
	connUpdaterFn := sw.OpenChannelsForScopes(scopes)
	for _, chainOrScope := range scopes {
		peers := sw.Peers(chainOrScope).Copy()
		for _, peer := range peers {
			connUpdaterFn(peer.MConn())
		}
	}

	return nil
}
