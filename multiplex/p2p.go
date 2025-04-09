package multiplex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/conn"
	"github.com/ice-blockchain/cometbft/p2p/pex"
)

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

	var (
		transport   *p2p.MultiplexTransport
		eventSwitch *p2p.Switch
	)
	if reactor.GetEventSwitchForCometBFT() != nil {
		eventSwitch = reactor.GetEventSwitchForCometBFT()
		transport = reactor.transport
	} else {
		p2pMetricsId := strings.Join([]string{
			globalConfig.Instrumentation.Namespace,
			string(reactor.nodeKey.ID()),
		}, "_")
		p2pMetricsProvider := p2p.PrometheusMetrics(p2pMetricsId,
			"node_id", string(reactor.nodeKey.ID()),
		)

		mConnConfig := p2p.MConnConfig(cometbftConfig.P2P)
		transport = p2p.NewMultiplexTransportWithCustomHandshake(
			reactor.nodeInfo,
			*reactor.nodeKey,
			mConnConfig,
			MultiplexTransportHandshake,
		)
		eventSwitch = p2p.NewSwitch(
			cometbftConfig.P2P,
			transport,
			p2p.WithMetrics(p2pMetricsProvider),
		)
		eventSwitch.SetLogger(p2pLogger)
		eventSwitch.SetNodeInfo(reactor.nodeInfo)
		eventSwitch.SetNodeKey(reactor.nodeKey)

		// Make sure we accept ChainReplicationRequest messages
		eventSwitch.AddReactor(conn.SharedChannelsNamespace, "MULTIPLEX", reactor)
	}

	// Used to retrieve configuration and state per chain.
	serviceProvider := reactor.GetServicesProvider()
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)

	// We iterate through an ordered list of known networks to create
	// one instance of [p2p.MultiplexTransport] and one instance of [p2p.Switch]
	// for each replicated chain.
	//
	// Additionally, we feed the previously created consensus reactors.
	for _, chainID := range networks {
		// The config overwrite notably contains P2P.Seeds overwrite
		cfgOverwrite := configProvider(chainID).(*config.Config)

		// 1) Create the p2p transport
		//
		// We use a legacy structure [p2p.MultiplexTransport], but inject
		// a custom TLS handshake implementation with [MultiplexTransportHandshake].
		var (
			connFilters        = []p2p.ConnFilterFunc{}
			persistentPeers    = splitAndTrimEmpty(cfgOverwrite.P2P.PersistentPeers, ",", " ")
			unconditionalPeers = splitAndTrimEmpty(cfgOverwrite.P2P.UnconditionalPeerIDs, ",", " ")
		)

		if !cfgOverwrite.P2P.AllowDuplicateIP {
			connFilters = append(connFilters, p2p.ConnDuplicateIPFilter())
		}

		// 2) Feed reactors from [CreateConsensusInstanceReactors]
		//
		// The event switch contains a pointer to internal module reactors.
		eventSwitch.AddReactor(chainID, "MEMPOOL",
			serviceProvider(ServiceKeyMempoolReactor, chainID).(*mempl.Reactor))
		eventSwitch.AddReactor(chainID, "BLOCKSYNC",
			serviceProvider(ServiceKeyBlockSyncReactor, chainID).(*blocksync.Reactor))
		eventSwitch.AddReactor(chainID, "CONSENSUS",
			serviceProvider(ServiceKeyConsensusReactor, chainID).(*cs.Reactor))
		eventSwitch.AddReactor(chainID, "EVIDENCE",
			serviceProvider(ServiceKeyEvidenceReactor, chainID).(*evidence.Reactor))

		if len(persistentPeers) > 0 {
			if err := eventSwitch.AddPersistentPeers(persistentPeers); err != nil {
				return fmt.Errorf("could not add peers from persistent_peers field: %w", err)
			}
		}

		if len(unconditionalPeers) > 0 {
			if err := eventSwitch.AddUnconditionalPeerIDs(unconditionalPeers); err != nil {
				return fmt.Errorf("could not add peer ids from unconditional_peer_ids field: %w", err)
			}
		}
	}

	reactor.cometbftSwitch = eventSwitch
	reactor.transport = transport

	p2pLogger.Info("P2P Node ID",
		"ID", reactor.nodeKey.ID(),
		"file", globalConfig.NodeKeyFile(),
		"info", reactor.nodeInfo,
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

	// Used to retrieve configuration and state per chain.
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	addressBookPath := filepath.Join(reactor.nodeConfig.RootDir, config.DefaultConfigDir)
	addrBookFile := filepath.Join(addressBookPath, config.DefaultAddrBookName)
	if _, err := os.Stat(addressBookPath); err != nil {
		return fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
	}

	addrBook := pex.NewAddrBook(addrBookFile, reactor.nodeConfig.P2P.AddrBookStrict)
	addrBook.SetLogger(p2pLogger.With("book", addrBookFile))

	for _, chainID := range networks {
		// The config overwrite notably contains P2P.Seeds overwrite
		cfgOverwrite := configProvider(chainID).(*config.Config)
		chainSeedNodes := splitAndTrimEmpty(cfgOverwrite.P2P.Seeds, ",", " ")

		// Add ourselves to addrbook to prevent dialing ourselves
		if cfgOverwrite.P2P.ExternalAddress != "" {
			externalAddress := p2p.IDAddressString(reactor.nodeKey.ID(), cfgOverwrite.P2P.ExternalAddress)
			addr, err := p2p.NewNetAddressString(externalAddress)
			if err != nil {
				return fmt.Errorf("p2p.external_address is incorrect: %w", err)
			}
			addrBook.AddOurAddress(addr)
		}
		if cfgOverwrite.P2P.ListenAddress != "" {
			internalAddress := p2p.IDAddressString(reactor.nodeKey.ID(), cfgOverwrite.P2P.ListenAddress)
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
		pexReactor := pex.NewReactor(addrBook,
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
		reactor.cometbftSwitch.AddReactor(chainID, "PEX", pexReactor)
	}

	reactor.cometbftSwitch.SetAddrBook(addrBook)
	return nil
}

// AddConnectionChannels opens reactor channels for listening to new messages.
// Accepts a [p2p.Switch], a list of ChainIDs and an optional list of channels.
// Leave channels empty to register all reactor's channels.
func (reactor *Reactor) AddConnectionChannels(
	sw *p2p.Switch,
	chainIds []string,
	channels []byte,
) error {
	sw.Peers().ForEach(func(peer p2p.Peer) {
		mconn := peer.MConn()

		for _, chainID := range chainIds {
			for name, r := range sw.Reactors(chainID) {
				for _, chDesc := range r.GetChannels() {
					if len(channels) > 0 && !slices.Contains(channels, chDesc.ID) {
						continue
					}

					// TODO(midas): remove debug logs
					reactor.logger.Debug("Adding connection channel",
						"chain_id", chainID,
						"reactor", name,
						"chID", chDesc.ID,
						"peer", peer.SocketAddr().String(),
					)

					mconn.AddChannel(chainID, *chDesc)
				}
			}
		}
	})

	return nil
}
