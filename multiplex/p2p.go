package multiplex

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/internal/blocksync"
	cs "github.com/ice-blockchain/cometbft/internal/consensus"
	"github.com/ice-blockchain/cometbft/internal/evidence"
	mempl "github.com/ice-blockchain/cometbft/mempool"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/p2p/pex"
)

func (reactor *Reactor) UpdateNodeInfo(
	newNodeInfo MultiNetworkNodeInfo,
) error {
	if reactor.eventSwitch == nil {
		return fmt.Errorf("invalid state, event switch not yet created")
	}

	reactor.eventSwitch.SetNodeInfo(newNodeInfo)
	return nil
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

	var (
		transport   *p2p.MultiplexTransport
		eventSwitch *p2p.Switch
	)
	if reactor.eventSwitch != nil {
		eventSwitch = reactor.eventSwitch
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

	reactor.eventSwitch = eventSwitch
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

	// We shall iterate through all known networks and create separate
	// multiplex transports and event switches for each replicated chain.
	// chainRegistry := reactor.GetChainRegistry()

	// Used to retrieve configuration and state per chain.
	configProvider := reactor.GetInstanceProvider(InstanceKeyConfig)
	// switchProvider := reactor.GetInstanceProvider(InstanceKeyP2PSwitch)

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
		// eventSwitch := switchProvider(chainID).(*p2p.Switch)

		chainSeedNodes := splitAndTrimEmpty(cfgOverwrite.P2P.Seeds, ",", " ")

		// We can safely ignore the error as we know an address is available.
		// Builds a custom address book path: %rootDir%/config/%address%/%chain%/
		// userAddress, _ := chainRegistry.GetAddress(chainID)
		// userConfDir := filepath.Join(cfgOverwrite.RootDir, config.DefaultConfigDir, userAddress)
		// addressBookPath := filepath.Join(userConfDir, chainID)

		// // Uses default address book file name: addrbook.json
		// addrBookFile := filepath.Join(addressBookPath, config.DefaultAddrBookName)
		// if _, err := os.Stat(addressBookPath); err != nil {
		// 	return fmt.Errorf("could not open address book file %s: %w", addrBookFile, err)
		// }

		// 1) Create an address book
		//
		// We shall also add our external/local addresses to it to prevent
		// dialing ourselves out of mistake.
		// addrBook := pex.NewAddrBook(addrBookFile, cfgOverwrite.P2P.AddrBookStrict)
		// addrBook.SetLogger(p2pLogger.With("book", addrBookFile))

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
			})
		pexReactor.SetLogger(pexLogger)

		// Set address book and PEX reactor on Switch
		reactor.eventSwitch.AddReactor(chainID, "PEX", pexReactor)
	}

	reactor.eventSwitch.SetAddrBook(addrBook)
	return nil
}
