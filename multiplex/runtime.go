package multiplex

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"sync"

	dbm "github.com/cometbft/cometbft-db"
	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	cmtcodec "github.com/ice-blockchain/cometbft/crypto/encoding"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	service "github.com/ice-blockchain/cometbft/libs/service"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	"github.com/ice-blockchain/cometbft/privval"
	sm "github.com/ice-blockchain/cometbft/state"
	bs "github.com/ice-blockchain/cometbft/store"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

// AllocateNetwork allocates the necessary resources including the filesystem
// structure, the databases and a priv validator.
func (reactor *Reactor) AllocateNetwork(
	chainID string,
) error {
	// Build the ExtendedChainID to retrieve user address from ChainID.
	extChainID, err := NewExtendedChainIDFromLegacy(chainID)
	if err != nil {
		return fmt.Errorf(
			"could not parse ChainID value %s: %w", chainID, err)
	}

	// ------------------------------------------------------------------------
	// Step 1: Create filesystem

	// Create the multiplex FS entry for this network.
	chainConfFolder,
		chainDataFolder,
		err := reactor.MakeNetworkFilesystem(extChainID)
	if err != nil {
		return fmt.Errorf(
			"could not create filesystem for ChainID %s: %w", chainID, err)
	}

	reactor.RegisterInstance(InstanceKeyStorage, chainID, chainDataFolder)

	// ------------------------------------------------------------------------
	// Step 2: Create databases

	// Create the databases and open connections for this network.
	databasesByName, err := reactor.MakeNetworkDatabases(extChainID, []string{
		"blockstore",
		"state",
		"tx_index",
		"evidence",
	})
	if err != nil {
		return fmt.Errorf(
			"could not create databases for ChainID %s: %w", chainID, err)
	}

	// Register/inject in running reactor
	reactor.RegisterInstance(InstanceKeyDatabaseBlock, chainID, databasesByName["blockstore"])
	reactor.RegisterInstance(InstanceKeyDatabaseState, chainID, databasesByName["state"])
	reactor.RegisterInstance(InstanceKeyDatabaseIndex, chainID, databasesByName["tx_index"])
	reactor.RegisterInstance(InstanceKeyDatabaseEvidence, chainID, databasesByName["evidence"])

	// ------------------------------------------------------------------------
	// Step 3: Create priv validator

	// Create the priv validator for this network. used in the genesis doc.
	privValidator, err := reactor.MakeNetworkValidator(
		chainConfFolder,
		chainDataFolder,
	)
	if err != nil {
		return fmt.Errorf(
			"could not create priv validator for ChainID %s: %w", chainID, err)
	}

	// TODO(midas): remove debug logs
	reactor.logger.Debug("Using file validator", "pv", privValidator.(*privval.FilePV).String())

	// Register/inject in running reactor
	reactor.RegisterInstance(InstanceKeyPrivValidator, chainID, privValidator)

	// Done with resource allocations
	return nil
}

// InjectGenesisDoc injects a genesisDoc in the reactor's [GenesisDocSet]
// and creates a individual genesis doc in the replicated chain config folder.
//
// Note that the [GenesisDocSet#Sha256Checksum] is also updated here.
func (reactor *Reactor) InjectGenesisDoc(
	chainID string,
	confDir string,
	genesisDoc types.GenesisDoc,
) (*ChecksummedGenesisDocSet, error) {
	// Store individual genesis docs (per replicated chain)
	// i.e.: %root%/config/%address%/%ChainID%/genesis.json
	genesisDocFile := filepath.Join(confDir, "genesis.json")
	if err := genesisDoc.SaveAs(genesisDocFile); err != nil {
		return nil, fmt.Errorf(
			"could not save genesis.json for ChainID %s: %w", chainID, err)
	}

	// Retrieve initialGenesisDocSet, then append new doc and reset providers.
	icsGenesisDocSet := reactor.GetChecksummedGenesisDocSet()
	icsGenesisDocSet.GenesisDocs = append(icsGenesisDocSet.GenesisDocs, genesisDoc)

	// Get JSON of GenesisDocSet to update SHA256 checksum
	genDocSetBytes, err := cmtjson.Marshal(icsGenesisDocSet.GenesisDocs)
	if err != nil {
		return nil, fmt.Errorf(
			"could not append genesis doc for ChainID %s: %w", chainID, err)
	}

	// Now re-generate the SHA-256 checksum of this GenesisDocSet
	icsGenesisDocSet.Sha256Checksum = tmhash.Sum(genDocSetBytes)

	// .. and persist the new genesis doc set with new network
	err = icsGenesisDocSet.GenesisDocs.SaveAs(reactor.GetNodeConfig().GenesisFile())
	if err != nil {
		return nil, fmt.Errorf(
			"could not save genesis.json with GenesisDocSet: %w", err)
	}

	// Register/inject in running reactor
	reactor.SetChecksummedGenesisDocSet(icsGenesisDocSet)
	return icsGenesisDocSet, nil
}

// InjectStateMachine injects a new state machine created from a GenesisDoc
// in the icsGenesisDocSet for a single chainID.
//
// This method is responsible for initializing a [sm.State] from genesisDoc
// and a block store [bs.Store] using the previously created databases,
// i.e. AllocateNetwork must have been called before.
func (reactor *Reactor) InjectStateMachine(
	chainID string,
	icsGenesisDocSet *ChecksummedGenesisDocSet,
) error {
	stateDatabaseProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseState)
	blockDatabaseProvider := reactor.GetInstanceProvider(InstanceKeyDatabaseBlock)

	// The state machine is created using the genesis doc.
	genesisDoc, err := icsGenesisDocSet.GenesisDocByChainID(chainID)
	if err != nil {
		return fmt.Errorf(
			"could not read newly created genesis doc for ChainID %s: %w", chainID, err)
	}

	stateDB := stateDatabaseProvider(chainID).(dbm.DB)
	blockDB := blockDatabaseProvider(chainID).(dbm.DB)

	// Create a state machine, a state store and a block store for this network.
	stateMachine,
		stateStore,
		blockStore,
		err := reactor.MakeNetworkStateMachine(
		genesisDoc,
		stateDB,
		blockDB,
	)
	if err != nil {
		return fmt.Errorf(
			"could not create state machine for ChainID %s: %w", chainID, err)
	}

	// Register/inject in running reactor
	reactor.RegisterInstance(InstanceKeyState, chainID, stateMachine)
	reactor.RegisterInstance(InstanceKeyStateStore, chainID, stateStore)
	reactor.RegisterInstance(InstanceKeyBlockStore, chainID, blockStore)

	return nil
}

// InjectNewNetwork bootstraps a new network using chainID and returns an
// error if any of the required steps do not succeed.
//
// Following services and resources are configured, in this order:
//
// - Creates the necessary filesystem structure.
// - Creates the databases connections for state and blocks.
// - Creates a priv validator that will be included in GenesisDoc.
// - Creates a new genesis document with one validator.
// - Initializes a new state machine around the genesis doc.
// - Initializes a configuration object for a new node.
// - Updates the ABCI client and MultiNetworkNodeInfo.
//
// See also: [InjectNewRuntime].
func (reactor *Reactor) InjectNewNetwork(
	chainID string,
	otherValidators []string,
) error {
	// Build the ExtendedChainID to retrieve user address from ChainID.
	extChainID, err := NewExtendedChainIDFromLegacy(chainID)
	if err != nil {
		return fmt.Errorf(
			"could not parse ChainID value %s: %w", chainID, err)
	}
	userAddress := extChainID.GetUserAddress()

	// CAUTION:
	// We already have a PrivValidator here because of calling AllocateNetwork
	// before this method. And remotely, we execute GetRemoteValidatorsInfo.

	// Retrieve pre-allocated resources for priv validator and fs
	privValProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)
	privValidator := privValProvider(chainID).(types.PrivValidator)

	reactor.envMutex.RLock()
	newConfFolder := reactor.configsPaths[chainID]
	reactor.envMutex.RUnlock()

	// Create the [types.GenesisDoc] instance for this network.
	if _, err := reactor.MakeNetworkGenesis(
		extChainID,
		newConfFolder,
		privValidator,
		otherValidators,
	); err != nil {
		return fmt.Errorf(
			"could not create genesis doc for ChainID %s: %w", chainID, err)
	}
	icsGenesisDocSet := reactor.GetChecksummedGenesisDocSet()

	// Interpret the GenesisDocSet to initialize a state machine for chainID
	if err := reactor.InjectStateMachine(chainID, icsGenesisDocSet); err != nil {
		return err
	}

	// Inject a configuration overwrite for this new network node runtime
	configOverwrite, err := reactor.MakeNetworkConfigOverwrite(extChainID)
	if err != nil {
		return fmt.Errorf(
			"could not create config overwrite for ChainID %s: %w", chainID, err)
	}

	// Register/inject in running reactor
	reactor.RegisterInstance(InstanceKeyConfig, chainID, configOverwrite)

	// Inject the new ChainID in the running reactor.
	// This call updates the internal chainRegistry, nodeInfo and ABCI.
	if err := reactor.RegisterNetwork(userAddress, chainID); err != nil {
		return fmt.Errorf(
			"could not register new network with ChainID %s: %w", chainID, err)
	}

	// The reactor may now be used to call startNodeListeners and to start
	// the consensus reactors and P2P communication for the new network.
	// i.e. a call to InjectNewRuntime may now succeed.

	return nil
}

// InjectNewRuntime begins by starting some mandatory node services such
// as the event bus, the priv validator and indexers. It will then issue a
// handshake using the ABCI client, and also initialize the necessary consensus
// reactors including: mempool, evidence, block executor and block-sync and
// finally, it shall create a Switch and AddrBook for the new network node.
//
// After having initialized all the required reactors, a [node.Node] instance
// is created and started in a parallel goroutine. Given a nil return value
// means that the node instance is running and that `Stop()` may be called.
//
// Caution: The method [InjectNewNetwork] must be called before.
func (reactor *Reactor) InjectNewRuntime(
	ctx context.Context,
	chainID string,
	options ...node.Option,
) error {
	clogger := reactor.logger.With("chain_id", chainID)

	// ------------------------------------------------------------------------
	// Step 1: Create runtime environment

	reactor.chainReadyMtx.Lock()
	reactor.chainReadyChs[chainID] = make(chan bool, 1)
	reactor.chainReadyMtx.Unlock()

	// Creates an event bus and loads the priv validator service.
	go func(network string) {
		// Lock the filesystem mutex while loading priv val (fs)
		reactor.runtimesMutex.Lock()
		defer reactor.runtimesMutex.Unlock()

		// Start node listeners
		if err := reactor.startNodeListeners(ctx, network); err != nil {
			clogger.Error("failed to start node for new runtime",
				"err", err,
			)
		}

		reactor.chainReadyMtx.RLock()
		chainReadyCh := reactor.chainReadyChs[chainID]
		reactor.chainReadyMtx.RUnlock()

		// Done starting node listeners
		chainReadyCh <- true
	}(chainID)

	reactor.chainReadyMtx.RLock()
	chainReadyCh := reactor.chainReadyChs[chainID]
	reactor.chainReadyMtx.RUnlock()

	// Waits until the reactor started the required node listeners
	// Blocks the main thread intentionally to wait for node services.
	select {
	case <-chainReadyCh:
		break
	case <-reactor.Quit():
		return errors.New("failed network injection: interrupted by shutdown")
	}

	// ------------------------------------------------------------------------
	// Step 2: Execute consensus handshake

	// Used to retrieve configuration and state machine.
	statesProvider := reactor.GetInstanceProvider(InstanceKeyState)
	privvalProvider := reactor.GetInstanceProvider(InstanceKeyPrivValidator)

	// The node config contains the configuration overwrite.
	stateMachine := statesProvider(chainID).(sm.State)
	privValidator := privvalProvider(chainID).(types.PrivValidator)

	// Make sure we can access the priv validator
	privValPubKey, err := privValidator.GetPubKey()
	if err != nil {
		return fmt.Errorf(
			"could not read public key from priv validator: %w", err)
	}

	// Since we do not run state-sync, we must execute a ABCI handshake
	// And following a successful handshake, we may load the state machine.
	//
	// e.g. This also happens on restart of a node.
	if err := reactor.PrepareConsensusInstanceWithReactor(ctx, chainID); err != nil {
		return fmt.Errorf(
			"error preparing consensus instance: %w", err)
	}

	// Inform about the state machine block height
	clogger.Info(
		"State machine loaded",
		"chain_id", stateMachine.ChainID,
		"height", stateMachine.LastBlockHeight,
	)

	// ------------------------------------------------------------------------
	// Step 3: Initialize consensus instance

	// We may need to run block-sync for existing networks.
	blockSync := !onlyValidatorIsUs(stateMachine.Copy(), privValPubKey) && !validatorsIncludesUs(stateMachine.Copy(), privValPubKey)
	waitSyncd := blockSync
	logNodeStartupInfo(stateMachine.Copy(), privValPubKey, clogger)

	// Configure the actual consensus instance.
	//
	// Creates a mempool, evidence pool, block executor, blocksync
	// and finally a consensus reactor.
	if err := reactor.CreateConsensusInstanceReactors(ctx, chainID, blockSync, waitSyncd); err != nil {
		return fmt.Errorf(
			"error creating consensus reactors: %w", err)
	}

	// Inform about the consensus readiness
	clogger.Info("Network is consensus ready",
		"waitSync", waitSyncd,
		"onlyValidatorIsUs", onlyValidatorIsUs(stateMachine.Copy(), privValPubKey),
		"privValIsValidator", validatorsIncludesUs(stateMachine.Copy(), privValPubKey),
	)

	// ------------------------------------------------------------------------
	// Step 4: Update transport protocol

	// Create the [p2p.MultiplexTransports] instance
	if err := reactor.CreateTransportSwitchesWithReactors(ctx, []string{chainID}); err != nil {
		return fmt.Errorf(
			"error creating p2p event switch: %w", err)
	}

	// Create the peer address books and set on switches
	if err := reactor.CreateAddressBooks(ctx, []string{chainID}); err != nil {
		return fmt.Errorf(
			"error creating the pex address books: %w", err)
	}

	// For injected ChainIDs, we do not need to start the RPC/P2P servers as
	// they are already started by [MultiplexBackend#OnStart].
	nodeOptions := []node.Option{
		node.NodeWithStartRPC(false),
		node.NodeWithStartP2P(false),
		node.NodeWithStartMonitor(false),
	}

	// Register the [node.Node] instance in servicesRegistry.
	if _, err := reactor.createMultiplexNodesWithServices(ctx,
		[]string{chainID},
		nodeOptions...,
	); err != nil {
		return fmt.Errorf(
			"error injecting node instance: %w", err)
	}

	// Inform about all replicated chains being configured
	clogger.Info("The new network is now configured",
		"nodeId", string(reactor.GetNodeKey().ID()))

	return nil
}

// InitAndStartNode creates a [node.Node] instance and calls the Start method
// and later registers new routes for this ChainID in the RPC server.
//
// TODO(midas): refactor to StartNodeInstance.
func (reactor *Reactor) InitAndStartNode(ctx context.Context, chainID string) error {
	clogger := reactor.logger.With("chain_id", chainID)

	var (
		nodeRuntime     *node.Node
		hasNodeRuntimes bool
		hasChainRuntime bool
	)

	reactor.servicesMutex.RLock()
	_, hasNodeRuntimes = reactor.servicesRegistry[ServiceKeyNodeRuntime]
	if hasNodeRuntimes {
		_, hasChainRuntime = reactor.servicesRegistry[ServiceKeyNodeRuntime][chainID]
	} else {
		hasChainRuntime = false
	}
	reactor.servicesMutex.RUnlock()

	// We may need to init the [node.Node] instance first.
	if !hasChainRuntime {
		// Create a MultiplexMap[*node.Node] with this new network.
		updatedMx, err := reactor.createMultiplexNodesWithServices(
			ctx,
			[]string{chainID},
		)
		if err != nil {
			return err
		}

		nodeRuntime = updatedMx[chainID].GetInstance().(*node.Node)
	} else { // hasChainRuntime
		servicesProvider := reactor.GetServicesProvider()
		nodeRuntime = servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
	}

	// If the node is already running, stop here
	if nodeRuntime.IsRunning() {
		return nil
	}

	// CAUTION:
	// Note that consensus reactors are not started here to prevent race
	// conditions between the replication routine and cometbft services.

	// Calls the Start method on the node.Node instance.
	go func(network string, n *node.Node) {
		if n.IsStopped() {
			n.Reset() // allows restart
		}

		clogger.Info("Starting new node", "chain_id", network)
		clogger.Info("Using custom listen addresses",
			"p2p", n.Config().P2P.ListenAddress,
			"rpc", n.Config().RPC.ListenAddress,
		)

		if err := n.Start(); err != nil {
			clogger.Error("failed to start node",
				"err", err,
			)
		}

		clogger.Info("Started node",
			"nodeInfo", n.Switch().NodeInfo(),
		)
	}(chainID, nodeRuntime)

	// Also hot-plug the RPC routes for added network
	reactor.networkMutex.RLock()
	rpcMultiplexer := reactor.rpcMultiplexer
	reactor.networkMutex.RUnlock()
	if rpcMultiplexer != nil {
		if err := reactor.EnableNewRuntimeRPC([]string{chainID}); err != nil {
			return fmt.Errorf(
				"error adding RPC routes for %s: %w", chainID, err)
		}
	}

	// The node instance is now running and producing blocks. Other relays
	// may now join the network and shall do so when the transaction broadcast
	// is executed as a follow-up of the networks creation routine.
	// i.e. a transaction broadcast with chainID may now succeed.

	return nil
}

// StartAllNodeInstances calls the Start method of [node.Node] instances that
// are registered in the services multiplex map of the reactor.
//
// TODO(midas): remove and refactor usage with StartNodeInstance.
func (reactor *Reactor) StartAllNodeInstances() error {
	chainIds := reactor.GetNetworks()
	if len(chainIds) == 0 {
		return nil
	}

	servicesProvider := reactor.GetServicesProvider()
	for _, chainID := range chainIds {
		// Type-assertion makes sure we have a [*node.Node]
		runNode, ok := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		if !ok {
			return errors.New("could not get node runtime in StartAllNodeInstances")
		}

		// Calls the Start method on the node.Node instance.
		// This goroutine produces a panic in case of errors.
		go func(network string, n *node.Node) {
			defer reactor.GetRuntimeRegistry().OnActivate(network)

			if n.IsStopped() {
				n.Reset() // allows restart
			}

			reactor.logger.Info("Starting new node", "chain_id", network)
			reactor.logger.Info("Using custom listen addresses",
				"p2p", n.Config().P2P.ListenAddress,
				"rpc", n.Config().RPC.ListenAddress,
			)

			if err := n.Start(); err != nil {
				reactor.logger.Error("failed to start node",
					"chain_id", network,
					"err", err,
				)
			}

			reactor.logger.Info("Started node",
				"chain_id", network,
				"nodeInfo", n.Switch().NodeInfo(),
			)
		}(chainID, runNode)
	}

	return nil
}

// StopNodeInstance calls the Stop method of [node.Node] instance by chainID.
func (reactor *Reactor) StopNodeInstance(chainID string) error {
	if !reactor.HasNetwork(chainID) {
		return nil
	}

	servicesProvider := reactor.GetServicesProvider()
	runNode, ok := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
	if !ok || !runNode.IsRunning() {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(1)

	// Calls the Stop method on the node.Node instance.
	go func(network string, n *node.Node, r *Reactor) {
		r.logger.Info("Stopping node runtime", "chain_id", network)

		defer wg.Done()
		if n.IsRunning() {
			if err := n.Stop(); err != nil {
				if err != service.ErrAlreadyStopped {
					reactor.logger.Error("failed to stop node",
						"chain_id", network,
						"err", err,
					)
				}
			}
		}

		cometbftSwitch := r.GetEventSwitchForCometBFT()
		r.StopPeersByScope(cometbftSwitch, network)

		discoverySwitch := r.GetEventSwitchForDiscovery()
		r.StopPeersByScope(discoverySwitch, network)

		r.logger.Info("Stopped node runtime", "chain_id", network)
	}(chainID, runNode, reactor)

	wg.Wait()
	return nil
}

func (reactor *Reactor) StopPeersByScope(sw *p2p.Switch, scope string) (size int) {
	defer func() {
		if r := recover(); r != nil {
			// do not panic when cleaning up peers.
			reactor.logger.Error(
				"stopping peers by scope panicked",
				"scope", scope,
				"err", r,
			)
			return
		}
	}()

	peersByScope := sw.Peers(scope).Copy()
	size = len(peersByScope)

	reactor.logger.Info("Stopping connections for scope", "scope", scope, "num_peers", size)
	for _, p := range peersByScope {
		relevantScopes := map[string]bool{}
		relevantScopes[scope] = true

		activeRuntimes := sw.GetActiveRuntimes()
		for _, activeChainID := range activeRuntimes {
			relevantScopes[activeChainID] = true
		}

		remainingChannelsForPeer := p.MConn().GetChannelsIdx()
		for chScope, _ := range remainingChannelsForPeer {
			relevantScopes[chScope] = true
		}

		// Cleanup the MConnection channels from switch
		sw.CloseChannelsForScopes(func(scopes map[string]bool) (out []string) {
			out = make([]string, 0, len(scopes))
			for scope, _ := range scopes {
				out = append(out, scope)
			}
			return // out
		}(relevantScopes))(p.MConn())

		// And stop the peer gracefully to remove from reactors.
		sw.StopPeerGracefully(p)
		sw.Transport().Cleanup(p)
	}

	return // size
}

// StopAllNodeInstances calls the Stop method of [node.Node] instances that
// are registered in the services multiplex map of the reactor.
func (reactor *Reactor) StopAllNodeInstances() error {
	chainIds := reactor.GetNetworks()
	if len(chainIds) == 0 {
		return nil
	}

	var wg sync.WaitGroup
	wg.Add(len(chainIds))

	servicesProvider := reactor.GetServicesProvider()
	for _, chainID := range chainIds {
		// Type-assertion makes sure we have a [*node.Node]
		runNode, ok := servicesProvider(ServiceKeyNodeRuntime, chainID).(*node.Node)
		if !ok || !runNode.IsRunning() {
			wg.Done()
			continue
		}

		// Calls the Stop method on the node.Node instance.
		go func(network string, n *node.Node) {
			reactor.logger.Info("Stopping node runtime", "chain_id", network)

			defer wg.Done()
			if n.IsRunning() {
				if err := n.Stop(); err != nil {
					if err != service.ErrAlreadyStopped {
						reactor.logger.Error("failed to stop node",
							"chain_id", network,
							"err", err,
						)
					}
				}
			}

			reactor.logger.Info("Stopped node runtime", "chain_id", network)
		}(chainID, runNode)
	}

	// Wait for all nodes to be stopped.
	wg.Wait()

	return nil
}

// -----------------------------------------------------------------------------
// Makers implementation for InjectNewNetwork units

// MakeNetworkFilesystem creates the filesystem structure for a network
// chainID and returns the configuration and data paths.
//
// Return order is: config folder, data folder, error.
func (reactor *Reactor) MakeNetworkFilesystem(
	chainID ExtendedChainID,
) (string, string, error) {
	// Uses the global node config
	reactor.envMutex.RLock()
	rootDir := reactor.nodeConfig.RootDir
	reactor.envMutex.RUnlock()

	// Uses default folder names
	dataDir := filepath.Join(rootDir, config.DefaultDataDir)
	confDir := filepath.Join(rootDir, config.DefaultConfigDir)

	// runtimesMutex shall be locked during filesystem ops.
	reactor.runtimesMutex.Lock()
	defer reactor.runtimesMutex.Unlock()

	chainConfDir,
		chainDataDir,
		err := EnsureNetworkFS(chainID, confDir, dataDir)

	keyChainID := chainID.String()
	reactor.storagePaths[keyChainID] = chainDataDir
	reactor.configsPaths[keyChainID] = chainConfDir
	return chainConfDir, chainDataDir, err
}

// MakeNetworkDatabases creates and opens database instances for a network
// chainID and for one or many database names.
func (reactor *Reactor) MakeNetworkDatabases(
	chainID ExtendedChainID,
	databases []string,
) (dbs map[string]dbm.DB, err error) {
	nodeConfig := reactor.GetNodeConfig()

	// Prepare database parameters
	dbBackend := dbm.BackendType(nodeConfig.DBBackend)
	dbStorage := filepath.Join(
		nodeConfig.DBDir(),
		chainID.GetUserAddress(),
		chainID.String(),
	)

	dbs = map[string]dbm.DB{}
	for _, dbName := range databases {
		dbs[dbName], err = dbm.NewDB(dbName, dbBackend, dbStorage)
		if err != nil {
			return map[string]dbm.DB{}, fmt.Errorf(
				"could not create database %s for ChainID %s: %w", dbName, chainID.String(), err)
		}
	}

	return dbs, nil
}

// MakeNetworkValidator creates the priv validator for a new network
// chainID which will also be added to the new GenesisDoc.
func (reactor *Reactor) MakeNetworkValidator(
	confDir string,
	dataDir string,
) (types.PrivValidator, error) {
	nodeConfig := reactor.GetNodeConfig()

	// Uses filenames from configuration
	keyFile := filepath.Base(nodeConfig.PrivValidatorKeyFile())
	stateFile := filepath.Base(nodeConfig.PrivValidatorStateFile())

	// runtimesMutex shall be locked during filesystem ops.
	reactor.runtimesMutex.Lock()
	defer reactor.runtimesMutex.Unlock()

	// NOTE: We force the ed25519 key type here.
	return privval.LoadOrGenFilePV(
		filepath.Join(confDir, keyFile),
		filepath.Join(dataDir, stateFile),
		func() (crypto.PrivKey, error) {
			return ed25519.GenPrivKey(), nil
		},
	)
}

// MakeNetworkGenesis creates the [types.GenesisDoc] for a new network
// chainID which contains a privValidator public key.
func (reactor *Reactor) MakeNetworkGenesis(
	chainID ExtendedChainID,
	confDir string,
	privValidator types.PrivValidator,
	otherValidators []string,
) (*ChecksummedGenesisDocSet, error) {
	// Priv validator pubkey is added to GenesisDoc
	localValidatorPubKey, err := privValidator.GetPubKey()
	if err != nil {
		return nil, fmt.Errorf(
			"could not read validator pubkey for ChainID %s: %w", chainID, err)
	}

	powerPerValidator := 10
	genesisValidators := make([]types.GenesisValidator, 0, len(otherValidators)+1)
	genesisValidators = append(genesisValidators, types.GenesisValidator{
		Address: localValidatorPubKey.Address(),
		PubKey:  localValidatorPubKey,
		Power:   int64(powerPerValidator),
	})

	for _, validatorPublicKeyHex := range otherValidators {
		validatorPubBz, err := hex.DecodeString(validatorPublicKeyHex)
		if err != nil {
			return nil, fmt.Errorf(
				"could not decode validator pubkey for ChainID %s: %w", chainID, err)
		}

		validatorPubKey, err := cmtcodec.PubKeyFromTypeAndBytes(ed25519.KeyType, validatorPubBz)
		if err != nil {
			return nil, fmt.Errorf(
				"could not parse validator pubkey for ChainID %s: %w", chainID, err)
		}

		genesisValidators = append(genesisValidators, types.GenesisValidator{
			Address: validatorPubKey.Address(),
			PubKey:  validatorPubKey,
			Power:   int64(powerPerValidator),
		})
	}

	// Create new GenesisDoc with genesis time "now" and one validator.
	genesisDoc := types.GenesisDoc{
		ChainID:         chainID.String(),
		GenesisTime:     cmttime.Now(),
		ConsensusParams: types.DefaultConsensusParams(),
		Validators:      genesisValidators,
	}

	return reactor.InjectGenesisDoc(chainID.String(), confDir, genesisDoc)
}

// MakeNetworkStateMachine creates instances of [sm.State], [sm.Store] and
// a [bs.BlockStore] for a new network chainID around its' genesisDoc and the
// previously created stateDB and blockstoreDB.
func (reactor *Reactor) MakeNetworkStateMachine(
	genesisDoc *types.GenesisDoc,
	stateDB dbm.DB,
	blockstoreDB dbm.DB,
) (sm.State, sm.Store, *bs.BlockStore, error) {
	nodeConfig := reactor.GetNodeConfig()
	dbKeyLayoutVersion := nodeConfig.Storage.ExperimentalKeyLayout
	dbCompactionMethod := nodeConfig.Storage.Compact
	dbCompactionPeriod := nodeConfig.Storage.CompactionInterval

	// Initialize a replicable sm.Store
	stateStore := sm.NewStore(stateDB, sm.StoreOptions{
		DBKeyLayout: dbKeyLayoutVersion,
	})

	// Load the state from GenesisDoc
	stateMachine, err := stateStore.LoadFromDBOrGenesisDoc(genesisDoc)
	if err != nil {
		return sm.State{}, nil, nil, fmt.Errorf(
			"could not load state machine from genesis doc: %w", err)
	}

	// TODO(midas): Save is necessary only if stateMachine.IsEmpty() before above load.

	// Persist the state machine as created from genesis doc
	if err := stateStore.Save(stateMachine); err != nil {
		return sm.State{}, nil, nil, fmt.Errorf(
			"could not save newly initialized state machine: %w", err)
	}

	// Also, initialize a [bs.BlockStore] (not snapshottable)
	blockStore := bs.NewBlockStore(
		blockstoreDB,
		bs.WithCompaction(dbCompactionMethod, dbCompactionPeriod),
		bs.WithDBKeyLayout(dbKeyLayoutVersion),
	)

	return stateMachine, stateStore, blockStore, nil
}

// MakeNetworkConfigOverwrite creates the [config.Config] instance for a
// new network chainID and overwrites the services listen addresses such that
// a node may be run using the current runtime.
func (reactor *Reactor) MakeNetworkConfigOverwrite(
	chainID ExtendedChainID,
) (*config.Config, error) {
	nodeConfig := reactor.GetNodeConfig()

	// Create a config overwrite without using the ChainRegistry
	// We only update the ChainRegistry after initializing the network.
	configOverwrite, err := NewConfigOverwriteWithParameters(
		nodeConfig,
		chainID.String(),
		"", // empty seed nodes (new network)
		config.DefaultStateSyncConfig(),
		int(nodeConfig.DiscoveryPort),
	)
	if err != nil {
		return nil, err
	}

	return configOverwrite, nil
}
