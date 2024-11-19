package commands

import (
	"fmt"
	"sync"

	"github.com/spf13/cobra"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	kt "github.com/ice-blockchain/cometbft/internal/keytypes"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	nm "github.com/ice-blockchain/cometbft/node"
)

var (
	cliParams nm.CliParams
	keyType   string

	// Added flags for nodes multiplex:
	// --multiplex: enables multiplex for init command
	// --seeds-file: config-overwrite for seed nodes by chain
	// --users-file: config-overwrite for user chains creation.
	enableMultiplex bool
	seedsFile       string
	usersFile       string
)

func genPrivKeyFromFlag() (crypto.PrivKey, error) {
	return kt.GenPrivKey(keyType)
}

// AddNodeFlags exposes some common configuration options on the command-line
// These are exposed for convenience of commands embedding a CometBFT node.
func AddNodeFlags(cmd *cobra.Command) {
	// bind flags
	cmd.Flags().String("moniker", config.Moniker, "node name")

	// priv val flags
	cmd.Flags().String(
		"priv_validator_laddr",
		config.PrivValidatorListenAddr,
		"socket address to listen on for connections from external priv_validator process")

	// node flags
	cmd.Flags().BytesHexVar(
		&cliParams.GenesisHash,
		"genesis_hash",
		[]byte{},
		"optional SHA-256 hash of the genesis file")
	cmd.Flags().Int64("consensus.double_sign_check_height", config.Consensus.DoubleSignCheckHeight,
		"how many blocks to look back to check existence of the node's "+
			"consensus votes before joining consensus")

	// abci flags
	cmd.Flags().String(
		"proxy_app",
		config.ProxyApp,
		"proxy app address, or one of: 'kvstore',"+
			" 'persistent_kvstore' or 'noop' for local testing.")
	cmd.Flags().String("abci", config.ABCI, "specify abci transport (socket | grpc)")

	// rpc flags
	cmd.Flags().String("rpc.laddr", config.RPC.ListenAddress, "RPC listen address. Port required")
	cmd.Flags().Bool("rpc.unsafe", config.RPC.Unsafe, "enabled unsafe rpc methods")
	cmd.Flags().String("rpc.pprof_laddr", config.RPC.PprofListenAddress, "pprof listen address (https://golang.org/pkg/net/http/pprof)")

	// p2p flags
	cmd.Flags().String(
		"p2p.laddr",
		config.P2P.ListenAddress,
		"node listen address. (0.0.0.0:0 means any interface, any port)")
	cmd.Flags().String("p2p.external_address", config.P2P.ExternalAddress, "ip:port address to advertise to peers for them to dial")
	cmd.Flags().String("p2p.seeds", config.P2P.Seeds, "comma-delimited ID@host:port seed nodes")
	cmd.Flags().String("p2p.persistent_peers", config.P2P.PersistentPeers, "comma-delimited ID@host:port persistent peers")
	cmd.Flags().String("p2p.unconditional_peer_ids",
		config.P2P.UnconditionalPeerIDs, "comma-delimited IDs of unconditional peers")
	cmd.Flags().Bool("p2p.pex", config.P2P.PexReactor, "enable/disable Peer-Exchange")
	cmd.Flags().Bool("p2p.seed_mode", config.P2P.SeedMode, "enable/disable seed mode")
	cmd.Flags().String("p2p.private_peer_ids", config.P2P.PrivatePeerIDs, "comma-delimited private peer IDs")

	// consensus flags
	cmd.Flags().Bool(
		"consensus.create_empty_blocks",
		config.Consensus.CreateEmptyBlocks,
		"set this to false to only produce blocks when there are txs or when the AppHash changes")
	cmd.Flags().String(
		"consensus.create_empty_blocks_interval",
		config.Consensus.CreateEmptyBlocksInterval.String(),
		"the possible interval between empty blocks")

	// db flags
	cmd.Flags().String(
		"db_backend",
		config.DBBackend,
		"database backend: goleveldb | cleveldb | boltdb | rocksdb | badgerdb | pebbledb")
	cmd.Flags().String(
		"db_dir",
		config.DBPath,
		"database directory")
	cmd.Flags().StringVarP(&keyType, "key-type", "k", ed25519.KeyType, fmt.Sprintf("private key type (one of %s)", kt.SupportedKeyTypesStr()))

	// multiplex optional --seeds-file config overwrite
	cmd.Flags().StringVarP(&seedsFile, "seeds-file", "", "", "path to a JSON file containing chain seeds mapped by ChainID.")
}

// NewRunNodeCmd returns the command that allows the CLI to start a node.
// It can be used with a custom PrivValidator and in-process ABCI application.
func NewRunNodeCmd(nodeProvider nm.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "start",
		Aliases: []string{"node", "run"},
		Short:   "Run the CometBFT node",
		RunE: func(_ *cobra.Command, _ []string) error {
			n, err := nodeProvider(config, logger, cliParams, genPrivKeyFromFlag)
			if err != nil {
				return fmt.Errorf("failed to create node: %w", err)
			}

			if err := n.Start(); err != nil {
				return fmt.Errorf("failed to start node: %w", err)
			}

			logger.Info("Started node", "nodeInfo", n.Switch().NodeInfo())

			// Stop upon receiving SIGTERM or CTRL-C.
			cmtos.TrapSignal(logger, func() {
				if n.IsRunning() {
					if err := n.Stop(); err != nil {
						logger.Error("unable to stop the node", "error", err)
					}
				}
			})

			// Run forever.
			select {}
		},
	}

	AddNodeFlags(cmd)
	return cmd
}

// NewRunMultiplexCmd returns the command that allows the CLI to start a nodes multiplex.
// It can be used with a custom PrivValidator and always runs the *snapsapp* in-process
// ABCI application- i.e. locally running an ABCI server.
//
// IMPORTANT:
// The execution of the `init --multiplex` command is *required* before you
// can run the `multiplex` command. The initialized `genesis.json` file will
// be used to find out the *known networks* of a nodes multiplex.
// Additionally, it is possible to provide a `--seeds-file` parameter to force
// seed nodes configuration for a given ChainID. Note, the client extension
// passed to [client.InjectSeedConfig] shall prevail and will provide the final
// seed nodes configuration, as it is called with [mx.NewChainRegistry].
//
// IMPORTANT:
// Note that this method spawns one goroutine per supported network.
// The [nm.Node.Start] method is called in a parallel goroutine for every
// replicated chain, and a [sync.WaitGroup] is used to *wait* for all nodes
// to be up and running.
// Additionally, we trap signals SIGTERM and SIGINT to gracefully terminate
// the node process on exit.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in
// a production environment. Please use this feature with
// caution as it is still being actively developed.
func NewRunMultiplexCmd(multiplexProvider mx.NodesMultiplexProvider) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "multiplex",
		Aliases: []string{"nodes"},
		Short:   "Run the CometBFT multiplex",
		RunE: func(_ *cobra.Command, _ []string) error {
			// Read the genesis.json file to re-create the map of slices with
			// ChainIDs by user addresses.
			userChains, err := mx.LoadChainsFromGenesisFile(config.GenesisFile())
			if err != nil {
				return fmt.Errorf("failed to load multiplex config: %w", err)
			}

			// Parse a --seeds-file option to force some chain seeds by config.
			// Note that client extensions prevail and overrule chain seeds config.
			// See also: client.InjectSeedConfig()
			chainSeeds := map[string]string{}
			if len(seedsFile) > 0 && cmtos.FileExists(seedsFile) {
				chainSeeds, err = mx.LoadSeedsFromFile(seedsFile)
				if err != nil {
					return fmt.Errorf("failed to load seeds config: %w", err)
				}
			}

			// Overwrite the UserChains but keep prepared rootDir
			rootDir := config.RootDir
			config.Strategy = mx.NetworkReplicationStrategy()
			config.ChainSeeds = chainSeeds
			config.UserChains = userChains
			config.SetRoot(rootDir)

			// Create the MultiplexMap of *node.Node instances
			nodesMultiplex, err := multiplexProvider(config, logger)
			if err != nil {
				return fmt.Errorf("failed to create multiplex: %w", err)
			}

			wg := sync.WaitGroup{}
			wg.Add(len(nodesMultiplex))

			for nodeChainID, nodeInstance := range nodesMultiplex {
				// Type-assertion makes sure we have a [*nm.Node]
				runNode := nodeInstance.GetInstance().(*nm.Node)

				// Here, we create one goroutine per replicated chain and each starts a
				// CometBFT node instance from the [MultiplexMap]. Additionally, we trap
				// SIGTERM and SIGINT to gracefully terminate the node process on exit.
				//
				// CAUTION - EXPERIMENTAL:
				// Running the following code is highly unrecommended in
				// a production environment. Please use this feature with
				// caution as it is still being actively developed.
				go func(chainId string, sn *nm.Node) {
					defer wg.Done()

					logger.Info("Starting new node", "chain", chainId)
					logger.Info("Using custom listen addresses",
						"p2p", sn.Config().P2P.ListenAddress,
						"rpc", sn.Config().RPC.ListenAddress,
					)

					if err := sn.Start(); err != nil {
						panic(fmt.Errorf("failed to start node: %w", err))
					}

					logger.Info("Started node", "chain", chainId, "nodeInfo", sn.Switch().NodeInfo())

					// Stop upon receiving SIGTERM or CTRL-C.
					cmtos.TrapSignal(logger, func() {
						if sn.IsRunning() {
							if err := sn.Stop(); err != nil {
								logger.Error("unable to stop the node", "error", err)
							}
						}
					})
				}(nodeChainID, runNode)
			}

			// Wait for all nodes to be up and running
			logger.Info("Waiting for nodes to be up and running.", "count", len(nodesMultiplex))
			wg.Wait()

			// Run forever.
			select {}
		},
	}

	AddNodeFlags(cmd)
	return cmd
}
