package commands

import (
	"fmt"
	"sync"

	"github.com/spf13/cobra"

	cfg "github.com/cometbft/cometbft/config"
	"github.com/cometbft/cometbft/crypto"
	"github.com/cometbft/cometbft/crypto/ed25519"
	kt "github.com/cometbft/cometbft/internal/keytypes"
	cmtos "github.com/cometbft/cometbft/internal/os"
	mx "github.com/cometbft/cometbft/multiplex"
	nm "github.com/cometbft/cometbft/node"
)

var (
	cliParams       nm.CliParams
	keyType         string
	enableMultiplex bool
	scopesFile      string
	seedsFile       string
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

// NewRunMultiplexCmd returns the command that allows the CLI to start a multiplex node.
// It can be used with a custom PrivValidator and in-process ABCI application.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in
// a production environment. Please use this feature with
// caution as it is still being actively researched.
func NewRunMultiplexCmd(multiplexProvider mx.Provider) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "multiplex",
		Aliases: []string{"nodes"},
		Short:   "Run the CometBFT multiplex",
		RunE: func(_ *cobra.Command, _ []string) error {
			// Ensure existence of genesis file, otherwise can't boot
			userScopes, err := loadScopesFromGenesisFile(config.GenesisFile())
			if err != nil {
				return fmt.Errorf("failed to load multiplex config: %w", err)
			}

			// Overwrite the UserConfig part with userScopes (but keep prepared rootDir)
			rootDir := config.RootDir
			config.BaseConfig = cfg.MultiplexBaseConfig(cfg.PluralReplicationMode(), userScopes)
			config.SetRoot(rootDir)

			// Create the NodeRegistry, uses DefaultMultiplexNode()
			r, err := multiplexProvider(config, logger)
			if err != nil {
				return fmt.Errorf("failed to create multiplex: %w", err)
			}

			// Retrieve multiplex reactor used in subroutines
			reactor := r.Reactor

			wg := sync.WaitGroup{}
			wg.Add(len(r.Nodes))

			for _, n := range r.Nodes {
				// Here, we create one goroutine per replicated chain and each starts a
				// CometBFT node instance from the [NodeRegistry]. Additionally, we trap
				// SIGTERM and SIGINT to gracefully terminate the node process on exit.
				//
				// CAUTION - EXPERIMENTAL:
				// Running the following code is highly unrecommended in
				// a production environment. Please use this feature with
				// caution as it is still being actively researched.
				go func(sn *mx.ScopedNode) {
					defer wg.Done()

					genesisDocProvider := reactor.GetGenesisProvider()
					genDoc, err := genesisDocProvider(sn.ScopeHash)
					if err != nil {
						panic(fmt.Errorf("failed to load genesis doc: %w", err))
					}

					logger.Info("Starting new node", "scope", sn.ScopeHash, "chain", genDoc.ChainID)
					logger.Info("Using custom listen addresses", "p2p", sn.Config().P2P.ListenAddress, "rpc", sn.Config().RPC.ListenAddress)

					if err := sn.Start(); err != nil {
						panic(fmt.Errorf("failed to start node: %w", err))
					}

					logger.Info("Started node", "scope", sn.ScopeHash, "nodeInfo", sn.Switch().NodeInfo())

					// Stop upon receiving SIGTERM or CTRL-C.
					cmtos.TrapSignal(logger, func() {
						if sn.IsRunning() {
							if err := sn.Stop(); err != nil {
								logger.Error("unable to stop the node", "error", err)
							}
						}
					})
				}(n)
			}

			// Wait for all nodes to be up and running
			logger.Info("Waiting for nodes to be up and running.", "count", len(r.Nodes))
			wg.Wait()

			// Run forever.
			select {}
		},
	}

	AddNodeFlags(cmd)
	return cmd
}
