package commands

import (
	"fmt"

	"github.com/spf13/cobra"

	"github.com/ice-blockchain/cometbft/crypto"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	kt "github.com/ice-blockchain/cometbft/internal/keytypes"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	nm "github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
)

var (
	cliParams nm.CliParams
	keyType   string

	// Added flags for nodes multiplex:
	// --relay-port: Overwrite the multiplex backend discovery port.
	// --users-file: Path to a `users.json` config-overwrite for user chains.
	// --seeds-file: Path to a `seeds.json` config-overwrite for chain seeds.
	usersFile     string
	seedsFile     string
	discoveryPort uint16
)

// genPrivKeyFromFlag returns the default private key provider
// using the optional keyType.
func genPrivKeyFromFlag() (crypto.PrivKey, error) {
	return kt.GenPrivKey(keyType)
}

// AddNodeFlags exposes some common configuration options on the command-line
// These are exposed for convenience of commands embedding a CometBFT node.
func AddNodeFlags(cmd *cobra.Command) {
	// bind flags
	cmd.Flags().String("moniker", nodeConfig.Moniker, "node name")

	// priv val flags
	cmd.Flags().String(
		"priv_validator_laddr",
		nodeConfig.PrivValidatorListenAddr,
		"socket address to listen on for connections from external priv_validator process")

	// node flags
	cmd.Flags().BytesHexVar(
		&cliParams.GenesisHash,
		"genesis_hash",
		[]byte{},
		"optional SHA-256 hash of the genesis file")
	cmd.Flags().Int64("consensus.double_sign_check_height", nodeConfig.Consensus.DoubleSignCheckHeight,
		"how many blocks to look back to check existence of the node's "+
			"consensus votes before joining consensus")

	// abci flags
	cmd.Flags().String(
		"proxy_app",
		nodeConfig.ProxyApp,
		"proxy app address, or one of: 'kvstore',"+
			" 'persistent_kvstore' or 'noop' for local testing.")
	cmd.Flags().String("abci", nodeConfig.ABCI, "specify abci transport (socket | grpc)")

	// rpc flags
	cmd.Flags().String("rpc.laddr", nodeConfig.RPC.ListenAddress, "RPC listen address. Port required")
	cmd.Flags().Bool("rpc.unsafe", nodeConfig.RPC.Unsafe, "enabled unsafe rpc methods")
	cmd.Flags().String("rpc.pprof_laddr", nodeConfig.RPC.PprofListenAddress, "pprof listen address (https://golang.org/pkg/net/http/pprof)")

	// p2p flags
	cmd.Flags().String(
		"p2p.laddr",
		nodeConfig.P2P.ListenAddress,
		"node listen address. (0.0.0.0:0 means any interface, any port)")
	cmd.Flags().String("p2p.external_address", nodeConfig.P2P.ExternalAddress, "ip:port address to advertise to peers for them to dial")
	cmd.Flags().String("p2p.seeds", nodeConfig.P2P.Seeds, "comma-delimited ID@host:port seed nodes")
	cmd.Flags().String("p2p.persistent_peers", nodeConfig.P2P.PersistentPeers, "comma-delimited ID@host:port persistent peers")
	cmd.Flags().String("p2p.unconditional_peer_ids",
		nodeConfig.P2P.UnconditionalPeerIDs, "comma-delimited IDs of unconditional peers")
	cmd.Flags().Bool("p2p.pex", nodeConfig.P2P.PexReactor, "enable/disable Peer-Exchange")
	cmd.Flags().Bool("p2p.seed_mode", nodeConfig.P2P.SeedMode, "enable/disable seed mode")
	cmd.Flags().String("p2p.private_peer_ids", nodeConfig.P2P.PrivatePeerIDs, "comma-delimited private peer IDs")

	// consensus flags
	cmd.Flags().Bool(
		"consensus.create_empty_blocks",
		nodeConfig.Consensus.CreateEmptyBlocks,
		"set this to false to only produce blocks when there are txs or when the AppHash changes")
	cmd.Flags().String(
		"consensus.create_empty_blocks_interval",
		nodeConfig.Consensus.CreateEmptyBlocksInterval.String(),
		"the possible interval between empty blocks")

	// db flags
	cmd.Flags().String(
		"db_backend",
		nodeConfig.DBBackend,
		"database backend: goleveldb | cleveldb | boltdb | rocksdb | badgerdb | pebbledb")
	cmd.Flags().String(
		"db_dir",
		nodeConfig.DBPath,
		"database directory")
	cmd.Flags().StringVarP(&keyType, "key-type", "k", ed25519.KeyType, fmt.Sprintf("private key type (one of %s)", kt.SupportedKeyTypesStr()))

	// multiplex optional --seeds-file config overwrite
	cmd.Flags().StringVarP(&seedsFile, "seeds-file", "", "", "path to a JSON file containing seed nodes mapped by ChainID.")
	cmd.Flags().Uint16VarP(&discoveryPort, "relay-port", "", 30001, "a port number used as the multiplex backend p2p discovery port.")
}

// NewRunMultiplexCmd returns the command that allows the CLI to start a nodes multiplex.
// It can be used with a custom PrivValidator and always runs the *snapsapp* in-process
// ABCI application- i.e. locally running an ABCI server.
//
// IMPORTANT:
// The execution of the `init` command is *required* before you can start the
// relay with the `run` command. The initialized `genesis.json` file will
// be used to find out the *known networks* of a nodes multiplex, if any.
//
// The [nm.Node#Start] method is called within [mx.Backend#MustStart] which
// spawns a goroutine for every replicated chain, and a [sync.WaitGroup] is
// used to *wait* for all nodes to be up and running.
// Additionally, we trap signals SIGTERM and SIGINT to gracefully terminate
// the node process on exit.
//
// CAUTION - EXPERIMENTAL:
// Running the following code is highly unrecommended in
// a production environment. Please use this feature with
// caution as it is still being actively developed.
func NewRunMultiplexCmd(multiplexProvider mx.NodesMultiplexProvider) *cobra.Command {
	cmd := &cobra.Command{
		Use:     "start",
		Aliases: []string{"node", "run"},
		Short:   "Run the CometBFT multiplex",
		RunE: func(_ *cobra.Command, _ []string) error {
			// Read the genesis.json file to re-create the map of slices with
			// ChainIDs by user addresses.
			userChains, err := mx.LoadChainsFromGenesisFile(nodeConfig.GenesisFile())
			if err != nil {
				return fmt.Errorf("failed to load multiplex config: %w", err)
			}

			// Report if we couldn't find a user chains configuration file
			if len(userChains) == 0 {
				nodeLogger.Info("Found 0 chains, running an empty multiplex backend")
			}

			// Parse a --seeds-file option to force some chain seeds by config
			chainSeeds := map[string]string{}
			if len(seedsFile) > 0 && cmtos.FileExists(seedsFile) {
				chainSeeds, err = mx.LoadSeedsFromFile(seedsFile)
				if err != nil {
					return fmt.Errorf("failed to load seeds config: %w", err)
				}
			}

			// Overwrite the UserChains
			nodeConfig.Strategy = mx.NetworkReplicationStrategy()
			nodeConfig.DBBackend = "goleveldb"
			nodeConfig.UserChains = userChains
			nodeConfig.DiscoveryPort = discoveryPort
			nodeConfig.Instrumentation.Prometheus = true
			nodeConfig.Instrumentation.PrometheusListenAddr = "tcp://127.0.0.1:30004"

			// Overwrite seed nodes to connect/synchronize with existing networks.
			// This is a runtime overwrite that can be omitted and passed to init cmd.
			if len(chainSeeds) > 0 {
				nodeConfig.ChainSeeds = chainSeeds
			}

			nodeKeyFile := nodeConfig.NodeKeyFile()
			nodeKey, err := p2p.LoadNodeKey(nodeKeyFile)
			if err != nil {
				return fmt.Errorf("failed to load node key file: %w", err)
			}

			// Configure the MultiplexMap of *node.Node instances
			backend, err := mx.NewServer(
				&client.DefaultAcceptor{},
				nodeConfig,
				nodeLogger,
				mx.WithMetrics(mx.PrometheusMetrics(
					nodeConfig.Instrumentation.Namespace+"_"+string(nodeKey.ID()),
					"node_id", string(nodeKey.ID()),
				)),
			)
			if err != nil {
				return fmt.Errorf("failed to create multiplex backend: %w", err)
			}

			// Start listening on DiscoveryPort (P2P + RPC)
			// Calls [node.Node#Start] in spawned goroutines.
			backend.MustStart()

			// Stop upon receiving SIGTERM or CTRL-C.
			cmtos.TrapSignal(nodeLogger, func() {
				if err := backend.Close(); err != nil {
					nodeLogger.Error("unable to stop the multiplex backend", "error", err)
				}
			})

			// Run forever.
			select {}
		},
	}

	AddNodeFlags(cmd)
	return cmd
}
