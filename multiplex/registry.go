package multiplex

import (
	"os"
	"path/filepath"

	cfg "github.com/cometbft/cometbft/config"
	cmtos "github.com/cometbft/cometbft/internal/os"
	"github.com/cometbft/cometbft/p2p"
)

// NodeRegistry contains private node configuration such as the PrivValidator
// and a Config instance, and a multiplex of bootstrapped node instances.
type NodeRegistry struct {
	config   *cfg.Config
	nodeInfo p2p.NodeInfo
	nodeKey  *p2p.NodeKey // our node privkey

	Nodes   MultiplexNode
	Scopes  []string
	Reactor *Reactor
}

// GetListenAddresses returns a service address map by user scope hash.
// Supports listen address rewrites for: p2p, rpc, grpc and grpc privileged.
func (r *NodeRegistry) GetListenAddresses() MultiplexServiceAddress {
	laddrs := make(MultiplexServiceAddress, len(r.Nodes))
	for scopeHash, node := range r.Nodes {
		laddrs[scopeHash] = map[string]string{
			"P2P":      node.Config().P2P.ListenAddress,
			"RPC":      node.Config().RPC.ListenAddress,
			"GRPC":     node.Config().GRPC.ListenAddress,
			"GRPCPriv": node.Config().GRPC.Privileged.ListenAddress,
		}
	}

	return laddrs
}

// MultiplexSeedNodesProvider returns a seed node provider which parses
// a replicated chain node configuration and returns the P2P.Seeds value.
// It returns an empty string if the seeds can't be determined.
func MultiplexSeedNodesProvider(config *cfg.Config) ScopedSeedNodesProvider {
	return func(userScopeHash string) string {
		confDir := filepath.Join(config.RootDir, cfg.DefaultConfigDir)
		scopeId := userScopeHash[:16] // 8 first bytes

		var replConfigFile string
		err := filepath.Walk(confDir, func(path string, info os.FileInfo, err error) error {
			if err == nil && info.Name() == scopeId {
				replConfigFile = filepath.Join(path, "config.toml")
			}
			return nil
		})
		if err != nil || len(replConfigFile) == 0 || !cmtos.FileExists(replConfigFile) {
			return ""
		}

		replConf := cfg.ReadConfigFile(replConfigFile)
		return replConf.P2P.Seeds
	}
}
