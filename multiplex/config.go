package multiplex

import (
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"time"

	cfg "github.com/cometbft/cometbft/config"
)

// ScopedUserConfig embeds a [UserConfig] instance and computes SHA256
// hashes by pairing user addresses and individual scopes such that every
// combination of user address and scope can be referred to by a scope hash.
type ScopedUserConfig struct {
	cfg.UserConfig

	// userAddresses contains addresses (20 bytes hex) by which state is replicated.
	// This property is populated automatically and should not be modified.
	userAddresses []string

	// userScopeHashes maps user addresses to the complete list of SHA256 hashes
	// computed from pairing said user address with the scopes as listed above.
	// This property is populated automatically and should not be modified.
	userScopeHashes map[string][]string
}

// computeUserScopeHashes uses DefaultScopeHashProvider to populate the config
// instance's userScopeHashes and userAddresses fields.
func (c *ScopedUserConfig) computeUserScopeHashes() error {
	if c.Replication == cfg.SingularReplicationMode() {
		return nil
	}

	c.userScopeHashes = map[string][]string{}
	if scopeRegistry, err := DefaultScopeHashProvider(&c.UserConfig); err == nil {
		c.userScopeHashes = scopeRegistry.ScopeHashes
	} else {
		return err
	}

	c.userAddresses = make([]string, len(c.UserScopes))
	for userAddress := range c.UserScopes {
		c.userAddresses = append(c.userAddresses, userAddress)
	}

	return nil
}

// GetScopeHashes returns a slice of the flattened user scope hashes
func (c *ScopedUserConfig) GetScopeHashes() []string {
	var allHashes []string
	for _, scopeHashes := range c.userScopeHashes {
		allHashes = append(allHashes, scopeHashes...)
	}

	// Sort hashes lexicographically
	sort.Strings(allHashes)
	return allHashes
}

// ----------------------------------------------------------------------------
// Builders

// NewUserConfig returns a new pointer to a ScopedUserConfig object.
func NewUserConfig(
	repl cfg.DataReplicationConfig,
	userScopes map[string][]string,
	startListenPort int,
) *ScopedUserConfig {
	config := &ScopedUserConfig{
		UserConfig: cfg.UserConfig{
			Replication: cfg.PluralReplicationMode(),
			UserScopes:  userScopes,
			ListenPort:  startListenPort,
		},
	}

	config.computeUserScopeHashes()
	return config
}

// NewScopedUserConfig returns a scoped configuration for UserConfig.
func NewScopedUserConfig(userScopes map[string][]string, startListenPort int) *ScopedUserConfig {
	config := NewUserConfig(
		cfg.PluralReplicationMode(),
		userScopes,
		startListenPort,
	)
	return config
}

// NewConsensusConfigWithWalFile returns configuration for the consensus service
// with a custom wal file path.
func NewConsensusConfigWithWalFile(walFile string) *cfg.ConsensusConfig {
	if len(walFile) == 0 {
		walFile = filepath.Join(cfg.DefaultDataDir, "cs.wal", "wal")
	}

	return &cfg.ConsensusConfig{
		WalPath:                          walFile,
		TimeoutPropose:                   3000 * time.Millisecond,
		TimeoutProposeDelta:              500 * time.Millisecond,
		TimeoutPrevote:                   1000 * time.Millisecond,
		TimeoutPrevoteDelta:              500 * time.Millisecond,
		TimeoutPrecommit:                 1000 * time.Millisecond,
		TimeoutPrecommitDelta:            500 * time.Millisecond,
		TimeoutCommit:                    1000 * time.Millisecond,
		SkipTimeoutCommit:                false,
		CreateEmptyBlocks:                true,
		CreateEmptyBlocksInterval:        0 * time.Second,
		PeerGossipSleepDuration:          100 * time.Millisecond,
		PeerQueryMaj23SleepDuration:      2000 * time.Millisecond,
		PeerGossipIntraloopSleepDuration: 0 * time.Second,
		DoubleSignCheckHeight:            int64(0),
	}
}

// NewMultiplexNodeConfig updates the node configuration in-place
// and overwrites the services listen addresses with the config:
//
// - P2P: legacy `26656`, multiplex `30001`...`3000x` with x the index of nodes
// - RPC: legacy `26657`, multiplex `31001`...`3100x`
// - gRPC: legacy `26670`, multiplex `32001`...`3200x`
// - Privileged gRPC: legacy `26671`, multiplex `33001`...`3300x`
//
// The index generally represents the index of the replicated chain's scope hash
// in the scope hash slices, e.g. with scope hashes ['A','B','C'], the index for
// the node replicating the chain with scope hash 'B' is 1.
// Hashes should always be sorted lexicographically.
//
// It returns the newly created deep-copy of the node configuration.
func NewMultiplexNodeConfig(
	nodeConfig *cfg.Config,
	userConfig *ScopedUserConfig,
	scopeRegistry *ScopeRegistry,
	scopeHash string,
	seedNodes string,
) *cfg.Config {
	// Multiplex can be configured to start at different port
	startPort := userConfig.GetListenPort() // defaults to 30001

	// Find index of scope hash (deterministic due to sorting)
	nodeIdx := scopeRegistry.FindScope(scopeHash)
	address, _ := scopeRegistry.GetAddress(scopeHash)
	scopeId := scopeHash[:16] // fingerprint 8 bytes

	newP2PPort := ":" + strconv.Itoa(startPort+nodeIdx)           // defaults to 30001, 2nd chain 30002, etc.
	newRPCPort := ":" + strconv.Itoa(startPort+1000+nodeIdx)      // defaults to 31001
	newGRPCPort := ":" + strconv.Itoa(startPort+2000+nodeIdx)     // defaults to 32001
	newGRPCPrivPort := ":" + strconv.Itoa(startPort+3000+nodeIdx) // defaults to 33001

	// Replace port with multiplex ports mapping
	re := regexp.MustCompile(`(.*)(\:\d+)(.*)`)
	newP2PAddr := re.ReplaceAllString(nodeConfig.P2P.ListenAddress, `$1`+newP2PPort+`$3`)
	newRPCAddr := re.ReplaceAllString(nodeConfig.RPC.ListenAddress, `$1`+newRPCPort+`$3`)
	newGRPCAddr := re.ReplaceAllString(nodeConfig.GRPC.ListenAddress, `$1`+newGRPCPort+`$3`)
	newGRPCPrivAddr := re.ReplaceAllString(nodeConfig.GRPC.Privileged.ListenAddress, `$1`+newGRPCPrivPort+`$3`)

	// Deep-copy the config object to create multiple nodes
	newNodeConfig := cfg.NewConfigCopy(nodeConfig)
	newNodeConfig.SetListenAddresses(
		newP2PAddr,
		newRPCAddr,
		newGRPCAddr,
		newGRPCPrivAddr,
	)

	// Overwrite seeds if any available
	if len(seedNodes) > 0 {
		newNodeConfig.P2P.Seeds = seedNodes
	}

	// Overwrite wal file on a per-node basis
	// i.e.: data/%address%/%fingerprint%/wal
	dataDir := filepath.Join(nodeConfig.RootDir, cfg.DefaultDataDir)
	walFile := filepath.Join(dataDir, address, scopeId, "wal")

	// WAL relative path
	walPath := filepath.Join(cfg.DefaultDataDir, address, scopeId, "wal")

	// We overwrite the wal file to allow parallel I/O for multiple nodes
	newNodeConfig.Consensus.SetWalFile(walFile)
	newNodeConfig.Consensus.WalPath = walPath

	// TODO(midas): ProxyApp overwrite should not be necessary but required
	//              until the proxy_app config is updated.
	newNodeConfig.ProxyApp = "kvstore"
	return newNodeConfig
}

// ----------------------------------------------------------------------------
// Testing builders

// TestUserConfig returns a basic user configuration for testing a CometBFT node.
func TestUserConfig() ScopedUserConfig {
	return ScopedUserConfig{
		UserConfig: cfg.DefaultUserConfig(),
	}
}

// TestScopedUserConfig returns a scoped configuration for UserConfig.
func TestScopedUserConfig(userScopes map[string][]string) ScopedUserConfig {
	configScopes := make(map[string][]string, len(userScopes))
	if len(userScopes) == 0 {
		configScopes = map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default"},
		}
	} else {
		for userAddress, scopes := range userScopes {
			configScopes[userAddress] = scopes
		}
	}

	cfg := NewUserConfig(
		cfg.PluralReplicationMode(),
		configScopes,
		30001,
	)
	return *cfg
}
