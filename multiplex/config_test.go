package multiplex_test

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	mx "github.com/ice-blockchain/cometbft/multiplex"
)

func TestMultiplexConfigDefaultLegacyFallback(t *testing.T) {
	// Must use EmptyMultiplexConfig()
	conf := config.TestConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), conf.Strategy)

	// Must use EmptyMultiplexConfig()
	baseConf := config.DefaultBaseConfig()
	assert.Equal(t, config.DefaultReplicationStrategy(), baseConf.Strategy)
}

func TestMultiplexConfigMultiplexBaseConfig(t *testing.T) {
	// Must detect empty multiplex config
	conf := config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{},
	)
	assert.Equal(t, config.DefaultReplicationStrategy(), conf.Strategy)

	// Must accept chainSeeds
	conf = config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{
			"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB": "id@host:port",
		},
		map[string][]string{},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.ChainSeeds)

	// Must accept userChains
	conf = config.MultiplexBaseConfig(
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
				"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
			},
		},
	)
	assert.NotEqual(t, config.DefaultReplicationStrategy(), conf.Strategy)
	assert.NotEmpty(t, conf.UserChains)
}

func TestMultiplexConfigNewConfigOverwrite(t *testing.T) {
	rootDir, err := os.MkdirTemp("", t.Name())
	require.NoError(t, err, "should create rootDir for tests")
	defer os.RemoveAll(rootDir)

	conf := config.TestConfig()
	conf.MultiplexConfig = makeRandomMultiplexConfig(t, 3)
	conf.SetRoot(rootDir)

	chainRegistry := makeChainRegistryFromConfig(t, conf.MultiplexConfig)

	// first ChainID has start ports
	chainID1 := chainRegistry.GetChains()[0]
	address1, err := chainRegistry.GetAddress(chainID1)
	expectWal1 := makeWalPath(rootDir, address1, chainID1)
	require.NoError(t, err)
	cfgOverwrite1 := mx.NewConfigOverwrite(conf, chainRegistry, chainID1)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite1.P2P.ListenAddress)
	assert.Contains(t, cfgOverwrite1.P2P.ListenAddress, strconv.Itoa(int(conf.P2PStartPort)))
	assert.Contains(t, cfgOverwrite1.RPC.ListenAddress, strconv.Itoa(int(conf.RPCStartPort)))
	assert.Equal(t, expectWal1, cfgOverwrite1.Consensus.WalFile())

	// second ChainID has starts ports + 1
	chainID2 := chainRegistry.GetChains()[1]
	address2, err := chainRegistry.GetAddress(chainID2)
	assert.NoError(t, err)

	expectWal2 := makeWalPath(rootDir, address2, chainID2)
	cfgOverwrite2 := mx.NewConfigOverwrite(conf, chainRegistry, chainID2)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite2.P2P.ListenAddress)
	assert.Contains(t, cfgOverwrite2.P2P.ListenAddress, strconv.Itoa(int(conf.P2PStartPort+1)))
	assert.Contains(t, cfgOverwrite2.RPC.ListenAddress, strconv.Itoa(int(conf.RPCStartPort+1)))
	assert.Equal(t, expectWal2, cfgOverwrite2.Consensus.WalFile())

	// third ChainID has starts ports + 2
	chainID3 := chainRegistry.GetChains()[2]
	address3, err := chainRegistry.GetAddress(chainID3)
	assert.NoError(t, err)

	expectWal3 := makeWalPath(rootDir, address3, chainID3)
	cfgOverwrite3 := mx.NewConfigOverwrite(conf, chainRegistry, chainID3)
	assert.NotEqual(t, conf.P2P.ListenAddress, cfgOverwrite3.P2P.ListenAddress)
	assert.Contains(t, cfgOverwrite3.P2P.ListenAddress, strconv.Itoa(int(conf.P2PStartPort+2)))
	assert.Contains(t, cfgOverwrite3.RPC.ListenAddress, strconv.Itoa(int(conf.RPCStartPort+2)))
	assert.Equal(t, expectWal3, cfgOverwrite3.Consensus.WalFile())
}

func makeWalPath(rootDir, address, chainID string) string {
	return filepath.Join(rootDir, config.DefaultDataDir, address, chainID, "wal")
}
