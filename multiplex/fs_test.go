package multiplex

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cometbft/cometbft/config"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
)

func TestMultiplexFSEnsureRootMultiplex(t *testing.T) {
	require := require.New(t)

	// setup temp dir for test
	tmpDir, err := os.MkdirTemp("", "config-test")
	require.NoError(err)
	defer os.RemoveAll(tmpDir)

	// create root dir
	config.EnsureRoot(tmpDir)
	config.EnsureRootMultiplex(tmpDir, &config.BaseConfig{
		UserConfig: config.UserConfig{
			Replication: config.PluralReplicationMode(),
			UserScopes: map[string][]string{
				"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
				"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default", "ReplChain"},
			},
		},
	})

	// make sure config is set properly
	data, err := os.ReadFile(filepath.Join(tmpDir, config.DefaultConfigDir, config.DefaultConfigFileName))
	require.NoError(err)

	assertValidConfig(t, string(data))

	testCases := []string{
		filepath.Join("data"),
		filepath.Join("config"),
		filepath.Join("data", "CC8E6555A3F401FF61DA098F94D325E7041BC43A"),
		filepath.Join("data", "CC8E6555A3F401FF61DA098F94D325E7041BC43A", "1A63C0E60122F9BB"), // "Default"
		filepath.Join("data", "CC8E6555A3F401FF61DA098F94D325E7041BC43A", "C26A93BCF48CCE18"), // "Other"
		filepath.Join("data", "FF1410CEEB411E55487701C4FEE65AACE7115DC0"),
		filepath.Join("data", "FF1410CEEB411E55487701C4FEE65AACE7115DC0", "D1ED2B487F2E93CC"), // "Default"
		filepath.Join("data", "FF1410CEEB411E55487701C4FEE65AACE7115DC0", "1A20753306FF8E39"), // "ReplChain"
		filepath.Join("config", "CC8E6555A3F401FF61DA098F94D325E7041BC43A"),
		filepath.Join("config", "CC8E6555A3F401FF61DA098F94D325E7041BC43A", "1A63C0E60122F9BB"),
		filepath.Join("config", "CC8E6555A3F401FF61DA098F94D325E7041BC43A", "C26A93BCF48CCE18"),
		filepath.Join("config", "FF1410CEEB411E55487701C4FEE65AACE7115DC0"),
		filepath.Join("config", "FF1410CEEB411E55487701C4FEE65AACE7115DC0", "D1ED2B487F2E93CC"),
		filepath.Join("config", "FF1410CEEB411E55487701C4FEE65AACE7115DC0", "1A20753306FF8E39"),
	}

	ensureFiles(t, tmpDir, testCases...)
}

func TestMultiplexFSProviderCorrectScopeHash(t *testing.T) {
	require := require.New(t)

	// setup temp dir for test
	tmpDir, err := os.MkdirTemp("", "config-test")
	require.NoError(err)
	defer os.RemoveAll(tmpDir)

	expectedNumChains := 2
	userConfig := config.UserConfig{
		Replication: config.PluralReplicationMode(),
		UserScopes: map[string][]string{
			// mind changing mxtest.TestScopeHash if you make a change here.
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
		},
	}

	// create root dir
	config.EnsureRoot(tmpDir)
	config.EnsureRootMultiplex(tmpDir, &config.BaseConfig{
		UserConfig: userConfig,
	})

	// use provider
	multiplexFS, err := MultiplexFSProvider(&config.Config{
		BaseConfig: config.BaseConfig{
			RootDir:    tmpDir,
			UserConfig: userConfig,
		},
	})
	require.NoError(err)
	assert.NotEmpty(t, multiplexFS, "the filesystem multiplex should not be empty")
	assert.Equal(t, expectedNumChains, len(multiplexFS))
	assert.Contains(t, multiplexFS, mxtest.TestScopeHash)
}

func TestMultiplexFSProviderDetectMissingDataFolder(t *testing.T) {
	require := require.New(t)

	// setup temp dir for test
	tmpDir, err := os.MkdirTemp("", "config-test")
	require.NoError(err)
	defer os.RemoveAll(tmpDir)

	// create root dir
	config.EnsureRoot(tmpDir)
	config.EnsureRootMultiplex(tmpDir, &config.BaseConfig{
		UserConfig: config.UserConfig{
			Replication: config.PluralReplicationMode(),
			UserScopes: map[string][]string{
				"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
			},
		},
	})

	// use provider
	multiplexFS, err := MultiplexFSProvider(&config.Config{
		BaseConfig: config.BaseConfig{
			RootDir: tmpDir,
			UserConfig: config.UserConfig{
				Replication: config.PluralReplicationMode(),
				UserScopes: map[string][]string{
					"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"DOES_NOT_EXIST"},
				},
			},
		},
	})
	assert.Nil(t, multiplexFS)
	assert.Error(t, err, "mandatory filesystem path cannot be missing")
}

func TestMultiplexFSProviderComplexReplicatedChains(t *testing.T) {
	require := require.New(t)

	// setup temp dir for test
	tmpDir, err := os.MkdirTemp("", "config-test")
	require.NoError(err)
	defer os.RemoveAll(tmpDir)

	expectedNumChains := 12
	complexUserConfig := config.UserConfig{
		Replication: config.PluralReplicationMode(),
		UserScopes: map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default", "ReplChain", "Third", "Fourth", "Fifth"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ReplChain", "Default"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"First", "Second"},
		},
	}

	// create root dir
	config.EnsureRoot(tmpDir)
	config.EnsureRootMultiplex(tmpDir, &config.BaseConfig{
		UserConfig: complexUserConfig,
	})

	// use provider
	multiplexFS, err := MultiplexFSProvider(&config.Config{
		BaseConfig: config.BaseConfig{
			RootDir:    tmpDir,
			UserConfig: complexUserConfig,
		},
	})
	require.NoError(err)
	assert.NotEmpty(t, multiplexFS, "the filesystem multiplex should not be empty")
	assert.Equal(t, expectedNumChains, len(multiplexFS))
}

func ensureFiles(t *testing.T, rootDir string, files ...string) {
	t.Helper()
	for _, f := range files {
		p := filepath.Join(rootDir, f)
		_, err := os.Stat(p)
		require.NoError(t, err, p)
	}
}

func assertValidConfig(t *testing.T, configFile string) {
	t.Helper()
	// list of words we expect in the config
	elems := []string{
		"moniker",
		"seeds",
		"proxy_app",
		"create_empty_blocks",
		"peer",
		"timeout",
		"broadcast",
		"send",
		"addr",
		"wal",
		"propose",
		"max",
		"genesis",
	}
	for _, e := range elems {
		assert.Contains(t, configFile, e)
	}
}
