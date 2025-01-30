package multiplex_test

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ice-blockchain/cometbft/config"
	cmtos "github.com/ice-blockchain/cometbft/internal/os"
	mx "github.com/ice-blockchain/cometbft/multiplex"
)

func ResetMultiplexFSTestRoot(t *testing.T, testName string) (string, *config.Config) {
	t.Helper()

	// create a unique, concurrency-safe test directory under os.TempDir()
	rootDir, err := os.MkdirTemp("", testName)
	if err != nil {
		panic(err)
	}

	conf := config.TestConfig()
	conf.SetRoot(rootDir)
	return rootDir, conf
}

func TestMultiplexFSDisabled(t *testing.T) {
	rootDir, conf := ResetMultiplexFSTestRoot(t, "test-mx-fs-disabled")
	defer os.RemoveAll(rootDir)

	multiplex, err := mx.NewMultiplexFS(conf)
	assert.NoError(t, err)
	assert.Len(t, multiplex, 1) // default disables multiplex

	// Sets empty string key to default data dir
	assert.Equal(t, config.DefaultDataDir, multiplex[""])
}

func TestMultiplexFSNewMultiplexFS(t *testing.T) {
	rootDir, conf := ResetMultiplexFSTestRoot(t, "test-mx-fs-new-multiplex-fs")
	defer os.RemoveAll(rootDir)

	// ----------------
	// Errors
	failCases := []string{
		"invalid-chain-id",
		"cosmoshub-4",
		"mx-chain-1234-5678",
		// invalid addresses
		"mx-chain-#000000000000000000000000000000000000000-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC4-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43AAB-1A63C0E60122F9BB",
		// invalid fingerprints
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-#000000000000000",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BBCC",
	}

	for _, failCaseChainID := range failCases {
		conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {failCaseChainID},
		})
		conf.SetRoot(rootDir)

		_, err := mx.NewMultiplexFS(conf)
		assert.Error(t, err, "should forward error given invalid configuration")
	}

	// ----------------
	// Successes
	exampleChains := []string{
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-1A63C0E60122F9BB",
		"mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-D1ED2B487F2E93CC",
		"mx-chain-FF1410CEEB411E55487701C4FEE65AACE7115DC0-79F77E672C1DB0BC",
	}

	conf.BaseConfig = config.MultiplexTestBaseConfig(map[string]string{}, map[string][]string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {exampleChains[0], exampleChains[1]},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {exampleChains[2]},
	})
	conf.SetRoot(rootDir)

	multiplex, err := mx.NewMultiplexFS(conf)

	assert.NoError(t, err, "should not error given valid configuration")
	assert.Len(t, multiplex, len(exampleChains), "should create correct number of paths")
	assert.Contains(t, multiplex, exampleChains[0])
	assert.Contains(t, multiplex, exampleChains[1])
	assert.Contains(t, multiplex, exampleChains[2])
	assert.NotEmpty(t, multiplex[exampleChains[0]])
	assert.NotEmpty(t, multiplex[exampleChains[1]])
	assert.NotEmpty(t, multiplex[exampleChains[2]])
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[0]]))
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[1]]))
	assert.Equal(t, true, cmtos.FileExists(multiplex[exampleChains[2]]))
}
