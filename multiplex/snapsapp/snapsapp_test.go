package snapsapp_test

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/client"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
)

type (
	SnapsAppSuite struct {
		snapsApp *snapsapp.SnapsApp
		backend  *mx.MultiplexBackend
		logger   cmtlog.Logger
		rootDir  string
	}
)

const (
	baseExampleChainID = "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-"
)

func NewSnapsAppSuite(t *testing.T, opts ...func(*snapsapp.SnapsApp)) *SnapsAppSuite {
	t.Helper()

	snapLogger := cmtlog.NewNopLogger() // for debug of SnapsApp change to TestingLogger()
	nodeLogger := cmtlog.NewNopLogger() // for debug of Backend change to TestingLogger()

	rootDir, testBackend := prepareMultiplexBackend(t, nodeLogger)

	app := snapsapp.NewSnapsApplication(
		testBackend,
		snapLogger,
		opts...,
	)

	return &SnapsAppSuite{
		snapsApp: app,
		backend:  testBackend,
		logger:   snapLogger,
		rootDir:  rootDir,
	}
}

// Creates a MultiplexBackend and initializes a testChainID.
func prepareMultiplexBackend(t *testing.T, withLogger cmtlog.Logger) (
	string,
	*mx.MultiplexBackend,
) {
	t.Helper()

	rootDir, err := os.MkdirTemp("", t.Name())
	if err != nil {
		panic(err)
	}

	// Create a custom chain id for each iteration (based on test name)
	// This should be random enough to produce non-repeating values
	testChainID := baseExampleChainID + strings.ToUpper(hex.EncodeToString(
		tmhash.Sum([]byte(t.Name()))[:8], // 8 bytes only
	))

	conf := config.TestConfig()
	conf.BaseConfig = config.MultiplexTestBaseConfig(
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			testChainID,
		}},
	)
	conf.SetRoot(rootDir)

	testBackend, err := mx.NewServer(
		t.Context(),
		&client.DefaultAcceptor{},
		conf,
		withLogger,
	)
	require.NoError(t, err, "should create a server instance")

	startErr := testBackend.Start()
	require.NoError(t, startErr, "should start a server instance")

	initErr := testBackend.RuntimeManager().InitRuntime(testChainID, []string{})
	require.NoError(t, initErr, "should initialize test network")

	runtimeErr := testBackend.RuntimeManager().StartRuntime(testChainID)
	require.NoError(t, runtimeErr, "should start network runtime")

	return rootDir, testBackend
}

func makeState(
	t *testing.T,
	chainID string,
	setHeight int64,
) (sm.State, []byte) {
	t.Helper()

	valPubKey := ed25519.GenPrivKey().PubKey()
	fakeAppHash := []byte{1, 2, 3}

	state, err := sm.MakeGenesisState(&types.GenesisDoc{
		GenesisTime:   cmttime.Now(),
		ChainID:       chainID,
		InitialHeight: 1000,
		Validators: []types.GenesisValidator{{
			Address: valPubKey.Address(),
			PubKey:  valPubKey,
			Power:   10,
			Name:    "myval",
		}},
		ConsensusParams: types.DefaultConsensusParams(),
		AppHash:         fakeAppHash,
		AppState:        []byte(`{"account_owner":"Alice"}`),
	})
	require.NoError(t, err, "should not error creating state machine")

	state.LastBlockHeight = setHeight
	state.LastBlockID = types.BlockID{}
	state.LastBlockTime = cmttime.Now()
	state.LastValidators = state.Validators

	return state, state.AppHash
}

// closeAndRemoveAll is a helper to shutdown a running [mx.MultiplexBackend] and
// remove all filesystem resources created under rootDir.
func closeAndRemoveAll(
	tb testing.TB,
	rootDir string,
	backend *mx.MultiplexBackend,
) {
	tb.Helper()

	defer os.RemoveAll(rootDir)

	if backend.IsRunning() {
		err := backend.Stop()
		assert.NoError(tb, err, "should shutdown backend gracefully")
	}
}

// shutdownBackends stops all backends concurrently.
func shutdownBackends(
	tb testing.TB,
	backends ...*mx.MultiplexBackend,
) {
	tb.Helper()

	for i := 0; i < len(backends); i++ {
		backend := backends[i]
		rootDir := backend.Config().RootDir
		go closeAndRemoveAll(tb, rootDir, backend)
	}
}
