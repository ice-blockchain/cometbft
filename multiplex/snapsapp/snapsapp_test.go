package snapsapp_test

import (
	"encoding/hex"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/config"
	"github.com/ice-blockchain/cometbft/crypto/ed25519"
	"github.com/ice-blockchain/cometbft/crypto/tmhash"
	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/snapsapp"
	"github.com/ice-blockchain/cometbft/node"
	"github.com/ice-blockchain/cometbft/p2p"
	sm "github.com/ice-blockchain/cometbft/state"
	"github.com/ice-blockchain/cometbft/types"
	cmttime "github.com/ice-blockchain/cometbft/types/time"
)

type (
	SnapsAppSuite struct {
		snapsApp *snapsapp.SnapsApp
		reactor  *mx.Reactor
		logger   cmtlog.Logger
		rootDir  string
	}
)

const (
	baseExampleChainID = "mx-chain-CC8E6555A3F401FF61DA098F94D325E7041BC43A-"
)

// mockGenesisDocSetProviderFunc mocks a GenesisDocSet provider helper.
func mockGenesisDocSetProviderFunc(withChainID string) node.GenesisDocProvider {
	return func() (node.IChecksummedGenesisDoc, error) {
		// random validators, careful with this provider.
		valPubKey := ed25519.GenPrivKey().PubKey()
		return &mx.ChecksummedGenesisDocSet{
			GenesisDocs: mx.GenesisDocSet{
				types.GenesisDoc{
					GenesisTime:   cmttime.Now(),
					ChainID:       withChainID,
					InitialHeight: 1000,
					Validators: []types.GenesisValidator{{
						Address: valPubKey.Address(),
						PubKey:  valPubKey,
						Power:   10,
						Name:    "myval",
					}},
					ConsensusParams: types.DefaultConsensusParams(),
					AppHash:         []byte{1, 2, 3},
					AppState:        []byte(`{"account_owner":"Bob"}`),
				},
			},
			Sha256Checksum: []byte{1, 2, 3},
		}, nil
	}
}

func NewSnapsAppSuite(t *testing.T, opts ...func(*snapsapp.SnapsApp)) *SnapsAppSuite {
	t.Helper()

	rootDir, _, testReactor := prepareMultiplexReactor(t)

	logger := cmtlog.NewNopLogger() // for debug change to TestingLogger()
	app := snapsapp.NewSnapsApplication(
		testReactor,
		config.NewSnapshotOptions(1, 1, 1),
		logger,
		opts...,
	)

	return &SnapsAppSuite{
		snapsApp: app,
		reactor:  testReactor,
		logger:   logger,
		rootDir:  rootDir,
	}
}

func prepareMultiplexReactor(t *testing.T) (
	string,
	*config.Config,
	*mx.Reactor,
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
		map[string]*config.StateSyncConfig{},
		map[string]string{},
		map[string][]string{"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			testChainID,
		}},
	)
	conf.SetRoot(rootDir)

	conf.SnapshotOptions = map[config.ReplicationStrategy]config.SnapshotOptions{}
	conf.SnapshotOptions[config.NewReplicationStrategy("Network")] = config.NewSnapshotOptions(1, 1000, 3)
	conf.SnapshotOptions[config.NewReplicationStrategy("History")] = config.NewSnapshotOptions(2, 2000, 3)

	nodeKey := makeRandomNodeKey()
	testChainRegistry, err := mx.NewChainRegistry(&conf.MultiplexConfig)
	require.NoError(t, err, "should create chain registry instance")

	// Test Reactor implementation in multiplex package
	testReactor := mx.NewReactor(
		nodeKey,
		conf,
		cmtlog.NewNopLogger(),
		testChainRegistry,
		mockGenesisDocSetProviderFunc(testChainID),
	)

	err = testReactor.Start()
	require.NoError(t, err, "should not error starting multiplex reactor")

	return rootDir, conf, testReactor
}

func makeRandomNodeKey() *p2p.NodeKey {
	priv := ed25519.GenPrivKey()
	return &p2p.NodeKey{PrivKey: priv}
}

func makeState(
	t *testing.T,
	chainID string,
	setHeight int64,
) (*mx.HistoricalState, []byte) {
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

	return &mx.HistoricalState{
		State: &state,
		Data:  []byte{},
	}, state.AppHash
}
