package multiplex

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	cfg "github.com/cometbft/cometbft/config"
	mxtest "github.com/cometbft/cometbft/multiplex/test"
)

func TestMultiplexScopeIDNewScopeID(t *testing.T) {
	scopeId := NewScopeID(mxtest.TestUserAddress, mxtest.TestScope)
	assert.NotEmpty(t, scopeId.String(), "scopeID data should not be empty")

	expectedDesc := fmt.Sprintf("%s:%s", mxtest.TestUserAddress, mxtest.TestScope)
	assert.Equal(t, expectedDesc, scopeId.String())
}

func TestMultiplexScopeIDNewScopeIDFromHash(t *testing.T) {
	scopeId := NewScopeIDFromHash(mxtest.TestScopeHash)
	assert.NotEmpty(t, scopeId.ScopeHash, "scopeID hash should not be empty")

	expectedHash := mxtest.TestScopeHash
	assert.Equal(t, expectedHash, scopeId.ScopeHash)
}

func TestMultiplexScopeIDNewScopeIDFromDesc(t *testing.T) {
	scopeDesc := fmt.Sprintf("%s:%s", mxtest.TestUserAddress, mxtest.TestScope)
	scopeId := NewScopeIDFromDesc(scopeDesc)
	assert.NotEmpty(t, scopeId.String(), "scopeID data should not be empty")
}

func TestMultiplexScopeIDTestScopeHash(t *testing.T) {
	scopeId := NewScopeID(mxtest.TestUserAddress, mxtest.TestScope)
	actualHash := scopeId.Hash()

	assert.NotEmpty(t, actualHash, "scope hash should not be empty")
	assert.Equal(t, mxtest.TestScopeHash, actualHash)
}

func TestMultiplexScopeIDTestFingerprint(t *testing.T) {
	scopeId := NewScopeID(mxtest.TestUserAddress, mxtest.TestScope)
	actualHash := scopeId.Hash()
	require.NotEmpty(t, actualHash, "scope hash should not be empty")

	actualFP := scopeId.Fingerprint()
	require.NotEmpty(t, actualHash, "fingerprint should not be empty")
	assert.Len(t, actualFP, fingerprintSize*2)
	assert.Equal(t, actualFP, mxtest.TestScopeHash[:fingerprintSize*2])
}

func TestMultiplexScopeRegistryGetScopeHashesByUser(t *testing.T) {
	actualResult, err := testScopeRegistry.GetScopeHashesByUser(mxtest.TestUserAddress)
	assert.NoError(t, err)
	assert.NotEmpty(t, actualResult, "scope slice by user should not be empty")
	assert.Len(t, actualResult, 1)
}

func TestMultiplexScopeRegistryGetScopeHashesByUserErrorNotExist(t *testing.T) {
	_, err := testScopeRegistry.GetScopeHashesByUser("incorrect address")
	assert.Error(t, err)
}

func TestMultiplexScopeRegistryGetScopeHash(t *testing.T) {
	actualResult, err := testScopeRegistry.GetScopeHash(mxtest.TestUserAddress, mxtest.TestScope)
	assert.NoError(t, err)
	assert.NotEmpty(t, actualResult, "scope hash by user and scope should not be empty")
	assert.Equal(t, mxtest.TestScopeHash, actualResult)
}

func TestMultiplexScopeRegistryGetAddress(t *testing.T) {
	actualResult, err := testScopeRegistry.GetAddress(mxtest.TestScopeHash)
	assert.NoError(t, err)
	assert.NotEmpty(t, actualResult, "address by scope hash should not be empty")
	assert.Equal(t, mxtest.TestUserAddress, actualResult)
}

func TestMultiplexScopeRegistryGetScopeHashErrorNotExist(t *testing.T) {
	testCases := map[string]string{
		"incorrect address":                        "incorrect scope",
		"other incorrect address":                  "Default",
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": "incorrect scope",
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": "",
	}

	for failAddress, failScope := range testCases {
		_, err := testScopeRegistry.GetScopeHash(failAddress, failScope)
		assert.Error(t, err)
	}
}

func TestMultiplexScopeDefaultScopeHashProvider(t *testing.T) {
	complexUserConfig := &cfg.UserConfig{
		Replication: cfg.PluralReplicationMode(),
		UserScopes: map[string][]string{
			"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
			"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default", "ReplChain", "Third", "Fourth", "Fifth"},
			"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ReplChain", "Default"},
			"5168FD905426DE2E0DB9990B35075EEC3B184977": {"First", "Second"},
		},
	}

	actualRegistry, err := DefaultScopeHashProvider(complexUserConfig)
	assert.NoError(t, err)
	assert.NotEmpty(t, actualRegistry.ScopeHashes)
	assert.NotEmpty(t, actualRegistry.UsersScopes)
}

func TestMultiplexScopeDefaultScopeHashProviderErrorMinScopes(t *testing.T) {
	// Plural replication must have minimum one scope
	complexUserConfig := &cfg.UserConfig{
		Replication: cfg.PluralReplicationMode(),
		UserScopes:  map[string][]string{},
	}

	_, err := DefaultScopeHashProvider(complexUserConfig)
	assert.Error(t, err)
}

func TestMultiplexScopeHashProviderBuildsRegistry(t *testing.T) {
	testCases := map[string][]string{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {"Default", "Other"},
		"FF1410CEEB411E55487701C4FEE65AACE7115DC0": {"Default", "ReplChain", "Third", "Fourth", "Fifth"},
		"BB2B85FABDAF8469F5A0F10AB3C060DE77D409BB": {"Cosmos", "ReplChain", "Default"},
		"5168FD905426DE2E0DB9990B35075EEC3B184977": {"First", "Second"},
	}

	for testAddress, testScopes := range testCases {
		testCase := map[string][]string{}
		testCase[testAddress] = testScopes

		userConf := &cfg.UserConfig{
			Replication: cfg.PluralReplicationMode(),
			UserScopes:  testCase,
		}

		actualRegistry, err := DefaultScopeHashProvider(userConf)
		assert.NoError(t, err)
		assert.NotEmpty(t, actualRegistry.ScopeHashes)
		assert.NotEmpty(t, actualRegistry.UsersScopes)
		assert.Contains(t, actualRegistry.ScopeHashes, testAddress)
		assert.Contains(t, actualRegistry.UsersScopes, testAddress)

		for _, expectedScope := range testScopes {
			assert.Contains(t, actualRegistry.UsersScopes[testAddress], expectedScope)
		}
	}
}

var testScopeRegistry = ScopeRegistry{
	ScopeHashes: UserScopeSet{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			"1A63C0E60122F9BB3DF9EA61C8184ED59C3936CAD1A3B35FA1D3BCA272E3F38B",
		},
	},
	UsersScopes: UserScopeMap{
		"CC8E6555A3F401FF61DA098F94D325E7041BC43A": {
			"Default": "1A63C0E60122F9BB3DF9EA61C8184ED59C3936CAD1A3B35FA1D3BCA272E3F38B",
		},
	},
}
