package client_test

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	abci "github.com/ice-blockchain/cometbft/abci/types"
	v1 "github.com/ice-blockchain/cometbft/api/cometbft/types/v1"
	cmtjson "github.com/ice-blockchain/cometbft/libs/json"
	"github.com/ice-blockchain/cometbft/multiplex/client"
)

const testValidatorJSON = `{
	"pub_key": {
		"type": "tendermint/PubKeyEd25519",
		"value":"AT/+aaL1eB0477Mud9JMm8Sh8BIvOYlPGC9KkIUmFaE="
	},
	"power": "10",
	"name": "test-validator"
}`

func TestMultiplexClientReportValidatorUpdate(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()

	// unmarshal a test validator
	testValidator := abci.ValidatorUpdate{}
	err := cmtjson.Unmarshal([]byte(testValidatorJSON), &testValidator)
	require.NoError(t, err, "should unmarshal a test validator from JSON")

	// Prepare a validators update set
	baseValidators := []abci.ValidatorUpdate{testValidator}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errReportValidatorUpdate error //nolint:gosimple
	errReportValidatorUpdate = client.ReportValidatorUpdate(
		testChainID,
		baseValidators,
		mockValidatorUpdateExtensionCountAsError, // default_test.go
	)

	// The extension should have formatted the validator set as an Error
	expectedMessage := fmt.Sprintf("Count validator updates: %d", len(baseValidators))
	assert.Equal(t, expectedMessage, errReportValidatorUpdate.Error())
}

func TestMultiplexClientReportConsensusUpdate(t *testing.T) {
	// address and fingerprint added to ChainID
	testChainID := makeRandomTestChainID()

	// Prepare a consensus params instance
	expectedMaxBytes := 123
	baseConsensusParams := &v1.ConsensusParams{
		Block: &v1.BlockParams{
			MaxBytes: int64(expectedMaxBytes),
		},
	}

	// Execute the extension / injection, we intentionally force the type
	// here to prevent compilation for extensions that wouldn't work correctly.
	var errReportConsensusUpdate error //nolint:gosimple
	errReportConsensusUpdate = client.ReportConsensusUpdate(
		testChainID,
		baseConsensusParams,
		mockConsensusUpdateExtensionMaxBytesAsError, // default_test.go
	)

	// The extension should have formatted the validator set as an Error
	expectedMessage := fmt.Sprintf("Max bytes: %v", expectedMaxBytes)
	assert.Equal(t, expectedMessage, errReportConsensusUpdate.Error())
}
