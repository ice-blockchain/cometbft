package server_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ice-blockchain/cometbft/multiplex/server"
	"github.com/ice-blockchain/cometbft/p2p"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexServerNewRelayAddress(t *testing.T) {
	// Test 1: fully qualified relay address
	addr1, err1 := server.NewRelayAddress("tcp://0.0.0.0:1234")
	assert.NoError(t, err1)
	assert.NotNil(t, addr1)

	assert.Equal(t, p2p.ID(""), addr1.ID())
	assert.Equal(t, "tcp://", addr1.Scheme())
	assert.Equal(t, uint16(1234), addr1.Port())
	assert.Equal(t, "0.0.0.0", addr1.Host())

	expectedFQRA1 := "tcp://0.0.0.0:1234"
	assert.Equal(t, expectedFQRA1, addr1.String())

	// Test 2: omiting scheme uses tcp://
	addr2, err2 := server.NewRelayAddress("0.0.0.0:1234")
	assert.NoError(t, err2)
	assert.NotNil(t, addr2)
	assert.Equal(t, "tcp://", addr2.Scheme())
	assert.Equal(t, addr1.Host(), addr2.Host())
	assert.Equal(t, addr1.Port(), addr2.Port())

	expectedFQRA2 := expectedFQRA1
	assert.Equal(t, expectedFQRA2, addr2.String())

	// Test 3: explicit scheme is used
	addr3, err3 := server.NewRelayAddress("wss://0.0.0.0:1234")
	assert.NoError(t, err3)
	assert.NotNil(t, addr3)
	assert.Equal(t, "wss://", addr3.Scheme())
	assert.Equal(t, addr1.Host(), addr3.Host())
	assert.Equal(t, addr1.Port(), addr3.Port())

	expectedFQRA3 := "wss://0.0.0.0:1234"
	assert.Equal(t, expectedFQRA3, addr3.String())

	// Test 4: providing relay ID is detected
	addr4, err4 := server.NewRelayAddress("wss://fake-id@0.0.0.0:1234")
	assert.NoError(t, err4)
	assert.NotNil(t, addr4)
	assert.Equal(t, "wss://", addr4.Scheme())
	assert.Equal(t, p2p.ID("fake-id"), addr4.ID())
	assert.Equal(t, addr1.Host(), addr4.Host())
	assert.Equal(t, addr1.Port(), addr4.Port())

	expectedFQRA4 := "wss://fake-id@0.0.0.0:1234"
	assert.Equal(t, expectedFQRA4, addr4.String())
}

func TestMultiplexServerNewRelayAddressErrors(t *testing.T) {
	// Test 1: invalid formats
	failAddresses1 := []string{
		"",
		"0.0.0.0:-1",
	}
	for _, failAddr := range failAddresses1 {
		_, err1 := server.NewRelayAddress(failAddr)
		assert.Error(t, err1)
		assert.Contains(t, err1.Error(), "invalid format")
	}

	// Test 2: empty hostname
	failAddresses2 := []string{
		"tcp://:1234",
		"tcp://fake-id@:1234",
	}
	for _, failAddr := range failAddresses2 {
		_, err2 := server.NewRelayAddress(failAddr)
		assert.Error(t, err2)
		assert.Contains(t, err2.Error(), "found empty hostname")
	}

	// Test 3: invalid port
	failAddresses3 := []string{
		"tcp://0.0.0.0:100000",
		"tcp://fake-id@0.0.0.0:150000",
	}
	for _, failAddr := range failAddresses3 {
		_, err3 := server.NewRelayAddress(failAddr)
		assert.Error(t, err3)
		assert.Contains(t, err3.Error(), "invalid port number")
	}
}

func TestMultiplexServerRelayAddressString(t *testing.T) {
	// Test 1: fully qualified relay address
	addr1, err1 := server.NewRelayAddress("tcp://fake-id@0.0.0.0:1234")
	require.NoError(t, err1)
	require.NotNil(t, addr1)

	actual1 := addr1.String()
	expectedFQRA1 := "tcp://fake-id@0.0.0.0:1234"
	assert.Equal(t, expectedFQRA1, actual1)

	// Test 2: skips ID given empty
	addr2, err2 := server.NewRelayAddress("tcp://0.0.0.0:1234")
	require.NoError(t, err2)
	require.NotNil(t, addr2)

	actual2 := addr2.String()
	expectedFQRA2 := "tcp://0.0.0.0:1234"
	assert.Equal(t, expectedFQRA2, actual2)
}

func TestMultiplexServerRelayAddressAddresses(t *testing.T) {
	// Test 1: fully qualified relay address
	addr1, err1 := server.NewRelayAddress("tcp://fake-id@0.0.0.0:1234")
	require.NoError(t, err1)
	require.NotNil(t, addr1)

	expectedAddresses := []string{
		"tcp://fake-id@0.0.0.0:1234", // p2p discovery
		"tcp://fake-id@0.0.0.0:1233", // rpc discovery (-1)
		"tcp://fake-id@0.0.0.0:1235", // p2p cometbft  (+1)
		"tcp://fake-id@0.0.0.0:1236", // rpc cometbft  (+2)
	}

	actualAddresses1 := addr1.Addresses()
	assert.Equal(t, expectedAddresses, actualAddresses1)
}
