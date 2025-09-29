package runtime_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeBroadcastPoolStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())
	testPool := runtime.NewBroadcastManager(t.Context(), resourceMgr, cmtlog.NewNopLogger())

	// Test simple pool start/stop
	startErr := testPool.Start()
	require.NoError(t, startErr, "should start first broadcast pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	stopErr := testPool.Stop()
	require.NoError(t, stopErr, "should stop first broadcast pool")
	assert.Equal(t, true, testPool.IsStopped())

	resetErr := testPool.Reset(context.TODO()) // new context!
	require.NoError(t, resetErr, "should reset first broadcast pool")
	assert.Equal(t, false, testPool.IsRunning())

	restartErr := testPool.Start()
	require.NoError(t, restartErr, "should restart first broadcast pool")
	assert.Equal(t, true, testPool.IsStarted())
	assert.Equal(t, true, testPool.IsRunning())

	actualPartners := testPool.Partners("test-hash-1")
	actualResponses := testPool.Responses("test-hash-1")
	assert.Len(t, actualPartners, 0)
	assert.Len(t, actualResponses, 0)

	restopErr := testPool.Stop()
	require.NoError(t, restopErr, "should stop first broadcast pool")
}

func TestMultiplexRuntimeBroadcastPoolInit(t *testing.T) {

}

func TestMultiplexRuntimeBroadcastPoolProcess(t *testing.T) {

}

func TestMultiplexRuntimeBroadcastPoolProcessWaitAccepted(t *testing.T) {

}

func TestMultiplexRuntimeBroadcastPoolWaitIndexed(t *testing.T) {

}
