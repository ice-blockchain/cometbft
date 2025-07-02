package runtime_test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexServerRuntimeRegistryNewRuntimeRegistry(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test default instance
	testReg1 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())
	assert.NotNil(t, testReg1)
	assert.NotNil(t, testReg1.Runtimes)
	assert.NotNil(t, testReg1.Sleeping)
	assert.NotNil(t, testReg1.Scheduler)
	assert.NotNil(t, testReg1.NumRuntimes())
	assert.NotNil(t, testReg1.NumSleeping())

	// Test instance with options
	testReg2 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger(),
		runtime.RuntimeRegistryCleanerInterval(1*time.Second),
		runtime.RuntimeRegistryOnIdle(func(chainID string) error {
			return nil
		}),
	)
	assert.NotNil(t, testReg2)
	assert.Equal(t, 1*time.Second, testReg2.CleanerInterval())
	assert.NotNil(t, testReg2.OnIdle)
}

func TestMultiplexServerRuntimeRegistryOnActivate(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())

	// Act: runtime activation should add to Runtimes
	err := testReg1.OnActivate("test-chain-1")
	actualRuntimes := testReg1.ActiveRuntimes()
	actualSleeping := testReg1.SleepingRuntimes()
	actualNumRuntimes := testReg1.NumRuntimes()
	actualNumSleeping := testReg1.NumSleeping()

	assert.NoError(t, err,
		"should not error upon activating runtime")
	assert.Empty(t, actualSleeping)
	assert.NotEmpty(t, actualRuntimes)
	assert.Contains(t, actualRuntimes, "test-chain-1")
	assert.Equal(t, uint64(1), actualNumRuntimes)
	assert.Equal(t, uint64(0), actualNumSleeping)

	// Act: next activation (of same runtime) should increment numr
	nextErr := testReg1.OnActivate("test-chain-1")
	assert.NoError(t, nextErr,
		"should not error upon reactivating runtime")

	otherErr := testReg1.OnActivate("test-chain-2")
	assert.NoError(t, otherErr,
		"should not error upon activating other runtime")

	nextActualNumRuntimes := testReg1.NumRuntimes()
	assert.Equal(t, uint64(3), nextActualNumRuntimes) // 2xtest-chain-1 + test-chain-2

	testReg2 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())

	// Act: concurrent OnActivate calls must succeed
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			testRuntime := "test-chain-1"
			if c%2 == 0 {
				testRuntime = "test-chain-2"
			}
			testReg2.OnActivate(testRuntime)
		}(i + 1)
	}
	waitAll.Wait()

	actualRuntimes = testReg2.ActiveRuntimes()
	actualNumRuntimes = testReg2.NumRuntimes()
	assert.Equal(t, uint64(100), actualNumRuntimes)
	assert.NotEmpty(t, actualRuntimes)
	assert.Contains(t, actualRuntimes, "test-chain-1")
	assert.Contains(t, actualRuntimes, "test-chain-2")
	assert.Equal(t, uint64(50), actualRuntimes["test-chain-1"])
	assert.Equal(t, uint64(50), actualRuntimes["test-chain-2"])
}

func TestMultiplexServerRuntimeRegistryOnComplete(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())
	activateErr := testReg1.OnActivate("test-chain-1")
	require.NoError(t, activateErr)

	// Act: runtime completion should remove from Runtimes and add to Sleeping
	completeErr := testReg1.OnComplete("test-chain-1")
	assert.NoError(t, completeErr, "should not error upon completing runtime")
	actualRuntimes := testReg1.ActiveRuntimes()
	actualSleeping := testReg1.SleepingRuntimes()
	actualNumRuntimes := testReg1.NumRuntimes()
	actualNumSleeping := testReg1.NumSleeping()

	assert.NotEmpty(t, actualSleeping)
	assert.Empty(t, actualRuntimes)
	assert.Equal(t, uint64(0), actualNumRuntimes)
	assert.Equal(t, uint64(1), actualNumSleeping)
}

func TestMultiplexServerRuntimeRegistryStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())

	// Act: Test simple registry start/stop
	startErr := testReg1.Start()
	assert.NoError(t, startErr)

	stopErr := testReg1.Stop()
	assert.NoError(t, stopErr)

	// Act: Test cleaner routine processing
	var OnIdleCalls atomic.Uint64
	testReg2 := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger(),
		runtime.RuntimeRegistryCleanerInterval(1*time.Second),   // run cleaner every sec
		runtime.RuntimeRegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
		runtime.RuntimeRegistryOnIdle(func(chainID string) error {
			OnIdleCalls.Add(1)
			return nil
		}),
	)

	newStartErr := testReg2.Start()
	assert.NoError(t, newStartErr)

	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			testRuntime := "test-chain-1"
			if c%2 == 0 {
				testRuntime = "test-chain-2"
			}

			testReg2.OnActivate(testRuntime)
			select {
			case <-time.After(100 * time.Millisecond):
				testReg2.OnComplete(testRuntime)
			}
		}(i + 1)
	}
	waitAll.Wait()

	// 3 seconds should be plenty to wait for full cleaner processing.
	time.Sleep(3 * time.Second)

	newStopErr := testReg2.Stop()
	assert.NoError(t, newStopErr)

	actualRuntimes := testReg1.ActiveRuntimes()
	actualSleeping := testReg1.SleepingRuntimes()
	actualNumRuntimes := testReg1.NumRuntimes()
	actualNumSleeping := testReg1.NumSleeping()

	assert.Empty(t, actualSleeping)
	assert.Empty(t, actualRuntimes)
	assert.Equal(t, uint64(0), actualNumRuntimes)
	assert.Equal(t, uint64(0), actualNumSleeping)

	expectedNumIdleCalls := uint64(2) // test-chain-1 & test-chain-2
	assert.Equal(t, expectedNumIdleCalls, OnIdleCalls.Load())
}

func TestMultiplexServerRuntimeRegistryReset(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test simple registry start/stop
	reg := runtime.NewRuntimeRegistry(t.Context(), cmtlog.NewNopLogger())

	startErr := reg.Start()
	assert.NoError(t, startErr)

	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	for i := 0; i < 100; i++ {
		go func(c int) {
			defer waitAll.Done()

			testRuntime := "test-chain-1"
			if c%2 == 0 {
				testRuntime = "test-chain-2"
			}
			reg.OnActivate(testRuntime)
		}(i + 1)
	}
	waitAll.Wait()

	actualRuntimes := reg.ActiveRuntimes()
	actualNumRuntimes := reg.NumRuntimes()
	require.Equal(t, uint64(100), actualNumRuntimes)
	require.NotEmpty(t, actualRuntimes)

	stopErr := reg.Stop()
	require.NoError(t, stopErr)

	// Act: reset should empty the runtimes storage
	resetErr := reg.Reset()
	assert.NoError(t, resetErr)

	defer func() {
		// Close quit channel
		_ = reg.Stop()
	}()

	actualRuntimes = reg.ActiveRuntimes()
	actualNumRuntimes = reg.NumRuntimes()
	assert.Empty(t, actualRuntimes)
	assert.Equal(t, uint64(0), actualNumRuntimes)
	assert.Equal(t, false, reg.IsRunning())
}
