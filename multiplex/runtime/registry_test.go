package runtime_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"

	mx "github.com/ice-blockchain/cometbft/multiplex"
	"github.com/ice-blockchain/cometbft/multiplex/e2e"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

func ResetTestMultiplexRuntimeManager(
	tb testing.TB,
	customLogger cmtlog.Logger,
	options ...runtime.RegistryOption,
) *runtime.Registry {
	testBackend := e2e.ResetTestMultiplexRelays(
		tb, 1,
		customLogger,
		mx.WithRuntimeManagerOptions(
			options...,
		),
	)[0]
	require.NotNil(tb, testBackend, "should create a backend instance")

	testRegistry := testBackend.RuntimeManager().(*runtime.Registry)
	return testRegistry
}

// ----------------------------------------------------------------------------
// Unit Tests

func TestMultiplexRuntimeRegistryNewRegistry(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test default instance
	testReg1 := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())

	assert.NotNil(t, testReg1)
	assert.NotNil(t, testReg1.Runtimes)
	assert.NotNil(t, testReg1.Sleeping)
	assert.NotNil(t, testReg1.Scheduler)
	assert.NotNil(t, testReg1.NumRuntimes())
	assert.NotNil(t, testReg1.NumSleeping())

	// Test instance with options
	testReg2 := ResetTestMultiplexRuntimeManager(t,
		cmtlog.NewNopLogger(),
		runtime.RegistryCleanerInterval(1*time.Second),
	)

	assert.NotNil(t, testReg2)
	assert.Equal(t, 1*time.Second, testReg2.CleanerInterval())
}

func TestMultiplexRuntimeRegistryOnActivate(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())

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

	testReg2 := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())

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

func TestMultiplexRuntimeRegistryOnComplete(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())
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

func TestMultiplexRuntimeRegistryStartStop(t *testing.T) {
	defer goleak.VerifyNone(t)

	testReg1 := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())

	// Act: Test simple registry start/stop
	startErr := testReg1.Start()
	require.NoError(t, startErr, "should start first runtime manager")

	stopErr := testReg1.Stop()
	require.NoError(t, stopErr, "should stop first runtime manager")

	// Act: Test cleaner routine processing
	testReg2 := ResetTestMultiplexRuntimeManager(t,
		cmtlog.NewNopLogger(),
		runtime.RegistryCleanerInterval(1*time.Second),   // run cleaner every sec
		runtime.RegistryIdleDuration(1*time.Millisecond), // 1ms means idle asap
	)

	newStartErr := testReg2.Start()
	require.NoError(t, newStartErr, "should start second runtime manager")

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

	// Test that we have some runtimes sleeping (will be idled)
	actualNumSleeping := testReg2.NumSleeping()
	assert.Equal(t, uint64(2), actualNumSleeping)

	// 3 seconds should be plenty to wait for full cleaner processing.
	time.Sleep(3 * time.Second)

	newStopErr := testReg2.Stop()
	require.NoError(t, newStopErr, "should stop second runtime manager")

	actualRuntimes := testReg2.ActiveRuntimes()
	actualSleeping := testReg2.SleepingRuntimes()
	actualNumRuntimes := testReg2.NumRuntimes()
	actualNumSleeping = testReg2.NumSleeping()

	// Test that NumSleeping is now 0, i.e. sleeping runtimes were idled.
	assert.Empty(t, actualSleeping)
	assert.Empty(t, actualRuntimes)
	assert.Equal(t, uint64(0), actualNumRuntimes)
	assert.Equal(t, uint64(0), actualNumSleeping)
}

func TestMultiplexRuntimeRegistryReset(t *testing.T) {
	defer goleak.VerifyNone(t)

	// Test simple registry start/stop
	reg := ResetTestMultiplexRuntimeManager(t, cmtlog.NewNopLogger())

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
	resetErr := reg.Reset(t.Context())
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
