package runtime_test

import (
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	cmtlog "github.com/ice-blockchain/cometbft/libs/log"
	"github.com/ice-blockchain/cometbft/multiplex/runtime"
)

// ----------------------------------------------------------------------------
// Unit Tests

type mockService struct {
	Name string
}

func TestMultiplexRuntimeResourceManagerSet(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())

	// Test simplet set.
	shouldNotErr1 := resourceMgr.Set("test-chain-0", "resource-1", &mockService{
		Name: "resource-0-1",
	})
	assert.NoError(t, shouldNotErr1)

	// Test same resource for different ChainID.
	shouldNotErr2 := resourceMgr.Set("test-chain-1", "resource-1", &mockService{
		Name: "resource-1-1",
	})
	assert.NoError(t, shouldNotErr2)

	// Test difference in two instances (see Name).
	got1 := resourceMgr.Get("test-chain-0", "resource-1").(*mockService)
	got2 := resourceMgr.Get("test-chain-1", "resource-1").(*mockService)
	require.NotNil(t, got1)
	require.NotNil(t, got2)
	assert.NotEqual(t, got1.Name, got2.Name)

	// Test overwrite of service per ChainID.
	overwriteVal := "resource-0-1-overwrite"
	shouldNotErr3 := resourceMgr.Set("test-chain-0", "resource-1", &mockService{
		Name: overwriteVal,
	})
	assert.NoError(t, shouldNotErr3)

	// Test that the overwrite was successful.
	got3 := resourceMgr.Get("test-chain-0", "resource-1").(*mockService)
	require.NotNil(t, got3)
	assert.NotEqual(t, got1.Name, got3.Name)
	assert.Equal(t, overwriteVal, got3.Name)

	// Test concurrent Set calls, must succeed.
	waitAll := sync.WaitGroup{}
	waitAll.Add(100)
	expectedNamesByChain := map[string][]string{}
	for i := 0; i < 100; i++ {
		withChainID := "test-chain-0"
		withValue := "test-resource-" + strconv.Itoa(i)
		if i%2 == 0 {
			withChainID = "test-chain-1"
		}
		expectedNamesByChain[withChainID] = append(expectedNamesByChain[withChainID], withValue)

		go func(testChainID, testValue string) {
			defer waitAll.Done()

			shouldNotErr := resourceMgr.Set(testChainID, testValue, &mockService{
				Name: testValue,
			})
			assert.NoError(t, shouldNotErr)
		}(withChainID, withValue)
	}
	waitAll.Wait()

	// Test that all services got set.
	for testChainID, expectedNames := range expectedNamesByChain {
		for _, expectedName := range expectedNames {
			actualResource := resourceMgr.Get(testChainID, expectedName)
			require.NotNil(t, actualResource)

			actualService, shouldCast := actualResource.(*mockService)
			assert.Equal(t, true, shouldCast)
			require.NotNil(t, actualService)

			assert.Equal(t, expectedName, actualService.Name)
		}
	}
}

func TestMultiplexRuntimeResourceManagerGet(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())

	// Test empty get.
	emptyResource := resourceMgr.Get("test-chain-0", "resource-1")
	assert.Nil(t, emptyResource)

	// Test simple set/get.
	err := resourceMgr.Set("test-chain-0", "resource-1", &mockService{
		Name: "resource-0-1",
	})
	require.NoError(t, err)

	actualResource := resourceMgr.Get("test-chain-0", "resource-1")
	require.NotNil(t, actualResource)

	actualService, shouldCast := actualResource.(*mockService)
	assert.Equal(t, true, shouldCast)
	require.NotNil(t, actualService)
	assert.Equal(t, "resource-0-1", actualService.Name)

	// Fill resources map with expected data.
	expectedNamesByChain := map[string][]string{}
	for i := 0; i < 100; i++ {
		withChainID := "test-chain-0"
		withValue := "test-resource-" + strconv.Itoa(i)
		if i%2 == 0 {
			withChainID = "test-chain-1"
		}
		expectedNamesByChain[withChainID] = append(expectedNamesByChain[withChainID], withValue)

		mustNotErr := resourceMgr.Set(withChainID, withValue, &mockService{
			Name: withValue,
		})
		require.NoError(t, mustNotErr)
	}

	// Test concurrent Get calls, must succeed.
	waitAll := sync.WaitGroup{}
	var actualNumNames atomic.Uint64
	for withChainID, withNames := range expectedNamesByChain {
		waitAll.Add(len(withNames))
		for _, withName := range withNames {
			go func(testChainID, testValue string) {
				defer waitAll.Done()

				actualResource := resourceMgr.Get(testChainID, testValue)
				require.NotNil(t, actualResource)

				actualService, shouldCast := actualResource.(*mockService)
				assert.Equal(t, true, shouldCast)
				require.NotNil(t, actualService)
				assert.Equal(t, testValue, actualService.Name)

				actualNumNames.Add(1)
			}(withChainID, withName)
		}
	}
	waitAll.Wait()

	assert.Equal(t, uint64(100), actualNumNames.Load())
}

func TestMultiplexRuntimeResourceManagerHas(t *testing.T) {
	defer goleak.VerifyNone(t)

	resourceMgr := runtime.NewResourceManager(t.Context(), cmtlog.NewNopLogger())

	// Test empty Has.
	shouldBeFalse := resourceMgr.Has("test-chain-0", "resource-1")
	assert.Equal(t, false, shouldBeFalse)

	// Test simple Has.
	err := resourceMgr.Set("test-chain-0", "resource-1", &mockService{})
	require.NoError(t, err)

	actualResult := resourceMgr.Has("test-chain-0", "resource-1")
	assert.Equal(t, true, actualResult)

	// Fill resources map with expected data.
	expectedNamesByChain := map[string][]string{}
	for i := 0; i < 100; i++ {
		withChainID := "test-chain-0"
		withValue := "test-resource-" + strconv.Itoa(i)
		if i%2 == 0 {
			withChainID = "test-chain-1"
		}
		expectedNamesByChain[withChainID] = append(expectedNamesByChain[withChainID], withValue)

		mustNotErr := resourceMgr.Set(withChainID, withValue, &mockService{})
		require.NoError(t, mustNotErr)
	}

	// Test concurrent Has calls, must succeed.
	waitAll := sync.WaitGroup{}
	var actualNumNames atomic.Uint64
	for withChainID, withNames := range expectedNamesByChain {
		waitAll.Add(len(withNames))
		for _, withName := range withNames {
			go func(testChainID, testValue string) {
				defer waitAll.Done()

				actualResult := resourceMgr.Has(testChainID, testValue)
				assert.Equal(t, true, actualResult)

				actualNumNames.Add(1)
			}(withChainID, withName)
		}
	}
	waitAll.Wait()

	assert.Equal(t, uint64(100), actualNumNames.Load())
}
