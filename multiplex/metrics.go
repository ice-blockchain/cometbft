package multiplex

import (
	"runtime"
	"runtime/metrics"
	"time"

	cmtmetrics "github.com/ice-blockchain/cometbft/libs/metrics"
)

const (
	// MetricsSubsystem is a subsystem shared by all metrics exposed by this
	// package.
	MetricsSubsystem = "multiplex"
)

//go:generate go run ../scripts/metricsgen -struct=Metrics

// Metrics contains metrics exposed by this package.
// see MetricsProvider for descriptions.
type Metrics struct {
	// Total CPU load percentage.
	ProcessorUsage cmtmetrics.Histogram `metrics_bucketsizes:"10, 10, 10" metrics_buckettype:"linear"`

	// Total RAM used in bytes.
	MemoryUsage cmtmetrics.Histogram `metrics_bucketsizes:"1, 3, 7" metrics_buckettype:"exp"`

	// Total bandwidth usage.
	TotalNetworkBytes cmtmetrics.Histogram `metrics_bucketsizes:"1, 3, 7" metrics_buckettype:"exp"`

	// Total number of blocks (across all networks).
	TotalBlocks cmtmetrics.Gauge

	// Total number of blocks by each user.
	// TotalBlocksPerUser cmtmetrics.Gauge `metrics_labels:"user_address"`

	// Total number of transactions (across all networks).
	TotalTxs cmtmetrics.Gauge

	// Total number of transactions per user.
	// TotalTxsPerUser cmtmetrics.Gauge `metrics_labels:"user_address"`

	// The duration of initialization of a relay in seconds.
	InitDurationSeconds cmtmetrics.Histogram `metrics_bucketsizes:"0.0002, 10, 7" metrics_buckettype:"exp"`

	// The duration of the nodes starting process in seconds.
	StartDurationSeconds cmtmetrics.Histogram `metrics_bucketsizes:"0.0002, 10, 8" metrics_buckettype:"exp"`

	// Number of errors caught.
	Errors cmtmetrics.Counter

	// Number of errors caught per user.
	// ErrorsPerUser cmtmetrics.Counter `metrics_labels:"user_address"`
}

// addTimeSample returns a function that, when called, adds an observation to m.
// The observation added to m is the number of seconds elapsed since start.
// addTimeSample is meant to be called in a defer to calculate the amount of
// time a function takes to complete.
func addTimeSample(m cmtmetrics.Histogram, start time.Time) func() {
	return func() { m.Observe(time.Since(start).Seconds()) }
}

// addTimeSampleNow returns a function that, when called, adds an observation to m.
// The observation added to m is the number of seconds elapsed since addTimeSampleNow
// was initially called. addTimeSampleNow is meant to be called in a defer to calculate
// the amount of time a function takes to complete.
//
//nolint:unused
func addTimeSampleNow(m cmtmetrics.Histogram) func() {
	start := time.Now()
	return addTimeSample(m, start)
}

// collectSampleRAM returns a function that, when called, adds an observation to m.
// The observation added to m is the total bytes allocated.
func collectSampleRAM(m cmtmetrics.Histogram) func() {
	return func() {
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)

		m.Observe(float64(mem.TotalAlloc))
	}
}

// collectSampleCPU returns a function that, when called, adds an observation to m.
// The observation added to m is the CPU load percentage.
func collectSampleCPU(m cmtmetrics.Histogram) func() {
	return func() {
		s := []metrics.Sample{
			{Name: "/cpu/classes/total:cpu-seconds"},
			{Name: "/cpu/classes/user:cpu-seconds"},
		}

		metrics.Read(s)

		availTime := ToFloat64(s[0])
		usageTime := ToFloat64(s[1])
		cpuLoadPc := (availTime - usageTime) / availTime

		m.Observe(cpuLoadPc)
	}
}

// collectSampleP2P returns a function that, when called, adds an observation to m.
// The observation added to m is the P2P bytes received and sent.
func collectSampleP2P(
	namespace string,
	ms *Metrics,
	samples []string,
) func() {
	return func() {
		p2p := namespace + "_p2p_"
		p2pSamples := make([]metrics.Sample, len(samples))
		for i, sample := range samples {
			p2pSamples[i] = metrics.Sample{Name: p2p + sample}
		}

		// Collect requested samples
		metrics.Read(p2pSamples)
		p2pValues := make([]float64, len(samples))
		for i := 0; i < len(samples); i++ {
			p2pValues[i] = ToFloat64(p2pSamples[i])
		}

		// Sum of received bytes and sent bytes.
		totalBytes := float64(0)
		for _, value := range p2pValues {
			totalBytes += value
		}

		ms.TotalNetworkBytes.Observe(totalBytes)
	}
}

// collectSampleCometBFT returns a function that, when called, adds an observation to m.
// The observation added to m is the number of CometBFT blocks and transactions.
func collectSampleCometBFT(
	// relayID string,
	// chainID string,
	namespace string,
	ms *Metrics,
) func() {
	return func() {
		// Collecting from blocksync and mempool subsystems
		bs := namespace + "_blocksync_"
		mempl := namespace + "_mempool_"

		cmtSamples := []metrics.Sample{
			{Name: bs + "latest_block_height"},
			{Name: bs + "total_txs"},
			{Name: mempl + "failed_txs"},
			{Name: mempl + "evicted_txs"},
		}

		// Collect requested samples
		metrics.Read(cmtSamples)

		totalBlocks := ToFloat64(cmtSamples[0])
		totalTxes := ToFloat64(cmtSamples[1])
		totalErrs := ToFloat64(cmtSamples[2]) + ToFloat64(cmtSamples[3])

		// Populate global metrics
		ms.TotalBlocks.Add(totalBlocks)
		ms.TotalTxs.Add(totalTxes)
		ms.Errors.Add(totalErrs)

		// Some metrics are grouped by user address
		// extdChainID, err := NewExtendedChainIDFromLegacy(chainID)
		// if err != nil {
		// 	return
		// }
		// userAddress := extdChainID.GetUserAddress()

		// Populate user-grouped metrics
		// ms.TotalBlocksPerUser.With("node_id", relayID, "user_address", userAddress).Add(totalBlocks)
		// ms.TotalTxsPerUser.With("node_id", relayID, "user_address", userAddress).Add(totalTxes)
		// ms.ErrorsPerUser.With("node_id", relayID, "user_address", userAddress).Add(totalErrs)
	}
}

func ToFloat64(s metrics.Sample) float64 {
	switch k := s.Value.Kind(); k {
	case metrics.KindFloat64:
		return s.Value.Float64()

	case metrics.KindUint64:
		return float64(s.Value.Uint64())

	default:
	}

	return float64(0)
}
