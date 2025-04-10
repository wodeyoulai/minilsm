package mini_lsm

import (
	"github.com/prometheus/client_golang/prometheus"
)

// In lsm_storage.go for the MiniLsm struct
type MiniLsmMetrics struct {
	// Operation counters
	putTotal    prometheus.Counter
	getTotal    prometheus.Counter
	deleteTotal prometheus.Counter
	scanTotal   prometheus.Counter

	// Latency histograms
	putLatency    prometheus.Histogram
	getLatency    prometheus.Histogram
	deleteLatency prometheus.Histogram
	scanLatency   prometheus.Histogram

	// Error counters
	operationErrors prometheus.Counter
}

func initMetrics(registry *prometheus.Registry) *MiniLsmMetrics {
	metrics := &MiniLsmMetrics{
		// Operation counters
		putTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minilsm_put_total",
			Help: "Total number of Put operations",
		}),
		getTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minilsm_get_total",
			Help: "Total number of Get operations",
		}),
		deleteTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minilsm_delete_total",
			Help: "Total number of Delete operations",
		}),
		scanTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minilsm_scan_total",
			Help: "Total number of Scan operations",
		}),

		// Latency histograms
		putLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "minilsm_put_duration_seconds",
			Help:    "Duration of Put operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		}),
		getLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "minilsm_get_duration_seconds",
			Help:    "Duration of Get operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		}),
		deleteLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "minilsm_delete_duration_seconds",
			Help:    "Duration of Delete operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		}),
		scanLatency: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "minilsm_scan_duration_seconds",
			Help:    "Duration of Scan operations in seconds",
			Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
		}),

		// Error counters
		operationErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "minilsm_operation_errors_total",
			Help: "Total number of failed operations",
		}),
	}
	// Register all metrics with Prometheus registry
	registry.MustRegister(
		metrics.putTotal,
		metrics.getTotal,
		metrics.deleteTotal,
		metrics.scanTotal,
		metrics.putLatency,
		metrics.getLatency,
		metrics.deleteLatency,
		metrics.scanLatency,
		metrics.operationErrors, memTableSize, memTableFlushTotal,

		// SSTable metrics
		sstableCount, sstableTotalSize, sstableCreatedTotal,

		// Compaction metrics
		compactionRunTotal, compactionDuration, bytesCompacted,

		memTableFlushDuration,

		// WAL metrics
		walSyncTotal, walSyncDuration, walSize, walWriteBatchSize,
		// Transaction metrics
		activeTxns, committedTxnsTotal, abortedTxnsTotal, txnConflictsTotal,
	)
	return metrics
}

// Compaction
var (
	compactionRunTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_compaction_runs_total",
		Help: "Total number of compaction operations run",
	})

	compactionDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minilsm_compaction_duration_seconds",
		Help:    "Duration of compaction operations",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 10), // 10ms to ~10s
	})

	bytesCompacted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_bytes_compacted_total",
		Help: "Total bytes processed during compaction",
	})
)

// Memtable
var (
	// In mem_table.go
	memTableSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "minilsm_memtable_size_bytes",
		Help: "Current size of memtables in bytes",
	}, []string{"table_type"}) // "active" or "immutable"

	memTableFlushTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_memtable_flush_total",
		Help: "Total number of memtable flushes",
	})

	memTableFlushDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minilsm_memtable_flush_duration_seconds",
		Help:    "Duration of memtable flush operations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 10), // 1ms to ~1s
	})
)

var (
	// In table.go
	sstableCount = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "minilsm_sstable_count",
		Help: "Number of SSTables per level",
	}, []string{"level"})

	sstableTotalSize = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "minilsm_sstable_size_bytes",
		Help: "Total size of SSTables in bytes per level",
	}, []string{"level"})

	sstableCreatedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_sstable_created_total",
		Help: "Total number of SSTables created",
	})
)

var (
	// In mvcc.go or transaction related files
	activeTxns = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minilsm_active_transactions",
		Help: "Current number of active transactions",
	})

	committedTxnsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_committed_transactions_total",
		Help: "Total number of committed transactions",
	})

	abortedTxnsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_aborted_transactions_total",
		Help: "Total number of aborted transactions",
	})

	txnConflictsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_transaction_conflicts_total",
		Help: "Total number of transaction conflicts",
	})
)
var (
	walSyncTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "minilsm_wal_sync_total",
		Help: "Total number of WAL sync operations",
	})

	walSyncDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "minilsm_wal_sync_duration_seconds",
		Help:    "Duration of WAL sync operations",
		Buckets: prometheus.ExponentialBuckets(0.0001, 2, 10), // 0.1ms to ~100ms
	})

	walSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minilsm_wal_size_bytes",
		Help: "Current size of the WAL in bytes",
	})
	walWriteBatchSize = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "minilsm_wal_write_batch_size",
		Help: "Wal write batch size ",
	})
)
