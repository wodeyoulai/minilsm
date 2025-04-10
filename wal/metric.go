package wal

import "github.com/prometheus/client_golang/prometheus"

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
)

func initMetric(r *prometheus.Registry) {
	if r == nil {
		return
	}
	r.MustRegister(walSyncTotal)
	r.MustRegister(walSyncDuration)
	r.MustRegister(walSize)
}
