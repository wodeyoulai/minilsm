package plsm

import (
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Db interface {
	Put(key, value []byte) error
	Get(key []byte) ([]byte, error)
	// WriteTx include read method
	WriteTx() WriteTx
	Close() error
}

// PLsm exposed storage engine interface
type PLsm struct {
	lg                 *zap.Logger
	inner              *LsmStorageInner
	stopping           chan struct{}
	flushNotifier      chan struct{}
	compactionNotifier chan struct{}
	wg                 *sync.WaitGroup
	wgMu               *sync.RWMutex
	mvcc               *LsmMvccInner
	metrics            *MiniLsmMetrics
	//compactionThread   *sync.WaitGroup
}

// NewPLsm create a new storage engine instance
func NewPLsm(log *zap.Logger, path string, registry *prometheus.Registry, opts LsmStorageOptions) (*PLsm, error) {
	inner, err := newLsmStorageInner(log, path, &opts)
	if err != nil {
		return nil, err
	}
	mvcc := NewLsmMvccInner(inner.recoverTimeStamp(), nil)
	lsm := &PLsm{
		lg:                 log,
		inner:              inner,
		stopping:           make(chan struct{}),
		flushNotifier:      make(chan struct{}),
		compactionNotifier: make(chan struct{}),
		wgMu:               &sync.RWMutex{},
		wg:                 &sync.WaitGroup{},
	}
	lsm.mvcc = mvcc
	lsm.metrics = initMetrics(registry)
	// start the background thread
	//lsm.GoAttach(func() { lsm.inner.StartFlushWorker() })
	lsm.goAttach(func() { _ = lsm.inner.StartCompactionWorker(context.Background()) })

	return lsm, nil
}

func (m *PLsm) goAttach(f func()) {
	m.wgMu.RLock() // this blocks with ongoing close(m.stopping)
	defer m.wgMu.RUnlock()
	select {
	case <-m.stopping:
		m.lg.Warn("lsm has stopped; skipping GoAttach")
		return
	default:
	}
	// now safe to add since waitgroup wait has not started yet
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		f()
	}()
}

func (m *PLsm) Put(key, value []byte) error {
	begin := time.Now()
	defer func() {
		elapsed := time.Since(begin)
		m.metrics.putLatency.Observe(float64(elapsed.Milliseconds()))
		m.metrics.putTotal.Add(1)
	}()
	return m.update(func(tx WriteTx) error {
		return tx.Put(key, value)
	})
}

func (m *PLsm) Get(key []byte) ([]byte, error) {
	begin := time.Now()
	defer func() {
		elapsed := time.Since(begin)
		m.metrics.getLatency.Observe(float64(elapsed.Milliseconds()))
		m.metrics.getTotal.Add(1)
	}()
	tx := NewReadTx(m.inner, m.mvcc, false)
	defer tx.Rollback()
	return tx.Get(key)
}

// Update functional transactions automatic processing rollback and commit
func (m *PLsm) update(fn func(tx WriteTx) error) error {

	tx := NewWriteTx(m.inner, m.mvcc, false)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// Delete implements logical deletion by writing a tombstone record
func (m *PLsm) Delete(key []byte) error {
	begin := time.Now()
	defer func() {
		elapsed := time.Since(begin)
		m.metrics.deleteLatency.Observe(float64(elapsed.Milliseconds()))
		m.metrics.deleteTotal.Add(1)
	}()
	return m.update(func(tx WriteTx) error {
		return tx.Delete(key)
	})
}

// Scan creates an iterator to scan over a range of keys
func (m *PLsm) Scan(start, end []byte, limit int64) ([][]byte, [][]byte, error) {
	begin := time.Now()
	defer func() {
		elapsed := time.Since(begin)
		m.metrics.scanLatency.Observe(float64(elapsed.Milliseconds()))
		m.metrics.scanTotal.Add(1)
	}()
	tx := NewReadTx(m.inner, m.mvcc, false)
	defer tx.Rollback()
	return tx.Scan(start, end, limit)
}

// Close gracefully shuts down the LSM storage engine and releases all resources.
// It ensures all pending writes are flushed to disk before shutdown.
func (m *PLsm) Close() error {
	// Set stopping flag to prevent new operations
	close(m.stopping)

	// Acquire write lock to ensure no new tasks are added to background workers
	m.wgMu.Lock()
	defer m.wgMu.Unlock()

	m.lg.Info("closing LSM storage engine")

	// Wait for background tasks to complete
	doneCh := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(doneCh)
	}()

	// Flush any pending data in the memtable
	if err := m.inner.triggerFlush(); err != nil {
		m.lg.Error("error flushing memtable during shutdown", zap.Error(err))
		// Continue with shutdown despite error
	}

	// Close all opened SSTables
	for id, sst := range m.inner.openedSstables {
		if err := sst.Close(); err != nil {
			m.lg.Warn("error closing SSTable", zap.Uint32("id", id), zap.Error(err))
		}
	}
	// Close inner storage
	if err := m.inner.Close(); err != nil {
		m.lg.Error("error closing inner storage", zap.Error(err))
		return err
	}

	// Wait for background tasks with timeout
	select {
	case <-doneCh:
		m.lg.Info("all background tasks completed")
	case <-time.After(5 * time.Second):
		m.lg.Warn("timeout waiting for background tasks to complete, forcing shutdown")
	}

	// Close the manifest
	if m.inner.manifest != nil {
		if err := m.inner.manifest.Close(); err != nil {
			m.lg.Error("error closing manifest", zap.Error(err))
			return err
		}
	}

	// Close all opened SSTable iterators
	//for id, iter := range m.inner.openedIterators {
	//	if err := iter.Close(); err != nil {
	//		m.lg.Warn("error closing SSTable iterator", zap.Uint32("id", id), zap.Error(err))
	//	}
	//}

	m.lg.Info("LSM storage engine closed successfully")
	return nil
}

func (m *PLsm) WriteTx() WriteTx {
	return NewWriteTx(m.inner, m.mvcc, true)
}

func (m *PLsm) ReadTx() ReadTx {
	return NewReadTx(m.inner, m.mvcc, true)
}
