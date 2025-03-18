package mini_lsm

import (
	"errors"
	"fmt"
	pWal "github.com/tidwall/wal"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

// Wal extended structure
type Wal struct {
	*pWal.Log
	mu              sync.Mutex
	currentLocation uint32
	logger          *zap.Logger
	closed          atomic.Bool
	closeOnce       sync.Once

	// Batch processing related fields
	batch         *pWal.Batch
	batchMu       sync.Mutex
	pendingWrites int               // Track pending write count
	maxBatchSize  int               // Maximum batch entries
	maxBatchBytes int               // Maximum batch bytes
	writeCh       chan writeRequest // Async write request channel
	flushTimer    *time.Timer
	flushInterval time.Duration

	index atomic.Uint64
}

// Write request structure
type writeRequest struct {
	index    uint64
	data     []byte
	resultCh chan error // Channel for write result notification
}

// Create enhanced Wal
func NewWal(logger *zap.Logger, path string, options *pWal.Options) *Wal {
	inWal, err := pWal.Open(path, options)
	if err != nil {
		logger.Fatal("open wal failed", zap.Error(err))
	}

	lastIndex, err := inWal.LastIndex()
	if err != nil {
		logger.Fatal("get wal last index failed", zap.Error(err))
	}

	w := &Wal{
		Log:             inWal,
		mu:              sync.Mutex{},
		currentLocation: uint32(lastIndex),
		logger:          logger,
		batch:           &pWal.Batch{},
		maxBatchSize:    100,         // Default batch size, configurable
		maxBatchBytes:   1024 * 1024, // Default 1MB, configurable
		writeCh:         make(chan writeRequest, 20000),
		flushInterval:   100 * time.Millisecond,
		index:           atomic.Uint64{},
	}
	w.index.Store(0)
	// Start background processing thread
	w.flushTimer = time.NewTimer(w.flushInterval)
	go w.backgroundProcessor()

	return w
}

// Synchronous batch write
func (w *Wal) WriteBatch(entries [][]byte) error {
	if w.closed.Load() {
		return errors.New("WAL is closed")
	}

	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	batch := &pWal.Batch{}
	lastIndex, err := w.LastIndex()
	if err != nil {
		return err
	}

	// Add all entries to batch
	for i, data := range entries {
		batch.Write(lastIndex+uint64(i+1), data)
	}

	// Execute batch write
	err = w.Log.WriteBatch(batch)
	if err != nil {
		return err
	}

	w.currentLocation = uint32(lastIndex + uint64(len(entries)))
	return nil
}

// Use select for non-blocki
func (w *Wal) WriteAsync(data []byte) (chan error, error) {
	if w.closed.Load() {
		return nil, errors.New("WAL is closed")
	}

	resultCh := make(chan error, 1)

	// Current index

	w.mu.Lock()
	defer w.mu.Unlock()
	nextIndex := w.index.Load() + 1
	w.index.Store(nextIndex)
	//w.mu.Unlock()

	select {
	case w.writeCh <- writeRequest{index: nextIndex, data: data, resultCh: resultCh}:
		return resultCh, nil
	default:
		// Channel full, return immediate error
		return nil, errors.New("WAL write channel is full")
	}
	//w.writeCh <- writeRequest{index: nextIndex, data: data, resultCh: resultCh}
	//return resultCh, nil
}

// Background processor thread
func (w *Wal) backgroundProcessor() {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("WAL processor panicked", zap.Any("panic", r))
			// Consider restarting the processor
		}
	}()
	pendingWrites := make([]writeRequest, 0, w.maxBatchSize)

	processBatch := func() {
		if len(pendingWrites) == 0 {
			return
		}
		batch := &pWal.Batch{}

		w.mu.Lock()
		nextIndex := w.currentLocation + 1
		for i := range pendingWrites {
			pendingWrites[i].index = uint64(nextIndex)
			batch.Write(pendingWrites[i].index, pendingWrites[i].data)
			nextIndex++
		}
		w.mu.Unlock()

		// Create new batch
		//sort.Slice(pendingWrites, func(i, j int) bool {
		//	return pendingWrites[i].index < pendingWrites[j].index
		//})

		// Execute batch write
		err := w.Log.WriteBatch(batch)

		// Update index
		if err == nil {
			w.mu.Lock()
			w.currentLocation = uint32(pendingWrites[len(pendingWrites)-1].index)
			w.mu.Unlock()
		}

		// Notify all waiting requests
		for _, req := range pendingWrites {
			//req.resultCh <- err
			select {
			case req.resultCh <- err:
				// Successfully sent result
			default:
				w.logger.Warn("Failed to send WAL result - receiver may be gone",
					zap.Uint64("index", req.index))
			}
		}

		// Clear pending list
		pendingWrites = pendingWrites[:0]
	}

	for {
		// Reset timer
		w.flushTimer.Reset(w.flushInterval)

		select {
		case req := <-w.writeCh:
			pendingWrites = append(pendingWrites, req)

			// Reached batch threshold, process immediately
			if len(pendingWrites) >= w.maxBatchSize {
				processBatch()
			}

		case <-w.flushTimer.C:
			// Periodic flush
			//fmt.Println("timer")
			processBatch()
		}

		// Check if closed
		if w.closed.Load() && len(pendingWrites) == 0 {
			return
		}
	}
}

// Configure batch parameters
func (w *Wal) ConfigureBatch(maxSize int, maxBytes int, flushInterval time.Duration) {
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	w.maxBatchSize = maxSize
	w.maxBatchBytes = maxBytes
	w.flushInterval = flushInterval
}

// Force flush all pending writes
func (w *Wal) Flush() error {
	done := make(chan error, 1)

	// Create special flush request
	flushReq := writeRequest{
		index:    0, // Special index indicating flush request
		data:     nil,
		resultCh: done,
	}

	w.writeCh <- flushReq
	return <-done
}

// Ensure all data is written when WAL closes
func (w *Wal) Close() error {
	var closeErr error

	w.closeOnce.Do(func() {
		// Mark as closed
		w.closed.Store(true)

		// Wait for final flush to complete
		if err := w.Flush(); err != nil {
			w.logger.Error("failed to flush WAL during close", zap.Error(err))
			closeErr = fmt.Errorf("flush WAL: %w", err)
			return
		}

		// Close underlying WAL
		if err := w.Log.Close(); err != nil {
			w.logger.Error("failed to close WAL", zap.Error(err))
			closeErr = fmt.Errorf("close WAL: %w", err)
			return
		}

		w.logger.Info("WAL closed successfully")
	})

	return closeErr
}

// IsClosed returns true if the WAL has been closed
func (w *Wal) IsClosed() bool {
	return w.closed.Load()
}

// Reopen reopens a closed WAL.
// Returns error if WAL is already open.
func (w *Wal) Reopen() error {
	if !w.IsClosed() {
		return errors.New("WAL is not closed")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Reopen logic
	// ...

	w.closed.Store(false)
	return nil
}

// CloseWithTimeout attempts to close the WAL with a timeout.
// Returns error if timeout is reached before close completes.
func (w *Wal) CloseWithTimeout(timeout time.Duration) error {
	done := make(chan error, 1)
	go func() {
		done <- w.Close()
	}()

	select {
	case err := <-done:
		return err
	case <-time.After(timeout):
		return errors.New("close WAL timeout")
	}
}
