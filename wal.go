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

// Wal extended structure,simply wrap the "github.com/tidwall/wal"
type Wal struct {
	*pWal.Log
	mu              sync.Mutex
	currentLocation uint64
	logger          *zap.Logger
	closed          atomic.Bool
	closeOnce       sync.Once

	// Batch processing related fields
	asyncBatch, batch *pWal.Batch
	batchMu           sync.Mutex
	pendingWrites     int               // Track pending write count
	maxBatchSize      int               // Maximum batch entries
	maxBatchBytes     int               // Maximum batch bytes
	writeCh           chan writeRequest // Async write request channel
	close             chan struct{}
	flushTimer        *time.Timer
	flushInterval     time.Duration

	bufferPool sync.Pool
	index      atomic.Uint64
}
type WalOptions struct {
	pWal.Options
	MaxBatchSize  int
	MaxBatchBytes int
	FlushInterval time.Duration
	WriteChanSize int
}

func DefaultWalOptions() *WalOptions {
	return &WalOptions{
		Options:       pWal.Options{},
		MaxBatchSize:  600,
		MaxBatchBytes: 1024 * 1024, // Default 1MB, configurable
		WriteChanSize: 10000,
		FlushInterval: 10,
	}
}

// Write request structure
type writeRequest struct {
	index    uint64
	data     []byte
	resultCh chan error // Channel for write result notification
}

// NewWal Create enhanced Wal
func NewWal(logger *zap.Logger, path string, options *WalOptions) *Wal {
	inWal, err := pWal.Open(path, &options.Options)
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
		currentLocation: lastIndex,
		logger:          logger,
		batch:           &pWal.Batch{},
		asyncBatch:      &pWal.Batch{},
		maxBatchSize:    options.MaxBatchSize,  // Default batch size, configurable
		maxBatchBytes:   options.MaxBatchBytes, // Default 1MB, configurable
		writeCh:         make(chan writeRequest, options.WriteChanSize),
		flushInterval:   options.FlushInterval * time.Millisecond,
		index:           atomic.Uint64{},
		close:           make(chan struct{}),
		bufferPool: sync.Pool{New: func() any {
			return make([]writeRequest, 0, options.MaxBatchSize)
		}},
	}
	w.index.Store(0)
	// Start background processing thread
	w.flushTimer = time.NewTimer(w.flushInterval)
	go w.backgroundProcessor()
	return w
}
func (w *Wal) getBatch() []writeRequest {
	return w.bufferPool.Get().([]writeRequest)[:0]
}
func (w *Wal) returnBatch(batch []writeRequest) {
	w.bufferPool.Put(batch[:0])
}

// WriteBatch Synchronous batch write
func (w *Wal) WriteBatch(entries [][]byte) error {
	if w.closed.Load() {
		return fmt.Errorf("wal is closed")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	w.batch.Clear()
	lastIndex, err := w.LastIndex()
	if err != nil {
		return err
	}
	nextIndex := w.currentLocation + 1
	for _, data := range entries {
		w.batch.Write(nextIndex, data)
		nextIndex++
	}

	// Execute batch write
	err = w.Log.WriteBatch(w.batch)
	if err != nil {
		return err
	}

	w.currentLocation = lastIndex + uint64(len(entries))
	return nil
}

// WriteSync writes data to WAL and immediately syncs it to disk.
// Index is handled internally. Returns error if WAL is closed.
func (w *Wal) WriteSync(data []byte) error {
	if w.IsClosed() {
		return errors.New("WAL is closed")
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	// Get the next index
	lastIndex, err := w.LastIndex()
	if err != nil {
		return fmt.Errorf("get last index: %w", err)
	}
	nextIndex := lastIndex + 1

	// Write data to log
	if err := w.Log.Write(nextIndex, data); err != nil {
		return fmt.Errorf("write to WAL: %w", err)
	}

	// Immediately sync to disk
	if err := w.Log.Sync(); err != nil {
		return fmt.Errorf("sync WAL: %w", err)
	}

	return nil
}

// WriteAsync Use select for non-blocking
func (w *Wal) WriteAsync(data []byte) (chan error, error) {

	if w.closed.Load() {
		return nil, errors.New("WAL is closed")
	}

	resultCh := make(chan error, 1)

	select {
	case w.writeCh <- writeRequest{index: 0, data: data, resultCh: resultCh}:
		return resultCh, nil
	default:
		// Channel full, return immediate error
		return nil, errors.New("WAL write channel is full")
	}
}

func (w *Wal) backgroundProcessor() {
	defer func() {
		if r := recover(); r != nil {
			w.logger.Error("WAL processor panicked", zap.Any("panic", r))
		}
	}()

	pendingWrites := make([]writeRequest, 0, w.maxBatchSize)
	batchCh := make(chan []writeRequest, 4) // Channel for batch processing

	// Start a separate goroutine for processing batches
	go func() {
		for batch := range batchCh {
			// Process the batch
			if len(batch) == 0 {
				continue
			}

			w.asyncBatch.Clear()
			w.mu.Lock()
			defer w.mu.Unlock()
			nextIndex := w.currentLocation + 1
			for i := range batch {
				batch[i].index = uint64(nextIndex)
				w.asyncBatch.Write(batch[i].index, batch[i].data)
				nextIndex++
			}

			err := w.Log.WriteBatch(w.asyncBatch)

			// Update current location on success
			if err == nil {
				//w.mu.Lock()
				w.currentLocation = batch[len(batch)-1].index
				//w.mu.Unlock()
			}

			// Notify all waiting requests
			for _, req := range batch {
				select {
				case req.resultCh <- err:
					// Successfully sent result
				default:
					w.logger.Warn("Failed to send WAL result - receiver may be gone",
						zap.Uint64("index", req.index))
				}
			}

			walWriteBatchSize.Set(float64(len(batch)))
			w.bufferPool.Put(batch)
		}
	}()

	// Helper to drain write channel into pending writes
	drainWriteChannel := func(maxItems int) {
	drainLoop:
		for len(pendingWrites) < maxItems {
			select {
			case req := <-w.writeCh:
				pendingWrites = append(pendingWrites, req)
			default:
				break drainLoop
			}
		}
	}

	// Helper to prepare a batch and send for processing
	prepareBatch := func() {
		if len(pendingWrites) == 0 {
			return
		}
		w.batchMu.Lock()
		defer w.batchMu.Unlock()
		startTime := time.Now()
		batch := w.getBatch()
		copy(batch, pendingWrites)
		pendingWrites = pendingWrites[:0]

		// Send batch for async processing
		batchCh <- batch
		walSyncDuration.Observe(float64(time.Since(startTime).Milliseconds()))
	}

	w.flushTimer.Reset(w.flushInterval)
	for {
		select {
		case req := <-w.writeCh:
			pendingWrites = append(pendingWrites, req)

			// Flush immediately if batch threshold reached
			if len(pendingWrites) >= w.maxBatchSize {
				// Try to gather more requests to maximize batch efficiency
				//drainWriteChannel(w.maxBatchSize)
				prepareBatch()

				// Reset the timer
				if !w.flushTimer.Stop() {
					select {
					case <-w.flushTimer.C:
					default:
					}
				}
				w.flushTimer.Reset(w.flushInterval)
			}

		case <-w.flushTimer.C:
			// Attempt to gather more requests before processing
			drainWriteChannel(w.maxBatchSize)
			prepareBatch()
			w.flushTimer.Reset(w.flushInterval)

		case <-w.close:
			// Final drain of the write channel
			drainWriteChannel(w.maxBatchSize * 2) // Allow gathering more than usual for final batch

			// Process any remaining writes
			prepareBatch()
			close(batchCh) // Signal the batch processor to stop
			return
		}

		// Exit if closed and no pending writes
		if w.closed.Load() && len(pendingWrites) == 0 {
			close(batchCh) // Signal the batch processor to stop
			return
		}
	}
}

// ConfigureBatch Configure batch parameters
func (w *Wal) ConfigureBatch(maxSize int, maxBytes int, flushInterval time.Duration) {
	w.batchMu.Lock()
	defer w.batchMu.Unlock()

	w.maxBatchSize = maxSize
	w.maxBatchBytes = maxBytes
	w.flushInterval = flushInterval
}

// Flush Force flush all pending writes
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

// Close Ensure all data is written when WAL closes
func (w *Wal) Close() error {
	var closeErr error

	w.closeOnce.Do(func() {
		// Mark as closed
		w.closed.Store(true)
		close(w.close)
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

	// todo Reopen logic
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
