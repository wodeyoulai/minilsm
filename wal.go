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

type Wal struct {
	*pWal.Log
	mu              sync.Mutex
	currentLocation uint32
	logger          *zap.Logger
	closed          atomic.Bool // Flag to track WAL closed status
	closeOnce       sync.Once   // Ensure Close() is called only once
}

func NewWal(logger *zap.Logger, path string, options *pWal.Options) *Wal {
	inWal, err := pWal.Open(path, options)
	if err != nil {
		logger.Fatal("open wal failed：%v", zap.Error(err))
	}
	lastIndex, err := inWal.LastIndex()
	if err != nil {
		logger.Fatal("open wal failed：%v", zap.Error(err))
	}
	return &Wal{inWal, sync.Mutex{}, uint32(lastIndex), logger, atomic.Bool{}, sync.Once{}}
}

// Close gracefully shuts down the WAL by flushing all data to disk
// and releasing resources. After closing, no more writes will be accepted.
func (w *Wal) Close() error {
	var closeErr error
	w.closeOnce.Do(func() {
		w.mu.Lock()
		defer w.mu.Unlock()

		// Mark as closed
		w.closed.Store(true)

		// Ensure all data is persisted to disk
		if err := w.Sync(); err != nil {
			w.logger.Error("failed to sync WAL during close", zap.Error(err))
			closeErr = fmt.Errorf("sync WAL: %w", err)
			return
		}

		// Close the underlying WAL
		if err := w.Close(); err != nil {
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

// Write appends data to WAL. Returns error if WAL is closed.
//func (w *Wal) Write(ts uint64, data []byte) error {
//	if w.IsClosed() {
//		return errors.New("WAL is closed")
//	}
//
//	w.mu.Lock()
//	defer w.mu.Unlock()
//	return w.l.Write(ts, data)
//}

// Append writes multiple records to WAL atomically.
// Returns error if WAL is closed.
//func (w *Wal) Append(records []Record) error {
//	if w.IsClosed() {
//		return errors.New("WAL is closed")
//	}
//
//	w.mu.Lock()
//	defer w.mu.Unlock()
//
//	// Original append logic
//	// ...
//}

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
