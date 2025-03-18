package mini_lsm

import (
	"bytes"
	"errors"
	"mini_lsm/pb"
	"sync"
	"sync/atomic"
)

const (
	DeletedBitMask byte = 1 << 0 // 0000 0001
)

type WriteTx interface {
	// Commit commits a previous tx and begins a new writable one.
	Commit() error
	Put(key []byte, value []byte) error
	Rollback()
}
type WriteTxImpl struct {
	*Tx
	inner        *LsmStorageInner
	readTs       uint64
	commitTs     uint64
	writes       map[string]*WriteEntry
	mvcc         *LsmMvccInner
	serializable bool
	closed       atomic.Bool
}

type WriteEntry struct {
	key   []byte
	value []byte
	meta  byte // For delete markers etc
}

// NewWriteTx creates a new writeTx transaction
func NewWriteTx(inner *LsmStorageInner, mvcc *LsmMvccInner, serializable bool) *WriteTxImpl {
	tx := &WriteTxImpl{
		Tx:           &Tx{readLock: sync.RWMutex{}},
		inner:        inner,
		readTs:       mvcc.ReadTimestamp(),
		writes:       make(map[string]*WriteEntry),
		mvcc:         mvcc,
		serializable: serializable,
	}
	return tx
}

func (w *WriteTxImpl) Put(key []byte, value []byte) error {
	if w.closed.Load() {
		return ErrTransactionClosed
	}

	// Store in pending writes
	w.writes[string(key)] = &WriteEntry{
		key:   key,
		value: value,
	}
	return nil
}

func (w *WriteTxImpl) Delete(key []byte) error {
	if w.closed.Load() {
		return ErrTransactionClosed
	}

	// Store delete marker
	w.writes[string(key)] = &WriteEntry{
		key:  key,
		meta: DeletedBitMask,
	}
	return nil
}

func (w *WriteTxImpl) Commit() error {
	if w.closed.Load() {
		return ErrTransactionClosed
	}
	defer w.closed.Store(true)

	// Get commit timestamp
	w.commitTs = w.mvcc.CommitTimestamp()

	// Check for conflicts if serializable
	if w.serializable {
		// Verify no concurrent modifications
		for key := range w.writes {
			if modified, err := w.checkKeyModified([]byte(key)); err != nil {
				return err
			} else if modified {
				return ErrConflict
			}
		}
	}

	// Prepare batch of writes with commit timestamp
	batch := make([]*KeyValue, 0, len(w.writes))
	for _, entry := range w.writes {
		kv := KeyValue{
			Key: &pb.Key{
				Key:       entry.key,
				Timestamp: w.commitTs,
			},
			Value: &pb.Value{
				Value: entry.value,
			},
		}
		batch = append(batch, &kv)
	}

	// Write batch to storage
	if err := w.inner.state.memTable.PutBatch(batch); err != nil {
		return err
	}

	// Update commit timestamp
	//w.mvcc.UpdateCommitTS(w.commitTs)

	return nil
}

func (w *WriteTxImpl) checkKeyModified(key []byte) (bool, error) {
	latestTs := w.mvcc.ReadTimestamp()
	if latestTs <= w.readTs {
		return false, nil
	}

	// Check if key was modified between readTs and latest
	iter, err := w.inner.Scan(key, nil)
	if err != nil {
		return false, err
	}

	if !iter.Valid() {
		return false, nil
	}

	iterKey := iter.Key()
	return bytes.Equal(iterKey, key), nil
}

func (w *WriteTxImpl) Rollback() {
	w.closed.Store(true)
	w.writes = nil
}

// Errors
var (
	ErrTransactionClosed = errors.New("transaction has been closed")
	ErrConflict          = errors.New("transaction conflict detected")
)

// Helper functions
func hash(b []byte) uint32 {
	// Use a proper hash function
	h := uint32(0)
	for i := 0; i < len(b); i++ {
		h = 31*h + uint32(b[i])
	}
	return h
}
