package plsm

import (
	"bytes"
	"errors"
	"github.com/wodeyoulai/plsm/pb"
	"google.golang.org/protobuf/proto"
	"sync/atomic"
)

const (
	DeletedBitMask byte = 1 << 0 // 0000 0001
)

// Errors
var (
	ErrTransactionClosed = errors.New("transaction has been closed")
	ErrConflict          = errors.New("transaction conflict detected")
)

type WriteTx interface {
	ReadTx
	Commit() error
	Put(key []byte, value []byte) error
	Delete(key []byte) error
}

type WriteTxImpl struct {
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
		inner:        inner,
		readTs:       mvcc.ReadTimestamp(),
		writes:       make(map[string]*WriteEntry),
		mvcc:         mvcc,
		serializable: serializable,
	}
	return tx
}

func (w *WriteTxImpl) Scan(start []byte, end []byte, limit int64) (keys [][]byte, vals [][]byte, err error) {
	if w.closed.Load() {
		return nil, nil, ErrTransactionClosed
	}

	// Create iterators for the scan
	iter, err := w.inner.Scan(start, end)
	if err != nil {
		return nil, nil, err
	}

	//var keys, values [][]byte
	count := int64(0)

	for iter.Valid() && (limit == 0 || count < limit) {
		key := iter.Key()
		if end != nil && bytes.Compare(key, end) >= 0 {
			break
		}

		// Add to read set
		//r.keyHashes.Store(hash(key), struct{}{})

		keys = append(keys, key)

		pbValue := &pb.Value{}
		err = proto.Unmarshal(iter.Value(), pbValue)
		if err != nil {
			w.inner.lg.Error("unmarshal fail")
			continue
		}
		vals = append(vals, pbValue.Value)
		count++

		if err := iter.Next(); err != nil {
			return nil, nil, err
		}
	}

	return keys, vals, nil
}

func (w *WriteTxImpl) Get(key []byte) (keys []byte, err error) {
	// Get retrieves a value for a key, respecting transaction isolation
	if w.closed.Load() {
		return nil, ErrTransactionClosed
	}

	// Convert key to pb.Key with version and timestamp
	pbKey := &pb.Key{
		Key:       key,
		Timestamp: w.readTs,
	}

	// Use storage layer Get with MVCC timestamp
	v, err := w.inner.get(pbKey)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

func (w *WriteTxImpl) Put(key []byte, value []byte) error {
	if w.closed.Load() {
		return ErrTransactionClosed
	}
	//w.inner.lg.Debug("TX-%d: Putting key %s", zap.Uint64("readtx", w.readTs), zap.String("key", string(key)))
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
	//xxkey := ""
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
		//xxkey = string(entry.key)
		kv.Value.IsDeleted = entry.meta == DeletedBitMask
		batch = append(batch, &kv)
	}

	// Write batch to storage
	if err := w.inner.state.memTable.PutBatch(batch); err != nil {
		return err
	}

	// Update commit timestamp
	w.mvcc.UpdateReadTimestamp()
	//w.inner.lg.Debug("TX-%d: Commting key %s", zap.Uint64("writetx", w.commitTs), zap.String("key", xxkey))

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

// Helper functions
func hash(b []byte) uint32 {
	// Use a proper hash function
	h := uint32(0)
	for i := 0; i < len(b); i++ {
		h = 31*h + uint32(b[i])
	}
	return h
}
