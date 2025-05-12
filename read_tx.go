package plsm

import (
	"bytes"
	"github.com/wodeyoulai/plsm/pb"
	"google.golang.org/protobuf/proto"
	"sync/atomic"
)

type ReadTx interface {
	Get(key []byte) (val []byte, err error)
	Scan(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte, err error)
	Rollback()
}

// ReadTx represents a read transaction with MVCC
type ReadTxImpl struct {
	inner  *LsmStorageInner
	readTs uint64 // Transaction read timestamp
	//keyHashes sync.Map // Track read keys for conflict detection
	mvcc   *LsmMvccInner
	closed atomic.Bool
}

// NewReadTx creates a new read transaction
func NewReadTx(inner *LsmStorageInner, mvcc *LsmMvccInner, serializable bool) *ReadTxImpl {
	tx := &ReadTxImpl{
		inner:  inner,
		readTs: mvcc.ReadTimestamp(),
		mvcc:   mvcc,
	}
	return tx
}

// Get retrieves a value for a key, respecting transaction isolation
func (r *ReadTxImpl) Get(key []byte) ([]byte, error) {
	if r.closed.Load() {
		return nil, ErrTransactionClosed
	}

	// Add key to read set for conflict detection
	//r.keyHashes.Store(hash(key), struct{}{})

	// Convert key to pb.Key with version and timestamp
	pbKey := &pb.Key{
		Key:       key,
		Timestamp: r.readTs,
	}

	// Use storage layer Get with MVCC timestamp
	v, err := r.inner.get(pbKey)
	if err != nil {
		return nil, err
	}
	return v.Value, nil
}

// Scan returns a range of key-value pairs
func (r *ReadTxImpl) Scan(start, end []byte, limit int64) ([][]byte, [][]byte, error) {
	if r.closed.Load() {
		return nil, nil, ErrTransactionClosed
	}

	// Create iterators for the scan
	iter, err := r.inner.Scan(start, end)
	if err != nil {
		return nil, nil, err
	}

	var keys, values [][]byte
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
			r.inner.lg.Error("unmarshal fail")
			continue
		}
		values = append(values, pbValue.Value)
		count++

		if err := iter.Next(); err != nil {
			return nil, nil, err
		}
	}

	return keys, values, nil
}

func (r *ReadTxImpl) Rollback() {
	r.closed.Store(true)
}
