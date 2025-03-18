package mini_lsm

import (
	"context"
	"errors"
	"go.uber.org/zap"
	"mini_lsm/iterators"
	"mini_lsm/pb"
	"sync"
	"time"
)

// MiniLsm exposed storage engine interface
type MiniLsm struct {
	lg                 *zap.Logger
	inner              *LsmStorageInner
	stopping           chan struct{}
	flushNotifier      chan struct{}
	compactionNotifier chan struct{}
	wg                 *sync.WaitGroup
	wgMu               *sync.RWMutex
	mvcc               *LsmMvccInner

	//compactionThread   *sync.WaitGroup
}

// NewMiniLsm create a new storage engine instance
func NewMiniLsm(log *zap.Logger, path string, opts LsmStorageOptions) (*MiniLsm, error) {
	inner, err := newLsmStorageInner(log, path, &opts)
	if err != nil {
		return nil, err
	}
	lsm := &MiniLsm{
		lg:                 log,
		inner:              inner,
		stopping:           make(chan struct{}),
		flushNotifier:      make(chan struct{}),
		compactionNotifier: make(chan struct{}),
		wgMu:               &sync.RWMutex{},
		wg:                 &sync.WaitGroup{},
	}
	// start the background thread
	//lsm.GoAttach(func() { lsm.inner.StartFlushWorker() })
	lsm.GoAttach(func() { lsm.inner.StartCompactionWorker(context.Background()) })

	return lsm, nil
}

func (m *MiniLsm) GoAttach(f func()) {
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

func (m *MiniLsm) Put(key, value []byte) error {
	return m.inner.put(key, value)
}

//	func (m *MiniLsm) Get(key []byte) ([]byte, error) {
//		pbKey := pb.Key{Key: key}
//		v, err := m.inner.get(&pbKey)
//		if err != nil {
//			return nil, err
//		}
//		return v.Value, nil
//	}
func (m *MiniLsm) Get(key []byte) ([]byte, error) {
	tx := NewReadTx(m.inner, m.mvcc, false)
	defer tx.Rollback()
	return tx.Get(key)
}

// update functional transactions automatic processing rollback and commit
func (m *MiniLsm) Update(fn func(tx WriteTx) error) error {
	tx := NewWriteTx(m.inner, m.mvcc, false)
	defer tx.Rollback()

	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

//func (m *MiniLsm) Delete(key []byte) error {
//	return m.inner.delete(key)
//}

//	func (m *MiniLsm) Scan(start, end []byte) (*Iterator, error) {
//		return m.inner.scan(start, end)
//	}
//
// Delete implements logical deletion by writing a tombstone record
func (m *MiniLsm) Delete(key []byte) error {
	if len(key) == 0 {
		return errors.New("empty key")
	}

	// Create a deletion marker using an empty value
	// The Delete operation is implemented as a Put with a special deletion marker
	//pbKey, err := proto.Marshal(&pb.Key{
	//	Key:       key,
	//	Timestamp: uint64(time.Now().UnixNano()),
	//})
	//if err != nil {
	//	return fmt.Errorf("failed to marshal key: %w", err)
	//}
	//
	//// Create a value with deletion marker
	//pbValue, err := proto.Marshal(&pb.Value{
	//	Value:     nil,
	//	IsDeleted: true,
	//})
	//if err != nil {
	//	return fmt.Errorf("failed to marshal value: %w", err)
	//}
	pbKey := &pb.Key{
		Key:       key,
		Timestamp: uint64(time.Now().UnixNano()),
	}

	// Create a value with deletion marker
	pbValue := &pb.Value{
		Value:     nil,
		IsDeleted: true,
	}

	// Use Put operation to writeTx the deletion marker
	return m.inner.state.memTable.Put(pbKey,
		pbValue)
}

// Scan creates an iterator to scan over a range of keys
func (m *MiniLsm) Scan(start, end []byte) (*Iterator[iterators.StorageIterator], error) {
	return m.inner.Scan(start, end)
}
