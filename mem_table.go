package mini_lsm

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/huandu/skiplist"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"log"
	"math"
	"mini_lsm/iterators"
	"mini_lsm/pb"
	"mini_lsm/table"
	"reflect"
	"sync"
)

type MemTable interface {
	Get(key *pb.Key) (*pb.Value, bool)
	Put(key *pb.Key, value *pb.Value) error
	Delete(key *pb.Key) error
	PutBatch(entries []*KeyValue) error
	Scan(start, end []byte) iterators.StorageIterator

	// Storage operations
	Flush(builder *table.SsTableBuilder) error
	SyncWAL() error

	// Metadata methods
	ID() uint32
	ApproximateSize() uint32
	Ts() uint64
	Empty() bool
	//IsEmpty() bool
}

type KeyValue struct {
	Key   *pb.Key
	Value *pb.Value
}
type SklElement struct {
	Key       []byte
	Version   uint64
	Timestamp uint64
	Value     []byte
}
type MTable struct {
	mu                  sync.Mutex
	skl                 *skiplist.SkipList
	id, approximateSize uint32
	wal                 *Wal
	logger              *zap.Logger
	maxTs               uint64
	mustSync            bool
}

var ErrNullKey = errors.New("key or value is nil")

// Create a new one MemTable

func NewMTable(logger *zap.Logger, id uint32, walPath string, mustSync bool) *MTable {
	list := createSkl()
	wal := NewWal(logger, walPath, DefaultWalOptions())
	return &MTable{
		mu:       sync.Mutex{},
		id:       id,
		skl:      list,
		wal:      wal,
		logger:   logger,
		mustSync: mustSync,
	}
}

func createSkl() *skiplist.SkipList {
	list := skiplist.New(skiplist.LessThanFunc(func(k1, k2 interface{}) int {
		k1E, k2E := k1.(SklElement), k2.(SklElement)
		res := bytes.Compare(k1E.Key, k2E.Key)
		if res != 0 {
			return -res
		}
		if k1E.Timestamp < k2E.Timestamp {
			return -1
		} else if k1E.Timestamp > k2E.Timestamp {
			return 1
		}
		return 0
	}))
	if list == nil {
		log.Fatal("cannot create skiplist")
	}
	return list
}
func (m *MTable) getWalIndex() (uint64, uint64) {
	begin, err := m.wal.FirstIndex()
	if err != nil {
		m.logger.Panic(err.Error())
	}
	end, err := m.wal.LastIndex()
	if err != nil {
		m.logger.Panic(err.Error())
	}
	return begin, end
}

func (m *MTable) readFromWal() {
	begin, end := m.getWalIndex()
	for ; begin <= end; begin++ {
		d, err := m.wal.Read(begin)
		if err != nil {
			m.logger.Panic(err.Error())
		}

		kv := pb.KeyValue{}
		err = proto.Unmarshal(d, &kv)
		if err != nil {
			m.logger.Panic("read wal failed", zap.Uint64("index", begin), zap.Error(err))
		}
		pbKey := pb.Key{}
		err = proto.Unmarshal(kv.Key, &pbKey)
		if err != nil {
			m.logger.Fatal("read wal failed", zap.Uint64("index", begin), zap.Error(err))
		}
		//key := keyUnmarshal(kv.Key)

		element := SklElement{
			Key:       pbKey.Key,
			Version:   pbKey.Version,
			Timestamp: pbKey.Timestamp,
		}
		m.maxTs = element.Timestamp
		m.skl.Set(element, kv.Value)
	}
}

// Create MTable from WAL

func NewMTableWithWAL(logger *zap.Logger, id uint32, path string, mustSync bool) (*MTable, error) {
	t := NewMTable(logger, id, path, mustSync)
	t.readFromWal()
	return t, nil
}

// 从 WAL 恢复 MTable
func NewMTablexx(id uint64, path string) (*MTable, error) {
	// TODO: 实现恢复逻辑
	return nil, nil
}

func (m *MTable) Empty() bool {
	return m.approximateSize == 0
}

func (m *MTable) Get(key *pb.Key) (*pb.Value, bool) {
	if key == nil {
		return nil, false
	}
	// Convert pb.Key to SklElement for internal usage
	element := pbKeytoSkl(key)

	// Use existing internal implementation
	rawValue, found := m.getInternal(element)
	if !found {
		return nil, false
	}

	// Unmarshal the value
	var value pb.Value
	if err := proto.Unmarshal(rawValue, &value); err != nil {
		return nil, false
	}

	return &value, true
}
func pbKeytoSkl(key *pb.Key) SklElement {
	keyCopy := make([]uint8, len(key.Key))
	copy(keyCopy, key.Key)
	return SklElement{
		Key:       keyCopy,
		Version:   key.Version,
		Timestamp: key.Timestamp,
	}
}

// internalGet retrieves a value from the skiplist using the internal SklElement representation
func (m *MTable) getInternal(key SklElement) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	v := m.skl.Find(key)
	if v == nil {
		return nil, false
	}
	if keystr, ok := v.Key().(SklElement); !ok {
		return nil, false
	} else {
		if bytes.Compare(keystr.Key, key.Key) != 0 {
			return nil, false
		}
	}

	// Extract value
	valueBytes, ok := v.Value.([]byte)
	if !ok {
		m.logger.Error("value in skiplist is not a byte slice",
			zap.Any("key", key),
			zap.Any("value_type", reflect.TypeOf(v.Value)))
		return nil, false
	}

	return valueBytes, true
}

func (m *MTable) Put(key *pb.Key, value *pb.Value) error {
	// Marshal the key and value for storage
	if key == nil || value == nil {
		return ErrNullKey
	}
	return m.putInternal(key, value)

}

func (m *MTable) Delete(key *pb.Key) error {
	// Marshal the key and value for storage
	if key == nil {
		return ErrNullKey
	}
	// Create a value with deletion marker
	pbValue := &pb.Value{
		Value:     nil,
		IsDeleted: true,
	}
	return m.putInternal(key, pbValue)

}

func (m *MTable) Ts() uint64 {
	// Marshal the key and value for storage
	return m.maxTs

}

// putInternal is the internal implementation of Put that works with raw KeyValue proto messages
func (m *MTable) putInternal(key *pb.Key, value *pb.Value) error {
	// Marshal for the WAL
	keyBytes, err := proto.Marshal(key)
	if err != nil {
		return err
	}

	valueBytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}

	keyVal := &pb.KeyValue{Key: keyBytes, Value: valueBytes, MemTableID: uint64(m.id)}
	write2wal, err := proto.Marshal(keyVal)
	if err != nil {
		return fmt.Errorf("failed to marshal KeyValue for WAL: %w", err)
	}

	// Async write to WAL
	//err = m.wal.WriteSync(write2wal)
	resultCh, err := m.wal.WriteAsync(write2wal)

	if err != nil {
		fmt.Printf("failed to write to WAL: %s", err.Error())
		return err
	}

	// Start goroutine to monitor write result (optional)
	go func() {
		if err := <-resultCh; err != nil {
			// Handle WAL write failure, such as logging or adding retry logic
			m.logger.Error("async WAL write failed", zap.Error(err))
			//return err
			// In extreme cases, consider removing key-value pair from memory table or triggering emergency flush
		}
	}()
	element := pbKeytoSkl(key)

	m.mu.Lock()
	defer m.mu.Unlock()
	// Store in skiplist
	m.skl.Set(element, valueBytes)

	// Update approximate size estimation
	// This is optional but helps with determining when to flush
	m.approximateSize += element.size() + uint32(len(value.Value))

	return nil
}

func (s *SklElement) size() uint32 {
	return uint32(len(s.Key) + 16)
}

// PutBatch 方法
func (m *MTable) PutBatch(data []*KeyValue) error {
	batchData := pb.KeyValueBatch{Entries: make([]*pb.KeyValue, len(data))}
	for i, ele := range data {
		keyBytes, err := proto.Marshal(ele.Key)
		if err != nil {
			return err
		}
		valueBytes, err := proto.Marshal(ele.Value)
		if err != nil {
			return err
		}
		keyVal := &pb.KeyValue{Key: keyBytes, Value: valueBytes, MemTableID: uint64(m.id)}
		batchData.Entries[i] = keyVal
	}
	write2wal, err := proto.Marshal(&batchData)
	if err != nil {
		fmt.Printf("failed to write to WAL: %s", err.Error())
		return err
	}
	// Async write to WAL
	//err = m.wal.WriteSync(write2wal)
	if m.mustSync {
		err = m.wal.WriteSync(write2wal)
		if err != nil {
			fmt.Printf("failed to write to WAL: %s", err.Error())
			return err
		}
	} else {
		c, err := m.wal.WriteAsync(write2wal)
		if err != nil {
			fmt.Printf("failed to write to WAL: %s", err.Error())
			return err
		}
		// Start goroutine to monitor write result (optional)
		go func() {
			if err := <-c; err != nil {
				// Handle WAL write failure, such as logging or adding retry logic
				m.logger.Error("async WAL write failed", zap.Error(err))
				//return err
				// In extreme cases, consider removing key-value pair from memory table or triggering emergency flush
			}
		}()
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	for i, keyVal := range data {
		element := pbKeytoSkl(keyVal.Key)
		m.skl.Set(element, data[i].Value)
		m.approximateSize += element.size() + uint32(len(data[i].Value.Value))
	}
	// This is optional but helps with determining when to flush
	return nil

}

func (s *SklElement) marshal() []byte {
	length := len(s.Key) + 16
	out := make([]byte, length)
	copy(out, s.Key)
	binary.BigEndian.PutUint64(out[len(s.Key):], s.Version)

	// ^ for compare
	binary.BigEndian.PutUint64(out[len(s.Key)+8:], ^s.Timestamp)
	return out
}

func unmarshalSklElement(data []byte) *SklElement {
	length := len(data)
	if length < 16 {
		// 返回空元素或错误处理
		return &SklElement{}
	}

	keyLength := length - 16
	element := &SklElement{
		Key:       make([]byte, keyLength),
		Version:   binary.BigEndian.Uint64(data[keyLength : keyLength+8]),
		Timestamp: ^binary.BigEndian.Uint64(data[keyLength+8:]), // 注意这里对时间戳取反
		Value:     nil,
	}

	copy(element.Key, data[:keyLength])
	return element
}

// Flush 方法
func (m *MTable) Flush(builder *table.SsTableBuilder) error {
	for elem := m.skl.Front(); elem != nil; elem = elem.Next() {
		ele, ok := elem.Key().(SklElement)
		if !ok {
			panic("invalid element")
		}
		err := builder.Add((&ele).marshal(), elem.Value.([]byte))
		if err != nil {
			m.logger.Error("flush failed", zap.Error(err))
			return err
		}
	}
	return nil
}

func (m *MTable) SyncWAL() error {
	if m.wal != nil {
		return m.wal.Sync()
	}
	return nil
}

func (m *MTable) ID() uint32 {
	return m.id
}

func (m *MTable) ApproximateSize() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.approximateSize
}

//
//	func (m *MTable) IsEmpty() bool {
//		return m.map.IsEmpty()
//	}
//

// 确保 MTableIterator 实现了 StorageIterator 接口
var _ iterators.StorageIterator = (*MTableIterator)(nil)

// MTableIterator implements the MemTableIterator interface
type MTableIterator struct {
	skl  *skiplist.SkipList
	item struct {
		key   []byte
		value []byte
	}
	current *skiplist.Element
	end     []byte
}

// internalGet retrieves a value from the skiplist using the internal SklElement representation
//func (m *MTable) internalGet(key SklElement) ([]byte, bool) {
//	m.mu.Lock()
//	defer m.mu.Unlock()
//
//	// Look up in skiplist
//	v := m.skl.Get(key)
//	if v == nil {
//		return nil, false
//	}
//
//	// Extract value
//	valueBytes, ok := v.Value.([]byte)
//	if !ok {
//		m.logger.Error("value in skiplist is not a byte slice",
//			zap.Any("key", key),
//			zap.Any("value_type", reflect.TypeOf(v.Value)))
//		return nil, false
//	}
//
//	return valueBytes, true
//}

// NewMTableIterator creates a new iterator for the memtable
func NewMTableIterator(skl *skiplist.SkipList, start, end []byte) *MTableIterator {
	iter := &MTableIterator{
		skl: skl,
		end: end,
	}

	// If start key is provided, seek to it
	if len(start) > 0 {
		startElement := SklElement{
			Key:       start,
			Version:   0,
			Timestamp: math.MaxUint64,
		}
		iter.current = skl.Find(startElement)
	} else {
		// Start from the beginning if no start key
		iter.current = skl.Front()
	}

	// Initialize current item if valid
	if iter.current != nil {
		iter.updateItem()
	}

	return iter
}

// Implement the StorageIterator interface

func (it *MTableIterator) IsValid() bool {
	if it.current == nil {
		return false
	}

	// Check if we've reached the end key
	if it.end != nil {
		currentElement := it.current.Key().(SklElement)
		if bytes.Compare(currentElement.Key, it.end) >= 0 {
			return false
		}
	}

	return true
}

func (it *MTableIterator) Key() []byte {
	if !it.IsValid() {
		return nil
	}
	return it.item.key
}

func (it *MTableIterator) Value() []byte {
	if !it.IsValid() {
		return nil
	}
	return it.item.value
}

func (it *MTableIterator) Next() error {
	if !it.IsValid() {
		return nil
	}

	it.current = it.current.Next()
	if it.current != nil {
		it.updateItem()
	}

	return nil
}

func (it *MTableIterator) updateItem() {
	element := it.current.Key().(SklElement)
	value := it.current.Value.([]byte)

	it.item.key = element.Key
	it.item.value = value
}

// Add Scan method to MTable
func (m *MTable) Scan(start, end []byte) iterators.StorageIterator {
	m.mu.Lock()
	defer m.mu.Unlock()

	return NewMTableIterator(m.skl, start, end)
}

func (it *MTableIterator) Close() error {
	return nil
}

// NumActiveIterators implements the StorageIterator interface
func (it *MTableIterator) NumActiveIterators() int {
	return 1
}
