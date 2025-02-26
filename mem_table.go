package mini_lsm

import (
	"bytes"
	"fmt"
	"github.com/huandu/skiplist"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
	"log"
	"mini_lsm/iterators"
	"mini_lsm/pb"
	"mini_lsm/table"
	"sync"
)

// 核心方法定义
type MemTable interface {
	// 创建相关方法
	//Create(id uint64) *MemTable
	//CreateWithWAL(id uint64, path string) (*MemTable, error)
	//RecoverFromWAL(id uint64, path string) (*MemTable, error)

	// 核心操作方法
	Get(key SklElement) ([]byte, bool)
	Put(value *pb.KeyValue) error
	PutBatch(data []*pb.KeyValue) error
	//SyncWAL() error
	//Scan(lower, upper Bound) *MTableIterator
	Flush(builder *table.SsTableBuilder) error

	// 元数据方法
	ID() uint32
	ApproximateSize() uint32
	//IsEmpty() bool
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
}

// Create a new one MemTable

func NewMTable(logger *zap.Logger, id uint32, walPath string) *MTable {
	list := createSkl()
	wal := NewWal(logger, walPath, nil)
	return &MTable{
		mu:     sync.Mutex{},
		id:     id,
		skl:    list,
		wal:    wal,
		logger: logger,
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
			return 1
		} else if k1E.Timestamp > k2E.Timestamp {
			return -1
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
		key := pb.Key{}
		_ = proto.Unmarshal(kv.Key, &key)
		out := keyMarshal(&key)
		for i := 0; i < len(out); i++ {
			fmt.Print(out[i])
		}
		m.skl.Set(out, kv.Value)
	}
}

// Create MTable from WAL

func NewMTableWithWAL(logger *zap.Logger, id uint32, path string) (*MTable, error) {
	t := NewMTable(logger, id, path)
	t.readFromWal()
	return t, nil
}

// 从 WAL 恢复 MTable
func NewMTablexx(id uint64, path string) (*MTable, error) {
	// TODO: 实现恢复逻辑
	return nil, nil
}

// Get 方法
func (m *MTable) Get(key SklElement) ([]byte, bool) {
	v := m.skl.Get(key)
	if v == nil {
		return nil, false
	}
	return v.Value.([]byte), false
}

// Put 方法
func (m *MTable) Put(value *pb.KeyValue) error {
	write2wal, err := proto.Marshal(value)
	key := pb.Key{}
	proto.Unmarshal(value.Key, &key)
	encodeKey := keyMarshal(&key)

	m.wal.mu.Lock()
	defer m.wal.mu.Unlock()
	last, err := m.wal.LastIndex()
	if err != nil {
		m.logger.Panic("wal died", zap.Error(err))
	}
	m.wal.Write(last+1, write2wal)
	m.skl.Set(encodeKey, value)
	return nil
}

// PutBatch 方法
func (m *MTable) PutBatch(data []*pb.KeyValue) error {
	for _, kv := range data {
		err := m.Put(kv)
		if err != nil {
			m.logger.Error("put failed", zap.String("key", string(kv.Key)), zap.Error(err))
		}
	}
	return nil
}

// Scan 方法
//func (m *MTable) Scan(lower, upper []byte) *MTableIterator {
//	// TODO: 实现范围扫描逻辑
//	return nil
//}
//

// Flush 方法
func (m *MTable) Flush(builder *table.SsTableBuilder) error {
	for elem := m.skl.Front(); elem != nil; elem = elem.Next() {
		err := builder.Add(elem.Key().([]byte), elem.Value.([]byte))
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
			Timestamp: 0,
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

// NumActiveIterators implements the StorageIterator interface
func (it *MTableIterator) NumActiveIterators() int {
	return 1
}
