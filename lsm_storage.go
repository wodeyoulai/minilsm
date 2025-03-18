package mini_lsm

import (
	"bytes"
	"context"
	"fmt"
	"google.golang.org/protobuf/proto"
	"mini_lsm/iterators"
	"mini_lsm/pb"
	"mini_lsm/table"
	"mini_lsm/tools/fileutil"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"go.uber.org/zap"
)

// WriteBatchRecord indicates an operation in a writeTx batch
type WriteBatchRecord struct {
	Key   []byte
	Value []byte
	Type  WriteBatchType // PUT or DELETE
}

type WriteBatchType int

const (
	WriteBatchPut WriteBatchType = iota
	WriteBatchDelete
)

var ErrKeyDeleted = errors.New("key is deleted")

// LsmStorageState indicates the status of the storage engine
type LsmStorageState struct {
	lg           *zap.Logger
	memTable     MemTable    // currently active memtable
	immMemTables []MemTable  // Immutable memtables, sorted from new to old
	l0SSTables   []uint32    // sstable file id of l0 layer
	levels       []LevelMeta // metadata for other layers
	//ssTables     map[uint32]*SSTable // sstable object mapping table
	mu sync.RWMutex
}

func (l *LsmStorageState) getLastImmTable() MemTable {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.immMemTables) > 0 {
		return l.immMemTables[len(l.immMemTables)-1]
	}
	return nil
}
func (l *LsmStorageState) clearLastImmTable() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.immMemTables) > 0 {
		l.immMemTables = l.immMemTables[:len(l.immMemTables)-1]
	}
}

type LevelMeta struct {
	level    uint32
	ssTables []uint32 // the sstable file id of this layer
}

// LsmStorageOptions storage engine configuration options
type LsmStorageOptions struct {
	BlockSize      uint32
	TargetSSTSize  uint32
	MemTableLimit  uint32
	CompactionOpts CompactionOptions
	EnableWAL      bool
	Serializable   bool
	Levels         uint32
}

type CompactionStrategy int

const (
	NoCompaction CompactionStrategy = iota
	LeveledCompaction
	TieredCompaction
	SimpleLeveledCompaction
)

// LsmStorageInner internal implementation of storage engine
type LsmStorageInner struct {
	readTx  ReadTx
	writeTx WriteTx

	lg                    *zap.Logger
	state                 *LsmStorageState
	stateLock, nextIDLock sync.Mutex
	path                  string
	//include memtable id

	nextSSTableID                       atomic.Uint32
	options                             *LsmStorageOptions
	compactionCtrl                      CompactionController
	stopping, forceFullCompactionNotify chan struct{}

	// record sstable id -> iterator
	openedIterators map[uint32]iterators.StorageIterator
	openedSstables  map[uint32]*table.SSTable

	manifest *Manifest
	//timestamp atomic.Uint64
	//compactionFilter []CompactionFilter
}

// newLsmStorageInner creates a new internal LSM storage engine instance
func newLsmStorageInner(logger *zap.Logger, path string, opts *LsmStorageOptions) (*LsmStorageInner, error) {
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}
	if path == "" {
		return nil, errors.New("storage path cannot be empty")
	}
	if opts == nil {
		return nil, errors.New("storage options cannot be nil")
	}

	// Create directories if they don't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, fmt.Errorf("failed to create storage directory: %w", err)
	}

	// Initialize storage inner structure
	inner := &LsmStorageInner{
		lg:      logger,
		path:    path,
		options: opts,
		state: &LsmStorageState{
			lg:           logger,
			memTable:     nil, // Will be created during recovery or initialization
			immMemTables: make([]MemTable, 0),
			l0SSTables:   make([]uint32, 0),
			levels:       make([]LevelMeta, opts.Levels),
		},
		stateLock:  sync.Mutex{},
		nextIDLock: sync.Mutex{},

		stopping:                  make(chan struct{}),
		forceFullCompactionNotify: make(chan struct{}),
		openedIterators:           make(map[uint32]iterators.StorageIterator),
		openedSstables:            make(map[uint32]*table.SSTable),
	}

	// Select compaction controller based on strategy
	switch opts.CompactionOpts.Strategy {
	// case TaskTypeSimple:
	// 	inner.compactionCtrl = NewDefaultSimpleLeveledCompactionController()
	// todo other strategy
	// case TaskTypeLeveled:
	//	inner.compactionCtrl = &LeveledCompactionController{
	//		opts: *opts.CompactionOpts.LeveledOpts,
	//	}
	//case TaskTypeTiered:
	//	inner.compactionCtrl = &TieredCompactionController{
	//		opts: *opts.CompactionOpts.TieredOpts,
	//	}
	//case TaskTypeForceFullCompaction:
	//	inner.compactionCtrl = &ForceFullCompactionController{
	//		opts: *opts.CompactionOpts.ForceFullOpts,
	//	}
	default:
		// Default to SimpleLeveled if not specified
		inner.compactionCtrl = NewDefaultSimpleLeveledCompactionController(opts.CompactionOpts.SimpleOpts)
	}

	// Recover state from manifest
	if err := inner.recoverFromManifest(); err != nil {
		return nil, fmt.Errorf("failed to recover from manifest: %w", err)
	}

	// Create MVCC instance
	mvcc := NewLsmMvccInner(uint64(time.Now().UnixNano()), nil)

	// Initialize transactions
	inner.readTx = NewReadTx(inner, mvcc, opts.Serializable)
	inner.writeTx = NewWriteTx(inner, mvcc, opts.Serializable)

	return inner, nil
}
func (l *LsmStorageInner) Scan(start, end []byte) (*Iterator[iterators.StorageIterator], error) {
	if len(start) > 0 && len(end) > 0 && bytes.Compare(start, end) >= 0 {
		return nil, errors.New("invalid range: start key must be less than end key")
	}

	// 1. Create iterators for all components that might contain the data

	// Create memtable iterator
	memIter := l.state.memTable.(*MTable).Scan(start, end)

	// Create iterators for immutable memtables
	var immIters []iterators.StorageIterator
	for _, imm := range l.state.immMemTables {
		immIter := imm.(*MTable).Scan(start, end)
		if immIter != nil {
			immIters = append(immIters, immIter)
		}
	}

	// Merge immutable memtable iterators
	var immMergeIter iterators.StorageIterator
	if len(immIters) > 0 {
		immMergeIter = iterators.NewMergeIterator(immIters)
	}

	// Create iterators for L0 SSTables
	var l0Iters []iterators.StorageIterator
	for i := len(l.state.l0SSTables) - 1; i >= 0; i-- {
		sstID := l.state.l0SSTables[i]
		sst, err := l.getOrOpenSSTable(sstID)
		if err != nil {
			return nil, fmt.Errorf("failed to open L0 SSTable %d: %w", sstID, err)
		}

		if !isRangeOverlap(start, end, sst.FirstKey(), sst.LastKey()) {
			continue
		}

		iter, err := table.CreateAndSeekToKey(sst, start)
		if err != nil {
			return nil, fmt.Errorf("failed to create iterator for L0 SSTable %d: %w", sstID, err)
		}
		l0Iters = append(l0Iters, iter)
	}

	// Merge L0 iterators
	var l0MergeIter iterators.StorageIterator
	if len(l0Iters) > 0 {
		l0MergeIter = iterators.NewMergeIterator(l0Iters)
	}

	// Create iterators for other levels
	var levelIters []iterators.StorageIterator
	for levelIdx, level := range l.state.levels {
		var levelSSTables []*table.SSTable
		for _, sstID := range level.ssTables {
			sst, err := l.getOrOpenSSTable(sstID)
			if err != nil {
				return nil, fmt.Errorf("failed to open SSTable %d in level %d: %w", sstID, levelIdx, err)
			}

			if isRangeOverlap(start, end, sst.FirstKey(), sst.LastKey()) {
				levelSSTables = append(levelSSTables, sst)
			}
		}

		if len(levelSSTables) > 0 {
			iter, err := table.NewSstConcatIteratorSeek(levelSSTables, start)
			if err != nil {
				return nil, fmt.Errorf("failed to create iterator for level %d: %w", levelIdx, err)
			}
			levelIters = append(levelIters, iter)
		}
	}

	// Merge all level iterators
	var levelMergeIter iterators.StorageIterator
	if len(levelIters) > 0 {
		levelMergeIter = iterators.NewMergeIterator(levelIters)
	}

	// 2. Create final merge iterator combining all components
	var iters []iterators.StorageIterator
	if memIter != nil {
		iters = append(iters, memIter)
	}
	if immMergeIter != nil {
		iters = append(iters, immMergeIter)
	}
	if l0MergeIter != nil {
		iters = append(iters, l0MergeIter)
	}
	if levelMergeIter != nil {
		iters = append(iters, levelMergeIter)
	}

	if len(iters) == 0 {
		return nil, errors.New("no iterators found")
	}

	return NewIterator(iters, start, end, true), nil
}

// Helper function to check if two ranges overlap
func isRangeOverlap(start1, end1, start2, end2 []byte) bool {
	// If either range is unbounded (nil), they might overlap
	if start1 == nil || end1 == nil || start2 == nil || end2 == nil {
		return true
	}

	// Check if ranges overlap
	return bytes.Compare(start1, end2) <= 0 && bytes.Compare(start2, end1) <= 0
}

func (l *LsmStorageInner) recoverFromManifest() error {
	// 1. read manifest record
	manifest, records, err := RecoverManifest(l.path)
	if err != nil {
		return err
	}

	// 2. Play back the record reconstruction status in sequence
	for _, record := range records {
		switch record.Type {
		case "memtable":
			// record the largest memtable id
			l.updateNextTableID(record.MemtableID)

		case "flush":
			// add the corresponding sst to l0
			err = l.addL0SSTable(record.FlushID)

		case "compaction":
			// update the hierarchy of the lsm tree
			err = l.applyCompactionFromManifest(record.CompactionTask)
		case "freeze_memtable":
			//todo
		}
	}
	l.manifest = manifest
	// 3. create a new memtable for writing
	return l.createNewMemtable()
}

func (l *LsmStorageInner) createNewMemtable() error {
	id := l.nextSSTableID.Load()
	l.state.memTable = NewMTable(l.lg, id, l.walPath(l.nextSSTableID.Load()))
	l.nextSSTableID.Add(1)
	// log events that create a new memtable to manifest
	record := NewMemtableRecord(id)
	if err := l.manifest.AddRecord(record); err != nil {
		return err
	}
	return nil
}

func (l *LsmStorageInner) applyCompactionFromManifest(task CompactionTask) error {

	compactLevel := task.UpperLevel()
	if compactLevel >= l.options.Levels-1 {
		l.lg.Fatal("overflow max levels")
	}
	upperMeta := LevelMeta{
		level:    compactLevel,
		ssTables: make([]uint32, 0),
	}
	lowerMeta := LevelMeta{
		level:    compactLevel + 1,
		ssTables: task.OutputSstables(),
	}
	l.state.levels[compactLevel] = upperMeta
	l.state.levels[compactLevel+1] = lowerMeta
	return nil
}
func (l *LsmStorageInner) addL0SSTable(id uint32) error {
	// open the sst file
	f, err := fileutil.OpenFileObject(l.sstPath(id))
	if err != nil {
		l.lg.Fatal("manifest file broken")
	}
	sst, err := table.OpenSSTable(id, nil, f)
	if err != nil {
		l.lg.Fatal("manifest file broken")
	}
	// add to memory map
	l.openedSstables[id] = sst

	// join L0
	// l.state.levels[0].ssTables = append(l.state.levels[0].ssTables, id)
	l.state.l0SSTables = append(l.state.l0SSTables, id)
	return nil
}

func (l *LsmStorageInner) updateNextTableID(id uint32) {
	l.nextSSTableID.Store(id + 1)
}

func (l *LsmStorageInner) put(key, value []byte) error {
	l.stateLock.Lock()
	defer l.stateLock.Unlock()

	// check if memtable needs to be frozen
	if l.state.memTable.ApproximateSize() >= l.options.TargetSSTSize {
		//fmt.Printf("in free key %s\n", key)
		if err := l.freezeMemTable(); err != nil {
			return err
		}
	}
	pbKey := &pb.Key{Key: key}
	pbValue := &pb.Value{Value: value}
	// writeTx to memtable
	return l.state.memTable.Put(pbKey, pbValue)
}

// memTable reaches the threshold, lock and create a new one
func (l *LsmStorageInner) freezeMemTable() error {
	//l.state.mu.Lock()
	//defer l.state.mu.Unlock()
	id := l.nextSSTableID.Load()
	newMemTable := NewMTable(l.state.lg, l.nextIdAndCrease(), l.path)

	newImtables := make([]MemTable, len(l.state.immMemTables)+1)
	if len(l.state.immMemTables) > 0 {
		copy(newImtables[1:], l.state.immMemTables)
	}
	l.state.immMemTables = newImtables
	l.state.immMemTables[0] = l.state.memTable
	l.state.memTable = newMemTable

	r := NewFreezeMemtableRecord(id)
	if err := l.manifest.AddRecord(r); err != nil {
		return err
	}
	return nil
}

func (l *LsmStorageInner) getFirstKeyOfSStableId(id uint32) ([]byte, error) {
	// if exists in buffer
	if iter, ok := l.openedIterators[id]; ok {
		err := iter.(*table.SsTableIterator).SeekToFirst()
		if err != nil {
			panic("seek to first failed")
			return nil, err
		}
		return iter.(*table.SsTableIterator).Key(), nil
	}
	//  not exists,make it

	bufferdTable := &table.SSTable{}
	if sstable, ok := l.openedSstables[id]; ok {
		bufferdTable = sstable
	} else {
		absPath := path.Join(l.path, strconv.Itoa(int(id))+".sst")
		f, err := fileutil.OpenFileObject(absPath)

		if err != nil {
			panic("file does not exist")
		}
		newSstable, err := table.OpenSSTable(id, nil, f)
		if err != nil {
			panic("sstable does not exist")
		}
		bufferdTable = newSstable
		l.openedSstables[id] = bufferdTable
	}

	it, err := table.CreateAndSeekToFirst(bufferdTable)
	if err != nil {
		panic("sstable does not exist")
	}
	l.openedIterators[id] = it
	return it.Key(), nil
}

func (l *LsmStorageInner) get(key *pb.Key) (*pb.Value, error) {
	if key == nil {
		return nil, errors.New("nil key")
	}

	// Convert key to internal format for comparison
	encodedKey := keyMarshal(key)

	// 1. Check memtable
	//e := SklElement{
	//	Key:       key.Key,
	//	Version:   key.Version,
	//	Timestamp: key.Timestamp,
	//	Value:     nil,
	//}
	if value, ok := l.state.memTable.Get(key); ok {
		if !value.IsDeleted {
			return value, nil
		}
		return nil, ErrKeyDeleted
	}

	// 2. Check immutable memtables from newest to oldest
	for _, imm := range l.state.immMemTables {
		if value, ok := imm.Get(key); ok {
			if !value.IsDeleted {
				return value, nil
			}
			return nil, ErrKeyDeleted
		}
	}

	// 3. Check L0 SSTables (newest to oldest as they may overlap)
	for i := len(l.state.l0SSTables) - 1; i >= 0; i-- {
		sstID := l.state.l0SSTables[i]
		sst, err := l.getOrOpenSSTable(sstID)
		if err != nil {
			return nil, fmt.Errorf("failed to open L0 SSTable %d: %w", sstID, err)
		}

		// Check if key might be in this SSTable
		if sst.MayContain(encodedKey) {
			iter, err := table.CreateAndSeekToKey(sst, encodedKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create iterator for L0 SSTable %d: %w", sstID, err)
			}

			if iter.IsValid() && bytes.Equal(iter.Key(), encodedKey) {
				v := pb.Value{}
				proto.Unmarshal(iter.Value(), &v)
				return &v, nil
			}
		}
	}

	// 4. Check other levels (they are sorted and non-overlapping)
	for levelIdx, level := range l.state.levels {
		// Binary search to find potential SSTable in this level
		sstables := level.ssTables
		left, right := 0, len(sstables)-1

		for left <= right {
			mid := (left + right) / 2
			sst, err := l.getOrOpenSSTable(sstables[mid])
			if err != nil {
				return nil, fmt.Errorf("failed to open SSTable %d in level %d: %w", sstables[mid], levelIdx, err)
			}

			// Check if key might be in this SSTable's range
			if !sst.MayContain(encodedKey) {
				// Adjust search range based on SSTable's key range
				if bytes.Compare(encodedKey, sst.FirstKey()) < 0 {
					right = mid - 1
				} else {
					left = mid + 1
				}
				continue
			}

			// Key might be in this SSTable, try to find it
			iter, err := table.CreateAndSeekToKey(sst, encodedKey)
			if err != nil {
				return nil, fmt.Errorf("failed to create iterator for SSTable %d: %w", sstables[mid], err)
			}

			if iter.IsValid() && bytes.Equal(iter.Key(), encodedKey) {
				v := pb.Value{}
				proto.Unmarshal(iter.Value(), &v)
				return &v, nil
			}

			// Key not found in this SSTable, continue binary search
			if bytes.Compare(encodedKey, sst.FirstKey()) < 0 {
				right = mid - 1
			} else {
				left = mid + 1
			}
		}
	}

	return nil, errors.New("key not found")
}

// Helper function to get or open an SSTable
func (l *LsmStorageInner) getOrOpenSSTable(id uint32) (*table.SSTable, error) {
	// Check if SSTable is already opened
	if sst, ok := l.openedSstables[id]; ok {
		return sst, nil
	}

	// Open the SSTable
	f, err := fileutil.OpenFileObject(l.sstPath(id))
	if err != nil {
		return nil, fmt.Errorf("failed to open SSTable file: %w", err)
	}

	sst, err := table.OpenSSTable(id, nil, f)
	if err != nil {
		f.File.Close()
		return nil, fmt.Errorf("failed to open SSTable: %w", err)
	}

	// Cache the opened SSTable
	l.openedSstables[id] = sst
	return sst, nil
}

// 工具方法
func (l *LsmStorageInner) sstPath(id uint32) string {
	return filepath.Join(l.path, fmt.Sprintf("%05d.sst", id))
}

func (l *LsmStorageInner) walPath(id uint32) string {
	return filepath.Join(l.path, fmt.Sprintf("%05d.wal", id))
}

func (l *LsmStorageInner) manifestPath() string {
	return filepath.Join(l.path, "MANIFEST")
}
func (l *LsmStorageInner) compact(task *CompactionTask) ([]*table.SSTable, error) {
	// TODO: implement
	return nil, nil
}

func (l *LsmStorageInner) forceFullCompaction() error {
	ctr := NewDefaultSimpleLeveledCompactionController(l.options.CompactionOpts.SimpleOpts)
	task, err := ctr.GenerateCompactionTask(l.state)
	if err != nil || task == nil {
		l.lg.Info("no need for compaction")
		return nil
	}
	if task.UpperLevel() == 0 {
		err = l.l0Compaction(task)
	} else {
		err = l.nonL0Compaction(task)
	}
	if err != nil {
		l.lg.Error("compaction task failed", zap.Error(err))
		return err
	}

	err = l.compactionCtrl.ApplyCompactionResult(l.state, task, false)
	if err != nil {
		l.lg.Error("compaction task failed", zap.Error(err))
		return err
	}

	if task.UpperLevel() == 0 {
		sort.Slice(l.state.levels[0].ssTables, func(i, j int) bool {
			//  get the first key of an SST
			return l.state.levels[0].ssTables[i] < l.state.levels[0].ssTables[j]
		})
	} else {
		sort.Slice(l.state.levels[task.UpperLevel()].ssTables, func(i, j int) bool {
			//  get the first key of an SST
			return l.state.levels[task.UpperLevel()].ssTables[i] < l.state.levels[task.UpperLevel()].ssTables[j]
		})
	}

	record := NewCompactionRecord(task)
	if err := l.manifest.AddRecord(record); err != nil {
		l.lg.Fatal("add record to manifest failed", zap.Error(err))
	}
	// Sort L1 files by first key if not in recovery mode
	// clean-old-sst-files
	if err := l.cleanupObsoleteFiles(task); err != nil {
		l.lg.Fatal("clean compaction files failed", zap.Error(err))
	}
	return nil
}

func (l *LsmStorageInner) cleanupObsoleteFiles(task CompactionTask) error {
	cleanIDs := append(task.UpperSstables(), task.LowerSstables()...)
	errRes := error(nil)
	for _, id := range cleanIDs {
		path := l.sstPath(uint32(id))
		if err := os.Remove(path); err != nil {
			errRes = err
			l.lg.Error("cleanup obsolete files failed", zap.Error(err))
		}
	}
	return errRes
}

func (l *LsmStorageInner) compactionTwoLevels(task CompactionTask, upper, lower iterators.StorageIterator) error {
	// create two layer iterator merge
	twoMergeIter, err := iterators.NewTwoMergeIterator(upper, lower)
	if err != nil {
		l.lg.Error("create iterator failed", zap.Error(err))
		return err
	}
	// create a new sstable Builder
	builder := table.NewSsTableBuilder(l.options.BlockSize)

	outPutIDS := []uint32{}
	// Iterate through the merged iterator and writeTx to a new SSTable

	count := 0
	for twoMergeIter.IsValid() {
		count++
		// writeTx key and value to builder
		if err := builder.Add(twoMergeIter.Key(), twoMergeIter.Value()); err != nil {
			return err
		}
		// If the builder reaches the size limit, flush is required and a new builder is created
		if builder.EstimatedSize() >= l.options.TargetSSTSize {
			// flush current builder
			id := l.nextIdAndCrease()
			newSst, err := builder.Build(id, nil, l.sstPath(id))
			if err != nil {
				return err
			}
			outPutIDS = append(outPutIDS, id)
			l.openedSstables[id] = newSst
			// create a new builder to continue
			builder = table.NewSsTableBuilder(l.options.BlockSize)
		}
		if err := twoMergeIter.Next(); err != nil {
			return err
		}
	}
	fmt.Printf("compaction count %d\n", count)
	// handle the last under full builder
	if builder.EstimatedSize() > 0 {
		id := l.nextIdAndCrease()
		newSst, err := builder.Build(id, nil, l.sstPath(id))
		if err != nil {
			return err
		}
		outPutIDS = append(outPutIDS, id)
		l.openedSstables[id] = newSst
	}
	task.SetOutputSstables(outPutIDS)
	return nil
}

func (l *LsmStorageInner) increaseNextSstableID() {
	l.nextSSTableID.Add(1)
}

func (l *LsmStorageInner) l0Compaction(task CompactionTask) error {
	// l0 use mergeiterator other contant
	// create upper level merge iterator
	tableIds := task.UpperSstables()
	l0Sstables := l.openSSTablesByIds(tableIds)
	l0Iters := make([]iterators.StorageIterator, len(tableIds))
	for i, sstable := range l0Sstables {
		iter, err := table.CreateAndSeekToFirst(sstable)
		if err != nil {
			panic("create iterator failed")
		}
		l0Iters[i] = iter
	}
	mergeIter := iterators.NewMergeIterator(l0Iters)

	// lower level
	lowerSstables := l.openSSTablesByIds(task.LowerSstables())
	contantIters, err := table.NewSstConcatIteratorFirst(lowerSstables)
	if err != nil {
		l.lg.Error("create iterator failed", zap.Error(err))
	}
	return l.compactionTwoLevels(task, mergeIter, contantIters)
}

func (l *LsmStorageInner) openSSTablesByIds(ids []uint32) []*table.SSTable {
	lowTables := make([]*table.SSTable, len(ids))
	for i, id := range ids {
		f, err := fileutil.OpenFileObject(l.sstPath(id))
		if err != nil {
			panic("sstable does not exist")
		}
		sstable, err := table.OpenSSTable(id, nil, f)
		if err != nil {
			panic("open sstable failed")
		}
		lowTables[i] = sstable
	}
	return lowTables
}
func (l *LsmStorageInner) nonL0Compaction(task CompactionTask) error {
	upperSstables := l.openSSTablesByIds(task.UpperSstables())
	lowerSstables := l.openSSTablesByIds(task.LowerSstables())
	upperIter, err := table.NewSstConcatIteratorFirst(upperSstables)
	if err != nil {
		panic("create iterator failed")
	}
	lowerIter, err := table.NewSstConcatIteratorFirst(lowerSstables)
	if err != nil {
		panic("create iterator failed")
	}
	return l.compactionTwoLevels(task, upperIter, lowerIter)
}
func (l *LsmStorageInner) triggerCompaction() error {
	return l.forceFullCompaction()
}

// Background worker management
func (l *LsmStorageInner) StartCompactionWorker(ctx context.Context) error {
	for {
		select {
		case <-l.stopping:
			l.lg.Error("LsmStorageInner is stopping")
		case <-l.forceFullCompactionNotify:
			if err := l.triggerCompaction(); err != nil {
			}
		}
	}
	return nil
}

func (l *LsmStorageInner) StartFlushWorker() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-l.stopping:
			l.lg.Error("LsmStorageInner is stopping")
		case <-ticker.C:
			err := l.triggerFlush()
			if err != nil {
				l.lg.Fatal("flush failed", zap.Error(err))
			}
		}
	}
}
func (l *LsmStorageInner) nextIdAndCrease() uint32 {
	l.nextIDLock.Lock()
	defer l.nextIDLock.Unlock()
	res := l.nextSSTableID.Load()
	l.nextSSTableID.Store(res + 1)
	return res

}

func (l *LsmStorageInner) triggerFlush() error {
	// copy the oldest memtable
	flushTable := l.state.getLastImmTable()
	if flushTable == nil {
		return nil
	}

	// flush to builder
	builder := table.NewSsTableBuilder(l.options.BlockSize)
	err := flushTable.Flush(builder)
	if err != nil {
		l.lg.Error("flush failed", zap.Error(err))
		return err
	}

	// writeTx to file
	id := flushTable.ID()
	path := l.sstPath(id)
	table, err := builder.Build(id, nil, path)
	if err != nil {
		l.lg.Error("flush failed id", zap.Uint32("id", id), zap.Error(err))
		return err
	}
	table.Close()

	// record flush operation to manifest
	record := NewFlushRecord(id)
	if err := l.manifest.AddRecord(record); err != nil {
		return err
	}
	l.state.clearLastImmTable()
	l.state.addToL0(id)
	// notify checking compaction
	l.forceFullCompactionNotify <- struct{}{}
	return nil
}

func (l *LsmStorageState) addToL0(id uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.l0SSTables = append(l.l0SSTables, id)
}
