package table

//
//import (
//	"github.com/wodeyoulai/plsm/block"
//	"github.com/wodeyoulai/plsm/tools/lru"
//	"os"
//	"reflect"
//	"sync"
//	"testing"
//)
//
//func TestBlockCache_Get(t *testing.T) {
//	type fields struct {
//		cache    *lru.LRUCache
//		capacity int
//	}
//	type args struct {
//		tableId  uint16
//		blockIdx uint16
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   *block.Block
//		want1  bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			bc := &BlockCache{
//				cache:    tt.fields.cache,
//				capacity: tt.fields.capacity,
//			}
//			got, got1 := bc.Get(tt.args.tableId, tt.args.blockIdx)
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Get() got = %v, want %v", got, tt.want)
//			}
//			if got1 != tt.want1 {
//				t.Errorf("Get() got1 = %v, want %v", got1, tt.want1)
//			}
//		})
//	}
//}
//
//func TestBlockCache_Put(t *testing.T) {
//	type fields struct {
//		cache    *lru.LRUCache
//		capacity int
//	}
//	type args struct {
//		tableId  uint16
//		blockIdx uint16
//		block    *block.Block
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			bc := &BlockCache{
//				cache:    tt.fields.cache,
//				capacity: tt.fields.capacity,
//			}
//			bc.Put(tt.args.tableId, tt.args.blockIdx, tt.args.block)
//		})
//	}
//}
//
//func TestCreateAndSeekToFirst(t *testing.T) {
//	type args struct {
//		table *SSTable
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *SsTableIterator
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := CreateAndSeekToFirst(tt.args.table)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("CreateAndSeekToFirst() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("CreateAndSeekToFirst() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestCreateAndSeekToKey(t *testing.T) {
//	type args struct {
//		table *SSTable
//		key   []byte
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *SsTableIterator
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := CreateAndSeekToKey(tt.args.table, tt.args.key)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("CreateAndSeekToKey() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("CreateAndSeekToKey() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestCreateFileObject(t *testing.T) {
//	type args struct {
//		path string
//		data []byte
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *FileObject
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := CreateFileObject(tt.args.path, tt.args.data)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("CreateFileObject() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("CreateFileObject() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestCreateMetaOnlySST(t *testing.T) {
//	type args struct {
//		id       uint16
//		fileSize uint16
//		firstKey []byte
//		lastKey  []byte
//	}
//	tests := []struct {
//		name string
//		args args
//		want *SSTable
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := CreateMetaOnlySST(tt.args.id, tt.args.fileSize, tt.args.firstKey, tt.args.lastKey); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("CreateMetaOnlySST() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestFileObject_Read(t *testing.T) {
//	type fields struct {
//		file *os.File
//		size uint16
//	}
//	type args struct {
//		offset uint16
//		length uint16
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    []byte
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			f := &FileObject{
//				file: tt.fields.file,
//				size: tt.fields.size,
//			}
//			got, err := f.Read(tt.args.offset, tt.args.length)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("Read() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Read() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestFileObject_Size(t *testing.T) {
//	type fields struct {
//		file *os.File
//		size uint16
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   uint16
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			f := &FileObject{
//				file: tt.fields.file,
//				size: tt.fields.size,
//			}
//			if got := f.Size(); got != tt.want {
//				t.Errorf("Size() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestNewBlockCache(t *testing.T) {
//	type args struct {
//		capacity int
//	}
//	tests := []struct {
//		name string
//		args args
//		want *BlockCache
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := NewBlockCache(tt.args.capacity); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("NewBlockCache() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestNewSsTableBuilder(t *testing.T) {
//	type args struct {
//		blockSize int
//	}
//	tests := []struct {
//		name string
//		args args
//		want *SsTableBuilder
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := NewSsTableBuilder(tt.args.blockSize); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("NewSsTableBuilder() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestOpenFileObject(t *testing.T) {
//	type args struct {
//		path string
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *FileObject
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := OpenFileObject(tt.args.path)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("OpenFileObject() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("OpenFileObject() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestOpenSSTable(t *testing.T) {
//	type args struct {
//		id         uint16
//		blockCache *BlockCache
//		file       *FileObject
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *SSTable
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := OpenSSTable(tt.args.id, tt.args.blockCache, tt.args.file)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("OpenSSTable() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("OpenSSTable() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_Close(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			sst.Close()
//		})
//	}
//}
//
//func TestSSTable_FindBlockIdx(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	type args struct {
//		key []byte
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.FindBlockIdx(tt.args.key); got != tt.want {
//				t.Errorf("FindBlockIdx() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_FirstKey(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   []byte
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.FirstKey(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("FirstKey() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_ID(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   uint16
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.ID(); got != tt.want {
//				t.Errorf("ID() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_LastKey(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   []byte
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.LastKey(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("LastKey() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_MaxTimestamp(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   uint16
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.MaxTimestamp(); got != tt.want {
//				t.Errorf("MaxTimestamp() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_NumBlocks(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   uint16
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.NumBlocks(); got != tt.want {
//				t.Errorf("NumBlocks() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_ReadBlockCached(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	type args struct {
//		blockIdx uint16
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    *block.Block
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			got, err := sst.ReadBlockCached(tt.args.blockIdx)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("ReadBlockCached() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("ReadBlockCached() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSSTable_TableSize(t *testing.T) {
//	type fields struct {
//		file            *FileObject
//		blockMetas      []BlockMeta
//		blockMetaOffset uint16
//		id              uint16
//		blockCache      *BlockCache
//		firstKey        []byte
//		lastKey         []byte
//		bloom           *BloomFilter
//		maxTs           uint16
//		mu              sync.RWMutex
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   uint16
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			sst := &SSTable{
//				file:            tt.fields.file,
//				blockMetas:      tt.fields.blockMetas,
//				blockMetaOffset: tt.fields.blockMetaOffset,
//				id:              tt.fields.id,
//				blockCache:      tt.fields.blockCache,
//				firstKey:        tt.fields.firstKey,
//				lastKey:         tt.fields.lastKey,
//				bloom:           tt.fields.bloom,
//				maxTs:           tt.fields.maxTs,
//				mu:              tt.fields.mu,
//			}
//			if got := sst.TableSize(); got != tt.want {
//				t.Errorf("TableSize() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_Add(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	type args struct {
//		key   []byte
//		value []byte
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			if err := b.Add(tt.args.key, tt.args.value); (err != nil) != tt.wantErr {
//				t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_Build(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	type args struct {
//		id         uint16
//		blockCache *BlockCache
//		path       string
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    *SSTable
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			got, err := b.Build(tt.args.id, tt.args.blockCache, tt.args.path)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("Build() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Build() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_EstimatedSize(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			if got := b.EstimatedSize(); got != tt.want {
//				t.Errorf("EstimatedSize() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_createFile(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	type args struct {
//		path string
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		want    *os.File
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			got, err := b.createFile(tt.args.path)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("createFile() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("createFile() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_encodeBlockMetas(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   []byte
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			if got := b.encodeBlockMetas(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("encodeBlockMetas() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_estimateMetaSize(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			if got := b.estimateMetaSize(); got != tt.want {
//				t.Errorf("estimateMetaSize() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableBuilder_finishBlock(t *testing.T) {
//	type fields struct {
//		builder    *block.BlockBuilder
//		firstKey   []byte
//		lastKey    []byte
//		data       []byte
//		meta       []BlockMeta
//		blockSize  int
//		blockCount int
//		dataSize   int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			b := &SsTableBuilder{
//				builder:    tt.fields.builder,
//				firstKey:   tt.fields.firstKey,
//				lastKey:    tt.fields.lastKey,
//				data:       tt.fields.data,
//				meta:       tt.fields.meta,
//				blockSize:  tt.fields.blockSize,
//				blockCount: tt.fields.blockCount,
//				dataSize:   tt.fields.dataSize,
//			}
//			if err := b.finishBlock(); (err != nil) != tt.wantErr {
//				t.Errorf("finishBlock() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_Close(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.Close(); (err != nil) != tt.wantErr {
//				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_IsValid(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if got := it.IsValid(); got != tt.want {
//				t.Errorf("IsValid() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_Key(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   []byte
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if got := it.Key(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Key() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_Next(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.Next(); (err != nil) != tt.wantErr {
//				t.Errorf("Next() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_SeekToFirst(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.SeekToFirst(); (err != nil) != tt.wantErr {
//				t.Errorf("SeekToFirst() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_SeekToKey(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	type args struct {
//		key []byte
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.SeekToKey(tt.args.key); (err != nil) != tt.wantErr {
//				t.Errorf("SeekToKey() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_Value(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   []byte
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if got := it.Value(); !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("Value() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_findBlockIndex(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	type args struct {
//		key []byte
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		args   args
//		want   int
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if got := it.findBlockIndex(tt.args.key); got != tt.want {
//				t.Errorf("findBlockIndex() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_getCurrentBlock(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		want    *block.Block
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			got, err := it.getCurrentBlock()
//			if (err != nil) != tt.wantErr {
//				t.Errorf("getCurrentBlock() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("getCurrentBlock() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_isLastBlock(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name   string
//		fields fields
//		want   bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if got := it.isLastBlock(); got != tt.want {
//				t.Errorf("isLastBlock() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_readBlock(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	type args struct {
//		blockIdx int
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		args    args
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.readBlock(tt.args.blockIdx); (err != nil) != tt.wantErr {
//				t.Errorf("readBlock() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func TestSsTableIterator_seekToNextBlock(t *testing.T) {
//	type fields struct {
//		table   *SSTable
//		blkIter *block.BlockIterator
//		blkIdx  int
//		err     error
//	}
//	tests := []struct {
//		name    string
//		fields  fields
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			it := &SsTableIterator{
//				table:   tt.fields.table,
//				blkIter: tt.fields.blkIter,
//				blkIdx:  tt.fields.blkIdx,
//				err:     tt.fields.err,
//			}
//			if err := it.seekToNextBlock(); (err != nil) != tt.wantErr {
//				t.Errorf("seekToNextBlock() error = %v, wantErr %v", err, tt.wantErr)
//			}
//		})
//	}
//}
//
//func Test_buildCacheKey(t *testing.T) {
//	type args struct {
//		tableId  uint16
//		blockIdx uint16
//	}
//	tests := []struct {
//		name string
//		args args
//		want string
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			if got := buildCacheKey(tt.args.tableId, tt.args.blockIdx); got != tt.want {
//				t.Errorf("buildCacheKey() = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
//
//func Test_newSsTableIterator(t *testing.T) {
//	type args struct {
//		table *SSTable
//	}
//	tests := []struct {
//		name    string
//		args    args
//		want    *SsTableIterator
//		wantErr bool
//	}{
//		// TODO: Add test cases.
//	}
//	for _, tt := range tests {
//		t.Run(tt.name, func(t *testing.T) {
//			got, err := newSSTableIterator(tt.args.table)
//			if (err != nil) != tt.wantErr {
//				t.Errorf("newSSTableIterator() error = %v, wantErr %v", err, tt.wantErr)
//				return
//			}
//			if !reflect.DeepEqual(got, tt.want) {
//				t.Errorf("newSSTableIterator() got = %v, want %v", got, tt.want)
//			}
//		})
//	}
//}
