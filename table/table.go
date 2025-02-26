package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"mini_lsm/block"
	"mini_lsm/tools/fileutil"
	"mini_lsm/tools/lru"
	"strconv"
	"sync"
	"sync/atomic"
)

// sstable
//----------------------------------------------------------------------------------------------------------
//|         Block Section         |          Meta Section         |                  Extra                 |
//----------------------------------------------------------------------------------------------------------
//| data block | ... | data block |            metadata           | meta block offset (u32) + max ts(u64)| |
//----------------------------------------------------------------------------------------------------------

// BlockMeta metadata information containing data blocks
type BlockMeta struct {
	// offset of data blocks
	Offset   uint16
	FirstKey []byte
	LastKey  []byte
}

// SSTable represents an ordered string table
type SSTable struct {
	// actual storage unit
	file *fileutil.FileObject
	// metadata blocks that save data block information
	blockMetas []BlockMeta

	blockMetaOffset uint16
	// SSTable ID
	id uint32
	// block cache
	blockCache *BlockCache
	firstKey   []byte
	lastKey    []byte
	// bloom filter
	bloom *BloomFilter
	// the maximum timestamp stored in sst
	maxTs uint64
	mu    sync.RWMutex
}

func OpenSSTable(id uint32, blockCache *BlockCache, file *fileutil.FileObject) (*SSTable, error) {
	// read metadata
	stat, err := file.File.Stat()
	if err != nil {
		return nil, err
	}
	size := stat.Size()

	// make sure the file is at least 4 bytes long
	if size < 4 {
		return nil, fmt.Errorf("file too small: %d bytes", size)
	}
	footer := make([]byte, 4)
	if _, err := file.File.ReadAt(footer, size-4); err != nil {
		return nil, err
	}

	metaOffset := binary.BigEndian.Uint32(footer)
	if metaOffset == 0 {
		fmt.Printf("empty sstable\n")
		return nil, fmt.Errorf("empty sstable")
	}
	metaSize := file.Size - metaOffset - 4

	metaData, err := file.Read(metaOffset, metaSize)
	if err != nil {
		return nil, err
	}
	// decode metadata
	var blockMetas []BlockMeta
	start := uint32(0)
	// offset + size + first key + size + last key
	for start < metaSize {
		offset := binary.BigEndian.Uint16(metaData[start : start+2])

		firstKeySize := binary.BigEndian.Uint16(metaData[start+2 : start+4])
		firstKey := make([]byte, firstKeySize)

		lastKeyOffset := uint32(firstKeySize) + 4 + start
		lastKeySize := binary.BigEndian.Uint16(metaData[lastKeyOffset : lastKeyOffset+2])

		copy(firstKey, metaData[start+4:uint32(firstKeySize)+start+4])
		lastKey := make([]byte, lastKeySize)
		nextMeta := uint32(lastKeySize) + 2 + lastKeyOffset
		copy(lastKey, metaData[lastKeyOffset+2:uint32(lastKeySize)+2+lastKeyOffset])
		newMeta := BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		}
		blockMetas = append(blockMetas, newMeta)
		start = nextMeta
	}

	lastKey := blockMetas[len(blockMetas)-1].LastKey
	if len(lastKey) < 8 {
		panic("invalid key format")
	}
	maxTs := binary.BigEndian.Uint64(lastKey[len(lastKey)-8:])
	return &SSTable{
		file:            file,
		blockMetas:      blockMetas,
		blockMetaOffset: uint16(metaOffset),
		id:              id,
		blockCache:      blockCache,
		firstKey:        blockMetas[0].FirstKey,
		lastKey:         blockMetas[len(blockMetas)-1].LastKey,
		maxTs:           maxTs,
	}, nil
}

// CreateMetaOnlySST 创建一个只包含元数据的SST
func CreateMetaOnlySST(id uint32, fileSize uint32, firstKey, lastKey []byte) *SSTable {
	return &SSTable{
		file:            &fileutil.FileObject{Size: fileSize},
		blockMetas:      []BlockMeta{},
		blockMetaOffset: 0,
		id:              id,
		blockCache:      nil,
		firstKey:        firstKey,
		lastKey:         lastKey,
		bloom:           nil,
		maxTs:           0,
	}
}

func (sst *SSTable) MayContain(key []byte) bool {
	// First check if key is within SSTable range
	beginCompare := bytes.Compare(sst.firstKey, key)
	endCompare := bytes.Compare(sst.lastKey, key)

	// Not in range, return false directly
	if beginCompare > 0 || endCompare < 0 {
		return false
	}

	// Equal to boundary values
	if beginCompare == 0 || endCompare == 0 {
		return true
	}

	// If bloom filter exists, use it for fast filtering
	if sst.bloom != nil && !sst.bloom.MayContain(key) {
		return false
	}

	// Finally check if key might be in a data block using binary search
	return sst.FindBlockIdx(key) >= 0
}
func (sst *SSTable) ReadBlockCached(blockIdx uint16) (*block.Block, error) {
	// Check cache first
	if sst.blockCache != nil {
		if cachedBlock, found := sst.blockCache.Get(sst.id, uint32(blockIdx)); found {
			return cachedBlock, nil
		}
	}

	// Validate block index
	if blockIdx >= uint16(len(sst.blockMetas)) {
		return nil, errors.New("block index out of range")
	}

	// Use already loaded metadata, avoid re-reading footer
	start := uint32(sst.blockMetas[blockIdx].Offset)
	end := uint32(sst.blockMetaOffset) // Use already loaded metadata offset
	if blockIdx < uint16(len(sst.blockMetas)-1) {
		end = uint32(sst.blockMetas[blockIdx+1].Offset)
	}

	// Read block data
	data := make([]byte, end-start)
	if _, err := sst.file.File.ReadAt(data, int64(start)); err != nil {
		return nil, err
	}

	newBlock := &block.Block{}
	newBlock.Decode(data)

	// Add to cache
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.id, uint32(blockIdx), newBlock)
	}

	return newBlock, nil
}

func (sst *SSTable) FindBlockIdx(key []byte) int {
	// Use binary search instead of linear search
	left, right := 0, len(sst.blockMetas)-1

	for left <= right {
		mid := (left + right) / 2
		meta := sst.blockMetas[mid]

		// Check if key is within current block range
		if bytes.Compare(key, meta.FirstKey) >= 0 && bytes.Compare(key, meta.LastKey) <= 0 {
			return mid
		}

		if bytes.Compare(key, meta.FirstKey) < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// If exact match not found, return first block that may contain keys greater than target
	if left < len(sst.blockMetas) {
		return left
	}
	return -1 // No suitable block found
}

// Add a method to preload multiple blocks, optimizing sequential scan performance
func (sst *SSTable) ReadBlocksRange(startIdx, endIdx uint16) ([]*block.Block, error) {
	if startIdx > endIdx || endIdx >= uint16(len(sst.blockMetas)) {
		return nil, errors.New("invalid block range")
	}

	blocks := make([]*block.Block, endIdx-startIdx+1)

	// Calculate continuous read range
	startOffset := uint32(sst.blockMetas[startIdx].Offset)
	endOffset := uint32(sst.blockMetaOffset)
	if endIdx < uint16(len(sst.blockMetas)-1) {
		endOffset = uint32(sst.blockMetas[endIdx+1].Offset)
	}

	// Read multiple blocks' data at once
	data := make([]byte, endOffset-startOffset)
	if _, err := sst.file.File.ReadAt(data, int64(startOffset)); err != nil {
		return nil, err
	}

	// Decompose and decode each block
	for i := startIdx; i <= endIdx; i++ {
		idx := i - startIdx
		blockStart := uint32(sst.blockMetas[i].Offset) - startOffset
		blockEnd := endOffset - startOffset
		if i < endIdx {
			blockEnd = uint32(sst.blockMetas[i+1].Offset) - startOffset
		}

		blockData := data[blockStart:blockEnd]
		block := &block.Block{}
		block.Decode(blockData)
		blocks[idx] = block

		// Add to cache
		if sst.blockCache != nil {
			sst.blockCache.Put(sst.id, uint32(i), block)
		}
	}

	return blocks, nil
}
func (sst *SSTable) NumBlocks() uint16 {
	return uint16(len(sst.blockMetas))
}

func (sst *SSTable) FirstKey() []byte {
	return sst.firstKey
}

func (sst *SSTable) LastKey() []byte {
	return sst.lastKey
}

func (sst *SSTable) TableSize() uint32 {
	return sst.file.Size
}

func (sst *SSTable) ID() uint32 {
	return sst.id
}

func (sst *SSTable) MaxTimestamp() uint64 {
	return sst.maxTs
}
func (sst *SSTable) Close() {
	sst.file.File.Close()
	sst.blockCache = nil
	sst.blockMetaOffset = 0
}

// BlockCache 块缓存结构

func NewBlockCache(capacity int) *BlockCache {
	cache, err := lru.Constructor(capacity)
	if err != nil {
		panic("Error creating block cache")
	}
	return &BlockCache{
		cache:    cache,
		capacity: capacity,
	}
}

func buildCacheKey(tableId, blockIdx uint32) string {
	return strconv.Itoa(int(tableId)) + "_" + strconv.Itoa(int(blockIdx))
}

func (bc *BlockCache) Put(tableId, blockIdx uint32, block *block.Block) {
	key := buildCacheKey(tableId, blockIdx)
	bc.cache.Put(key, block)
}

// Add more cache strategies and metrics collection
type BlockCache struct {
	cache    *lru.LRUCache
	capacity int
	hits     int64 // Hit count
	misses   int64 // Miss count
	mu       sync.RWMutex
}

func (bc *BlockCache) Get(tableId, blockIdx uint32) (*block.Block, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	key := buildCacheKey(tableId, blockIdx)
	value, err := bc.cache.Get(key)
	if err == nil {
		atomic.AddInt64(&bc.hits, 1)
		return value.(*block.Block), true
	}

	atomic.AddInt64(&bc.misses, 1)
	return nil, false
}

func (bc *BlockCache) Stats() (hits, misses int64) {
	return atomic.LoadInt64(&bc.hits), atomic.LoadInt64(&bc.misses)
}
