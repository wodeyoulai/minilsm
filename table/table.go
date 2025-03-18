package table

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"mini_lsm/block"
	"mini_lsm/tools/fileutil"
	"mini_lsm/tools/lru"
	"strconv"
	"sync"
)

//-------------------------------------------------------------------------------------------
//|         Block Section         |          Meta Section         |          Extra          |
//-------------------------------------------------------------------------------------------
//| data block | ... | data block |version uint32| metadata       | meta block offset (u32) |
//-------------------------------------------------------------------------------------------

// File format constants
const (
	// Version tag placed at beginning of file
	FORMAT_VERSION = 1

	// Align to 4KB boundary for direct IO
	BLOCK_ALIGN_SIZE = 4096

	// Footer size (4 bytes for meta offset + 8 bytes for max timestamp)
	FOOTER_SIZE = 12
)

// Common errors
var (
	ErrInvalidMetadata = errors.New("invalid sstable metadata")
	ErrBlockNotFound   = errors.New("block not found")
	ErrCorruptTable    = errors.New("corrupt sstable")
	ErrEmptyTable      = errors.New("empty sstable")
)

// BlockMeta metadata information containing data blocks
type BlockMeta struct {
	// Offset of data blocks
	Offset   uint32
	FirstKey []byte
	LastKey  []byte
}

// SSTable represents an ordered string table
type SSTable struct {
	// Actual storage unit
	file *fileutil.FileObject
	// Metadata blocks that save data block information
	blockMetas []BlockMeta

	blockMetaOffset uint32
	// SSTable ID
	id uint32
	// Block cache
	blockCache *BlockCache
	firstKey   []byte
	lastKey    []byte
	// Bloom filter
	bloom *BloomFilter
	// The maximum timestamp stored in sst
	maxTs uint64
	mu    sync.RWMutex
	// Track if the table is closed
	closed bool
}

// OpenSSTable opens an existing SSTable file
func OpenSSTable(id uint32, blockCache *BlockCache, file *fileutil.FileObject) (*SSTable, error) {
	if file == nil {
		return nil, errors.New("file object cannot be nil")
	}

	stat, err := file.File.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	size := stat.Size()
	if size < FOOTER_SIZE {
		return nil, fmt.Errorf("file too small: %d bytes", size)
	}

	// Read footer (meta offset + max timestamp)
	footer := make([]byte, FOOTER_SIZE)
	if _, err := file.File.ReadAt(footer, size-FOOTER_SIZE); err != nil {
		return nil, fmt.Errorf("read footer: %w", err)
	}

	// Parse metadata offset and max timestamp
	metaOffset := binary.BigEndian.Uint32(footer[:4])
	maxTs := binary.BigEndian.Uint64(footer[4:])

	if metaOffset == 0 || metaOffset >= uint32(size) {
		return nil, ErrInvalidMetadata
	}

	// Calculate metadata size
	metaSize := uint32(size) - metaOffset - FOOTER_SIZE
	if metaSize == 0 {
		return nil, ErrEmptyTable
	}

	// Read metadata section
	metaData, err := file.Read(metaOffset, metaSize)
	if err != nil {
		return nil, fmt.Errorf("read metadata: %w", err)
	}

	// Decode metadata
	blockMetas, err := decodeBlockMetas(metaData, metaSize)
	if err != nil {
		return nil, err
	}

	if len(blockMetas) == 0 {
		return nil, ErrInvalidMetadata
	}

	return &SSTable{
		file:            file,
		blockMetas:      blockMetas,
		blockMetaOffset: metaOffset,
		id:              id,
		blockCache:      blockCache,
		firstKey:        blockMetas[0].FirstKey,
		lastKey:         blockMetas[len(blockMetas)-1].LastKey,
		maxTs:           maxTs,
		closed:          false,
	}, nil
}

// decodeBlockMetas decodes the metadata section into BlockMeta objects
func decodeBlockMetas(metaData []byte, metaSize uint32) ([]BlockMeta, error) {
	var blockMetas []BlockMeta
	// todo version has 4bytes
	var pos uint32 = 4

	for pos < metaSize {
		// Check if we have at least 4 bytes for the offset
		if pos+4 > metaSize {
			return nil, fmt.Errorf("truncated metadata at offset %d", pos)
		}
		offset := binary.BigEndian.Uint32(metaData[pos : pos+4])
		pos += 4

		// Read first key size
		if pos+2 > metaSize {
			return nil, fmt.Errorf("truncated metadata at first key size offset %d", pos)
		}
		firstKeySize := binary.BigEndian.Uint16(metaData[pos : pos+2])
		pos += 2

		// Read first key
		if pos+uint32(firstKeySize) > metaSize {
			return nil, fmt.Errorf("truncated metadata at first key offset %d", pos)
		}
		firstKey := make([]byte, firstKeySize)
		copy(firstKey, metaData[pos:pos+uint32(firstKeySize)])
		pos += uint32(firstKeySize)

		// Read last key size
		if pos+2 > metaSize {
			return nil, fmt.Errorf("truncated metadata at last key size offset %d", pos)
		}
		lastKeySize := binary.BigEndian.Uint16(metaData[pos : pos+2])
		pos += 2

		// Read last key
		if pos+uint32(lastKeySize) > metaSize {
			return nil, fmt.Errorf("truncated metadata at last key offset %d", pos)
		}
		lastKey := make([]byte, lastKeySize)
		copy(lastKey, metaData[pos:pos+uint32(lastKeySize)])
		pos += uint32(lastKeySize)

		// Create BlockMeta
		blockMetas = append(blockMetas, BlockMeta{
			Offset:   offset,
			FirstKey: firstKey,
			LastKey:  lastKey,
		})
	}

	return blockMetas, nil
}

// CreateMetaOnlySST creates an SSTable that only contains metadata (for testing/recovery)
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

// MayContain checks if the SSTable might contain the given key
func (sst *SSTable) MayContain(key []byte) bool {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return false
	}

	// Check key range
	begin := bytes.Compare(sst.firstKey, key)
	end := bytes.Compare(sst.lastKey, key)

	// Key is within range if equal to first/last or between them
	return (begin <= 0 && end >= 0)
}

// ReadBlockCached reads a block by index, using cache if available
func (sst *SSTable) ReadBlockCached(blockIdx uint32) (*block.Block, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return nil, ErrTableClosed
	}

	if int(blockIdx) >= len(sst.blockMetas) {
		return nil, fmt.Errorf("block index out of range: %d", blockIdx)
	}

	// Check cache first if available
	if sst.blockCache != nil {
		if block, ok := sst.blockCache.Get(sst.id, blockIdx); ok {
			return block, nil
		}
	}

	// Cache miss, read from file
	blockData, err := sst.readBlockData(blockIdx)
	if err != nil {
		return nil, err
	}

	// Decode block
	newBlock := &block.Block{}
	if err := newBlock.Decode(blockData); err != nil {
		return nil, fmt.Errorf("decode block: %w", err)
	}

	// Put in cache if available
	if sst.blockCache != nil {
		sst.blockCache.Put(sst.id, blockIdx, newBlock)
	}

	return newBlock, nil
}

// readBlockData reads raw block data from file
func (sst *SSTable) readBlockData(blockIdx uint32) ([]byte, error) {
	if int(blockIdx) >= len(sst.blockMetas) {
		return nil, fmt.Errorf("block index out of range: %d", blockIdx)
	}

	// Calculate block size
	start := uint32(sst.blockMetas[blockIdx].Offset)
	var end uint32

	if int(blockIdx) < len(sst.blockMetas)-1 {
		// Not the last block, end is the start of next block
		end = uint32(sst.blockMetas[blockIdx+1].Offset)
	} else {
		// Last block, end is the metadata offset
		end = sst.blockMetaOffset
	}

	if end <= start {
		return nil, fmt.Errorf("invalid block boundaries: start=%d, end=%d", start, end)
	}

	// Read block data
	data := make([]byte, end-start)
	if _, err := sst.file.File.ReadAt(data, int64(start)); err != nil && err != io.EOF {
		return nil, fmt.Errorf("read block data: %w", err)
	}

	return data, nil
}

// FindBlockIdx returns the index of the block that may contain the key
func (sst *SSTable) FindBlockIdx(key []byte) (int, error) {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return -1, ErrTableClosed
	}

	// Binary search for the block that may contain the key
	left, right := 0, len(sst.blockMetas)-1

	for left <= right {
		mid := (left + right) / 2
		meta := sst.blockMetas[mid]

		// Check if key is within block range
		if bytes.Compare(key, meta.FirstKey) >= 0 && bytes.Compare(key, meta.LastKey) <= 0 {
			return mid, nil
		}

		if bytes.Compare(key, meta.FirstKey) < 0 {
			right = mid - 1
		} else {
			left = mid + 1
		}
	}

	// Key not found in any block
	return -1, ErrBlockNotFound
}

// NumBlocks returns the number of data blocks in the SSTable
func (sst *SSTable) NumBlocks() uint32 {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	return uint32(len(sst.blockMetas))
}

// FirstKey returns the first key in the SSTable
func (sst *SSTable) FirstKey() []byte {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return nil
	}

	return sst.firstKey
}

// LastKey returns the last key in the SSTable
func (sst *SSTable) LastKey() []byte {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return nil
	}

	return sst.lastKey
}

// TableSize returns the total size of the SSTable in bytes
func (sst *SSTable) TableSize() uint32 {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return 0
	}

	return sst.file.Size
}

// ID returns the SSTable ID
func (sst *SSTable) ID() uint32 {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	return sst.id
}

// MaxTimestamp returns the maximum timestamp stored in the SSTable
func (sst *SSTable) MaxTimestamp() uint64 {
	sst.mu.RLock()
	defer sst.mu.RUnlock()

	if sst.closed {
		return 0
	}

	return sst.maxTs
}

// Close releases resources associated with the SSTable
func (sst *SSTable) Close() error {
	sst.mu.Lock()
	defer sst.mu.Unlock()

	if sst.closed {
		return nil // Already closed
	}

	sst.closed = true

	// Close file
	if sst.file != nil && sst.file.File != nil {
		if err := sst.file.File.Close(); err != nil {
			return fmt.Errorf("close file: %w", err)
		}
		sst.file = nil
	}

	// Release references
	sst.blockCache = nil
	sst.blockMetaOffset = 0
	sst.blockMetas = nil

	return nil
}

// BloomFilter structure for key existence checking

// BlockCache structure for caching blocks in memory
type BlockCache struct {
	cache    *lru.LRUCache
	capacity int
	mu       sync.RWMutex // Protect cache operations
}

// NewBlockCache creates a new block cache with the given capacity
func NewBlockCache(capacity int) *BlockCache {
	cache, err := lru.Constructor(capacity)
	if err != nil {
		panic("Error creating block cache: " + err.Error())
	}
	return &BlockCache{
		cache:    cache,
		capacity: capacity,
	}
}

// buildCacheKey generates a cache key from table ID and block index
func buildCacheKey(tableId, blockIdx uint32) string {
	return strconv.Itoa(int(tableId)) + "_" + strconv.Itoa(int(blockIdx))
}

// Get retrieves a block from the cache
func (bc *BlockCache) Get(tableId, blockIdx uint32) (*block.Block, bool) {
	if bc == nil {
		return nil, false
	}

	bc.mu.RLock()
	defer bc.mu.RUnlock()

	key := buildCacheKey(tableId, blockIdx)
	value, err := bc.cache.Get(key)
	if err == nil {
		return value.(*block.Block), true
	}
	return nil, false
}

// Put adds a block to the cache
func (bc *BlockCache) Put(tableId, blockIdx uint32, block *block.Block) {
	if bc == nil || block == nil {
		return
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	key := buildCacheKey(tableId, blockIdx)
	bc.cache.Put(key, block)
}

// Clear empties the cache
func (bc *BlockCache) Clear() {
	if bc == nil {
		return
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	bc.cache.Clear()
}

// Size returns the number of entries in the cache
func (bc *BlockCache) Size() int {
	if bc == nil {
		return 0
	}

	bc.mu.RLock()
	defer bc.mu.RUnlock()

	return int(bc.cache.Size())
}
