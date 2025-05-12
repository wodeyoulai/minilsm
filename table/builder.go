package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/wodeyoulai/plsm/block"
	"github.com/wodeyoulai/plsm/tools/fileutil"
	"os"
	"path/filepath"
	"sync"
)

// Constants for SSTable construction
const (
// Align to 4KB boundary for direct IO
// BLOCK_ALIGN_SIZE = 4096
//
// // SSTable format version
// FORMAT_VERSION = 1
)

// Common errors
var (
	ErrEmptyKey         = errors.New("key cannot be empty")
	ErrNilBuilder       = errors.New("block builder is nil")
	ErrDirectoryFailure = errors.New("failed to create directory")
	ErrCreateFile       = errors.New("failed to create SSTable file")
	ErrWriteFile        = errors.New("failed to write to SSTable file")
	ErrFlushFile        = errors.New("failed to flush SSTable file")
)

// SsTableBuilder helps construct an SSTable file
type SsTableBuilder struct {
	// Current block builder
	builder *block.BlockBuilder

	// First key in the SSTable
	firstKey []byte

	// Last key in the SSTable
	lastKey []byte

	// Accumulated data blocks
	data []byte

	// Block metadata
	meta []BlockMeta

	// Target block size
	blockSize uint32

	// Max timestamp seen
	maxTs uint64

	// Statistics
	blockCount uint32
	dataSize   uint32

	// Mutex to protect concurrent access
	mu sync.Mutex
}

// NewSsTableBuilder creates a new SSTable builder
func NewSsTableBuilder(blockSize uint32) *SsTableBuilder {
	// Ensure blockSize is aligned to 4KB
	alignedSize := (blockSize + BLOCK_ALIGN_SIZE - 1) & ^uint32(BLOCK_ALIGN_SIZE-1)
	if alignedSize < BLOCK_ALIGN_SIZE {
		alignedSize = BLOCK_ALIGN_SIZE
	}

	return &SsTableBuilder{
		builder:    block.NewBlockBuilder(alignedSize),
		firstKey:   nil,
		lastKey:    nil,
		data:       make([]byte, 0, alignedSize),
		meta:       make([]BlockMeta, 0),
		blockSize:  alignedSize,
		blockCount: 0,
		dataSize:   0,
		mu:         sync.Mutex{},
	}
}

// Add adds a key-value pair to the SSTable
func (b *SsTableBuilder) Add(key, value []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(key) == 0 {
		return ErrEmptyKey
	}

	if b.builder == nil {
		return ErrNilBuilder
	}

	// Extract timestamp from key if present (assumed to be last 8 bytes)
	if len(key) >= 8 {
		ts := ^binary.BigEndian.Uint64(key[len(key)-8:])
		if ts > b.maxTs {
			b.maxTs = ts
		}
	}

	// Update first key if not set
	if b.firstKey == nil {
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey, key)
	}

	// Always update last key
	if b.lastKey == nil || len(b.lastKey) != len(key) {
		b.lastKey = make([]byte, len(key))
	}
	copy(b.lastKey, key)

	// Try to add to current block
	err := b.builder.Add(key, value)
	if err != nil {
		if errors.Is(err, block.ErrBlockSizeExceeded) {
			// Finish current block
			if err := b.finishBlock(); err != nil {
				return fmt.Errorf("finish block: %w", err)
			}

			// Create a new block builder and try again
			b.builder = block.NewBlockBuilder(b.blockSize)
			return b.builder.Add(key, value)
		}
		return err
	}

	return nil
}

// finishBlock completes the current block and adds it to the SSTable
func (b *SsTableBuilder) finishBlock() error {
	if b.builder.Count() == 0 {
		return nil // Nothing to finish
	}

	// Get first and last keys from block
	blockFirstKey := b.builder.FirstKey()
	blockLastKey := b.builder.LastKey()

	if blockFirstKey == nil || blockLastKey == nil {
		return errors.New("block has no keys")
	}

	// Build the block
	builtBlock := b.builder.Build()
	encodedBlock := builtBlock.Encode()

	// Create metadata for this block
	meta := BlockMeta{
		Offset:   uint32(len(b.data)),
		FirstKey: make([]byte, len(blockFirstKey)),
		LastKey:  make([]byte, len(blockLastKey)),
	}
	copy(meta.FirstKey, blockFirstKey)
	copy(meta.LastKey, blockLastKey)

	// Add block data and metadata
	b.data = append(b.data, encodedBlock...)
	b.meta = append(b.meta, meta)
	b.blockCount++
	b.dataSize += uint32(len(encodedBlock))

	return nil
}

// EstimatedSize returns the current estimated size of the SSTable
func (b *SsTableBuilder) EstimatedSize() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()

	currentBlockSize := uint32(0)
	if b.builder != nil {
		currentBlockSize = uint32(b.builder.EstimatedSize())
	}

	return b.dataSize + currentBlockSize + uint32(b.estimateMetaSize()) + 12 // Include footer size
}

// estimateMetaSize estimates the size of metadata section
func (b *SsTableBuilder) estimateMetaSize() int {
	//metaSize := 4 // 4 bytes for version
	//
	//for _, m := range b.meta {
	//	// For each meta: 2 bytes offset + 2 bytes first key size + first key + 2 bytes last key size + last key
	//	metaSize += 2 + 2 + len(m.FirstKey) + 2 + len(m.LastKey)
	//}
	//
	//return metaSize

	metaSize := 4
	for _, m := range b.meta {
		metaSize += 4 + len(m.FirstKey) + len(m.LastKey) // offset + size + keys
	}
	return metaSize
}

// createFile creates and opens a file for the SSTable
func (b *SsTableBuilder) createFile(path string) (*os.File, error) {
	// Ensure directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrDirectoryFailure, err)
	}

	// Create or truncate file
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrCreateFile, err)
	}

	return file, nil
}

// Build finalizes the SSTable and writes it to disk
func (b *SsTableBuilder) Build(id uint32, blockCache *BlockCache, path string) (*SSTable, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// Finish the current block if not empty
	if b.builder != nil && b.builder.Count() > 0 {
		if err := b.finishBlock(); err != nil {
			return nil, err
		}
	}

	// If no blocks were added, return an error
	if len(b.meta) == 0 {
		return nil, errors.New("cannot build empty SSTable")
	}

	// Encode metadata section
	meta := b.encodeBlockMetas()

	// Create the file
	file, err := b.createFile(path)
	if err != nil {
		return nil, err
	}

	// Write data blocks
	_, err = file.Write(b.data)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("%w: %s", ErrWriteFile, err)
	}

	// Calculate metadata offset
	metaOffset := uint32(len(b.data))

	// Write metadata section
	_, err = file.Write(meta)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("%w: %s", ErrWriteFile, err)
	}

	// Write footer (metadata offset + max timestamp)
	footer := make([]byte, 12)
	binary.BigEndian.PutUint32(footer, metaOffset)
	binary.BigEndian.PutUint64(footer[4:], b.maxTs)

	_, err = file.Write(footer)
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("%w: %s", ErrWriteFile, err)
	}

	// Flush to disk
	if err := file.Sync(); err != nil {
		file.Close()
		return nil, fmt.Errorf("%w: %s", ErrFlushFile, err)
	}

	// Calculate total file size
	totalSize := uint32(len(b.data) + len(meta) + 12)

	// Create file object
	fileObj := &fileutil.FileObject{
		File: file,
		Size: totalSize,
	}

	// Open the SSTable
	return OpenSSTable(id, blockCache, fileObj)
}

// encodeBlockMetas encodes block metadata into bytes
func (b *SsTableBuilder) encodeBlockMetas() []byte {
	// Initialize buffer with estimated size
	buf := make([]byte, 0, b.estimateMetaSize())

	// Write version
	versionBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(versionBuf, FORMAT_VERSION)
	buf = append(buf, versionBuf...)

	// Write each meta entry
	for _, meta := range b.meta {
		// Write offset
		offsetBuf := make([]byte, 4)
		binary.BigEndian.PutUint32(offsetBuf, meta.Offset)
		buf = append(buf, offsetBuf...)

		// Write first key
		keyLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(keyLenBuf, uint16(len(meta.FirstKey)))
		buf = append(buf, keyLenBuf...)
		buf = append(buf, meta.FirstKey...)

		// Write last key
		binary.BigEndian.PutUint16(keyLenBuf, uint16(len(meta.LastKey)))
		buf = append(buf, keyLenBuf...)
		buf = append(buf, meta.LastKey...)
	}

	return buf
}

// Reset clears the builder for reuse
func (b *SsTableBuilder) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.builder = block.NewBlockBuilder(b.blockSize)
	b.firstKey = nil
	b.lastKey = nil
	b.data = make([]byte, 0, b.blockSize)
	b.meta = make([]BlockMeta, 0)
	b.blockCount = 0
	b.dataSize = 0
	b.maxTs = 0
}

// FirstKey returns the first key added to the SSTable
func (b *SsTableBuilder) FirstKey() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.firstKey == nil {
		return nil
	}

	result := make([]byte, len(b.firstKey))
	copy(result, b.firstKey)
	return result
}

// LastKey returns the last key added to the SSTable
func (b *SsTableBuilder) LastKey() []byte {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.lastKey == nil {
		return nil
	}

	result := make([]byte, len(b.lastKey))
	copy(result, b.lastKey)
	return result
}

// BlockCount returns the number of blocks in the SSTable
func (b *SsTableBuilder) BlockCount() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.blockCount
}

// DataSize returns the size of data blocks in bytes
func (b *SsTableBuilder) DataSize() uint32 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.dataSize
}

// MaxTimestamp returns the maximum timestamp encountered
func (b *SsTableBuilder) MaxTimestamp() uint64 {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.maxTs
}
