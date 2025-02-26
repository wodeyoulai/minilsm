package block

import (
	"encoding/binary"
	"errors"
)

//----------------------------------------------------------------------------------------------------
//|             Data Section             |              Offset Section             |      Extra      |
//----------------------------------------------------------------------------------------------------
//| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
//----------------------------------------------------------------------------------------------------

const (
	METAENTRYSIZE  = 2
	NUMELEMENTSIZE = 2 // size for each offset entry
)

var (
	// ErrEmptyKey indicates that the key is empty
	ErrEmptyKey = errors.New("key cannot be empty")
	// ErrKeyValueTooLarge indicates that the key or value exceeds the maximum size (65535)
	ErrKeyValueTooLarge = errors.New("key or value too large")
	// ErrBlockSizeExceeded indicates that adding the entry would exceed the target block size
	ErrBlockSizeExceeded = errors.New("block would exceed target size")
)

// BlockBuilder for construction block
type BlockBuilder struct {
	data        []byte   // Storing the actual key-value data
	offsets     []uint16 // Storing the offset of each entry
	targetSize  uint16   // block target size limit
	currentSize uint16
	firstKey    []byte // the size of the current block
	lenBuf      []byte
}

// NewBlockBuilder 创建一个新的 BlockBuilder
func NewBlockBuilder(targetSize uint16) *BlockBuilder {
	estimatedEntries := targetSize / 32
	return &BlockBuilder{
		data:        make([]byte, 0, targetSize),
		offsets:     make([]uint16, 0, estimatedEntries),
		targetSize:  targetSize,
		currentSize: 0,
		firstKey:    make([]byte, 0, 16),
		lenBuf:      make([]byte, 2),
	}
}
func (b *BlockBuilder) EstimatedSize() uint16 {
	return b.currentSize
}

// Add add a key value pair to the block
func (b *BlockBuilder) Add(key, value []byte) error {
	if len(key) == 0 {
		return ErrEmptyKey
	}
	if len(key) > 65535 || len(value) > 65535 {
		return ErrKeyValueTooLarge
	}

	// calculate the total size of the current entry
	entrySize := 2 + len(key) + 2 + len(value) // 2 byte key length + key + 2 byte value length + value

	// calculate whether the target size will exceed
	// also need to reserve space for offsets and number of elements
	totalSize := b.currentSize + uint16(entrySize) + METAENTRYSIZE
	if len(b.data) == 0 {
		// Add the last num_of_elements field for the first time
		totalSize += NUMELEMENTSIZE
	}
	// If this is the first entry, add it even if it exceeds the size
	if len(b.offsets) > 0 && totalSize > b.targetSize {
		return ErrBlockSizeExceeded
	}
	b.currentSize = totalSize
	// record the current offset
	b.offsets = append(b.offsets, uint16(len(b.data)))

	// write key length
	binary.BigEndian.PutUint16(b.lenBuf, uint16(len(key)))
	b.data = append(b.data, b.lenBuf...)

	// write key
	b.data = append(b.data, key...)

	// write value length
	binary.BigEndian.PutUint16(b.lenBuf, uint16(len(value)))
	b.data = append(b.data, b.lenBuf...)

	// write value
	b.data = append(b.data, value...)
	if b.offsets != nil && len(b.offsets) == 1 {
		b.firstKey = key
	}
	return nil
}

// Build build the final Block
func (b *BlockBuilder) Build() *Block {
	return &Block{
		Data:    b.data,
		Offsets: b.offsets,
	}
}
func (b *BlockBuilder) Count() int {
	return len(b.offsets)
}
