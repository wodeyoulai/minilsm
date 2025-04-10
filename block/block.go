package block

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Block format:
//----------------------------------------------------------------------------------------------------
//|             Data Section             |              Offset Section             |      Extra      |
//----------------------------------------------------------------------------------------------------
//| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
//----------------------------------------------------------------------------------------------------

// Entry format:
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------

// Constants for block structure
const (
	// OFFSET_ENTRY_SIZE Size of each offset entry (uint16)
	OFFSET_ENTRY_SIZE = 2

	// NUM_ELEMENTS_SIZE Size of element count field (uint16)
	NUM_ELEMENTS_SIZE = 2

	// MIN_BLOCK_SIZE Minimum valid block size
	MIN_BLOCK_SIZE = 4

	// MAX_KEY_VALUE_SIZE Maximum key/value size (limited by uint16 size field)
	MAX_KEY_VALUE_SIZE = 65535
)

// Common errors
var (
	ErrInvalidBlockData = errors.New("invalid block data")
	ErrInvalidOffset    = errors.New("invalid offset in block")
	ErrEmptyBlock       = errors.New("empty block")
)

// Block represents a data block containing sorted key-value pairs
// not Concurrency safe
type Block struct {
	Data    []byte   // Raw key-value data
	Offsets []uint16 // Offsets to the start of each entry
}

// NewBlock creates an empty block
func NewBlock() *Block {
	return &Block{
		Data:    make([]byte, 0),
		Offsets: make([]uint16, 0),
	}
}

// Encode serializes the block into a byte array
func (b *Block) Encode() []byte {
	if len(b.Offsets) == 0 {
		// Return a minimal valid empty block
		result := make([]byte, NUM_ELEMENTS_SIZE)
		binary.BigEndian.PutUint16(result, 0)
		return result
	}

	// Calculate total size
	totalSize := len(b.Data) + len(b.Offsets)*OFFSET_ENTRY_SIZE + NUM_ELEMENTS_SIZE
	result := make([]byte, totalSize)

	// Copy data section
	copy(result, b.Data)

	// Write offset section
	offset := len(b.Data)
	for _, off := range b.Offsets {
		binary.BigEndian.PutUint16(result[offset:], off)
		offset += OFFSET_ENTRY_SIZE
	}

	// Write number of elements
	binary.BigEndian.PutUint16(result[offset:], uint16(len(b.Offsets)))

	return result
}

// Decode deserializes a byte array into the block
func (b *Block) Decode(data []byte) error {
	if data == nil {
		return ErrInvalidBlockData
	}

	if len(data) < MIN_BLOCK_SIZE {
		return fmt.Errorf("%w: block size too small (%d bytes)", ErrInvalidBlockData, len(data))
	}

	// Read number of elements
	numElements := binary.BigEndian.Uint16(data[len(data)-NUM_ELEMENTS_SIZE:])

	// Check if the data is large enough to contain the offsets
	offsetsSectionSize := int(numElements) * OFFSET_ENTRY_SIZE
	dataSectionEnd := len(data) - offsetsSectionSize - NUM_ELEMENTS_SIZE

	if dataSectionEnd < 0 {
		return fmt.Errorf("%w: not enough data for %d elements", ErrInvalidBlockData, numElements)
	}

	// Read offsets
	b.Offsets = make([]uint16, numElements)
	for i := 0; i < int(numElements); i++ {
		offsetPos := dataSectionEnd + i*OFFSET_ENTRY_SIZE
		b.Offsets[i] = binary.BigEndian.Uint16(data[offsetPos : offsetPos+OFFSET_ENTRY_SIZE])

		// Validate offset
		if int(b.Offsets[i]) >= dataSectionEnd {
			return fmt.Errorf("%w: offset %d out of range", ErrInvalidOffset, b.Offsets[i])
		}
	}

	// Copy data section
	b.Data = make([]byte, dataSectionEnd)
	copy(b.Data, data[:dataSectionEnd])

	return nil
}

// GetEntry retrieves the key and value at the specified index
func (b *Block) GetEntry(index int) ([]byte, []byte, error) {
	if index < 0 || index >= len(b.Offsets) {
		return nil, nil, fmt.Errorf("index out of range: %d", index)
	}

	// Determine entry boundaries
	entryStart := int(b.Offsets[index])
	var entryEnd int
	if index < len(b.Offsets)-1 {
		entryEnd = int(b.Offsets[index+1])
	} else {
		entryEnd = len(b.Data)
	}

	if entryStart >= entryEnd || entryStart >= len(b.Data) {
		return nil, nil, ErrInvalidOffset
	}

	// Read key length
	if entryStart+2 > len(b.Data) {
		return nil, nil, ErrInvalidBlockData
	}
	keyLen := binary.BigEndian.Uint16(b.Data[entryStart : entryStart+2])
	keyStart := entryStart + 2
	keyEnd := keyStart + int(keyLen)

	// Validate key boundaries
	if keyEnd > len(b.Data) || keyEnd > entryEnd {
		return nil, nil, ErrInvalidBlockData
	}

	// Read value length
	if keyEnd+2 > len(b.Data) {
		return nil, nil, ErrInvalidBlockData
	}
	valueLen := binary.BigEndian.Uint16(b.Data[keyEnd : keyEnd+2])
	valueStart := keyEnd + 2
	valueEnd := valueStart + int(valueLen)

	// Validate value boundaries
	if valueEnd > len(b.Data) || valueEnd > entryEnd {
		return nil, nil, ErrInvalidBlockData
	}

	key := b.Data[keyStart:keyEnd]
	value := b.Data[valueStart:valueEnd]

	return key, value, nil
}

// BlockBuilder helps construct a Block with key-value pairs
type BlockBuilder struct {
	data        []byte   // Raw key-value data
	offsets     []uint16 // Offsets to the start of each entry
	targetSize  uint32   // Block size limit
	currentSize uint32   // Current block size
	firstKey    []byte   // First key in the block
	lenBuf      []byte   // Reusable buffer for length encoding
}

// NewBlockBuilder creates a new BlockBuilder with the specified target size
func NewBlockBuilder(targetSize uint32) *BlockBuilder {
	if targetSize < MIN_BLOCK_SIZE {
		targetSize = 4096 // Use a reasonable default if target is too small
	}

	// Align to 4KB boundary
	alignedSize := (targetSize + 4095) & ^uint32(4095)

	// Estimate number of entries based on target size and average entry size
	estimatedEntries := alignedSize / 32 // Assuming average entry size of 32 bytes

	return &BlockBuilder{
		data:        make([]byte, 0, alignedSize),
		offsets:     make([]uint16, 0, estimatedEntries),
		targetSize:  alignedSize,
		currentSize: NUM_ELEMENTS_SIZE, // Start with size of num_elements field
		firstKey:    nil,
		lenBuf:      make([]byte, 2),
	}
}

// EstimatedSize returns the current estimated size of the block
func (b *BlockBuilder) EstimatedSize() uint32 {
	return b.currentSize
}

// Count returns the number of entries added to the builder
func (b *BlockBuilder) Count() int {
	return len(b.offsets)
}

// Add adds a key-value pair to the block
func (b *BlockBuilder) Add(key, value []byte) error {
	// Validate key and value
	if len(key) == 0 {
		return ErrEmptyKey
	}

	if len(key) > MAX_KEY_VALUE_SIZE || len(value) > MAX_KEY_VALUE_SIZE {
		return ErrKeyValueTooLarge
	}

	// Calculate entry size
	entrySize := 2 + len(key) + 2 + len(value) // key_len + key + value_len + value

	// Calculate total size after adding this entry
	newSize := b.currentSize + uint32(entrySize) + OFFSET_ENTRY_SIZE

	// Check if it exceeds target size, unless it's the first entry
	if len(b.offsets) > 0 && newSize > b.targetSize {
		return ErrBlockSizeExceeded
	}

	// Update current size
	b.currentSize = newSize

	// Save first key if this is the first entry
	if len(b.offsets) == 0 {
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey, key)
	}

	// Record the offset of this entry
	b.offsets = append(b.offsets, uint16(len(b.data)))

	// Write key length
	binary.BigEndian.PutUint16(b.lenBuf, uint16(len(key)))
	b.data = append(b.data, b.lenBuf...)

	// Write key
	b.data = append(b.data, key...)

	// Write value length
	binary.BigEndian.PutUint16(b.lenBuf, uint16(len(value)))
	b.data = append(b.data, b.lenBuf...)

	// Write value
	b.data = append(b.data, value...)

	return nil
}

// Build builds and returns the final Block
func (b *BlockBuilder) Build() *Block {
	if len(b.offsets) == 0 {
		return NewBlock()
	}

	return &Block{
		Data:    b.data,
		Offsets: b.offsets,
	}
}

// Reset clears the builder for reuse
func (b *BlockBuilder) Reset() {
	b.data = b.data[:0]
	b.offsets = b.offsets[:0]
	b.currentSize = NUM_ELEMENTS_SIZE
	b.firstKey = nil
}

// FirstKey returns the first key added to the builder
func (b *BlockBuilder) FirstKey() []byte {
	if b.firstKey == nil {
		return nil
	}

	result := make([]byte, len(b.firstKey))
	copy(result, b.firstKey)
	return result
}

// LastKey returns the last key added to the builder
func (b *BlockBuilder) LastKey() []byte {
	if len(b.offsets) == 0 {
		return nil
	}

	lastIdx := len(b.offsets) - 1
	offset := b.offsets[lastIdx]

	// Read key length
	keyLen := binary.BigEndian.Uint16(b.data[offset : offset+2])

	// Extract key
	keyStart := offset + 2
	keyEnd := keyStart + uint16(keyLen)

	result := make([]byte, keyLen)
	copy(result, b.data[keyStart:keyEnd])

	return result
}
