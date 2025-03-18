package block

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
)

// Common errors
var (
	ErrInvalidIterator = errors.New("invalid block iterator state")
	ErrEndOfIterator   = errors.New("end of block iterator")
)

// BlockIterator provides iteration over entries in a Block
type BlockIterator struct {
	// The underlying Block
	block *Block

	// Current key
	key []byte

	// Range for current value in Block.Data
	valueRange struct {
		start uint32
		end   uint32
	}

	// Index of current entry [0, numOfKeys)
	idx int

	// Total number of entries
	numOfEntries int

	// Whether the iterator is valid
	valid bool
}

// NewBlockIterator creates a new block iterator
func NewBlockIterator(block *Block) (*BlockIterator, error) {
	if block == nil {
		return nil, errors.New("block cannot be nil")
	}

	return &BlockIterator{
		block:        block,
		key:          nil,
		valueRange:   struct{ start, end uint32 }{0, 0},
		idx:          -1, // Not positioned yet
		numOfEntries: len(block.Offsets),
		valid:        false,
	}, nil
}

// CreateAndSeekToFirst creates a block iterator and positions it at the first entry
func CreateAndSeekToFirst(block *Block) (*BlockIterator, error) {
	iterator, err := NewBlockIterator(block)
	if err != nil {
		return nil, err
	}

	if err := iterator.SeekToFirst(); err != nil {
		return nil, err
	}

	return iterator, nil
}

// CreateAndSeekToKey creates a block iterator and positions it at the first entry >= key
func CreateAndSeekToKey(block *Block, key []byte) (*BlockIterator, error) {
	iterator, err := NewBlockIterator(block)
	if err != nil {
		return nil, err
	}

	if err := iterator.SeekToKey(key); err != nil {
		return nil, err
	}

	return iterator, nil
}

// Key returns the current key
func (it *BlockIterator) Key() []byte {
	if !it.valid {
		return nil
	}
	return it.key
}

// Value returns the current value
func (it *BlockIterator) Value() []byte {
	if !it.valid || it.block == nil {
		return nil
	}

	if int(it.valueRange.start) >= len(it.block.Data) || int(it.valueRange.end) > len(it.block.Data) {
		return nil
	}

	return it.block.Data[it.valueRange.start:it.valueRange.end]
}

// IsValid returns whether the iterator is valid (positioned at an entry)
func (it *BlockIterator) IsValid() bool {
	return it.valid
}

// SeekToFirst positions the iterator at the first entry
func (it *BlockIterator) SeekToFirst() error {
	if it.block == nil {
		return ErrInvalidIterator
	}

	if it.numOfEntries == 0 {
		it.valid = false
		return nil
	}

	it.idx = 0
	return it.readCurrentEntry()
}

// Next advances to the next entry
func (it *BlockIterator) Next() error {
	if !it.valid {
		return ErrInvalidIterator
	}

	it.idx++
	if it.idx >= it.numOfEntries {
		it.valid = false
		return ErrEndOfIterator
	}

	return it.readCurrentEntry()
}

// SeekToKey positions the iterator at the first entry >= key
func (it *BlockIterator) SeekToKey(key []byte) error {
	if it.block == nil {
		return ErrInvalidIterator
	}

	// If block is empty, there's nothing to seek
	if it.numOfEntries == 0 {
		it.valid = false
		return nil
	}

	// Binary search for the first entry >= key
	left, right := 0, it.numOfEntries-1

	// Sequential scan is often faster for small blocks
	if it.numOfEntries < 8 {
		return it.linearSeekToKey(key)
	}

	for left <= right {
		mid := (left + right) / 2

		// Read key at mid
		if err := it.readEntry(mid); err != nil {
			return err
		}

		cmp := bytes.Compare(it.key, key)
		if cmp == 0 {
			// Found exact match
			it.idx = mid
			it.valid = true
			return nil
		} else if cmp < 0 {
			// Current key is smaller, look in right half
			left = mid + 1
		} else {
			// Current key is larger, look in left half
			right = mid - 1
		}
	}

	// Position at first key >= target
	it.idx = left
	if it.idx >= it.numOfEntries {
		it.valid = false
		return nil
	}

	return it.readCurrentEntry()
}

// linearSeekToKey performs a linear scan to find the first entry >= key
func (it *BlockIterator) linearSeekToKey(key []byte) error {
	if err := it.SeekToFirst(); err != nil {
		return err
	}

	for it.valid {
		if bytes.Compare(it.key, key) >= 0 {
			return nil
		}

		if err := it.Next(); err != nil && err != ErrEndOfIterator {
			return err
		}
	}

	return nil
}

// readCurrentEntry reads the entry at the current index
func (it *BlockIterator) readCurrentEntry() error {
	return it.readEntry(it.idx)
}

// readEntry reads the entry at the specified index
func (it *BlockIterator) readEntry(index int) error {
	if index < 0 || index >= it.numOfEntries {
		it.valid = false
		return fmt.Errorf("index out of range: %d", index)
	}

	entryOffset := it.block.Offsets[index]

	// Ensure we have at least 2 bytes for key length
	if int(entryOffset+2) > len(it.block.Data) {
		it.valid = false
		return fmt.Errorf("invalid entry offset: %d", entryOffset)
	}

	// Read key length
	keyLen := binary.BigEndian.Uint16(it.block.Data[entryOffset : entryOffset+2])
	keyStart := entryOffset + 2
	keyEnd := keyStart + keyLen

	// Ensure key is within bounds
	if int(keyEnd+2) > len(it.block.Data) {
		it.valid = false
		return fmt.Errorf("key extends beyond block data: offset=%d, len=%d", keyStart, keyLen)
	}

	// Read value length
	valueLen := binary.BigEndian.Uint16(it.block.Data[keyEnd : keyEnd+2])
	valueStart := keyEnd + 2
	valueEnd := valueStart + valueLen

	// Ensure value is within bounds
	if int(valueEnd) > len(it.block.Data) {
		it.valid = false
		return fmt.Errorf("value extends beyond block data: offset=%d, len=%d", valueStart, valueLen)
	}

	// Copy key
	if it.key == nil || cap(it.key) < int(keyLen) {
		it.key = make([]byte, keyLen)
	} else {
		it.key = it.key[:keyLen]
	}
	copy(it.key, it.block.Data[keyStart:keyEnd])

	// Set value range
	it.valueRange.start = uint32(valueStart)
	it.valueRange.end = uint32(valueEnd)

	it.valid = true
	return nil
}

// Reset resets the iterator to be reused with a new block
func (it *BlockIterator) Reset(block *Block) error {
	if block == nil {
		return errors.New("block cannot be nil")
	}

	it.block = block
	it.key = nil
	it.valueRange.start = 0
	it.valueRange.end = 0
	it.idx = -1
	it.numOfEntries = len(block.Offsets)
	it.valid = false

	return nil
}

// Close releases resources and invalidates the iterator
func (it *BlockIterator) Close() {
	it.block = nil
	it.key = nil
	it.valid = false
}
