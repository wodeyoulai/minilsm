package table

import (
	"bytes"
	"errors"
	"fmt"
	"mini_lsm/block"
	"mini_lsm/iterators"
	"sync"
)

// Common errors
var (
	ErrInvalidIterator = errors.New("invalid iterator state")
	ErrTableClosed     = errors.New("sstable is closed")
	ErrNoMoreBlocks    = errors.New("no more blocks")
	ErrIteratorClosed  = errors.New("iterator is closed")
	ErrKeyOutOfRange   = errors.New("key is outside the table's range")
)

// SSTableIterator defines the interface for iterating over an SSTable
type SSTableIterator interface {
	iterators.StorageIterator   // Basic storage iterator interface
	SeekToKey(key []byte) error // Seek to the first key >= the given key
	SeekToFirst() error         // Seek to the first key
	Close() error               // Release resources
}

// Implementation of SSTableIterator
type SsTableIterator struct {
	table     *SSTable             // The SSTable being iterated
	blockIter *block.BlockIterator // Current block iterator
	blockIdx  int                  // Current block index
	closed    bool                 // Whether the iterator is closed
	mu        sync.Mutex           // Protects state during concurrent operations
}

// NewSsTableIterator creates a new iterator without positioning it
func NewSsTableIterator(table *SSTable) (SSTableIterator, error) {
	if table == nil {
		return nil, errors.New("table cannot be nil")
	}

	return &SsTableIterator{
		table:     table,
		blockIter: nil,
		blockIdx:  -1,
		closed:    false,
	}, nil
}

// advanceToNextValidTable moves to the next valid SSTable
func (it *SstConcatIterator) advanceToNextValidTable() error {
	for it.nextSstIdx < len(it.sstables) {
		current, err := CreateAndSeekToFirst(it.sstables[it.nextSstIdx])
		if err != nil {
			return fmt.Errorf("create iterator for table %d: %w", it.nextSstIdx, err)
		}
		it.nextSstIdx++

		if current.IsValid() {
			it.current = current
			return nil
		}
	}

	// No more valid tables
	it.current = nil
	return nil
}

// CreateAndSeekToFirst creates a new iterator and seeks to the first key
func CreateAndSeekToFirst(table *SSTable) (SSTableIterator, error) {
	iter, err := NewSsTableIterator(table)
	if err != nil {
		return nil, err
	}

	if err := iter.SeekToFirst(); err != nil {
		_ = iter.Close() // Close on error
		return nil, fmt.Errorf("seek to first: %w", err)
	}

	return iter, nil
}

// CreateAndSeekToKey creates a new iterator and seeks to the first key >= the given key
func CreateAndSeekToKey(table *SSTable, key []byte) (SSTableIterator, error) {
	iter, err := NewSsTableIterator(table)
	if err != nil {
		return nil, err
	}

	if err := iter.SeekToKey(key); err != nil {
		_ = iter.Close() // Close on error
		return nil, fmt.Errorf("seek to key: %w", err)
	}

	return iter, nil
}

// findBlockIndex finds the index of the block that may contain the key
func (it *SsTableIterator) findBlockIndex(key []byte) (int, error) {
	//it.mu.Lock()
	//defer it.mu.Unlock()

	if it.closed {
		return -1, ErrIteratorClosed
	}

	if it.table == nil {
		return -1, ErrInvalidIterator
	}

	// Use binary search to find the appropriate block
	left, right := 0, len(it.table.blockMetas)-1

	for left <= right {
		mid := (left + right) / 2
		meta := it.table.blockMetas[mid]

		// Check if key is within the block's range
		compareFirst := bytes.Compare(key, meta.FirstKey)
		compareLast := bytes.Compare(key, meta.LastKey)

		if compareFirst >= 0 && compareLast <= 0 {
			// Key is in this block's range
			return mid, nil
		}

		if compareFirst < 0 {
			// Key is before this block, look left
			right = mid - 1
		} else {
			// Key is after this block, look right
			left = mid + 1
		}
	}

	// If not found, return the first block that may contain larger keys
	if left < len(it.table.blockMetas) {
		return left, nil
	}

	// Key is beyond the last block
	return -1, ErrKeyOutOfRange
}

// readBlock reads the block at the specified index
func (it *SsTableIterator) readBlock(blockIdx int) error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return ErrIteratorClosed
	}

	if it.table == nil {
		return ErrInvalidIterator
	}

	if blockIdx < 0 || blockIdx >= len(it.table.blockMetas) {
		return fmt.Errorf("block index out of range: %d", blockIdx)
	}

	// Load block from cache or file
	blockData, err := it.table.ReadBlockCached(uint32(blockIdx))
	if err != nil {
		return fmt.Errorf("read block: %w", err)
	}

	// Create or reset block iterator
	if it.blockIter == nil {
		it.blockIter, err = block.CreateAndSeekToFirst(blockData)
	} else {
		err = it.blockIter.Reset(blockData)
		if err == nil {
			err = it.blockIter.SeekToFirst()
		}
	}

	if err != nil {
		return fmt.Errorf("initialize block iterator: %w", err)
	}

	it.blockIdx = blockIdx
	return nil
}

// seekToNextBlock advances to the next block
func (it *SsTableIterator) seekToNextBlock() error {
	it.mu.Lock()

	if it.closed {
		it.mu.Unlock()
		return ErrIteratorClosed
	}

	if it.table == nil {
		it.mu.Unlock()
		return ErrInvalidIterator
	}

	// Check if we're at the last block
	if it.blockIdx >= len(it.table.blockMetas)-1 {
		it.blockIter = nil
		it.mu.Unlock()
		return ErrNoMoreBlocks
	}

	nextBlockIdx := it.blockIdx + 1
	it.mu.Unlock()

	return it.readBlock(nextBlockIdx)
}

// SeekToFirst seeks to the first key in the SSTable
func (it *SsTableIterator) SeekToFirst() error {
	it.mu.Lock()

	if it.closed {
		it.mu.Unlock()
		return ErrIteratorClosed
	}

	if it.table == nil {
		it.mu.Unlock()
		return ErrInvalidIterator
	}

	// Check if table has any blocks
	if len(it.table.blockMetas) == 0 {
		it.blockIter = nil
		it.blockIdx = -1
		it.mu.Unlock()
		return nil
	}

	it.mu.Unlock()

	// Read the first block
	return it.readBlock(0)
}

// SeekToKey seeks to the first key >= the given key
func (it *SsTableIterator) SeekToKey(key []byte) error {
	it.mu.Lock()

	if it.closed {
		it.mu.Unlock()
		return ErrIteratorClosed
	}

	if it.table == nil {
		it.mu.Unlock()
		return ErrInvalidIterator
	}

	// Find appropriate block
	blockIdx, err := it.findBlockIndex(key)

	// No suitable block found
	if err != nil || blockIdx < 0 {
		it.blockIter = nil
		it.blockIdx = -1
		it.mu.Unlock()
		// Not an error, just means we're positioned past the end
		return nil
	}

	it.mu.Unlock()

	// Read the block
	if err := it.readBlock(blockIdx); err != nil {
		return err
	}

	// Seek to the key within the block
	if err := it.blockIter.SeekToKey(key); err != nil {
		return fmt.Errorf("seek within block: %w", err)
	}

	// If not found in this block, try the next one
	if !it.blockIter.IsValid() {
		err := it.seekToNextBlock()
		// No more blocks is not an error here
		if err == ErrNoMoreBlocks {
			return nil
		}
		return err
	}

	return nil
}

// Key returns the current key
func (it *SsTableIterator) Key() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || !it.isValidInternal() {
		return nil
	}

	return it.blockIter.Key()
}

// Value returns the current value
func (it *SsTableIterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || !it.isValidInternal() {
		return nil
	}

	return it.blockIter.Value()
}

// IsValid returns whether the iterator is valid
func (it *SsTableIterator) IsValid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	return it.isValidInternal()
}

// Create an internal version of IsValid that doesn't lock the mutex
func (it *SsTableIterator) isValidInternal() bool {
	return !it.closed && it.table != nil && it.blockIter != nil && it.blockIter.IsValid()
}

// Next moves to the next key
func (it *SsTableIterator) Next() error {
	it.mu.Lock()

	if it.closed {
		it.mu.Unlock()
		return ErrIteratorClosed
	}

	if it.table == nil || it.blockIter == nil {
		it.mu.Unlock()
		return ErrInvalidIterator
	}

	// Move to next entry in current block
	if err := it.blockIter.Next(); err != nil && err != block.ErrEndOfIterator {
		it.mu.Unlock()
		return fmt.Errorf("block iterator next: %w", err)
	}

	// If we've reached the end of the current block, move to next block
	if !it.blockIter.IsValid() {
		it.mu.Unlock()

		err := it.seekToNextBlock()
		// No more blocks is not an error here, just means we're done
		if err == ErrNoMoreBlocks {
			return nil
		}
		return err
	}

	it.mu.Unlock()
	return nil
}

// Close releases resources
func (it *SsTableIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil // Already closed
	}

	it.closed = true

	// Clean up block iterator
	if it.blockIter != nil {
		it.blockIter.Close()
		it.blockIter = nil
	}

	// Clear references but don't close the table
	it.table = nil

	return nil
}

// NumActiveIterators returns the number of active iterators (always 1)
func (it *SsTableIterator) NumActiveIterators() int {
	return 1
}

//-----------------------------------------------------------------------------------
// SSTable Concatenation Iterator
//-----------------------------------------------------------------------------------

// SstConcatIterator concatenates multiple non-overlapping SSTable iterators
type SstConcatIterator struct {
	current    SSTableIterator // Current active iterator
	nextSstIdx int             // Next table index to use
	sstables   []*SSTable      // Tables to iterate over
	closed     bool            // Whether the iterator is closed
	mu         sync.Mutex      // Protects state
}

// NewSstConcatIteratorFirst creates an iterator starting from the first key
func NewSstConcatIteratorFirst(sstables []*SSTable) (*SstConcatIterator, error) {
	iter := &SstConcatIterator{
		nextSstIdx: 0,
		sstables:   sstables,
		closed:     false,
	}

	// Position at first table if available
	if len(sstables) > 0 {
		current, err := CreateAndSeekToFirst(sstables[0])
		if err != nil {
			return nil, fmt.Errorf("create first iterator: %w", err)
		}
		iter.current = current
		iter.nextSstIdx = 1
	}

	return iter, nil
}

// NewSstConcatIteratorSeek creates an iterator seeking to a specific key
func NewSstConcatIteratorSeek(sstables []*SSTable, key []byte) (*SstConcatIterator, error) {
	iter := &SstConcatIterator{
		nextSstIdx: 0,
		sstables:   sstables,
		closed:     false,
	}

	// Find the first table that may contain the key
	for i, sst := range sstables {
		if sst.MayContain(key) {
			current, err := CreateAndSeekToKey(sst, key)
			if err != nil {
				return nil, fmt.Errorf("create iterator for table %d: %w", i, err)
			}
			iter.current = current
			iter.nextSstIdx = i + 1
			break
		}
	}

	return iter, nil
}

// Key returns the current key
func (it *SstConcatIterator) Key() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.current == nil {
		return nil
	}

	return it.current.Key()
}

// Value returns the current value
func (it *SstConcatIterator) Value() []byte {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed || it.current == nil {
		return nil
	}

	return it.current.Value()
}

// IsValid returns whether the iterator is valid
func (it *SstConcatIterator) IsValid() bool {
	it.mu.Lock()
	defer it.mu.Unlock()

	return !it.closed && it.current != nil && it.current.IsValid()
}

// Next moves to the next key
func (it *SstConcatIterator) Next() error {
	it.mu.Lock()

	if it.closed {
		it.mu.Unlock()
		return ErrIteratorClosed
	}

	if it.current == nil {
		it.mu.Unlock()
		return nil // Nothing to iterate
	}

	// Move current iterator forward
	err := it.current.Next()

	// Check if current iterator is exhausted
	if err != nil || !it.current.IsValid() {
		// Clean up current iterator
		if it.current != nil {
			_ = it.current.Close()
		}

		// Try to move to next table
		if it.nextSstIdx < len(it.sstables) {
			nextTable := it.sstables[it.nextSstIdx]
			nextIter, err := CreateAndSeekToFirst(nextTable)
			if err != nil {
				it.current = nil
				it.mu.Unlock()
				return fmt.Errorf("create iterator for next table: %w", err)
			}

			it.current = nextIter
			it.nextSstIdx++
		} else {
			// No more tables
			it.current = nil
		}

		it.mu.Unlock()
		return nil
	}

	it.mu.Unlock()
	return nil
}

// Close releases resources
func (it *SstConcatIterator) Close() error {
	it.mu.Lock()
	defer it.mu.Unlock()

	if it.closed {
		return nil // Already closed
	}

	it.closed = true

	// Close current iterator
	if it.current != nil {
		err := it.current.Close()
		it.current = nil
		return err
	}

	return nil
}

// NumActiveIterators returns the number of active iterators (always 1)
func (it *SstConcatIterator) NumActiveIterators() int {
	if it.current != nil {
		return 1
	}
	return 0
}
