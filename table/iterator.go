package table

// StorageIterator defines the interface for storage iteration
//type SSTableIterator interface {
//	// Key returns the current key
//	iterators.StorageIterator
//	SeekToKey(key []byte) error
//	SeekToFirst() error
//	Close() error
//}

// SsTableIterator implements iteration over SSTable contents
//type SsTableIterator struct {
//	table   *SSTable
//	blkIter *block.BlockIterator
//	blkIdx  int
//	err     error
//}
//
//// NewSsTableIterator creates a new iterator and seeks to the first key-value pair
//func newSSTableIterator(table *SSTable) (SSTableIterator, error) {
//	ss := SsTableIterator{
//		table:  table,
//		blkIdx: 0,
//	}
//	return &ss, nil
//}
//
//func (it *SsTableIterator) findBlockIndex(key []byte) int {
//	// binary search optimization
//	left, right := 0, len(it.table.blockMetas)-1
//
//	for left <= right {
//		mid := (left + right) / 2
//		meta := it.table.blockMetas[mid]
//
//		// Check whether the key is within the current block range
//		if bytes.Compare(key, meta.FirstKey) >= 0 && bytes.Compare(key, meta.LastKey) <= 0 {
//			return mid
//		}
//
//		if bytes.Compare(key, meta.FirstKey) < 0 {
//			right = mid - 1
//		} else {
//			left = mid + 1
//		}
//	}
//
//	// If an exact match is not found, return the first block that may contain a larger than that key
//	return left
//}
//
//func (it *SsTableIterator) seekToNextBlock() error {
//	if it.isLastBlock() {
//		it.blkIter = nil
//		return nil
//	}
//
//	it.blkIdx++
//	return it.readBlock(it.blkIdx)
//}
//
//func (it *SsTableIterator) readBlock(blockIdx int) error {
//	if blockIdx >= len(it.table.blockMetas) {
//		fmt.Printf("block index out of range: %d", blockIdx)
//		return nil
//	}
//
//	// first try to get it from cache
//	if blockCache, ok := it.table.blockCache.Get(it.table.id, uint32(blockIdx)); ok {
//		it.blkIter = block.CreateAndSeekToFirst(blockCache)
//		return nil
//	}
//
//	// Cache miss, read from a file
//	//meta := it.table.blockMetas[blockIdx]
//	rBlock, err := it.table.ReadBlockCached(uint16(blockIdx))
//	if err != nil {
//		return fmt.Errorf("read block failed: %w", err)
//	}
//
//	// put in cache
//	if it.table.blockCache != nil {
//		it.table.blockCache.Put(it.table.id, uint32(blockIdx), rBlock)
//	}
//
//	it.blkIter = block.CreateAndSeekToFirst(rBlock)
//	return nil
//}
//
//// CreateAndSeekToFirst creates a new iterator and seeks to the first key-value pair
//// in the first data block
//func CreateAndSeekToFirst(table *SSTable) (SSTableIterator, error) {
//	iter, err := newSSTableIterator(table)
//	if err != nil {
//		fmt.Printf("CreateAndSeekToFirst failed.error: %v\n", err)
//		return nil, err
//	}
//	err = iter.SeekToFirst()
//	if err != nil {
//		fmt.Printf("CreateAndSeekToFirst failed in find block.error: %v\n", err)
//		return nil, err
//	}
//	return iter, err
//}
//
//// SeekToFirst seeks to the first key-value pair in the first data block
//func (it *SsTableIterator) SeekToFirst() error {
//	return it.readBlock(0)
//}
//
//// CreateAndSeekToKey creates a new iterator and seeks to the first key-value pair which >= key
//func CreateAndSeekToKey(table *SSTable, key []byte) (SSTableIterator, error) {
//	iter, err := newSSTableIterator(table)
//	if err != nil {
//		fmt.Printf("CreateAndSeekToFirst failed.error: %v\n", err)
//		return nil, err
//	}
//	err = iter.SeekToKey(key)
//	if err != nil {
//		fmt.Printf("CreateAndSeekToFirst failed in find block.error: %v\n", err)
//		return nil, err
//	}
//	return iter, err
//}
//
//// SeekToKey seeks to the first key-value pair which >= key
//// Note: Review the documentation for detailed explanation when implementing this function
//func (it *SsTableIterator) SeekToKey(key []byte) error {
//	// find the target block
//	blockIdx := it.findBlockIndex(key)
//	if blockIdx >= len(it.table.blockMetas) {
//		it.blkIter = nil
//		return nil
//	}
//
//	// read block
//	if err := it.readBlock(blockIdx); err != nil {
//		return err
//	}
//	it.blkIdx = blockIdx
//
//	// find-keys-in-block
//	it.blkIter.SeekToKey(key)
//
//	// If the current block cannot be found, try the next block
//	for !it.blkIter.IsValid() && !it.isLastBlock() {
//		return it.seekToNextBlock()
//	}
//	return nil
//}
//
//// Key returns the current key held by the underlying block iterator
//func (it *SsTableIterator) Key() []byte {
//	return it.blkIter.Key()
//}
//
//// Value returns the current value held by the underlying block iterator
//func (it *SsTableIterator) Value() []byte {
//	return it.blkIter.Value()
//}
//
//// IsValid returns whether the current block iterator is valid
//func (it *SsTableIterator) IsValid() bool {
//	return it.blkIter != nil && it.blkIter.IsValid()
//}
//
//func (it *SsTableIterator) Close() error {
//	it.blkIter = nil
//	//it.table.Close()
//	return nil
//}
//
//// Next moves to the next key in the block
//// Note: Check if the current block iterator is valid after the move
//func (it *SsTableIterator) Next() error {
//	if it.blkIter == nil {
//		return fmt.Errorf("invalid iterator state")
//	}
//
//	it.blkIter.Next()
//	if !it.blkIter.IsValid() && !it.isLastBlock() {
//		return it.seekToNextBlock()
//	}
//
//	return nil
//}
//
//// Helper functions
//
//// isLastBlock checks if current block is the last block
//func (it *SsTableIterator) isLastBlock() bool {
//	return it.blkIdx == len(it.table.blockMetas)
//}
//
//// getCurrentBlock returns the current block being iterated
//func (it *SsTableIterator) getCurrentBlock() (*block.Block, error) {
//	// TODO: Implement this helper method
//	return nil, errors.New("not implemented")
//}
