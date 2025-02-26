package table

// SstConcatIterator concatenates multiple non-overlapping SST iterators
type SstConcatIterator struct {
	current    SSTableIterator
	nextSstIdx int
	sstables   []*SSTable
}

// NewSstConcatIteratorFirst creates iterator starting from first key
func NewSstConcatIteratorFirst(sstables []*SSTable) (*SstConcatIterator, error) {
	iter := &SstConcatIterator{
		nextSstIdx: 0,
		sstables:   sstables,
	}

	if len(sstables) > 0 {
		current, err := CreateAndSeekToFirst(sstables[0])
		if err != nil {
			return nil, err
		}
		iter.current = current
		iter.nextSstIdx = 1
	}

	return iter, nil
}

// NewSstConcatIteratorSeek creates iterator seeking to specific key
func NewSstConcatIteratorSeek(sstables []*SSTable, key []byte) (*SstConcatIterator, error) {
	iter := &SstConcatIterator{
		nextSstIdx: 0,
		sstables:   sstables,
	}

	// Binary search to find first table containing key
	for i, sst := range sstables {
		if sst.MayContain(key) {
			current, err := CreateAndSeekToKey(sst, key)
			if err != nil {
				return nil, err
			}
			iter.current = current
			iter.nextSstIdx = i + 1
			break
		}
	}

	return iter, nil
}

func (it *SstConcatIterator) Key() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.Key()
}

func (it *SstConcatIterator) Value() []byte {
	if it.current == nil {
		return nil
	}
	return it.current.Value()
}

func (it *SstConcatIterator) IsValid() bool {
	return it.current != nil && it.current.IsValid()
}

func (it *SstConcatIterator) Next() error {
	if it.current == nil {
		return nil
	}

	if err := it.current.Next(); err != nil {
		return err
	}

	// Move to next table if current iterator exhausted
	if !it.current.IsValid() && it.nextSstIdx < len(it.sstables) {
		current, err := CreateAndSeekToFirst(it.sstables[it.nextSstIdx])
		if err != nil {
			return err
		}
		it.current = current
		it.nextSstIdx++
	}

	return nil
}

func (it *SstConcatIterator) NumActiveIterators() int {
	//todo
	return 1
}
