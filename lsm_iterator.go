package mini_lsm

import "mini_lsm/iterators"

// iterators.StorageIterator defines the interface for storage iteration
// Note: In Go we use []byte for keys since we can't replicate Rust's
// generic associated type KeyType<'a> with lifetime parameters

//type LsmIteratorInner struct {
//	iterators.TwoMergeIterator
//	//memIter *iterators.MergeIterator[MemTableIterator]
//	//ssIter  *iterators.MergeIterator[table.SSTableIterator]
//}

type MemTableIterator interface {
	iterators.StorageIterator
}

type MergeIterator struct {
	MemTableIterator
}

type LsmIterator struct {
	inner *iterators.TwoMergeIterator
}

func NewLsmIterator(inner *iterators.TwoMergeIterator) (*LsmIterator, error) {
	return &LsmIterator{inner: inner}, nil
}

func (l *LsmIterator) IsValid() bool {
	return l.inner.IsValid()
}

func (l *LsmIterator) Key() []byte {
	return l.inner.Key()
}

func (l *LsmIterator) Value() []byte {
	return l.inner.Value()
}

func (l *LsmIterator) Next() error {
	return l.inner.Next()
}

type FusedIterator struct {
	iter       iterators.StorageIterator
	hasErrored bool
}

func NewFusedIterator(iter iterators.StorageIterator) *FusedIterator {
	return &FusedIterator{
		iter:       iter,
		hasErrored: false,
	}
}

func (f *FusedIterator) IsValid() bool {
	panic("not implemented")
}

func (f *FusedIterator) Key() []byte {
	panic("not implemented")
}

func (f *FusedIterator) Value() []byte {
	panic("not implemented")
}

func (f *FusedIterator) Next() error {
	panic("not implemented")
}
