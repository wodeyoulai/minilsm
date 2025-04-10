package mini_lsm

import (
	"bytes"
	"mini_lsm/iterators"
)

type MemTableIterator interface {
	iterators.StorageIterator
}

type MergeIterator struct {
	MemTableIterator
}

type LsmIterator struct {
	inner *iterators.TwoMergeIterator
}

// Iterator represents a way to iterate over the LSM tree
type Iterator[I iterators.StorageIterator] struct {
	inner *iterators.MergeIterator[I]
	start []byte
	end   []byte
	valid bool
}

// NewIterator for Scan
func NewIterator[I iterators.StorageIterator](iters []I, start, end []byte, valid bool) *Iterator[I] {
	// start by creating an internal one MergeIterator
	mergeIter := iterators.NewMergeIterator(iters)

	// then create and return to ours Iterator
	it := &Iterator[I]{
		inner: mergeIter,
		start: start,
		end:   end,
		valid: true, // The initial state is set to invalid until the first call Next()
	}

	return it
}

// Iterator methods

func (it *Iterator[I]) Valid() bool {
	if !it.valid {
		return false
	}
	if it.inner == nil || !it.inner.IsValid() {
		return false
	}
	// Check if current key is within range
	if it.end != nil && bytes.Compare(it.inner.Key(), it.end) >= 0 {
		return false
	}
	return true
}

func (it *Iterator[I]) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return it.inner.Key()
}

func (it *Iterator[I]) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return it.inner.Value()
}

func (it *Iterator[I]) Next() error {
	if !it.Valid() {
		return nil
	}
	return it.inner.Next()
}

func (it *Iterator[I]) Close() error {
	it.valid = false
	it.inner = nil
	return nil
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
