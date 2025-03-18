package iterators

import (
	"bytes"
	"container/heap"
)

type HeapWrapper[I StorageIterator] struct {
	index uint
	iter  I
}
type MergeHeap[I StorageIterator] struct {
	items []*HeapWrapper[I]
}

func NewMergeHeap[I StorageIterator](n int) *MergeHeap[I] {
	return &MergeHeap[I]{
		items: make([]*HeapWrapper[I], n),
	}
}
func (h *MergeHeap[I]) Len() int {
	return len(h.items)
}

func (h *MergeHeap[I]) Less(i, j int) bool {
	a, b := h.items[i], h.items[j]
	keyComp := bytes.Compare(a.iter.Key(), b.iter.Key())
	if keyComp != 0 {
		return keyComp < 0
	}
	return a.index < b.index
}

func (h *MergeHeap[I]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
}

func (h *MergeHeap[I]) Push(x any) {
	h.items = append(h.items, x.(*HeapWrapper[I]))
}

func (h *MergeHeap[I]) Pop() any {
	old := h.items
	n := len(old)
	x := old[n-1]
	h.items = old[0 : n-1]
	return x
}
func (h *MergeHeap[I]) Top() any {
	n := len(h.items)
	if n == 0 {
		return nil
	}
	return h.items[n-1]
}

type MergeIterator[I StorageIterator] struct {
	iters    *MergeHeap[I]
	current  *HeapWrapper[I]
	curKey   []byte
	curValue []byte
}

func NewMergeIterator[I StorageIterator](iters []I) *MergeIterator[I] {
	newH := NewMergeHeap[I](0)
	for i := len(iters) - 1; i >= 0; i-- {
		if iters[i].IsValid() {
			heap.Push(newH, &HeapWrapper[I]{
				index: uint(i),
				iter:  iters[i],
			})
		}
	}
	m := &MergeIterator[I]{iters: newH}
	if newH.Len() > 0 {
		m.current = heap.Pop(newH).(*HeapWrapper[I])
		m.curKey = m.current.iter.Key()
		m.curValue = m.current.iter.Value()
	}
	return m
}

func (m *MergeIterator[I]) Key() []byte {
	if m.current == nil || !m.current.iter.IsValid() {
		return nil
	}
	return m.curKey
}

func (m *MergeIterator[I]) Value() []byte {
	if m.current == nil || !m.current.iter.IsValid() {
		return nil
	}
	return m.curValue
}

func (m *MergeIterator[I]) IsValid() bool {
	if m.current == nil {
		return false
	}
	return m.current.iter.IsValid()
}

func (m *MergeIterator[I]) nextIterator() {
	// If the heap is empty, there really isn't any more data
	if m.iters.Len() == 0 {
		m.current = nil
		m.curKey = nil
		m.curValue = nil
	}

	// eject the next iterator from the heap
	m.current = heap.Pop(m.iters).(*HeapWrapper[I])

	//make sure that the acquired ̨iterator is valid
	for !m.current.iter.IsValid() && m.iters.Len() > 0 {
		m.current = heap.Pop(m.iters).(*HeapWrapper[I])
	}

	// If a valid iterator is found, update the current key and value
	if m.current.iter.IsValid() {
		m.curKey = m.current.iter.Key()
		m.curValue = m.current.iter.Value()
	} else {
		m.current = nil
		m.curKey = nil
		m.curValue = nil
	}
}
func (m *MergeIterator[I]) Next() error {
	// If no current valid iterator, nothing to do
	if m.current == nil || !m.current.iter.IsValid() {
		m.curKey = nil
		m.curValue = nil
		return nil
	}
	// Save the current key to detect duplicates
	prevKey := make([]byte, len(m.current.iter.Key()))
	copy(prevKey, m.current.iter.Key())

	// Advance the current iterator
	if err := m.current.iter.Next(); err != nil {
		return err
	}

	// If still valid, push it back to the heap
	if m.current.iter.IsValid() {
		heap.Push(m.iters, m.current)
	}

	// If heap is empty, we're done
	if m.iters.Len() == 0 {
		m.current = nil
		m.curKey = nil
		m.curValue = nil
		return nil
	}

	// Get the next iterator with the smallest key
	m.current = heap.Pop(m.iters).(*HeapWrapper[I])

	// Skip any duplicates of the previous key
	for m.current.iter.IsValid() && bytes.Equal(m.current.iter.Key(), prevKey) {
		if err := m.current.iter.Next(); err != nil {
			return err
		}

		if m.current.iter.IsValid() {
			heap.Push(m.iters, m.current)
		} else if m.iters.Len() == 0 {
			// No more valid iterators
			m.current = nil
			m.curKey = nil
			m.curValue = nil
			return nil
		} else {
			m.current = heap.Pop(m.iters).(*HeapWrapper[I])
		}
	}

	// Update cached key and value
	if m.current.iter.IsValid() {
		m.curKey = m.current.iter.Key()
		m.curValue = m.current.iter.Value()
	} else {
		m.current = nil
		m.curKey = nil
		m.curValue = nil
	}

	return nil
}
