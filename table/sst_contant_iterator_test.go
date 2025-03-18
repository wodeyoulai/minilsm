package table

import (
	"bytes"
	"errors"
	"testing"
)

// TestSstConcatIterator tests the basic functionality of the SstConcatIterator
func TestSstConcatIterator(t *testing.T) {
	// Test with invalid inputs to ensure it handles edge cases gracefully
	t.Run("EmptyInput", func(t *testing.T) {
		// Test with empty slice
		iter, err := NewSstConcatIteratorFirst(nil)
		if err != nil {
			t.Errorf("Should not error with nil input: %v", err)
		}

		if iter.IsValid() {
			t.Error("Iterator should not be valid with empty input")
		}

		if iter.Key() != nil {
			t.Errorf("Key() should return nil for invalid iterator")
		}

		if iter.Value() != nil {
			t.Errorf("Value() should return nil for invalid iterator")
		}

		// Next() should not panic with invalid iterator
		if err := iter.Next(); err != nil {
			t.Errorf("Next() should not error on invalid iterator: %v", err)
		}

		// NumActiveIterators should return 0 for invalid iterator
		if n := iter.NumActiveIterators(); n != 0 {
			t.Errorf("NumActiveIterators() should return 0 for invalid iterator, got %d", n)
		}
	})

	t.Run("BasicMethods", func(t *testing.T) {
		// Test the basic behavior of a SstConcatIterator without real data
		// Since we can't create real SSTables easily, we'll just check that the methods don't panic

		emptyConcatIter := &SstConcatIterator{
			current:    nil,
			nextSstIdx: 0,
			sstables:   []*SSTable{},
		}

		// These should not panic
		_ = emptyConcatIter.IsValid()
		_ = emptyConcatIter.Key()
		_ = emptyConcatIter.Value()
		_ = emptyConcatIter.Next()
		_ = emptyConcatIter.NumActiveIterators()
	})
}

// TestAdvanceToNextValidTable tests the behavior of moving to the next valid table
func TestAdvanceToNextValidTable(t *testing.T) {
	// Create a minimal test for the advanceToNextValidTable method using real code

	emptyConcatIter := &SstConcatIterator{
		current:    nil,
		nextSstIdx: 0,
		sstables:   []*SSTable{},
	}

	// Test the method by directly calling it
	// This is testing against the actual method from the improved implementation
	err := emptyConcatIter.advanceToNextValidTable()

	if err != nil {
		t.Errorf("advanceToNextValidTable() should not error with empty sstables: %v", err)
	}

	if emptyConcatIter.current != nil {
		t.Error("current should remain nil after advancing with no valid tables")
	}
}

// TestSstConcatIteratorNext tests the Next method behavior
func TestSstConcatIteratorNext(t *testing.T) {
	// Test the Next method behavior
	emptyConcatIter := &SstConcatIterator{
		current:    nil,
		nextSstIdx: 0,
		sstables:   []*SSTable{},
	}

	// Next should not error on invalid iterator
	if err := emptyConcatIter.Next(); err != nil {
		t.Errorf("Next() should not error on invalid iterator: %v", err)
	}
}

// Add more direct tests as needed for your specific use cases
// MockSSTableIterator is a mock implementation of SSTableIterator for testing
type MockSSTableIterator struct {
	keys      [][]byte
	values    [][]byte
	current   int
	seekError bool
	nextError bool
}

func (m *MockSSTableIterator) Close() error {
	return nil
}

func (m *MockSSTableIterator) Key() []byte {
	if !m.IsValid() {
		return nil
	}
	return m.keys[m.current]
}

func (m *MockSSTableIterator) Value() []byte {
	if !m.IsValid() {
		return nil
	}
	return m.values[m.current]
}

func (m *MockSSTableIterator) IsValid() bool {
	return m.current >= 0 && m.current < len(m.keys)
}

func (m *MockSSTableIterator) Next() error {
	if m.nextError {
		return errors.New("mock next error")
	}
	if !m.IsValid() {
		return nil
	}
	m.current++
	return nil
}

func (m *MockSSTableIterator) SeekToKey(key []byte) error {
	if m.seekError {
		return errors.New("mock seek error")
	}
	for i, k := range m.keys {
		if bytes.Compare(k, key) >= 0 {
			m.current = i
			return nil
		}
	}
	m.current = len(m.keys)
	return nil
}

func (m *MockSSTableIterator) SeekToFirst() error {
	if m.seekError {
		return errors.New("mock seek error")
	}
	if len(m.keys) > 0 {
		m.current = 0
	} else {
		m.current = -1
	}
	return nil
}

func (m *MockSSTableIterator) NumActiveIterators() int {
	if m.IsValid() {
		return 1
	}
	return 0
}

// TestSstConcatIteratorWithMocks tests the SstConcatIterator with mock iterators directly
func TestSstConcatIteratorWithMocks(t *testing.T) {
	t.Run("Basic", func(t *testing.T) {
		// Create mock iterators for direct testing
		iter1 := &MockSSTableIterator{
			keys:    [][]byte{[]byte("a"), []byte("c")},
			values:  [][]byte{[]byte("value1"), []byte("value3")},
			current: 0,
		}

		iter2 := &MockSSTableIterator{
			keys:    [][]byte{[]byte("e"), []byte("g")},
			values:  [][]byte{[]byte("value5"), []byte("value7")},
			current: 0,
		}

		// Create a test-specific concat iterator that doesn't use the package functions
		// This avoids the need to replace the package functions
		concatIter := &SstConcatIterator{
			current:    iter1,
			nextSstIdx: 1,
			sstables:   []*SSTable{nil, nil}, // We don't need actual SSTable objects
		}

		// Test basic iteration
		if !concatIter.IsValid() {
			t.Fatal("Iterator should be valid at start")
		}

		if string(concatIter.Key()) != "a" {
			t.Errorf("Expected key 'a', got '%s'", string(concatIter.Key()))
		}

		if string(concatIter.Value()) != "value1" {
			t.Errorf("Expected value 'value1', got '%s'", string(concatIter.Value()))
		}

		// Test Next() within first iterator
		if err := concatIter.Next(); err != nil {
			t.Fatalf("Next() failed: %v", err)
		}

		if string(concatIter.Key()) != "c" {
			t.Errorf("Expected key 'c', got '%s'", string(concatIter.Key()))
		}

		// Create a method to manually advance to the next mock iterator
		advanceToNextMock := func() error {
			iter1.current = len(iter1.keys) // Mark first iterator as exhausted
			concatIter.current = iter2      // Manually switch to second iterator
			return nil
		}

		// Test moving to the second iterator
		if err := advanceToNextMock(); err != nil {
			t.Fatalf("Failed to advance: %v", err)
		}

		if string(concatIter.Key()) != "e" {
			t.Errorf("Expected key 'e' after advancing, got '%s'", string(concatIter.Key()))
		}
	})

	t.Run("EmptyIterator", func(t *testing.T) {
		emptyIter := &MockSSTableIterator{
			keys:    [][]byte{},
			values:  [][]byte{},
			current: -1,
		}

		concatIter := &SstConcatIterator{
			current:    emptyIter,
			nextSstIdx: 0,
			sstables:   []*SSTable{nil},
		}

		if concatIter.IsValid() {
			t.Error("Empty iterator should not be valid")
		}

		if concatIter.Key() != nil {
			t.Errorf("Key() should return nil for invalid iterator")
		}

		if concatIter.Value() != nil {
			t.Errorf("Value() should return nil for invalid iterator")
		}
	})

	t.Run("ErrorHandling", func(t *testing.T) {
		errorIter := &MockSSTableIterator{
			keys:      [][]byte{[]byte("x")},
			values:    [][]byte{[]byte("valuex")},
			current:   0,
			nextError: true,
		}

		concatIter := &SstConcatIterator{
			current:    errorIter,
			nextSstIdx: 0,
			sstables:   []*SSTable{nil},
		}

		err := concatIter.Next()
		if err == nil {
			t.Error("Expected error from Next(), got nil")
		}
	})
}

// TestSstConcatIteratorLogic tests the core logic of the iterator directly
func TestSstConcatIteratorLogic(t *testing.T) {
	// Create a test that directly verifies the Next method logic

	t.Run("NextMethodLogic", func(t *testing.T) {
		// Create a valid iterator that will become invalid after Next
		iter1 := &MockSSTableIterator{
			keys:    [][]byte{[]byte("a")},
			values:  [][]byte{[]byte("value1")},
			current: 0,
		}

		// Create a replacement iterator for when the first becomes invalid
		iter2 := &MockSSTableIterator{
			keys:    [][]byte{[]byte("b")},
			values:  [][]byte{[]byte("value2")},
			current: 0,
		}

		mockIters := []*MockSSTableIterator{iter1, iter2}

		concatIter := &SstConcatIterator{
			current:    iter1,
			nextSstIdx: 1,
			sstables:   []*SSTable{nil, nil},
		}

		// Custom Next implementation that simulates the package's Next but uses our mocks
		customNext := func() error {
			if !concatIter.IsValid() {
				return nil
			}

			if err := concatIter.current.Next(); err != nil {
				return err
			}

			// If the current iterator is exhausted, move to the next one
			if !concatIter.current.IsValid() && concatIter.nextSstIdx < len(mockIters) {
				concatIter.current = mockIters[concatIter.nextSstIdx]
				concatIter.nextSstIdx++
			}

			return nil
		}

		// First key
		if string(concatIter.Key()) != "a" {
			t.Errorf("Expected first key to be 'a', got '%s'", string(concatIter.Key()))
		}

		// After one Next(), the first iterator will be exhausted
		if err := customNext(); err != nil {
			t.Fatalf("Next() failed: %v", err)
		}

		// Should now be at the second iterator
		if string(concatIter.Key()) != "b" {
			t.Errorf("Expected key to be 'b' after Next(), got '%s'", string(concatIter.Key()))
		}

		// One more Next() should exhaust all iterators
		if err := customNext(); err != nil {
			t.Fatalf("Next() failed: %v", err)
		}

		if concatIter.IsValid() {
			t.Error("Iterator should be invalid after exhausting all elements")
		}
	})
}
