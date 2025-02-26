package iterators

import (
	"bytes"
)

type TwoMergeIterator struct {
	iterA   StorageIterator
	iterB   StorageIterator
	current StorageIterator // Tracks which iterator is currently active
}

func NewTwoMergeIterator(iterA, iterB StorageIterator) (*TwoMergeIterator, error) {
	t := &TwoMergeIterator{
		iterA: iterA,
		iterB: iterB,
	}
	// Initialize the current iterator
	t.chooseNextIterator()
	return t, nil
}

// chooseNextIterator determines which iterator should be current based on key ordering
func (t *TwoMergeIterator) chooseNextIterator() {
	// Handle cases where one or both iterators are invalid
	if !t.iterA.IsValid() {
		t.current = t.iterB
		return
	}
	if !t.iterB.IsValid() {
		t.current = t.iterA
		return
	}

	// Both are valid, compare keys
	comparison := bytes.Compare(t.iterA.Key(), t.iterB.Key())
	if comparison <= 0 {
		// Use iterA if its key is smaller or equal (preferring iterA for equal keys)
		t.current = t.iterA
	} else {
		t.current = t.iterB
	}
}

func (t *TwoMergeIterator) Key() []byte {
	if !t.IsValid() {
		return nil
	}
	return t.current.Key()
}

func (t *TwoMergeIterator) Value() []byte {
	if !t.IsValid() {
		return nil
	}
	return t.current.Value()
}

func (t *TwoMergeIterator) IsValid() bool {
	return t.iterA.IsValid() || t.iterB.IsValid()
}

func (t *TwoMergeIterator) Next() error {
	if !t.IsValid() {
		return nil
	}

	// Remember current key to handle duplicates
	currentKey := t.current.Key()

	// Advance the current iterator
	var err error
	if t.current == t.iterA {
		err = t.iterA.Next()
	} else {
		err = t.iterB.Next()
	}
	if err != nil {
		return err
	}

	// Skip duplicate keys in the other iterator
	if t.current == t.iterA && t.iterB.IsValid() && bytes.Equal(t.iterB.Key(), currentKey) {
		if err = t.iterB.Next(); err != nil {
			return err
		}
	} else if t.current == t.iterB && t.iterA.IsValid() && bytes.Equal(t.iterA.Key(), currentKey) {
		if err = t.iterA.Next(); err != nil {
			return err
		}
	}

	// Choose which iterator should be current now
	t.chooseNextIterator()
	return nil
}
