package iterators

type StorageIterator interface {
	// Value returns the current value
	Value() []byte

	// Key returns the current key
	// Note: In Go we use []byte instead of the generic KeyType<'a>
	Key() []byte

	// IsValid returns whether the iterator is valid
	IsValid() bool

	// Next moves to the next position
	Next() error

	Close() error
	// NumActiveIterators returns the number of underlying active iterators
	// Default implementation returns 1
	//NumActiveIterators() int

}
