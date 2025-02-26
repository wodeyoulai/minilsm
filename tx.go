package mini_lsm

import "sync"

// Tx represents a transaction
type Tx struct {
	readLock sync.RWMutex
}

// RLock acquires a read lock
func (t *Tx) RLock() {
	t.readLock.RLock()
}

// RUnlock releases a read lock
func (t *Tx) RUnlock() {
	t.readLock.RUnlock()
}
