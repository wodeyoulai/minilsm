package mini_lsm

import (
	"sync"
	"sync/atomic"
	"time"
)

const (
	// DefaultGCDiscardRatio is the ratio of versions that can be discarded during GC
	DefaultGCDiscardRatio = 0.7
	// DefaultGCInterval is the interval between GC runs
	DefaultGCInterval = 1 * time.Minute
)

// LsmMvccInner implements the core MVCC mechanism
type LsmMvccInner struct {
	// Lock for writeTx operations
	writeLock sync.Mutex
	// Lock for committing transactions
	commitLock sync.Mutex
	// Timestamp management
	ts *TimestampState
	// Active transactions info
	committedTxns *sync.Map
	// GC related
	discardTs atomic.Uint64
	gcLock    sync.Mutex
	// Options
	opts *MVCCOptions
}

// MVCCOptions contains configuration for MVCC
type MVCCOptions struct {
	GCDiscardRatio float64
	GCInterval     time.Duration
	EnableGC       bool
}

// NewDefaultMVCCOptions creates default options
func NewDefaultMVCCOptions() *MVCCOptions {
	return &MVCCOptions{
		GCDiscardRatio: DefaultGCDiscardRatio,
		GCInterval:     DefaultGCInterval,
		EnableGC:       true,
	}
}

// NewLsmMvccInner creates a new MVCC instance
func NewLsmMvccInner(initialTS uint64, opts *MVCCOptions) *LsmMvccInner {
	if opts == nil {
		opts = NewDefaultMVCCOptions()
	}

	ts := atomic.Uint64{}
	ts.Store(initialTS)

	writeTs := atomic.Uint64{}
	writeTs.Store(initialTS)

	mvcc := &LsmMvccInner{
		ts: &TimestampState{
			timestamp:      &ts,
			writeTimeStamp: &writeTs,
			watermark:      NewWatermark(),
		},
		committedTxns: &sync.Map{},
		opts:          opts,
	}

	// Start GC if enabled
	if opts.EnableGC {
		go mvcc.gcLoop()
	}

	return mvcc
}

// ReadTimestamp allocates a read timestamp
func (l *LsmMvccInner) ReadTimestamp() uint64 {
	return l.ts.timestamp.Load()
}

// CommitTimestamp allocates a commit timestamp
func (l *LsmMvccInner) CommitTimestamp() uint64 {
	l.commitLock.Lock()
	defer l.commitLock.Unlock()

	commitTs := l.ts.writeTimeStamp.Add(1)
	return commitTs
}

func (l *LsmMvccInner) UpdateReadTimestamp() {
	// You may want to consider the minimum read timestamp of an active transaction
	// This is usually done when there are no active transactions or on a regular basis
	l.ts.timestamp.Store(l.ts.writeTimeStamp.Load())
}

// IsCommitted checks if a transaction is committed
func (l *LsmMvccInner) IsCommitted(txnTs uint64) bool {
	_, exists := l.committedTxns.Load(txnTs)
	return exists
}

// AddCommittedTxn records a committed transaction
func (l *LsmMvccInner) AddCommittedTxn(txn *CommittedTxnData) {
	l.committedTxns.Store(txn.CommitTS, txn)
	l.updateWatermark(txn.ReadTS)
}

// HasConflict checks if there are conflicts for the transaction
func (l *LsmMvccInner) HasConflict(txn *CommittedTxnData) bool {
	// Check all transactions committed after txn's read timestamp
	var hasConflict bool
	l.committedTxns.Range(func(key, value interface{}) bool {
		commitTs := key.(uint64)
		if commitTs <= txn.ReadTS {
			return true
		}

		otherTxn := value.(*CommittedTxnData)
		// Check for overlapping writeTx sets
		for hash := range txn.KeyHashes {
			if _, exists := otherTxn.KeyHashes[hash]; exists {
				hasConflict = true
				return false
			}
		}
		return true
	})
	return hasConflict
}

// Watermark returns the current MVCC watermark
func (l *LsmMvccInner) Watermark() uint64 {
	return l.ts.watermark.Read()
}

// UpdateWatermark updates the MVCC watermark
func (l *LsmMvccInner) updateWatermark(readTs uint64) {
	l.ts.watermark.Update(readTs)
}

// GC related methods

func (l *LsmMvccInner) gcLoop() {
	ticker := time.NewTicker(l.opts.GCInterval)
	defer ticker.Stop()

	for range ticker.C {
		l.runGC()
	}
}

func (l *LsmMvccInner) runGC() {
	l.gcLock.Lock()
	defer l.gcLock.Unlock()

	// Get current watermark
	watermark := l.Watermark()
	if watermark == 0 {
		return
	}

	// Find a suitable discard timestamp
	var oldestActiveTs uint64 = ^uint64(0)
	l.committedTxns.Range(func(key, value interface{}) bool {
		txn := value.(*CommittedTxnData)
		if txn.ReadTS < oldestActiveTs {
			oldestActiveTs = txn.ReadTS
		}
		return true
	})

	// Calculate new discard timestamp
	discardTs := uint64(float64(watermark-oldestActiveTs) * l.opts.GCDiscardRatio)
	if discardTs > l.discardTs.Load() {
		l.discardTs.Store(discardTs)
	}
}

// GetDiscardTimestamp returns the timestamp before which versions can be discarded
func (l *LsmMvccInner) GetDiscardTimestamp() uint64 {
	return l.discardTs.Load()
}

// Watermark implementation for managing the low watermark
type Watermark struct {
	value atomic.Uint64
	mu    sync.RWMutex
}

func NewWatermark() *Watermark {
	return &Watermark{}
}

func (w *Watermark) Read() uint64 {
	return w.value.Load()
}

func (w *Watermark) Update(ts uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	current := w.value.Load()
	if ts < current {
		w.value.Store(ts)
	}
}

// TimestampState manages the timestamp allocation
type TimestampState struct {
	mu             sync.Mutex
	timestamp      *atomic.Uint64
	writeTimeStamp *atomic.Uint64
	watermark      *Watermark
}

// Transaction related types
type TransactionState int

const (
	TxnPending TransactionState = iota
	TxnCommitted
	TxnAborted
)

// CommittedTxnData stores metadata for committed transactions
type CommittedTxnData struct {
	KeyHashes map[uint32]struct{}
	ReadTS    uint64
	CommitTS  uint64
}

// NewCommittedTxnData creates new transaction metadata
func NewCommittedTxnData(readTs uint64, commitTs uint64) *CommittedTxnData {
	return &CommittedTxnData{
		KeyHashes: make(map[uint32]struct{}),
		ReadTS:    readTs,
		CommitTS:  commitTs,
	}
}
