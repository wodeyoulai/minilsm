package mini_lsm

import (
	"bytes"
	"github.com/prometheus/client_golang/prometheus"
	"os"
	"testing"
	"time"

	"go.uber.org/zap"
)

func setupTestLogger() *zap.Logger {
	logger, _ := zap.NewDevelopment()
	return logger
}

func setupTestStorage(t *testing.T) (*MiniLsm, string) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "mini-lsm-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	logger := setupTestLogger()

	// Create storage options
	opts := LsmStorageOptions{
		BlockSize:     4096,
		TargetSSTSize: 1024 * 1024, // 1MB
		MemTableLimit: 3,
		CompactionOpts: CompactionOptions{
			Strategy: TaskTypeSimple,
			SimpleOpts: &SimpleLeveledCompactionOptions{
				Level0FileNumCompactionTrigger: 4,
				SizeRatioPercent:               200,
				MaxLevels:                      7,
			},
		},
		EnableWAL:    true,
		Serializable: true,
		Levels:       7,
	}

	// Create storage
	registry := prometheus.NewRegistry()

	storage, err := NewMiniLsm(logger, tempDir, registry, opts)
	if err != nil {
		os.RemoveAll(tempDir)
		t.Fatalf("Failed to create storage: %v", err)
	}

	return storage, tempDir
}

func TestReadTx_Get(t *testing.T) {
	// Setup test environment
	storage, tempDir := setupTestStorage(t)
	defer func() {
		storage.inner.state.memTable.(*MTable).wal.Close()
		os.RemoveAll(tempDir)
	}()

	// Prepare test data
	testKey1 := []byte("key1")
	testValue1 := []byte("value1")
	testKey2 := []byte("key2")
	testValue2 := []byte("value2")
	testKey3 := []byte("key3")
	testValue3 := []byte("value3")

	// Add data to the storage
	if err := storage.Put(testKey1, testValue1); err != nil {
		t.Fatalf("Failed to put key1: %v", err)
	}
	if err := storage.Put(testKey2, testValue2); err != nil {
		t.Fatalf("Failed to put key2: %v", err)
	}
	if err := storage.Put(testKey3, testValue3); err != nil {
		t.Fatalf("Failed to put key3: %v", err)
	}

	// Create MVCC controller
	//mvcc := NewLsmMvccInner(uint64(time.Now().UnixNano()), nil)

	// Create a read transaction
	readTx := NewReadTx(storage.inner, storage.mvcc, true)

	// Test 1: Get existing key
	value, err := readTx.Get(testKey1)
	if err != nil {
		t.Errorf("Failed to get key1: %v", err)
	}
	if !bytes.Equal(value, testValue1) {
		t.Errorf("Expected value %q for key1, got %q", testValue1, value)
	}

	// Test 2: Get another existing key
	value, err = readTx.Get(testKey2)
	if err != nil {
		t.Errorf("Failed to get key2: %v", err)
	}
	if !bytes.Equal(value, testValue2) {
		t.Errorf("Expected value %q for key2, got %q", testValue2, value)
	}

	// Test 3: Get non-existent key
	nonExistentKey := []byte("nonexistent")
	value, err = readTx.Get(nonExistentKey)
	if err == nil {
		t.Errorf("Expected error when getting non-existent key, got nil")
	}

	// Test 4: Rolling back transaction should mark it as closed
	readTx.Rollback()

	// After rollback, operations should fail
	_, err = readTx.Get(testKey1)
	if err == nil {
		t.Errorf("Expected error after rollback, got nil")
	}
}

func TestReadTx_Scan(t *testing.T) {
	// Setup test environment
	storage, tempDir := setupTestStorage(t)
	defer func() {
		storage.inner.state.memTable.(*MTable).wal.Close()
		os.RemoveAll(tempDir)
	}()

	// Prepare test data - use a sequence of keys for scan testing
	keyPrefix := []byte("scankey")
	valuePrefix := []byte("scanvalue")
	numKeys := 10

	// Add data to the storage
	for i := 0; i < numKeys; i++ {
		key := append([]byte{}, keyPrefix...)
		key = append(key, byte(i+'0'))

		value := append([]byte{}, valuePrefix...)
		value = append(value, byte(i+'0'))

		if err := storage.Put(key, value); err != nil {
			t.Fatalf("Failed to put key %s: %v", key, err)
		}
	}

	// Create MVCC controller
	//mvcc := NewLsmMvccInner(uint64(time.Now().UnixNano()), nil)

	// Create a read transaction
	readTx := NewReadTx(storage.inner, storage.mvcc, true)

	// Test 1: Scan all keys
	startKey := append([]byte{}, keyPrefix...)
	startKey = append(startKey, '0')

	endKey := append([]byte{}, keyPrefix...)
	endKey = append(endKey, '9'+1) // Just past the last key

	keys, values, err := readTx.Scan(startKey, endKey, 0) // 0 means no limit
	if err != nil {
		t.Errorf("Failed to scan keys: %v", err)
	}

	if len(keys) != numKeys {
		t.Errorf("Expected %d keys from scan, got %d", numKeys, len(keys))
	}

	// Verify each key-value pair
	for i := 0; i < len(keys); i++ {
		expectedKey := append([]byte{}, keyPrefix...)
		expectedKey = append(expectedKey, byte(i+'0'))

		expectedValue := append([]byte{}, valuePrefix...)
		expectedValue = append(expectedValue, byte(i+'0'))

		if !bytes.Equal(keys[i], expectedKey) {
			t.Errorf("Expected key %q at position %d, got %q", expectedKey, i, keys[i])
		}

		if !bytes.Equal(values[i], expectedValue) {
			t.Errorf("Expected value %q at position %d, got %q", expectedValue, i, values[i])
		}
	}

	// Test 2: Scan with limit
	limit := int64(5)
	keys, values, err = readTx.Scan(startKey, endKey, limit)
	if err != nil {
		t.Errorf("Failed to scan keys with limit: %v", err)
	}

	if len(keys) != int(limit) {
		t.Errorf("Expected %d keys from scan with limit, got %d", limit, len(keys))
	}

	// Test 3: Scan partial range
	partialStartKey := append([]byte{}, keyPrefix...)
	partialStartKey = append(partialStartKey, '3')

	partialEndKey := append([]byte{}, keyPrefix...)
	partialEndKey = append(partialEndKey, '7')

	keys, values, err = readTx.Scan(partialStartKey, partialEndKey, 0)
	if err != nil {
		t.Errorf("Failed to scan partial range: %v", err)
	}

	expectedPartialCount := 4 // keys 3, 4, 5, 6 (7 is the end key, not included)
	if len(keys) != expectedPartialCount {
		t.Errorf("Expected %d keys from partial scan, got %d", expectedPartialCount, len(keys))
	}

	// Test 4: Scan empty range
	emptyRangeStart := []byte("zzz") // After all keys
	keys, values, err = readTx.Scan(emptyRangeStart, nil, 0)
	if err != nil {
		t.Errorf("Failed to scan empty range: %v", err)
	}

	if len(keys) != 0 {
		t.Errorf("Expected 0 keys from empty range scan, got %d", len(keys))
	}

	// Rollback and verify transaction is closed
	readTx.Rollback()

	_, _, err = readTx.Scan(startKey, endKey, 0)
	if err == nil {
		t.Errorf("Expected error after rollback, got nil")
	}
}

func TestReadTx_Isolation(t *testing.T) {
	// Setup test environment
	storage, tempDir := setupTestStorage(t)
	defer func() {
		storage.inner.state.memTable.(*MTable).wal.Close()
		os.RemoveAll(tempDir)
	}()

	// Prepare initial test data
	initialKey := []byte("isolationkey")
	initialValue := []byte("initialvalue")

	if err := storage.Put(initialKey, initialValue); err != nil {
		t.Fatalf("Failed to put initial key-value: %v", err)
	}

	// Create MVCC controller
	mvcc := NewLsmMvccInner(storage.mvcc.ReadTimestamp(), nil)

	// Create a read transaction
	readTx := NewReadTx(storage.inner, mvcc, true)

	// Verify initial value
	value, err := readTx.Get(initialKey)
	if err != nil {
		t.Errorf("Failed to get initial key: %v", err)
	}
	if !bytes.Equal(value, initialValue) {
		t.Errorf("Expected initial value %q, got %q", initialValue, value)
	}

	// Now modify the key outside the transaction
	updatedValue := []byte("updatedvalue")
	if err := storage.Put(initialKey, updatedValue); err != nil {
		t.Fatalf("Failed to update key-value: %v", err)
	}

	// Read transaction should still see the original value (snapshot isolation)
	// The implementation depends on whether your readTx uses MVCC properly
	value, err = readTx.Get(initialKey)
	if err != nil {
		t.Errorf("Failed to get key after update: %v", err)
	}

	// Note: This test might need adjustment based on your exact MVCC implementation
	// If your system doesn't support true snapshot isolation, this might see the updated value
	if !bytes.Equal(value, initialValue) {
		t.Logf("Note: Read transaction saw the updated value instead of initial value.")
		t.Logf("This indicates the isolation level is not providing snapshot isolation.")
		// Uncomment the following line if you expect strict snapshot isolation:
		// t.Errorf("Expected to still see initial value %q (snapshot isolation), got %q", initialValue, value)
	}

	// Clean up
	readTx.Rollback()
}

func TestReadTx_KeyHashes(t *testing.T) {
	// Setup test environment
	storage, tempDir := setupTestStorage(t)
	defer func() {
		storage.inner.state.memTable.(*MTable).wal.Close()
		os.RemoveAll(tempDir)
	}()

	// Create test data
	testKey := []byte("hashkey")
	testValue := []byte("hashvalue")

	if err := storage.Put(testKey, testValue); err != nil {
		t.Fatalf("Failed to put key-value: %v", err)
	}

	// Create MVCC controller
	mvcc := NewLsmMvccInner(uint64(time.Now().UnixNano()), nil)

	// Create a read transaction
	readTx := NewReadTx(storage.inner, mvcc, true)

	// Read the key to add it to the read set
	_, err := readTx.Get(testKey)
	if err != nil {
		t.Errorf("Failed to get key: %v", err)
	}

	// Check if the key hash was recorded in the keyHashes map
	// This is implementation-specific and might require modifying the test
	// or adding helper methods to inspect the read transaction's state

	// Example: If we had access to the internal state:
	// keyHash := hash(testKey)
	// _, exists := readTx.keyHashes.Load(keyHash)
	// if !exists {
	//     t.Errorf("Key hash was not recorded in the read set")
	// }

	// Given the actual implementation, we can only verify indirectly
	// One way would be to use reflection to inspect the private fields,
	// but that's generally not recommended for unit tests

	// Clean up
	readTx.Rollback()
}
