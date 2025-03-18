package mini_lsm

import (
	"bytes"
	"fmt"
	"mini_lsm/pb"
	"mini_lsm/table"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"

	"go.uber.org/zap"
)

func setupLogger(t *testing.T) *zap.Logger {
	logger, err := zap.NewDevelopment()
	if err != nil {
		t.Fatalf("Failed to create logger: %v", err)
	}
	return logger
}

func createTempWALPath(t *testing.T) string {
	// Create a temporary directory for WAL
	tempDir, err := os.MkdirTemp("", "memtable-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	t.Cleanup(func() {
		os.RemoveAll(tempDir)
	})

	return filepath.Join(tempDir, "test.wal")
}

func TestNewMTable(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)

	// Create a new MTable
	mtable := NewMTable(logger, 1, walPath)

	// Verify properties
	if mtable.id != 1 {
		t.Errorf("Expected ID 1, got %d", mtable.id)
	}

	if mtable.approximateSize != 0 {
		t.Errorf("Expected size 0, got %d", mtable.approximateSize)
	}

	if mtable.skl == nil {
		t.Error("SkipList should not be nil")
	}

	if mtable.wal == nil {
		t.Error("WAL should not be nil")
	}
}

func TestMTablePutAndGet(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Prepare test data
	key := &pb.Key{
		Key:       []byte("test-key"),
		Version:   1,
		Timestamp: 123456,
	}

	value := &pb.Value{
		Value: []byte("test-value"),
	}

	// Put value
	err := mtable.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get value
	retrievedValue, found := mtable.Get(key)

	if !found {
		t.Error("Value should be found")
	}

	// Verify the value matches
	if retrievedValue == nil {
		t.Fatal("Retrieved value should not be nil")
	}

	if !bytes.Equal(retrievedValue.Value, value.Value) {
		t.Errorf("Expected value %v, got %v", value.Value, retrievedValue.Value)
	}
}

func TestMTableGetNonExistent(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Try to get a non-existent key
	key := &pb.Key{
		Key:       []byte("non-existent-key"),
		Version:   1,
		Timestamp: 123456,
	}

	value, found := mtable.Get(key)

	if found {
		t.Error("Non-existent key should not be found")
	}

	if value != nil {
		t.Errorf("Value for non-existent key should be nil, got %v", value)
	}
}

func TestMTablePutBatch(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Prepare a batch of 3 key-value pairs
	var batch []*KeyValue

	for i := 1; i <= 3; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("batch-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i * 1000),
		}

		value := &pb.Value{
			Value: []byte(fmt.Sprintf("batch-value-%d", i)),
		}

		batch = append(batch, &KeyValue{
			Key:   key,
			Value: value,
		})
	}

	// Perform batch put
	err := mtable.PutBatch(batch)
	if err != nil {
		t.Fatalf("PutBatch failed: %v", err)
	}

	// Verify all items were inserted
	for i := 1; i <= 3; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("batch-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i * 1000),
		}

		value, found := mtable.Get(key)
		if !found {
			t.Errorf("Item %d not found after batch put", i)
			continue
		}

		expectedValue := []byte(fmt.Sprintf("batch-value-%d", i))
		if !bytes.Equal(value.Value, expectedValue) {
			t.Errorf("Expected value %s, got %s", expectedValue, value.Value)
		}
	}
}

func TestMTableScanAndIterator(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Insert some ordered data
	keys := []string{"a", "b", "c", "d", "e"}
	for i, keyStr := range keys {
		key := &pb.Key{
			Key:       []byte(keyStr),
			Version:   1,
			Timestamp: uint64(i),
		}

		value := &pb.Value{
			Value: []byte(fmt.Sprintf("value-%s", keyStr)),
		}

		err := mtable.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Test full scan
	iter := mtable.Scan(nil, nil)

	count := 0
	scannedKeys := make([]string, 0)

	for iter.IsValid() {
		keyBytes := iter.Key()
		if keyBytes == nil {
			t.Error("Iterator key should not be nil")
		}

		valueBytes := iter.Value()
		if valueBytes == nil {
			t.Error("Iterator value should not be nil")
		}

		// Extract key from raw bytes
		keyStr := string(keyBytes)
		scannedKeys = append(scannedKeys, keyStr)
		count++

		err := iter.Next()
		if err != nil {
			t.Fatalf("Next failed: %v", err)
		}
	}

	if count != len(keys) {
		t.Errorf("Expected %d items in full scan, got %d", len(keys), count)
	}

	// Since the skiplist orders keys, we can check the order is correct
	if !reflect.DeepEqual(scannedKeys, keys) {
		t.Errorf("Expected keys in order %v, got %v", keys, scannedKeys)
	}

	// Test range scan
	start := []byte("b")

	end := []byte("e")

	rangeIter := mtable.Scan(start, end)

	rangeKeys := make([]string, 0)
	for rangeIter.IsValid() {
		rangeKeys = append(rangeKeys, string(rangeIter.Key()))
		err := rangeIter.Next()
		if err != nil {
			t.Errorf("Next failed in range scan: %v", err)
		}
	}

	expectedRangeKeys := []string{"b", "c", "d"}
	if !reflect.DeepEqual(rangeKeys, expectedRangeKeys) {
		t.Errorf("Expected range keys %v, got %v", expectedRangeKeys, rangeKeys)
	}
}

func TestMTableFlush(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Insert some data
	for i := 1; i <= 5; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("flush-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i * 1000),
		}

		value := &pb.Value{
			Value: []byte(fmt.Sprintf("flush-value-%d", i)),
		}

		err := mtable.Put(key, value)
		if err != nil {
			t.Fatalf("Put failed: %v", err)
		}
	}

	// Create SSTable builder for flush
	builder := table.NewSsTableBuilder(4096)

	// Flush to builder
	err := mtable.Flush(builder)
	if err != nil {
		t.Fatalf("Flush failed: %v", err)
	}

	// Verify builder has entries
	if builder.EstimatedSize() == 0 {
		t.Error("Builder should have non-zero size after flush")
	}
}

func TestMTableVersioning(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	keyBytes := []byte("versioned-key")

	// Insert value with version 1
	key1 := &pb.Key{
		Key:       keyBytes,
		Version:   1,
		Timestamp: 1000,
	}

	value1 := &pb.Value{
		Value: []byte("value-v1"),
	}

	err := mtable.Put(key1, value1)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Insert same key with version 2
	key2 := &pb.Key{
		Key:       keyBytes,
		Version:   2,
		Timestamp: 2000,
	}

	value2 := &pb.Value{
		Value: []byte("value-v2"),
	}

	err = mtable.Put(key2, value2)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Get with version 1
	retrievedValue1, found1 := mtable.Get(key1)

	if !found1 {
		t.Error("Version 1 should be found")
	} else if !bytes.Equal(retrievedValue1.Value, value1.Value) {
		t.Errorf("Wrong value for version 1, expected %s, got %s",
			value1.Value, retrievedValue1.Value)
	}

	// Get with version 2
	retrievedValue2, found2 := mtable.Get(key2)

	if !found2 {
		t.Error("Version 2 should be found")
	} else if !bytes.Equal(retrievedValue2.Value, value2.Value) {
		t.Errorf("Wrong value for version 2, expected %s, got %s",
			value2.Value, retrievedValue2.Value)
	}
}

func TestMTableEmptyKeyValue(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Test with empty key content (not nil key)
	emptyKeyContent := &pb.Key{
		Key:       []byte{},
		Version:   1,
		Timestamp: 123456,
	}

	emptyKeyValue := &pb.Value{
		Value: []byte("value for empty key"),
	}

	err := mtable.Put(emptyKeyContent, emptyKeyValue)
	if err != nil {
		t.Errorf("Put with empty key content failed: %v", err)
	}

	// Test with empty value content
	normalKey := &pb.Key{
		Key:       []byte("normal-key"),
		Version:   1,
		Timestamp: 123456,
	}

	emptyValue := &pb.Value{
		Value: []byte{},
	}

	err = mtable.Put(normalKey, emptyValue)
	if err != nil {
		t.Errorf("Put with empty value content failed: %v", err)
	}

	// Verify retrieval works
	retrievedEmptyKeyValue, found := mtable.Get(emptyKeyContent)

	if !found {
		t.Error("Empty key content should be retrievable")
	} else if !bytes.Equal(retrievedEmptyKeyValue.Value, emptyKeyValue.Value) {
		t.Errorf("Wrong value for empty key content")
	}

	// Verify nil key/value handling
	err = mtable.Put(nil, emptyValue)
	if err == nil {
		t.Error("Put with nil key should return error")
	}

	err = mtable.Put(normalKey, nil)
	if err == nil {
		t.Error("Put with nil value should return error")
	}

	_, found = mtable.Get(nil)
	if found {
		t.Error("Get with nil key should not find anything")
	}
}

func TestMTableConcurrency(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)
	mtable := NewMTable(logger, 1, walPath)

	// Number of concurrent operations
	const numOps = 100

	// Wait group to coordinate goroutines
	var wg sync.WaitGroup
	wg.Add(numOps)

	// Perform concurrent puts
	for i := 0; i < numOps; i++ {
		go func(id int) {
			defer wg.Done()

			key := &pb.Key{
				Key:       []byte(fmt.Sprintf("concurrent-key-%d", id)),
				Version:   1,
				Timestamp: uint64(id * 1000),
			}

			value := &pb.Value{
				Value: []byte(fmt.Sprintf("concurrent-value-%d", id)),
			}

			_ = mtable.Put(key, value)
		}(i)
	}

	// Wait for all operations to complete
	wg.Wait()

	// Verify all entries are present
	for i := 0; i < numOps; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("concurrent-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i * 1000),
		}

		_, found := mtable.Get(key)
		if !found {
			t.Errorf("Key %d not found after concurrent puts", i)
		}
	}
}

func TestMTableRecoveryFromWAL(t *testing.T) {
	logger := setupLogger(t)
	walPath := createTempWALPath(t)

	// Create a table and add some data
	originalTable := NewMTable(logger, 42, walPath)

	// Insert data
	key := &pb.Key{
		Key:       []byte("recovery-key"),
		Version:   1,
		Timestamp: 123456,
	}

	value := &pb.Value{
		Value: []byte("recovery-value"),
	}

	err := originalTable.Put(key, value)
	if err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Sync WAL to ensure it's written to disk
	err = originalTable.SyncWAL()
	if err != nil {
		t.Fatalf("SyncWAL failed: %v", err)
	}

	// Create a new table that should recover from WAL
	recoveredTable, err := NewMTableWithWAL(logger, 42, walPath)
	if err != nil {
		t.Fatalf("Recovery failed: %v", err)
	}

	// Verify recovered data
	recoveredValue, found := recoveredTable.Get(key)

	if !found {
		t.Error("Value should be found after recovery")
	} else if !bytes.Equal(recoveredValue.Value, value.Value) {
		t.Errorf("Expected value %v, got %v", value.Value, recoveredValue.Value)
	}
}

func BenchmarkMTablePut(b *testing.B) {
	logger, _ := zap.NewDevelopment()

	// Create a temporary directory for WAL
	tempDir, err := os.MkdirTemp("", "memtable-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "bench.wal")
	mtable := NewMTable(logger, 1, walPath)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("bench-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i),
		}

		value := &pb.Value{
			Value: []byte(fmt.Sprintf("bench-value-%d", i)),
		}

		mtable.Put(key, value)
	}
}

func BenchmarkMTableGet(b *testing.B) {
	logger, _ := zap.NewDevelopment()

	// Create a temporary directory for WAL
	tempDir, err := os.MkdirTemp("", "memtable-bench-*")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	walPath := filepath.Join(tempDir, "bench.wal")
	mtable := NewMTable(logger, 1, walPath)

	// Pre-populate table
	const numEntries = 10000
	for i := 0; i < numEntries; i++ {
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("bench-key-%d", i)),
			Version:   1,
			Timestamp: uint64(i),
		}

		value := &pb.Value{
			Value: []byte(fmt.Sprintf("bench-value-%d", i)),
		}

		mtable.Put(key, value)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % numEntries
		key := &pb.Key{
			Key:       []byte(fmt.Sprintf("bench-key-%d", idx)),
			Version:   1,
			Timestamp: uint64(idx),
		}

		mtable.Get(key)
	}
}

// Mock implementation to help transition
