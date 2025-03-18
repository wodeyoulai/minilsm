package mini_lsm

import (
	"bytes"
	"fmt"
	"mini_lsm/pb"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestHelper for setting up and cleaning up test environments
type testHelper struct {
	lsm     *MiniLsm
	tempDir string
	cleanup func()
}

func newTestHelper(t *testing.T) *testHelper {
	// Create temp directory
	tempDir, err := os.MkdirTemp("", "lsm_test_")
	require.NoError(t, err, "Failed to create temp directory")

	// Setup logger
	logger, _ := zap.NewDevelopment()

	// Create LSM options
	opts := LsmStorageOptions{
		BlockSize:     4096,
		TargetSSTSize: 1 << 20, // 1MB
		MemTableLimit: 3,
		CompactionOpts: CompactionOptions{
			Strategy: TaskTypeSimple,
			SimpleOpts: &SimpleLeveledCompactionOptions{
				SizeRatioPercent:               200,
				Level0FileNumCompactionTrigger: 4,
				MaxLevels:                      7,
				ThresholdFilesCount:            4,
			},
		},
		EnableWAL:    true,
		Serializable: true,
		Levels:       7,
	}

	// Create LSM
	lsm, err := NewMiniLsm(logger, tempDir, opts)
	require.NoError(t, err, "Failed to create MiniLsm")

	// Setup cleanup function
	cleanup := func() {
		if lsm.stopping != nil {
			close(lsm.stopping)
		}
		time.Sleep(100 * time.Millisecond) // Give workers time to stop
		//os.RemoveAll(tempDir)
	}

	return &testHelper{
		lsm:     lsm,
		tempDir: tempDir,
		cleanup: cleanup,
	}
}

func (h *testHelper) inner() *LsmStorageInner {
	return h.lsm.inner
}

func (h *testHelper) doCleanup() {
	h.cleanup()
}

// Test initialization
func TestLsmStorageInnerInitialization(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Check initial state
	assert.NotNil(t, inner.state.memTable, "Memtable should be initialized")
	assert.Empty(t, inner.state.immMemTables, "Immutable memtables should be empty")
	assert.Empty(t, inner.state.l0SSTables, "L0 SSTables should be empty")
	assert.Len(t, inner.state.levels, int(inner.options.Levels), "Should have correct number of levels")

	// Check that the manifest was initialized
	assert.NotNil(t, inner.manifest, "Manifest should be initialized")

	// Check that the next SSTable ID was initialized
	assert.Greater(t, inner.nextSSTableID.Load(), uint32(0), "Next SSTable ID should be initialized")
}

// Test basic put/get operations
func TestLsmStorageInnerPutGet(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Test data
	testKey := []byte("test_key")
	testValue := []byte("test_value")

	// Put
	err := inner.put(testKey, testValue)
	assert.NoError(t, err, "Put should succeed")

	// Get
	value, err := inner.get(&pb.Key{Key: testKey})
	assert.NoError(t, err, "Get should succeed")
	assert.Equal(t, testValue, value, "Retrieved value should match")

	// Get non-existent key
	nonExistentKey := []byte("non_existent")
	value, err = inner.get(&pb.Key{Key: nonExistentKey})
	assert.Error(t, err, "Get for non-existent key should fail")
	assert.Nil(t, value, "Value for non-existent key should be nil")

	// Test multiple puts and gets
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		val := []byte(fmt.Sprintf("val_%d", i))

		err := inner.put(key, val)
		assert.NoError(t, err, "Put should succeed")

		// Verify immediately
		result, err := inner.get(&pb.Key{Key: key})
		assert.NoError(t, err, "Get should succeed")
		assert.Equal(t, val, result, "Retrieved value should match")
	}
}

// Test memtable freeze
func TestLsmStorageInnerFreezeMemtable(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Remember original memtable
	origMemtable := inner.state.memTable
	origID := origMemtable.ID()

	// Freeze memtable
	err := inner.freezeMemTable()
	assert.NoError(t, err, "Freezing memtable should succeed")

	// Check new state
	assert.NotEqual(t, origMemtable, inner.state.memTable, "New memtable should be created")
	assert.NotZero(t, len(inner.state.immMemTables), "Immutable memtables should not be empty")
	assert.Equal(t, origID, inner.state.immMemTables[0].ID(), "Original memtable should be in immutable list")
	assert.Greater(t, inner.state.memTable.ID(), origID, "New memtable should have higher ID")

	// Add some data to the new memtable
	testKey := []byte("new_memtable_key")
	testValue := []byte("new_memtable_value")

	err = inner.put(testKey, testValue)
	assert.NoError(t, err, "Put to new memtable should succeed")

	// Verify data is in the new memtable
	value, err := inner.get(&pb.Key{Key: testKey})
	assert.NoError(t, err, "Get from new memtable should succeed")
	assert.Equal(t, testValue, value, "Retrieved value should match")
}

// Test scan operation
func TestLsmStorageInnerScan(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Insert test data
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("scan_key_%02d", i))
		value := []byte(fmt.Sprintf("value_%02d", i))

		err := inner.put(key, value)
		require.NoError(t, err, "Put should succeed")
	}

	// Test full scan
	iter, err := inner.Scan([]byte("scan_key_"), []byte("scan_key_~"))
	require.NoError(t, err, "Scan should succeed")

	count := 0
	for iter.Valid() {
		// Check key format
		assert.True(t, bytes.HasPrefix(iter.Key(), []byte("scan_key_")), "Key should have correct prefix")

		// Check value format
		suffix := string(iter.Key()[len("scan_key_"):])
		expectedValue := []byte(fmt.Sprintf("value_%s", suffix))
		v := pb.Value{}
		proto.Unmarshal(iter.Value(), &v)
		assert.Equal(t, expectedValue, v.Value, "Value should match")

		count++
		err = iter.Next()
		require.NoError(t, err, "Iterator Next should succeed")
	}

	assert.Equal(t, 10, count, "Should scan all 10 items")

	// Test partial scan
	iter, err = inner.Scan([]byte("scan_key_03"), []byte("scan_key_07"))
	require.NoError(t, err, "Partial scan should succeed")

	count = 0
	expectedKeys := []string{"scan_key_03", "scan_key_04", "scan_key_05", "scan_key_06"}
	for iter.Valid() {
		assert.Equal(t, []byte(expectedKeys[count]), iter.Key(), "Key should match")

		suffix := string(iter.Key()[len("scan_key_"):])
		expectedValue := []byte(fmt.Sprintf("value_%s", suffix))
		v := pb.Value{}
		proto.Unmarshal(iter.Value(), &v)
		assert.Equal(t, expectedValue, v.Value, "Value should match")

		count++
		err = iter.Next()
		require.NoError(t, err, "Iterator Next should succeed")
	}

	assert.Equal(t, len(expectedKeys), count, "Should scan expected number of items in range")

	// Test invalid range
	_, err = inner.Scan([]byte("z"), []byte("a"))
	assert.Error(t, err, "Scan with invalid range should fail")
}

// Test flush operation
func TestLsmStorageInnerFlush(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Load test data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("flush_key_%03d", i))
		value := []byte(fmt.Sprintf("value_%03d", i))

		err := inner.put(key, value)
		require.NoError(t, err, "Put should succeed")
	}

	// Freeze memtable multiple times to reach limit
	for i := 0; i < int(inner.options.MemTableLimit); i++ {
		err := inner.freezeMemTable()
		require.NoError(t, err, "Freezing memtable should succeed")
	}

	// Get initial L0 count
	initialL0Count := len(inner.state.l0SSTables)
	initialImmCount := len(inner.state.immMemTables)

	// Trigger flush
	err := inner.triggerFlush()
	require.NoError(t, err, "Flush should succeed")

	// Check that L0 has a new SSTable (or that L1 has it if compaction happened)
	l0Increase := len(inner.state.l0SSTables) > initialL0Count
	l1Exists := len(inner.state.levels) > 0 && len(inner.state.levels[0].ssTables) > 0
	assert.True(t, l0Increase || l1Exists, "Should have new SSTable in L0 or L1")

	// Check that immutable memtable count decreased
	assert.Less(t, len(inner.state.immMemTables), initialImmCount,
		"Immutable memtable count should decrease")

	// Check that data is still accessible
	for i := 0; i < 10; i++ {
		key := []byte(fmt.Sprintf("flush_key_%03d", i))
		expectedValue := []byte(fmt.Sprintf("value_%03d", i))

		value, err := inner.get(&pb.Key{Key: key})

		assert.NoError(t, err, "Get after flush should succeed")
		assert.Equal(t, expectedValue, value.Value, "Value after flush should match")
	}

	// Verify that an SSTable file was created
	files, err := os.ReadDir(inner.path)
	require.NoError(t, err, "Reading directory should succeed")

	foundSST := false
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sst" {
			foundSST = true
			break
		}
	}
	assert.True(t, foundSST, "Should find an SSTable file")
}

// Test recovery from manifest
func TestLsmStorageInnerRecovery(t *testing.T) {
	// First create and populate a storage
	helper := newTestHelper(t)
	tempDir := helper.tempDir

	// Add data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("recovery_key_%03d", i))
		value := []byte(fmt.Sprintf("value_%03d", i))

		err := helper.lsm.Put(key, value)
		require.NoError(t, err, "Put should succeed")
	}

	// Create some immutable memtables
	inner := helper.inner()
	for i := 0; i < int(inner.options.MemTableLimit); i++ {
		err := inner.freezeMemTable()
		require.NoError(t, err, "Freezing memtable should succeed")
	}

	// Flush to create an SSTable
	err := inner.triggerFlush()
	require.NoError(t, err, "Flush should succeed")

	// Close original storage
	helper.doCleanup()

	// Create new storage at same path
	logger, _ := zap.NewDevelopment()
	newLsm, err := NewMiniLsm(logger, tempDir, *inner.options)
	require.NoError(t, err, "Creating new LSM should succeed")

	// Check that recovery worked
	//newInner := newLsm.inner

	// Verify data access
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("recovery_key_%03d", i))
		expectedValue := []byte(fmt.Sprintf("value_%03d", i))

		value, err := newLsm.Get(key)
		v := pb.Value{}
		proto.Unmarshal(value, &v)
		assert.NoError(t, err, "Get after recovery should succeed")
		assert.Equal(t, expectedValue, v.Value, "Value after recovery should match")
	}

	// Clean up
	close(newLsm.stopping)
	os.RemoveAll(tempDir)
}

// Test concurrent operations
func TestLsmStorageInnerConcurrency(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	lsm := helper.lsm

	// Number of operations and goroutines
	const numOps = 100
	const numGoroutines = 5

	// Wait group to synchronize goroutines
	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	// Channel to collect errors
	errChan := make(chan error, numGoroutines*numOps)

	// Launch goroutines
	for g := 0; g < numGoroutines; g++ {
		go func(id int) {
			defer wg.Done()

			for i := 0; i < numOps; i++ {
				// Create unique key-value pair
				key := []byte(fmt.Sprintf("concurrent_key_g%d_%03d", id, i))
				value := []byte(fmt.Sprintf("value_g%d_%03d", id, i))

				// Put
				err := lsm.Put(key, value)
				if err != nil {
					errChan <- fmt.Errorf("put failed: %w", err)
					continue
				}

				// Get
				result, err := lsm.Get(key)
				if err != nil {
					errChan <- fmt.Errorf("get failed: %w", err)
					continue
				}

				if !bytes.Equal(value, result) {
					errChan <- fmt.Errorf("value mismatch: got %s, want %s", result, value)
				}
			}
		}(g)
	}

	// Wait for all goroutines to finish
	wg.Wait()
	close(errChan)

	// Check for errors
	var errors []error
	for err := range errChan {
		errors = append(errors, err)
	}

	assert.Empty(t, errors, "Should have no errors in concurrent operations")

	// Verify that all data is accessible
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < numOps; i += 10 { // Sample every 10th item for speed
			key := []byte(fmt.Sprintf("concurrent_key_g%d_%03d", g, i))
			expectedValue := []byte(fmt.Sprintf("value_g%d_%03d", g, i))

			value, err := lsm.Get(key)
			assert.NoError(t, err, "Get after concurrent operations should succeed")
			assert.Equal(t, expectedValue, value, "Value after concurrent operations should match")
		}
	}
}

// Test path utilities
func TestLsmStorageInnerPaths(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Test SST path
	id := uint32(12345)
	sstPath := inner.sstPath(id)
	expected := filepath.Join(inner.path, "12345.sst")
	assert.Equal(t, expected, sstPath, "SST path should be correctly formatted")

	// Test WAL path
	walPath := inner.walPath(id)
	expected = filepath.Join(inner.path, "12345.wal")
	assert.Equal(t, expected, walPath, "WAL path should be correctly formatted")

	// Test manifest path
	manifestPath := inner.manifestPath()
	expected = filepath.Join(inner.path, "MANIFEST")
	assert.Equal(t, expected, manifestPath, "Manifest path should be correctly formatted")
}

// Test large insertion with multiple flushes
func TestLsmStorageInnerMultipleFlushes(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Insert lots of data to trigger multiple flushes
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("multiflush_key_%04d", i))
		value := []byte(fmt.Sprintf("value_%04d", i))

		err := inner.put(key, value)
		require.NoError(t, err, "Put should succeed")

		// Periodically freeze memtable to trigger flushes
		if i > 0 && i%100 == 0 {
			err = inner.freezeMemTable()
			require.NoError(t, err, "Freezing memtable should succeed")

			if len(inner.state.immMemTables) >= int(inner.options.MemTableLimit) {
				err = inner.triggerFlush()
				require.NoError(t, err, "Flush should succeed")
			}
		}
	}

	// Verify data is accessible
	for i := 0; i < 1000; i += 50 { // Sample every 50th key
		key := []byte(fmt.Sprintf("multiflush_key_%04d", i))
		expectedValue := []byte(fmt.Sprintf("value_%04d", i))

		value, err := inner.get(&pb.Key{Key: key})

		assert.NoError(t, err, "Get after multiple flushes should succeed")
		assert.Equal(t, expectedValue, value.Value, "Value after multiple flushes should match")
	}

	// Check that multiple SST files were created
	files, err := os.ReadDir(inner.path)
	require.NoError(t, err, "Reading directory should succeed")

	sstCount := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".sst" {
			sstCount++
		}
	}
	assert.Greater(t, sstCount, 1, "Should create multiple SST files")
}

// Test error cases for get
func TestLsmStorageInnerGetErrors(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Test get with nil key
	value, err := inner.get(nil)
	assert.Error(t, err, "Get with nil key should fail")
	assert.Nil(t, value, "Value should be nil for nil key")

	// Test get with empty key
	value, err = inner.get(&pb.Key{Key: []byte{}})
	assert.Error(t, err, "Get with empty key should fail")
	assert.Nil(t, value, "Value should be nil for empty key")
}

// Test SSTable-related functionality
func TestLsmStorageInnerSSTables(t *testing.T) {
	helper := newTestHelper(t)
	defer helper.doCleanup()

	inner := helper.inner()

	// Insert data
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("sstable_key_%03d", i))
		value := []byte(fmt.Sprintf("value_%03d", i))

		err := inner.put(key, value)
		require.NoError(t, err, "Put should succeed")
	}

	// Freeze memtable
	err := inner.freezeMemTable()
	require.NoError(t, err, "Freezing memtable should succeed")

	// Flush to create SSTable
	err = inner.triggerFlush()
	require.NoError(t, err, "Flush should succeed")

	// Check that at least one SSTable was created
	assert.NotEmpty(t, inner.state.l0SSTables, "L0 should have at least one SSTable")

	if len(inner.state.l0SSTables) > 0 {
		// Get the SSTable ID
		sstID := inner.state.l0SSTables[0]

		// Test getOrOpenSSTable
		sst, err := inner.getOrOpenSSTable(sstID)
		assert.NoError(t, err, "Getting SSTable should succeed")
		assert.NotNil(t, sst, "SSTable should not be nil")

		// Test opening already opened SSTable
		sst2, err := inner.getOrOpenSSTable(sstID)
		assert.NoError(t, err, "Getting already opened SSTable should succeed")
		assert.Equal(t, sst, sst2, "Should return the same SSTable object")

		// Test getFirstKeyOfSStableId
		key, err := inner.getFirstKeyOfSStableId(sstID)
		assert.NoError(t, err, "Getting first key should succeed")
		assert.NotNil(t, key, "First key should not be nil")
	}
}
