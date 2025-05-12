package plsm

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// setupTestManifest sets up a test directory and creates a manifest
func setupTestManifest(t *testing.T) (*Manifest, string) {
	// Create temporary directory for test
	tempDir, err := ioutil.TempDir("", "manifest-test-")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	manifest, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to create manifest: %v", err)
	}

	return manifest, tempDir
}

// cleanupTestManifest cleans up resources after test
func cleanupTestManifest(t *testing.T, manifest *Manifest, tempDir string) {
	if manifest != nil {
		manifest.Close()
	}
	os.RemoveAll(tempDir)
}

// TestNewManifest 测试创建新的 manifest 文件
func TestNewManifest(t *testing.T) {
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "manifest_test.xx")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建新的 manifest
	m, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("创建 manifest 失败: %v", err)
	}
	defer m.Close()

	// 验证文件是否已创建
	mainPath := filepath.Join(tempDir, "MANIFEST")
	if _, err := os.Stat(mainPath); os.IsNotExist(err) {
		t.Errorf("主 manifest 文件未创建")
	}

	// 验证备份文件是否已创建
	backupPath := filepath.Join(tempDir, "MANIFEST.bak")
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Errorf("备份 manifest 文件未创建")
	}
}

// TestAddRecord 测试添加记录功能
func TestAddRecord(t *testing.T) {
	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "manifest_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 manifest
	m, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("创建 manifest 失败: %v", err)
	}

	// 添加记录
	record := ManifestRecord{
		Type:       "memtable",
		MemtableID: 123,
	}

	if err := m.AddRecord(record); err != nil {
		t.Fatalf("添加记录失败: %v", err)
	}

	// 关闭 manifest
	if err := m.Close(); err != nil {
		t.Fatalf("关闭 manifest 失败: %v", err)
	}

	// 重新打开 manifest 并验证记录
	m2, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("恢复 manifest 失败: %v", err)
	}
	defer m2.Close()

	if len(records) != 1 {
		t.Errorf("期望 1 条记录, 得到 %d 条", len(records))
	}

	if records[0].Type != "memtable" || records[0].MemtableID != 123 {
		t.Errorf("记录内容不匹配: %+v", records[0])
	}

	// 验证记录包含校验和
	if records[0].Checksum == 0 {
		t.Errorf("记录没有包含校验和")
	}
}

// TestRecoverFromCorruptedManifest 测试从损坏的 manifest 恢复
func TestRecoverFromCorruptedManifest(t *testing.T) {
	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "manifest_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 manifest 并添加记录
	m, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("创建 manifest 失败: %v", err)
	}

	// 添加一条有效记录
	record := ManifestRecord{
		Type:       "memtable",
		MemtableID: 123,
	}
	if err := m.AddRecord(record); err != nil {
		t.Fatalf("添加记录失败: %v", err)
	}
	m.Close()

	// 复制主文件到备份
	mainPath := filepath.Join(tempDir, "MANIFEST")
	backupPath := filepath.Join(tempDir, "MANIFEST.bak")
	copyFile(mainPath, backupPath)

	// 损坏主 manifest 文件
	f, err := os.OpenFile(mainPath, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		t.Fatalf("打开 manifest 文件失败: %v", err)
	}
	f.WriteString("这是一个损坏的记录\n")
	f.Close()

	// 尝试恢复
	m2, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("从备份恢复失败: %v", err)
	}
	defer m2.Close()

	// 应该从备份恢复一条记录
	if len(records) != 1 {
		t.Errorf("期望 1 条记录, 得到 %d 条", len(records))
	}

	if records[0].Type != "memtable" || records[0].MemtableID != 123 {
		t.Errorf("记录内容不匹配: %+v", records[0])
	}
}

// TestRecoverFromWAL 测试从 WAL 恢复中断的操作
func TestRecoverFromWAL(t *testing.T) {
	// 创建临时目录
	tempDir, err := ioutil.TempDir("", "manifest_test")
	if err != nil {
		t.Fatalf("创建临时目录失败: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 创建 manifest
	m, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("创建 manifest 失败: %v", err)
	}

	// 添加一条记录
	record1 := ManifestRecord{
		Type:       "memtable",
		MemtableID: 100,
	}
	if err := m.AddRecord(record1); err != nil {
		t.Fatalf("添加记录失败: %v", err)
	}
	m.Close()

	// 手动创建 WAL 文件模拟中断的写入
	record2 := ManifestRecord{
		Type:      "flush",
		FlushID:   200,
		Timestamp: time.Now().UnixNano(),
	}

	// 计算校验和
	record2Checksum := calculateChecksum(record2)
	record2.Checksum = record2Checksum

	// 序列化并写入 WAL
	walPath := filepath.Join(tempDir, "MANIFEST.wal")
	walData, _ := json.Marshal(record2)
	walData = append(walData, '\n')
	err = ioutil.WriteFile(walPath, walData, 0644)
	if err != nil {
		t.Fatalf("创建 WAL 文件失败: %v", err)
	}

	// 尝试恢复
	m2, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	defer m2.Close()

	// 应该有两条记录 (原始记录 + 从 WAL 恢复的记录)
	if len(records) != 2 {
		t.Errorf("期望 2 条记录, 得到 %d 条", len(records))
	}

	// 验证第一条记录
	if records[0].Type != "memtable" || records[0].MemtableID != 100 {
		t.Errorf("第一条记录内容不匹配: %+v", records[0])
	}

	// 验证从 WAL 恢复的记录
	if records[1].Type != "flush" || records[1].FlushID != 200 {
		t.Errorf("第二条记录内容不匹配: %+v", records[1])
	}

	// 验证 WAL 文件已被清理
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Errorf("WAL 文件应该被删除")
	}
}

// TestManifestBackupCreation tests that backups are properly created
func TestManifestBackupCreation(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, manifest, tempDir)

	// Add a record to trigger backup creation
	record := NewMemtableRecord(42)
	err := manifest.AddRecord(record)
	if err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// Check that backup file exists
	backupPath := filepath.Join(tempDir, "MANIFEST.bak")
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		t.Errorf("Backup file was not created")
	}

	// Verify backup content
	backupContent, err := ioutil.ReadFile(backupPath)
	if err != nil {
		t.Fatalf("Failed to read backup file: %v", err)
	}

	if len(backupContent) == 0 {
		t.Errorf("Backup file is empty")
	}
}

// TestRecoverFromBackup tests recovery when main file is corrupted but backup is good
func TestRecoverFromBackup(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)

	// Add records
	for i := 0; i < 5; i++ {
		record := NewMemtableRecord(uint32(i))
		if err := manifest.AddRecord(record); err != nil {
			t.Fatalf("Failed to add record: %v", err)
		}
	}

	// Close manifest to ensure everything is written
	manifest.Close()

	// Corrupt the main manifest file
	mainPath := filepath.Join(tempDir, "MANIFEST")
	if err := ioutil.WriteFile(mainPath, []byte("corrupted data"), 0644); err != nil {
		t.Fatalf("Failed to corrupt manifest: %v", err)
	}

	// Recover from the corrupted state
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover from backup: %v", err)
	}
	defer recoveredManifest.Close()

	// Verify records were recovered from backup
	if len(records) == 0 {
		t.Errorf("No records recovered from backup")
	} else if len(records) != 5 {
		t.Errorf("Expected 5 records, got %d", len(records))
	}

	// Verify record content
	var foundIDs []uint32
	for _, record := range records {
		if record.Type == "memtable" {
			foundIDs = append(foundIDs, record.MemtableID)
		}
	}

	// Check if we found all expected IDs
	for i := 0; i < 5; i++ {
		found := false
		for _, id := range foundIDs {
			if id == uint32(i) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Record with ID %d not recovered", i)
		}
	}

	cleanupTestManifest(t, recoveredManifest, tempDir)
}

// TestChecksumValidation tests that records with invalid checksums are detected
func TestChecksumValidation(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, manifest, tempDir)

	// Add a valid record
	record := NewMemtableRecord(1)
	if err := manifest.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// Close manifest to ensure everything is written
	manifest.Close()

	// Manually corrupt the record in the file by changing a byte
	mainPath := filepath.Join(tempDir, "MANIFEST")
	content, err := ioutil.ReadFile(mainPath)
	if err != nil {
		t.Fatalf("Failed to read manifest: %v", err)
	}

	// Change a byte in the middle of the file
	if len(content) > 20 {
		content[len(content)/2] ^= 0xFF // Flip bits
		if err := ioutil.WriteFile(mainPath, content, 0644); err != nil {
			t.Fatalf("Failed to write corrupted manifest: %v", err)
		}
	}

	// Try to recover - this should ignore the corrupted record
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
	defer recoveredManifest.Close()

	// The corrupted record should be skipped
	// May return 0 records depending on what was corrupted
	if len(records) > 1 {
		t.Errorf("Expected 0 or 1 records, got %d", len(records))
	}
}

// TestCompaction tests the manifest compaction functionality
func TestCompaction(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, manifest, tempDir)

	// Access the compactThresh field to set a smaller threshold for testing
	manifest.compactThresh = 100 // Set a small threshold to trigger compaction

	// Add many records to force compaction
	for i := 0; i < 10; i++ {
		record := NewMemtableRecord(uint32(i))
		if err := manifest.AddRecord(record); err != nil {
			t.Fatalf("Failed to add record: %v", err)
		}
	}

	// Manually trigger compaction
	err := manifest.Compact()
	if err != nil {
		t.Fatalf("Compaction failed: %v", err)
	}

	// Verify the manifest still works after compaction
	record := NewMemtableRecord(100)
	if err := manifest.AddRecord(record); err != nil {
		t.Errorf("Failed to add record after compaction: %v", err)
	}

	// Close and recover to verify integrity
	manifest.Close()

	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover after compaction: %v", err)
	}
	defer recoveredManifest.Close()

	// All records should still be there (or consolidated versions of them)
	if len(records) == 0 {
		t.Errorf("No records found after compaction")
	}

	// Verify the last record is present
	var found bool
	for _, rec := range records {
		if rec.Type == "memtable" && rec.MemtableID == 100 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Last added record not found after compaction")
	}
}

// TestConcurrentWrites tests concurrent writes to the manifest
func TestConcurrentWrites(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, manifest, tempDir)

	var wg sync.WaitGroup
	recordCount := 100
	wg.Add(recordCount)

	for i := 0; i < recordCount; i++ {
		go func(id uint32) {
			defer wg.Done()
			record := NewMemtableRecord(id)
			err := manifest.AddRecord(record)
			if err != nil {
				t.Errorf("Failed to add record %d: %v", id, err)
			}
		}(uint32(i))
	}

	wg.Wait()

	// Close and recover to verify all records were properly written
	manifest.Close()

	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
	defer recoveredManifest.Close()

	// Count memtable records
	var memtableCount int
	for _, rec := range records {
		if rec.Type == "memtable" {
			memtableCount++
		}
	}

	if memtableCount != recordCount {
		t.Errorf("Expected %d memtable records, got %d", recordCount, memtableCount)
	}
}

// TestCloseAndReopen tests closing and reopening the manifest
func TestCloseAndReopen(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)

	// Add a record
	record := NewMemtableRecord(1)
	if err := manifest.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// Close manifest
	if err := manifest.Close(); err != nil {
		t.Fatalf("Failed to close manifest: %v", err)
	}

	// Reopen manifest
	reopenedManifest, err := NewManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to reopen manifest: %v", err)
	}

	// Add another record
	record = NewMemtableRecord(2)
	if err := reopenedManifest.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record to reopened manifest: %v", err)
	}

	// Close again
	reopenedManifest.Close()

	// Recover and verify both records are there
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
	defer cleanupTestManifest(t, recoveredManifest, tempDir)

	var ids []uint32
	for _, rec := range records {
		if rec.Type == "memtable" {
			ids = append(ids, rec.MemtableID)
		}
	}

	if len(ids) != 2 || !containsID(ids, 1) || !containsID(ids, 2) {
		t.Errorf("Expected to find records with IDs 1 and 2, got %v", ids)
	}
}

// TestPartialWrite simulates a partial write and tests recovery
func TestPartialWrite(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)

	// Add a valid record
	record := NewMemtableRecord(1)
	if err := manifest.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// Create a WAL entry but don't complete the write (simulating interrupted write)
	walPath := filepath.Join(tempDir, "MANIFEST.wal")
	record = NewMemtableRecord(2)

	// Add timestamp (normally done by AddRecord)
	record.Timestamp = time.Now().UnixNano()

	// Calculate and set checksum
	record.Checksum = 0
	data, _ := json.Marshal(record)
	record.Checksum = calculateChecksum(record)

	// Marshal again with checksum
	data, _ = json.Marshal(record)
	data = append(data, '\n')

	// Write to WAL but don't update the main manifest (simulating crash)
	if err := ioutil.WriteFile(walPath, data, 0644); err != nil {
		t.Fatalf("Failed to create WAL file: %v", err)
	}

	// Close current manifest
	manifest.Close()

	// Recover - this should apply the WAL entry
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover: %v", err)
	}
	defer cleanupTestManifest(t, recoveredManifest, tempDir)

	// Check that both records are recovered
	var ids []uint32
	for _, rec := range records {
		if rec.Type == "memtable" {
			ids = append(ids, rec.MemtableID)
		}
	}

	if len(ids) != 2 || !containsID(ids, 1) || !containsID(ids, 2) {
		t.Errorf("Expected to find records with IDs 1 and 2, got %v", ids)
	}

	// Verify WAL is cleaned up
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Errorf("WAL file was not cleaned up after recovery")
	}
}

// TestManifestGrowth tests manifest behavior with a large number of records
func TestManifestGrowth(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping large manifest test in short mode")
	}

	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, manifest, tempDir)

	// Set reasonable compaction threshold for testing
	manifest.compactThresh = 10 * 1024 // 10KB

	// Add a large number of records
	recordCount := 1000
	for i := 0; i < recordCount; i++ {
		record := NewMemtableRecord(uint32(i))
		if err := manifest.AddRecord(record); err != nil {
			t.Fatalf("Failed to add record %d: %v", i, err)
		}
	}

	// Check manifest file size
	mainPath := filepath.Join(tempDir, "MANIFEST")
	info, err := os.Stat(mainPath)
	if err != nil {
		t.Fatalf("Failed to stat manifest: %v", err)
	}

	fmt.Printf("Manifest size after %d records: %d bytes\n", recordCount, info.Size())

	// Close and recover
	manifest.Close()

	start := time.Now()
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover large manifest: %v", err)
	}
	defer recoveredManifest.Close()

	recoveryTime := time.Since(start)
	fmt.Printf("Recovery time for %d records: %v\n", recordCount, recoveryTime)

	// Verify record count
	memtableCount := 0
	for _, rec := range records {
		if rec.Type == "memtable" {
			memtableCount++
		}
	}

	if memtableCount != recordCount {
		t.Errorf("Expected %d memtable records, got %d", recordCount, memtableCount)
	}
}

// Helper function to check if a slice contains a specific ID
func containsID(ids []uint32, target uint32) bool {
	for _, id := range ids {
		if id == target {
			return true
		}
	}
	return false
}

// TestCorruptedWAL tests recovery when the WAL file is corrupted
func TestCorruptedWAL(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)

	// Add a valid record
	record := NewMemtableRecord(1)
	if err := manifest.AddRecord(record); err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}

	// Close manifest
	manifest.Close()

	// Create a corrupted WAL file
	walPath := filepath.Join(tempDir, "MANIFEST.wal")
	if err := ioutil.WriteFile(walPath, []byte("corrupted data"), 0644); err != nil {
		t.Fatalf("Failed to create corrupted WAL: %v", err)
	}

	// Recover - this should handle the corrupted WAL gracefully
	recoveredManifest, records, err := RecoverManifest(tempDir)
	if err != nil {
		t.Fatalf("Failed to recover with corrupted WAL: %v", err)
	}
	defer cleanupTestManifest(t, recoveredManifest, tempDir)

	// The original record should still be there
	var found bool
	for _, rec := range records {
		if rec.Type == "memtable" && rec.MemtableID == 1 {
			found = true
			break
		}
	}

	if !found {
		t.Errorf("Original record not found after recovery with corrupted WAL")
	}

	// WAL file should be cleaned up
	if _, err := os.Stat(walPath); !os.IsNotExist(err) {
		t.Errorf("Corrupted WAL file was not cleaned up after recovery")
	}
}

// TestAddRecordAfterClose ensures that adding a record to a closed manifest fails appropriately
func TestAddRecordAfterClose(t *testing.T) {
	manifest, tempDir := setupTestManifest(t)
	defer cleanupTestManifest(t, nil, tempDir) // Pass nil since we're closing manually

	// Close the manifest
	if err := manifest.Close(); err != nil {
		t.Fatalf("Failed to close manifest: %v", err)
	}

	// Try to add a record after closing
	record := NewMemtableRecord(1)
	err := manifest.AddRecord(record)

	if err == nil {
		t.Errorf("Expected error when adding record to closed manifest, but got nil")
	}
}
