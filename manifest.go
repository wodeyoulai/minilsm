package plsm

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Enhanced ManifestRecord with integrity fields
type ManifestRecord struct {
	Type           string         `json:"type"`
	FlushID        uint32         `json:"flushID,omitempty"`
	MemtableID     uint32         `json:"memtableID,omitempty"`
	CompactionTask CompactionTask `json:"compactionTask,omitempty"`
	ProducedSSTs   []uint32       `json:"producedSsts,omitempty"`
	Timestamp      int64          `json:"timestamp,omitempty"` // When the record was created
	Checksum       uint32         `json:"checksum,omitempty"`  // For integrity validation
}

// Manifest manages the state of the LSM tree
type Manifest struct {
	file          *os.File
	path          string     // Main manifest path
	backupPath    string     // Backup manifest path
	walPath       string     // Write-ahead log path
	compactThresh int64      // Size threshold for compaction
	mu            sync.Mutex // Protects concurrent operations
	closed        bool       // Flag to track if manifest is closed
}

func isDirectory(path string) bool {
	// 获取路径的信息
	info, err := os.Stat(path)
	if err != nil {
		// 如果路径不存在，返回 false
		return false
	}

	// 检查是否是文件夹
	return info.IsDir()
}

// NewManifest creates a new manifest file with enhanced reliability
func NewManifest(path string) (*Manifest, error) {
	//dirPath := filepath.Dir(path)
	if !isDirectory(path) {
		return nil, fmt.Errorf("%s is not a directory", path)
	}
	mainPath := filepath.Join(path, "MANIFEST")
	backupPath := filepath.Join(path, "MANIFEST.bak")
	walPath := filepath.Join(path, "MANIFEST.wal")

	// Clean up any existing WAL from previous crashes
	_ = os.Remove(walPath)

	file, err := os.OpenFile(mainPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	// Create backup manifest if it doesn't exist
	if _, err := os.Stat(backupPath); os.IsNotExist(err) {
		if err := copyFile(mainPath, backupPath); err != nil {
			// Not critical, continue anyway
			fmt.Printf("Warning: Failed to create backup manifest: %v\n", err)
		}
	}

	return &Manifest{
		file:          file,
		path:          mainPath,
		backupPath:    backupPath,
		walPath:       walPath,
		compactThresh: 10 * 1024 * 1024, // 10MB default threshold for compaction
	}, nil
}

// RecoverManifest reads an existing manifest file and returns the records with enhanced recovery
func RecoverManifest(path string) (*Manifest, []ManifestRecord, error) {
	dirPath := path
	mainPath := filepath.Join(dirPath, "MANIFEST")
	backupPath := filepath.Join(dirPath, "MANIFEST.bak")
	walPath := filepath.Join(dirPath, "MANIFEST.wal")

	// Check if there's a WAL file from a previous interrupted operation
	if stat, err := os.Stat(walPath); err == nil && stat.Size() > 0 {
		// WAL exists, try to recover the operation
		if err := recoverFromWAL(walPath, mainPath); err != nil {
			fmt.Printf("Warning: Failed to recover from WAL: %v\n", err)
		}
		// Always clean up the WAL file after recovery attempt
		_ = os.Remove(walPath)
	}

	// Try to recover from main manifest
	file, records, err := tryRecoverFile(mainPath)
	if err != nil || len(records) == 0 {
		// Try backup if main is corrupt or empty
		_, backupRecords, backupErr := tryRecoverFile(backupPath)
		if backupErr == nil && len(backupRecords) > 0 {
			// Backup is good, restore to main
			if file != nil {
				file.Close()
			}

			// Restore from backup
			if err := copyFile(backupPath, mainPath); err != nil {
				return nil, nil, fmt.Errorf("failed to restore from backup: %w", err)
			}

			file, err = os.OpenFile(mainPath, os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				return nil, nil, err
			}

			records = backupRecords
		} else if err != nil {
			// Both main and backup failed
			return nil, nil, fmt.Errorf("failed to recover manifest: %w", err)
		}
	} else {
		// Main manifest is good, update backup
		if err := copyFile(mainPath, backupPath); err != nil {
			fmt.Printf("Warning: Failed to update backup manifest: %v\n", err)
		}
	}

	m := &Manifest{
		file:          file,
		path:          mainPath,
		backupPath:    backupPath,
		walPath:       walPath,
		compactThresh: 10 * 1024 * 1024, // 10MB default
	}

	return m, records, nil
}

// tryRecoverFile attempts to read and validate a manifest file
func tryRecoverFile(path string) (*os.File, []ManifestRecord, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}

	// Seek to beginning for reading
	if _, err := file.Seek(0, 0); err != nil {
		file.Close()
		return nil, nil, err
	}

	// Read and validate records
	var records []ManifestRecord
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var record ManifestRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			fmt.Printf("Warning: Invalid JSON at line %d: %v\n", lineNum, err)
			continue // Skip corrupt record
		}

		// Validate checksum if present
		if record.Checksum != 0 {
			// Calculate checksum (excluding the checksum field itself)
			savedChecksum := record.Checksum
			record.Checksum = 0 // Zero out for checksum calculation
			data, _ := json.Marshal(record)
			calculatedChecksum := crc32.ChecksumIEEE(data)

			if calculatedChecksum != savedChecksum {
				fmt.Printf("Warning: Checksum mismatch at line %d\n", lineNum)
				continue // Skip corrupt record
			}

			// Restore checksum
			record.Checksum = savedChecksum
		}

		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		file.Close()
		return nil, nil, err
	}

	return file, records, nil
}

// AddRecord adds a new record to the manifest file with WAL protection
func (m *Manifest) AddRecord(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	// Add timestamp
	record.Timestamp = time.Now().UnixNano()

	// Marshal without checksum for checksum calculation
	record.Checksum = 0
	preChecksumData, err := json.Marshal(record)
	if err != nil {
		return err
	}

	// Calculate and set checksum
	record.Checksum = crc32.ChecksumIEEE(preChecksumData)

	// Marshal again with checksum
	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	// 1. Write to WAL first
	if err := writeToWAL(m.walPath, data); err != nil {
		return fmt.Errorf("WAL write failed: %w", err)
	}

	// 2. Now write to main manifest
	if _, err := m.file.Write(data); err != nil {
		return err
	}

	// 3. Ensure data is persisted
	if err := m.file.Sync(); err != nil {
		return err
	}

	// 4. Operation complete, remove WAL
	_ = os.Remove(m.walPath)

	// 5. Update backup manifest periodically
	if stat, err := os.Stat(m.path); err == nil {
		// Update backup every 1MB of changes
		if stat.Size()%1048576 < 1024 { // When crossing a 1MB boundary
			if err := copyFile(m.path, m.backupPath); err != nil {
				fmt.Printf("Warning: Failed to update backup manifest: %v\n", err)
			}
		}

		// Check if compaction is needed
		if stat.Size() > m.compactThresh {
			go m.Compact() // Compact in background
		}
	}

	return nil
}

// writeToWAL safely writes data to the WAL file
func writeToWAL(walPath string, data []byte) error {
	// Create WAL file (or truncate if exists)
	walFile, err := os.OpenFile(walPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer walFile.Close()

	// Write data
	if _, err := walFile.Write(data); err != nil {
		return err
	}

	// Ensure data is on disk
	return walFile.Sync()
}

// recoverFromWAL attempts to apply a pending operation from WAL
func recoverFromWAL(walPath, manifestPath string) error {
	// Open WAL file
	walFile, err := os.Open(walPath)
	if err != nil {
		return err
	}
	defer walFile.Close()

	// Read record from WAL
	scanner := bufio.NewScanner(walFile)
	if !scanner.Scan() {
		return fmt.Errorf("empty WAL file")
	}

	walData := scanner.Bytes()
	if len(walData) == 0 {
		return fmt.Errorf("invalid WAL data")
	}

	// Validate record
	var record ManifestRecord
	if err := json.Unmarshal(walData, &record); err != nil {
		return fmt.Errorf("invalid WAL JSON: %w", err)
	}

	// Validate checksum
	savedChecksum := record.Checksum
	record.Checksum = 0
	preChecksumData, _ := json.Marshal(record)
	calculatedChecksum := crc32.ChecksumIEEE(preChecksumData)

	if calculatedChecksum != savedChecksum {
		return fmt.Errorf("WAL checksum mismatch")
	}

	// Checksum valid, apply to manifest
	manifestFile, err := os.OpenFile(manifestPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer manifestFile.Close()

	// Restore checksum and write
	record.Checksum = savedChecksum
	data, _ := json.Marshal(record)
	data = append(data, '\n')

	if _, err := manifestFile.Write(data); err != nil {
		return err
	}

	return manifestFile.Sync()
}

// Compact creates a more efficient representation of the manifest
func (m *Manifest) Compact() error {
	// We need to acquire the lock to prevent concurrent writes during compaction
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return fmt.Errorf("manifest is closed")
	}

	// Create temporary file for compaction
	compactPath := m.path + ".compact"
	compactFile, err := os.OpenFile(compactPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer compactFile.Close()

	// Read current manifest
	if _, err := m.file.Seek(0, 0); err != nil {
		return err
	}

	// Parse and compact records
	records, err := readAndCompactRecords(m.file)
	if err != nil {
		return err
	}

	// Write compacted records
	for _, record := range records {
		// Update timestamp and checksum
		record.Timestamp = time.Now().UnixNano()
		record.Checksum = 0
		preChecksumData, _ := json.Marshal(record)
		record.Checksum = crc32.ChecksumIEEE(preChecksumData)

		data, _ := json.Marshal(record)
		data = append(data, '\n')

		if _, err := compactFile.Write(data); err != nil {
			return err
		}
	}

	// Ensure data is persisted
	if err := compactFile.Sync(); err != nil {
		return err
	}

	// Use WAL to indicate compaction in progress
	walData := []byte("COMPACTING\n")
	if err := writeToWAL(m.walPath, walData); err != nil {
		return err
	}

	// Close current file to release handles
	currentPath := m.path
	m.file.Close()

	// Rename compacted file to replace current manifest
	if err := os.Rename(compactPath, currentPath); err != nil {
		// Reopen original file on error
		m.file, _ = os.OpenFile(currentPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
		return err
	}

	// Reopen the new compacted file
	m.file, err = os.OpenFile(currentPath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	// Update backup
	if err := copyFile(currentPath, m.backupPath); err != nil {
		fmt.Printf("Warning: Failed to update backup after compaction: %v\n", err)
	}

	// Remove WAL
	_ = os.Remove(m.walPath)

	return nil
}

// readAndCompactRecords reads all records and applies compaction logic
func readAndCompactRecords(file *os.File) ([]ManifestRecord, error) {
	var allRecords []ManifestRecord
	scanner := bufio.NewScanner(file)

	// Read all valid records
	for scanner.Scan() {
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var record ManifestRecord
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			continue // Skip invalid records
		}

		// Skip records with invalid checksums
		if record.Checksum != 0 {
			savedChecksum := record.Checksum
			record.Checksum = 0
			data, _ := json.Marshal(record)
			if crc32.ChecksumIEEE(data) != savedChecksum {
				continue
			}
			record.Checksum = savedChecksum
		}

		allRecords = append(allRecords, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	// Apply compaction logic
	// This is where you'd implement logic to merge/discard obsolete records
	// For now, we'll just return all valid records as a minimal implementation
	return allRecords, nil
}

// AddRecordInit adds a record during initialization (compatibility method)
func (m *Manifest) AddRecordInit(record ManifestRecord) error {
	return m.AddRecord(record)
}

// Close closes the manifest file
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.closed {
		return nil
	}

	m.closed = true

	// Final sync before closing
	if err := m.file.Sync(); err != nil {
		return err
	}

	return m.file.Close()
}

// Helper function to copy a file
func copyFile(src, dst string) error {
	sourceFile, err := os.Open(src)
	if err != nil {
		return err
	}
	defer sourceFile.Close()

	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	if _, err := io.Copy(destFile, sourceFile); err != nil {
		return err
	}

	return destFile.Sync()
}

// Helper function to calculate checksum for a record
func calculateChecksum(record ManifestRecord) uint32 {
	// Zero out checksum field for calculation
	record.Checksum = 0
	data, _ := json.Marshal(record)
	return crc32.ChecksumIEEE(data)
}

// // Helper functions to create different types of records
func NewFlushRecord(id uint32) ManifestRecord {
	return ManifestRecord{
		Type:    "flush",
		FlushID: id,
	}
}

func NewMemtableRecord(id uint32) ManifestRecord {
	return ManifestRecord{
		Type:       "memtable",
		MemtableID: id,
	}
}

func NewCompactionRecord(task CompactionTask) ManifestRecord {
	return ManifestRecord{
		Type:           "compaction",
		CompactionTask: task,
		ProducedSSTs:   task.OutputSstables(),
	}
}

// NewFreezeMemtableRecord creates a record for freezing a memtable
func NewFreezeMemtableRecord(id uint32) ManifestRecord {
	return ManifestRecord{
		Type:       "freeze_memtable",
		MemtableID: id,
	}
}
