package mini_lsm

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
)

// ManifestRecord represents different types of records that can be stored in the manifest
type ManifestRecord struct {
	Type           string         `json:"type"`
	FlushID        uint32         `json:"flushID,omitempty"`
	MemtableID     uint32         `json:"memtableID,omitempty"`
	CompactionTask CompactionTask `json:"compactionTask,omitempty"`
	ProducedSSTs   []uint32       `json:"producedSsts,omitempty"`
}

// Manifest manages the state of the LSM tree
type Manifest struct {
	file *os.File
	mu   sync.Mutex
}

// NewManifest creates a new manifest file
func NewManifest(path string) (*Manifest, error) {
	file, err := os.OpenFile(filepath.Join(path, "MANIFEST"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &Manifest{
		file: file,
	}, nil
}

// Recover reads an existing manifest file and returns the records and a new manifest instance

func RecoverManifest(path string) (*Manifest, []ManifestRecord, error) {
	file, err := os.OpenFile(filepath.Join(path, "MANIFEST"),
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, nil, err
	}

	m := &Manifest{
		file: file,
	}

	// Read and decode existing records
	var records []ManifestRecord
	decoder := json.NewDecoder(file)
	for decoder.More() {
		var record ManifestRecord
		if err := decoder.Decode(&record); err != nil {
			return nil, nil, err
		}
		records = append(records, record)
	}
	return m, records, nil
}

// AddRecord adds a new record to the manifest file
func (m *Manifest) AddRecord(record ManifestRecord) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := json.Marshal(record)
	if err != nil {
		return err
	}
	data = append(data, '\n')

	if _, err := m.file.Write(data); err != nil {
		return err
	}

	return m.file.Sync()
}

// AddRecordInit adds a record during initialization
func (m *Manifest) AddRecordInit(record ManifestRecord) error {
	return m.AddRecord(record)
}

// Close closes the manifest file
func (m *Manifest) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.file.Close()
}

// Helper functions to create different types of records

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
