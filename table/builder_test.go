package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/wodeyoulai/plsm/pb"
	"os"
	"path/filepath"
	"testing"
)

func keyMarshal(key *pb.Key) []byte {
	length := len(key.Key) + 16
	out := make([]byte, length)
	copy(out, key.Key)
	binary.BigEndian.PutUint64(out[len(key.Key):], key.Version)

	// ^ for compare
	binary.BigEndian.PutUint64(out[len(key.Key)+8:], ^key.Timestamp)
	return out
}

// 测试辅助函数
func createTempDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "sstable-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})
	return dir
}

func TestSsTableBuilder_New(t *testing.T) {
	tests := []struct {
		name      string
		blockSize uint32
		wantSize  uint32 // 期望对齐后的大小
	}{
		{
			name:      "4KB aligned block size",
			blockSize: 4096,
			wantSize:  4096,
		},
		{
			name:      "unaligned block size should align to 4KB",
			blockSize: 4000,
			wantSize:  4096,
		},
		{
			name:      "larger unaligned size",
			blockSize: 8100,
			wantSize:  8192,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewSsTableBuilder(tt.blockSize)
			if builder.blockSize != tt.wantSize {
				t.Errorf("NewSsTableBuilder() block size = %v, want %v",
					builder.blockSize, tt.wantSize)
			}
		})
	}
}

func TestSsTableBuilder_Add(t *testing.T) {
	tests := []struct {
		name    string
		keys    [][]byte
		values  [][]byte
		wantErr bool
	}{
		{
			name:    "empty key should fail",
			keys:    [][]byte{{}},
			values:  [][]byte{[]byte("value")},
			wantErr: true,
		},
		{
			name:    "single key-value pair",
			keys:    [][]byte{[]byte("key1")},
			values:  [][]byte{[]byte("value1")},
			wantErr: false,
		},
		{
			name: "multiple key-value pairs",
			keys: [][]byte{
				[]byte("key1"),
				[]byte("key2"),
				[]byte("key3"),
			},
			values: [][]byte{
				[]byte("value1"),
				[]byte("value2"),
				[]byte("value3"),
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewSsTableBuilder(4096)

			for i := range tt.keys {
				err := builder.Add(tt.keys[i], tt.values[i])
				if (err != nil) != tt.wantErr {
					t.Errorf("Add() error = %v, wantErr %v", err, tt.wantErr)
					return
				}
				if err != nil {
					return
				}
			}

			// 验证first key和last key
			if len(tt.keys) > 0 && !tt.wantErr {
				if !bytes.Equal(builder.firstKey, tt.keys[0]) {
					t.Errorf("First key = %v, want %v", builder.firstKey, tt.keys[0])
				}
				if !bytes.Equal(builder.lastKey, tt.keys[len(tt.keys)-1]) {
					t.Errorf("Last key = %v, want %v", builder.lastKey, tt.keys[len(tt.keys)-1])
				}
			}
		})
	}
}

func TestSsTableBuilder_Build(t *testing.T) {
	dir := createTempDir(t)

	tests := []struct {
		name     string
		buildFn  func(*SsTableBuilder)
		validate func(*testing.T, *SSTable)
	}{
		{
			name:    "empty table",
			buildFn: func(b *SsTableBuilder) {},
			validate: func(t *testing.T, sst *SSTable) {
				if sst != nil && len(sst.blockMetas) != 0 {
					t.Errorf("Expected no blocks, got %d", len(sst.blockMetas))
				}
			},
		},
		{
			name: "single block table",
			buildFn: func(b *SsTableBuilder) {
				keyObj1 := &pb.Key{Key: []byte("key1"), Version: 0, Timestamp: 0}
				keyObj2 := &pb.Key{Key: []byte("key2"), Version: 0, Timestamp: 0}
				b.Add(keyMarshal(keyObj1), []byte("value1"))
				b.Add(keyMarshal(keyObj2), []byte("value2"))
			},
			validate: func(t *testing.T, sst *SSTable) {
				if len(sst.blockMetas) != 1 {
					t.Errorf("Expected 1 block, got %d", len(sst.blockMetas))
				}
				keyObj1 := &pb.Key{Key: []byte("key1"), Version: 0, Timestamp: 0}
				keyObj2 := &pb.Key{Key: []byte("key2"), Version: 0, Timestamp: 0}
				// 验证元数据
				if !bytes.Equal(sst.blockMetas[0].FirstKey, keyMarshal(keyObj1)) {
					t.Errorf("First key mismatch")
				}
				if !bytes.Equal(sst.blockMetas[0].LastKey, keyMarshal(keyObj2)) {
					t.Errorf("Last key mismatch")
				}
			},
		},
		{
			name: "multiple blocks table",
			buildFn: func(b *SsTableBuilder) {
				// 添加足够多的数据以触发新block创建
				for i := 0; i < 1000; i++ {
					key := []byte(fmt.Sprintf("key%d", i))
					value := []byte(fmt.Sprintf("value%d", i))
					if err := b.Add(key, value); err != nil {
						t.Fatalf("Add failed: %v", err)
					}
				}
			},
			validate: func(t *testing.T, sst *SSTable) {
				if len(sst.blockMetas) <= 1 {
					t.Errorf("Expected multiple blocks, got %d", len(sst.blockMetas))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder := NewSsTableBuilder(4096)
			tt.buildFn(builder)

			// 构建SSTable
			sstPath := filepath.Join(dir, "test.sst")
			sst, err := builder.Build(1, nil, sstPath)
			if err != nil && t.Name() != "TestSsTableBuilder_Build/empty_table" {
				t.Fatalf("Build failed: %v", err)
			}

			// 运行验证
			tt.validate(t, sst)

			// 验证文件是否存在
			if _, err := os.Stat(sstPath); os.IsNotExist(err) && sst != nil {
				t.Errorf("SSTable file was not created")
			}
		})
	}
}

// 测试文件创建功能
func TestSsTableBuilder_CreateFile(t *testing.T) {
	dir := createTempDir(t)

	tests := []struct {
		name    string
		path    string
		wantErr bool
	}{
		{
			name:    "valid path",
			path:    filepath.Join(dir, "test.sst"),
			wantErr: false,
		},
		{
			name:    "nested path",
			path:    filepath.Join(dir, "nested", "dir", "test.sst"),
			wantErr: false,
		},
		{
			name:    "invalid path",
			path:    "",
			wantErr: true,
		},
	}

	builder := NewSsTableBuilder(4096)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := builder.createFile(tt.path)
			if (err != nil) != tt.wantErr {
				t.Errorf("createFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				f.Close()
				// 验证文件是否创建
				if _, err := os.Stat(tt.path); os.IsNotExist(err) {
					t.Errorf("File was not created")
				}
			}
		})
	}
}
