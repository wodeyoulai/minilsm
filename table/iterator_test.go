package table

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
)

// Helper function to marshal keys in the expected format
func marshalTestKey(keyStr string, version uint64, timestamp uint64) []byte {
	// Mimic the keyMarshal function from kv.go
	key := []byte(keyStr)
	length := len(key) + 16
	out := make([]byte, length)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], version)
	binary.BigEndian.PutUint64(out[len(key)+8:], ^timestamp) // Note the bitwise NOT for timestamp
	return out
}

// 测试辅助函数
func createTestSSTable(t *testing.T, pairs []struct{ key, value []byte }) (*SSTable, string) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.sst")

	// 创建SSTable
	builder := NewSsTableBuilder(4096)
	for _, pair := range pairs {
		err := builder.Add(pair.key, pair.value)
		if err != nil {
			t.Fatalf("failed to add key-value pair: %v", err)
		}
	}

	// 构建SSTable
	cache := NewBlockCache(100)
	sst, err := builder.Build(1, cache, path)
	if err != nil {
		t.Fatalf("failed to build SSTable: %v", err)
	}

	return sst, path
}

func TestSsTableIterator_SeekToFirst(t *testing.T) {
	testData := []struct {
		key, value []byte
	}{
		{marshalTestKey("key1", 1, 100), []byte("value1")},
		{marshalTestKey("key2", 1, 100), []byte("value2")},
		{marshalTestKey("key3", 1, 100), []byte("value3")},
	}

	sst, _ := createTestSSTable(t, testData)

	tests := []struct {
		name      string
		wantKey   []byte
		wantValue []byte
		wantValid bool
	}{
		{
			name:      "seek to first should return first key-value pair",
			wantKey:   marshalTestKey("key1", 1, 100),
			wantValue: []byte("value1"),
			wantValid: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := CreateAndSeekToFirst(sst)
			if err != nil {
				t.Fatalf("CreateAndSeekToFirst failed: %v", err)
			}
			defer iter.Close()

			if iter.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v", iter.IsValid(), tt.wantValid)
			}

			if !bytes.Equal(iter.Key(), tt.wantKey) {
				t.Errorf("Key() = %v, want %v", iter.Key(), tt.wantKey)
			}

			if !bytes.Equal(iter.Value(), tt.wantValue) {
				t.Errorf("Value() = %s, want %s", iter.Value(), tt.wantValue)
			}
		})
	}
}

func TestSsTableIterator_SeekToKey(t *testing.T) {
	testData := []struct {
		key, value []byte
	}{
		{marshalTestKey("key1", 1, 100), []byte("value1")},
		{marshalTestKey("key3", 1, 100), []byte("value3")},
		{marshalTestKey("key5", 1, 100), []byte("value5")},
	}

	sst, _ := createTestSSTable(t, testData)

	tests := []struct {
		name      string
		seekKey   []byte
		wantKey   []byte
		wantValue []byte
		wantValid bool
	}{
		{
			name:      "seek existing key",
			seekKey:   marshalTestKey("key3", 1, 100),
			wantKey:   marshalTestKey("key3", 1, 100),
			wantValue: []byte("value3"),
			wantValid: true,
		},
		{
			name:      "seek non-existing key (between existing keys)",
			seekKey:   marshalTestKey("key2", 1, 100),
			wantKey:   marshalTestKey("key3", 1, 100),
			wantValue: []byte("value3"),
			wantValid: true,
		},
		{
			name:      "seek key before first key",
			seekKey:   marshalTestKey("key0", 1, 100),
			wantKey:   marshalTestKey("key1", 1, 100),
			wantValue: []byte("value1"),
			wantValid: true,
		},
		{
			name:      "seek key after last key",
			seekKey:   marshalTestKey("key9", 1, 100),
			wantValid: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter, err := CreateAndSeekToKey(sst, tt.seekKey)
			if err != nil {
				t.Fatalf("CreateAndSeekToKey failed: %v", err)
			}
			defer iter.Close()

			if iter.IsValid() != tt.wantValid {
				t.Errorf("IsValid() = %v, want %v", iter.IsValid(), tt.wantValid)
			}

			if tt.wantValid {
				if !bytes.Equal(iter.Key(), tt.wantKey) {
					t.Errorf("Key() = %v, want %v", iter.Key(), tt.wantKey)
				}

				if !bytes.Equal(iter.Value(), tt.wantValue) {
					t.Errorf("Value() = %s, want %s", iter.Value(), tt.wantValue)
				}
			}
		})
	}
}

func TestSsTableIterator_Next(t *testing.T) {
	testData := []struct {
		key, value []byte
	}{
		{marshalTestKey("key1", 1, 100), []byte("value1")},
		{marshalTestKey("key2", 1, 100), []byte("value2")},
		{marshalTestKey("key3", 1, 100), []byte("value3")},
	}

	sst, _ := createTestSSTable(t, testData)

	t.Run("iterate through all keys", func(t *testing.T) {
		iter, err := CreateAndSeekToFirst(sst)
		if err != nil {
			t.Fatalf("CreateAndSeekToFirst failed: %v", err)
		}
		defer iter.Close()

		// 验证遍历所有键值对
		for i, pair := range testData {
			if !iter.IsValid() {
				t.Fatalf("iterator invalid at position %d", i)
			}

			if !bytes.Equal(iter.Key(), pair.key) {
				t.Errorf("position %d: Key() = %v, want %v", i, iter.Key(), pair.key)
			}

			if !bytes.Equal(iter.Value(), pair.value) {
				t.Errorf("position %d: Value() = %s, want %s", i, iter.Value(), pair.value)
			}

			err := iter.Next()
			if err != nil {
				t.Fatalf("Next() failed: %v", err)
			}
		}

		// 验证遍历结束后迭代器状态
		if iter.IsValid() {
			t.Error("iterator should be invalid after last key")
		}
	})
}

func TestSsTableIterator_MultipleBlocks(t *testing.T) {
	// 创建足够多的数据以跨越多个块
	var testData []struct{ key, value []byte }
	for i := 0; i < 1000; i++ {
		keyStr := fmt.Sprintf("key%03d", i)
		valueStr := fmt.Sprintf("value%03d", i)
		testData = append(testData, struct{ key, value []byte }{
			key:   marshalTestKey(keyStr, 1, uint64(100+i)), // Use increasing timestamps
			value: []byte(valueStr),
		})
	}

	sst, _ := createTestSSTable(t, testData)

	t.Run("iterate across blocks", func(t *testing.T) {
		iter, err := CreateAndSeekToFirst(sst)
		if err != nil {
			t.Fatalf("CreateAndSeekToFirst failed: %v", err)
		}
		defer iter.Close()

		count := 0
		for iter.IsValid() {
			expectedKey := marshalTestKey(fmt.Sprintf("key%03d", count), 1, uint64(100+count))
			expectedValue := fmt.Sprintf("value%03d", count)

			if !bytes.Equal(iter.Key(), expectedKey) {
				t.Errorf("Key() doesn't match at position %d", count)
			}

			if !bytes.Equal(iter.Value(), []byte(expectedValue)) {
				t.Errorf("Value() = %s, want %s", iter.Value(), expectedValue)
			}

			err := iter.Next()
			if err != nil {
				t.Fatalf("Next() failed: %v", err)
			}
			count++
		}

		if count != len(testData) {
			t.Errorf("iterated through %d items, want %d", count, len(testData))
		}
	})
}
