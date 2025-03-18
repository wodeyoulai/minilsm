package block

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

// 创建一个测试用的Block
func createTestBlock(t *testing.T, entries []struct{ key, value string }) *Block {
	builder := NewBlockBuilder(1024)
	for _, entry := range entries {
		err := builder.Add([]byte(entry.key), []byte(entry.value))
		assert.NoError(t, err)
	}
	return builder.Build()
}

func TestBlockIterator_SeekToFirst(t *testing.T) {
	// 测试数据
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	block := createTestBlock(t, entries)

	// 创建迭代器并定位到第一个元素
	iter, _ := CreateAndSeekToFirst(block)

	// 验证第一个元素
	assert.Equal(t, []byte("key1"), iter.Key())
	assert.Equal(t, []byte("value1"), iter.Value())
}

func TestBlockIterator_Empty(t *testing.T) {
	// 创建空block
	block := NewBlockBuilder(1024).Build()

	// 验证空block的迭代器
	iter, _ := CreateAndSeekToFirst(block)
	assert.False(t, iter.IsValid()) // 空block应该返回无效迭代器
}

func TestBlockIterator_Next(t *testing.T) {
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	block := createTestBlock(t, entries)
	iter, _ := CreateAndSeekToFirst(block)

	// 验证遍历所有元素
	for i := 0; i < len(entries); i++ {
		assert.True(t, iter.IsValid())
		assert.Equal(t, []byte(entries[i].key), iter.Key())
		assert.Equal(t, []byte(entries[i].value), iter.Value())
		iter.Next()
	}

	// 验证遍历结束后的状态
	assert.False(t, iter.IsValid())
}

func TestBlockIterator_SeekToKey(t *testing.T) {
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key3", "value3"},
		{"key5", "value5"},
	}

	block := createTestBlock(t, entries)

	testCases := []struct {
		seekKey       string
		expectedKey   string
		expectedValue string
		shouldBeValid bool
	}{
		{"key0", "key1", "value1", true}, // seek到第一个元素
		{"key2", "key3", "value3", true}, // seek到中间元素
		{"key4", "key5", "value5", true}, // seek到最后一个元素
		{"key6", "", "", false},          // seek超出范围
	}

	for _, tc := range testCases {
		t.Run(tc.seekKey, func(t *testing.T) {
			iter, _ := CreateAndSeekToKey(block, []byte(tc.seekKey))
			assert.Equal(t, tc.shouldBeValid, iter.IsValid())
		})
	}
}

func TestBlockIterator_SingleEntry(t *testing.T) {
	// 测试只有一个entry的情况
	entries := []struct {
		key   string
		value string
	}{
		{"single_key", "single_value"},
	}

	block := createTestBlock(t, entries)

	// 测试SeekToFirst
	iter, _ := CreateAndSeekToFirst(block)
	assert.True(t, iter.IsValid())
	assert.Equal(t, []byte("single_key"), iter.Key())
	assert.Equal(t, []byte("single_value"), iter.Value())

	// 测试Next
	iter.Next()
	assert.False(t, iter.IsValid())
}

func TestBlockIterator_LargeEntries(t *testing.T) {
	// 测试大key和value的情况
	largeKey := make([]byte, 1000)
	largeValue := make([]byte, 1000)
	for i := range largeKey {
		largeKey[i] = byte(i % 256)
		largeValue[i] = byte((i + 128) % 256)
	}

	entries := []struct {
		key   string
		value string
	}{
		{string(largeKey), string(largeValue)},
	}

	block := createTestBlock(t, entries)
	iter, _ := CreateAndSeekToFirst(block)

	assert.True(t, iter.IsValid())
	assert.Equal(t, largeKey, iter.Key())
	assert.Equal(t, largeValue, iter.Value())
}

func TestBlockIterator_SequentialAccess(t *testing.T) {
	// 测试连续访问所有元素
	entries := []struct {
		key   string
		value string
	}{
		{"a", "1"},
		{"b", "2"},
		{"c", "3"},
		{"d", "4"},
		{"e", "5"},
	}

	block := createTestBlock(t, entries)
	iter, _ := CreateAndSeekToFirst(block)

	// 先正向遍历
	for i := 0; i < len(entries); i++ {
		assert.True(t, iter.IsValid())
		assert.Equal(t, []byte(entries[i].key), iter.Key())
		assert.Equal(t, []byte(entries[i].value), iter.Value())
		iter.Next()
	}

	// 验证遍历结束
	assert.False(t, iter.IsValid())
}

func TestBlockIterator_SeekStress(t *testing.T) {
	// 创建大量entry进行seek测试
	var entries []struct{ key, value string }
	for i := 0; i < 100; i += 2 {
		entries = append(entries, struct{ key, value string }{
			key:   string([]byte{byte(i)}),
			value: string([]byte{byte(i + 1)}),
		})
	}

	block := createTestBlock(t, entries)

	// 对每个可能的值进行seek测试
	for i := 0; i < 100; i += 1 {
		iter, _ := CreateAndSeekToKey(block, []byte{byte(i)})
		if i%2 == 0 && i < len(entries)*2 {
			// 应该正好找到这个key
			assert.Equal(t, []byte{byte(i)}, iter.Key())
			assert.Equal(t, []byte{byte(i + 1)}, iter.Value())
		} else if i >= len(entries)*2 {
			// 超出范围
			assert.False(t, iter.IsValid())
		} else {
			// 应该找到下一个key
			if i != 99 {
				assert.Equal(t, []byte{byte(i + 1)}, iter.Key())
			}
		}
	}
}
