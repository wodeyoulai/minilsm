package block

import (
	"encoding/binary"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBlockBuilder_Basic(t *testing.T) {
	builder := NewBlockBuilder(100)

	// 测试初始状态
	assert.Equal(t, 0, builder.Count())
	assert.Equal(t, uint32(2), builder.EstimatedSize())

	// 添加一条记录并验证
	err := builder.Add([]byte("key1"), []byte("value1"))
	assert.NoError(t, err)
	assert.Equal(t, 1, builder.Count())

	// 构建 block 并验证内容
	block := builder.Build()
	assert.NotNil(t, block)
	assert.Equal(t, 1, len(block.Offsets))
	assert.Equal(t, uint16(0), block.Offsets[0])
}

func TestBlockBuilder_MultipleEntries(t *testing.T) {
	builder := NewBlockBuilder(200)

	// 添加多条记录
	entries := []struct {
		key   string
		value string
	}{
		{"key1", "value1"},
		{"key2", "value2"},
		{"key3", "value3"},
	}

	for _, entry := range entries {
		err := builder.Add([]byte(entry.key), []byte(entry.value))
		assert.NoError(t, err)
	}

	// 验证数量
	assert.Equal(t, len(entries), builder.Count())

	// 构建并验证
	block := builder.Build()
	assert.Equal(t, len(entries), len(block.Offsets))

	// 验证每个 offset 是否正确递增
	for i := 1; i < len(block.Offsets); i++ {
		assert.True(t, block.Offsets[i] > block.Offsets[i-1])
	}
}

func TestBlockBuilder_DataLayout(t *testing.T) {
	builder := NewBlockBuilder(100)

	key := []byte("test-key")
	value := []byte("test-value")
	err := builder.Add(key, value)
	assert.NoError(t, err)

	block := builder.Build()

	// 验证数据布局
	// 读取key长度
	keyLen := binary.BigEndian.Uint16(block.Data[0:2])
	assert.Equal(t, uint16(len(key)), keyLen)

	// 验证key内容
	actualKey := block.Data[2 : 2+keyLen]
	assert.Equal(t, key, actualKey)

	// 读取value长度
	valueLen := binary.BigEndian.Uint16(block.Data[2+keyLen : 4+keyLen])
	assert.Equal(t, uint16(len(value)), valueLen)

	// 验证value内容
	actualValue := block.Data[4+keyLen : 4+keyLen+valueLen]
	assert.Equal(t, value, actualValue)
}

func TestBlockBuilder_FirstKey(t *testing.T) {
	builder := NewBlockBuilder(100)

	// 添加多个key，验证firstKey是否为第一个添加的key
	firstKey := []byte("first-key")
	err := builder.Add(firstKey, []byte("value1"))
	assert.NoError(t, err)

	err = builder.Add([]byte("second-key"), []byte("value2"))
	assert.NoError(t, err)

	// 验证firstKey是否保持为第一个key
	assert.Equal(t, firstKey, builder.firstKey)
}

func TestBlockBuilder_EstimatedSize(t *testing.T) {
	builder := NewBlockBuilder(1000)

	initialSize := builder.EstimatedSize()

	// 添加数据并验证大小增长
	err := builder.Add([]byte("key"), []byte("value"))
	assert.NoError(t, err)

	// 验证大小确实增长了
	newSize := builder.EstimatedSize()
	assert.True(t, newSize > initialSize)

	// 验证预估大小包含了所有组成部分
	// key_len(2) + key(3) + value_len(2) + value(5) + offset(2) + num_elements(2)
	expectedSize := uint32(2 + 3 + 2 + 5 + 2 + 2)
	assert.Equal(t, expectedSize, newSize)
}
