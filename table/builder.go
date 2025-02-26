package table

import (
	"encoding/binary"
	"errors"
	"fmt"
	"mini_lsm/block"
	"mini_lsm/tools/fileutil"
	"os"
	"path/filepath"
)

const (
	// align to 4kb boundary for direct io
	BLOCKALIGNSIZE = 4096
	// metadata encoding version number
	METAVERSIONTAG = uint32(1)
)

type SsTableBuilder struct {
	builder   *block.BlockBuilder
	firstKey  []byte
	lastKey   []byte
	data      []byte
	meta      []BlockMeta
	blockSize uint32
	// add statistics
	blockCount uint32
	dataSize   uint32
}

func NewSsTableBuilder(blockSize uint32) *SsTableBuilder {
	// 确保blockSize是4KB对齐的
	alignedSize := int((blockSize + BLOCKALIGNSIZE - 1)) & (^(BLOCKALIGNSIZE - 1))
	return &SsTableBuilder{
		builder:    block.NewBlockBuilder(uint16(alignedSize)),
		firstKey:   nil, // 改用nil表示空
		lastKey:    nil,
		data:       make([]byte, 0, alignedSize), // 预分配内存
		meta:       make([]BlockMeta, 0),
		blockSize:  uint32(alignedSize),
		blockCount: 0,
		dataSize:   0,
	}
}

func (b *SsTableBuilder) Add(key, value []byte) error {
	if len(key) == 0 {
		return errors.New("key cannot be empty")
	}

	// update first key
	if b.firstKey == nil {
		b.firstKey = make([]byte, len(key))
		copy(b.firstKey, key)
	}

	// always update last key
	if b.lastKey == nil || len(b.lastKey) != len(key) {
		b.lastKey = make([]byte, len(key))
	}
	copy(b.lastKey, key)

	// check if the block needs to be split
	err := b.builder.Add(key, value)
	if err != nil {
		if errors.Is(err, block.ErrBlockSizeExceeded) {
			if err := b.finishBlock(); err != nil {
				return fmt.Errorf("finish block failed: %w", err)
			}
			// try to add again
			return b.builder.Add(key, value)
		}
		return err
	}

	return nil
}

func (b *SsTableBuilder) finishBlock() error {
	if b.builder.Count() == 0 {
		return nil
	}

	blockToWrite := b.builder.Build()
	encodedBlock := blockToWrite.Encode()

	meta := BlockMeta{
		FirstKey: make([]byte, len(b.firstKey)),
		LastKey:  make([]byte, len(b.lastKey)),
		Offset:   uint16(len(b.data)),
	}
	copy(meta.FirstKey, b.firstKey)
	copy(meta.LastKey, b.lastKey)

	// add block data and metadata
	b.data = append(b.data, encodedBlock...)
	b.meta = append(b.meta, meta)
	b.blockCount++
	b.dataSize += uint32(len(encodedBlock))

	// reset builder status
	b.builder = block.NewBlockBuilder(uint16(b.blockSize))
	b.firstKey = nil // 下一个block的first key将由新数据设置

	return nil
}

func (b *SsTableBuilder) EstimatedSize() uint32 {
	currentBlockSize := b.builder.EstimatedSize()
	return b.dataSize + uint32(currentBlockSize) + uint32(b.estimateMetaSize())
}

func (b *SsTableBuilder) estimateMetaSize() int {
	// metadata roughly includes
	// - version number 4 bytes
	// - meta array length 4 bytes
	// - each meta entry size
	metaSize := 2
	for _, m := range b.meta {
		metaSize += 2 + len(m.FirstKey) + len(m.LastKey) // offset + size + keys
	}
	return metaSize
}
func (b *SsTableBuilder) createFile(path string) (*os.File, error) {
	// make sure the directory exists
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create directory failed: %w", err)
	}

	// open the file create it if it does not exist
	// O_WRONLY - write only mode
	// O_CREATE - create if it does not exist
	// O_TRUNC - clear if it exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("create file failed: %w", err)
	}

	return file, nil
}
func (b *SsTableBuilder) Build(id uint32, blockCache *BlockCache, path string) (*SSTable, error) {
	// 完成最后一个block
	if b.builder.Count() > 0 {
		if err := b.finishBlock(); err != nil {
			return nil, err
		}
	}

	// encoded metadata
	meta := b.encodeBlockMetas()

	// write-to-the-file
	file, err := b.createFile(path)
	if err != nil {
		return nil, fmt.Errorf("create file failed: %w", err)
	}
	//defer file.Close()

	// write data blocks
	if _, err := file.Write(b.data); err != nil {
		return nil, fmt.Errorf("write blocks failed: %w", err)
	}

	// 写入元数据
	if _, err := file.Write(meta); err != nil {
		return nil, fmt.Errorf("write meta failed: %w", err)
	}

	// write metadata offset(4 bytes) + maxts (8 bytes)
	offset := uint32(len(b.data))
	offsetBuf := make([]byte, 4)
	binary.BigEndian.PutUint32(offsetBuf, offset)
	if _, err = file.Write(offsetBuf); err != nil {
		return nil, fmt.Errorf("write offset failed: %w", err)
	}
	// forced flashing
	if err := file.Sync(); err != nil {
		return nil, fmt.Errorf("sync file failed: %w", err)
	}

	// open SSTable
	return OpenSSTable(id, blockCache, &fileutil.FileObject{
		File: file,
		Size: uint32(len(b.data) + len(meta) + 4),
	})
}

func (b *SsTableBuilder) encodeBlockMetas() []byte {
	// Estimated size
	size := b.estimateMetaSize()
	buf := make([]byte, 0, size)

	// write every meta entry
	for _, meta := range b.meta {
		// offset
		offsetBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(offsetBuf, meta.Offset)
		buf = append(buf, offsetBuf...)

		// first key
		keyLenBuf := make([]byte, 2)
		binary.BigEndian.PutUint16(keyLenBuf, uint16(len(meta.FirstKey)))
		buf = append(buf, keyLenBuf...)
		buf = append(buf, meta.FirstKey...)

		// last key
		binary.BigEndian.PutUint16(keyLenBuf, uint16(len(meta.LastKey)))
		buf = append(buf, keyLenBuf...)
		buf = append(buf, meta.LastKey...)
	}

	return buf
}
