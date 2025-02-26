package block

import (
	"bytes"
	"encoding/binary"
)

// mini_lsm.BlockIterator used to traverse data in blocks

type BlockIterator struct {
	// the bottom layer mini_lsm.Block
	block *Block
	// The current key, empty means that the iterator is invalid
	key []byte
	// The current value is in the range of mini_lsm.Block.data, corresponding to the current key
	valueRange struct {
		start uint16
		end   uint16
	}
	// The index of the current key-value pair should be within the range [0, num_of_elements)
	idx uint
	// the first key in the block
	firstKey []byte
	// total num of block
	numOfKeys uint
	// end==true iterate over.active by seekToFirst
	end bool
}

// Newmini_lsm.BlockIterator create a new block iterator
func newBlockIterator(block *Block) *BlockIterator {
	return &BlockIterator{
		block: block,
		key:   make([]byte, 0),
		valueRange: struct {
			start uint16
			end   uint16
		}{0, 0},
		idx:       0,
		firstKey:  make([]byte, 0),
		numOfKeys: 0,
	}
}

// CreateAndSeekToFirst create a block iterator and locate the first entry
func CreateAndSeekToFirst(block *Block) *BlockIterator {
	b := newBlockIterator(block)
	b.SeekToFirst()
	return b
}

// CreateAndSeekToKey Create a block iterator and locate the first entry greater than or equal to the given key
func CreateAndSeekToKey(block *Block, key []byte) *BlockIterator {
	b := newBlockIterator(block)
	b.SeekToKey(key)
	return b
}

// Key return the key to the current entry
func (it *BlockIterator) Key() []byte {
	return it.key
}

// Value return the value of the current entry
func (it *BlockIterator) Value() []byte {
	return it.block.Data[it.valueRange.start:it.valueRange.end]
}

// IsValid return whether the iterator is valid
func (it *BlockIterator) IsValid() bool {
	return it.numOfKeys > 0 && it.idx < it.numOfKeys && len(it.key) > 0
}

// SeekToFirst positioning to the first key in the block
func (it *BlockIterator) SeekToFirst() {
	numOfKeys := len(it.block.Offsets)
	if numOfKeys == 0 {
		it.key = nil // the tag iterator is invalid
		return
	}

	it.numOfKeys = uint(numOfKeys)
	it.idx = 0
	element := []byte{}
	if numOfKeys == 1 {
		element = it.block.Data[:len(it.block.Data)-4]
	} else {
		element = it.block.Data[:it.block.Offsets[1]]
	}
	length := binary.BigEndian.Uint16(element)
	it.key = it.block.Data[2 : length+2]
	it.firstKey = it.key
	it.valueRange.start = length + 4
	it.valueRange.end = length + 4 + binary.BigEndian.Uint16(it.block.Data[length+2:])
}

// Next move to the next key in the block
func (it *BlockIterator) Next() {
	if !it.IsValid() {
		return
	}
	it.idx++
	if it.idx >= it.numOfKeys {
		it.key = nil // the tag iterator is invalid
		return
	}
	keyStart := it.valueRange.end
	keyLength := binary.BigEndian.Uint16(it.block.Data[keyStart : keyStart+2])
	keyEnd := keyStart + 2 + keyLength
	it.key = it.block.Data[keyStart+2 : keyEnd]
	it.valueRange.start = keyEnd + 2
	it.valueRange.end = keyEnd + 2 + binary.BigEndian.Uint16(it.block.Data[keyEnd:])
}

// SeekToKey Position to the first entry greater than or equal to the given key
// Note: You should assume that the key-value pairs in the block are sorted when added by the callee
func (it *BlockIterator) SeekToKey(key []byte) {
	it.SeekToFirst()
	for bytes.Compare(it.Key(), key) < 0 && it.IsValid() {
		it.Next()
	}
}
