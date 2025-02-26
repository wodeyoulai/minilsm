package block

import (
	"encoding/binary"
	"fmt"
)

//----------------------------------------------------------------------------------------------------
//|             Data Section             |              Offset Section             |      Extra      |
//----------------------------------------------------------------------------------------------------
//| Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
//----------------------------------------------------------------------------------------------------

// -----------------------------------------------------------------------
// |                           Entry #1                            | ... |
// -----------------------------------------------------------------------
// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
// -----------------------------------------------------------------------

type Block struct {
	Data    []byte
	Offsets []uint16
}

func NewBlock() *Block {
	return &Block{make([]byte, 0), make([]uint16, 0)}
}
func (b *Block) Encode() []byte {
	totalSize := len(b.Data) + len(b.Offsets)*2 + 2
	result := make([]byte, totalSize)

	// copy the data section
	copy(result, b.Data)

	// write offset
	offset := len(b.Data)
	for _, off := range b.Offsets {
		binary.BigEndian.PutUint16(result[offset:], off)
		offset += 2
	}

	// number of elements written
	binary.BigEndian.PutUint16(result[offset:], uint16(len(b.Offsets)))

	return result
}

func (b *Block) Decode(data []byte) {
	if data == nil {
		fmt.Println("mini_lsm block decode error:input nil []byte")
		return
	}
	if len(data) < 2 {
		panic("mini_lsm block decode error: broken data")
	}
	num := binary.BigEndian.Uint16(data[len(data)-2:])
	start := len(data) - 2 - int(num*2)
	b.Offsets = make([]uint16, num)
	for i := 0; i < int(num); i++ {
		b.Offsets[i] = binary.BigEndian.Uint16(data[start : start+2])
		start += 2
	}
	b.Data = make([]byte, start)
	copy(b.Data, data[:start])
	return
}
