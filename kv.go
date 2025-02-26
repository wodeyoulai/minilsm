package mini_lsm

import (
	"encoding/binary"
	"mini_lsm/pb"
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

func keyUnmarshal(data []byte) *pb.Key {
	length := len(data)
	if length < 16 {
		return &pb.Key{}
	}
	key := pb.Key{
		Key:       data[:length-16],
		Version:   binary.BigEndian.Uint64(data[length-16 : length-8]),
		Timestamp: ^binary.BigEndian.Uint64(data[length-8:]),
	}
	return &key
}
