package table

import "math"

// Bloom filter structure
type BloomFilter struct {
	bits []byte // Bit array
	k    uint   // Number of hash functions
	m    uint   // Number of bits in the filter
}

func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	// Calculate required number of bits
	m := uint(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2)))
	k := uint(math.Ceil(math.Log(2) * float64(m) / float64(expectedItems)))

	return &BloomFilter{
		bits: make([]byte, (m+7)/8), // Round up to nearest byte
		k:    k,
		m:    m,
	}
}

func (bf *BloomFilter) Add(key []byte) {
	for i := uint(0); i < bf.k; i++ {
		h := bf.hash(key, i)
		pos := h % bf.m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

func (bf *BloomFilter) MayContain(key []byte) bool {
	for i := uint(0); i < bf.k; i++ {
		h := bf.hash(key, i)
		pos := h % bf.m
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

func (bf *BloomFilter) hash(key []byte, seed uint) uint {
	// Simple hash mixing function
	h := uint(seed) + 1
	for _, b := range key {
		h = h*131 + uint(b)
	}
	return h
}
