package table

import "math"

// BloomFilter Bloom filter structure
type BloomFilter struct {
	bits []byte // Bit array
	k    uint   // Number of hash functions
	m    uint   // Number of bits in the filter
}

func NewBloomFilter(expectedItems int, falsePositiveRate float64) *BloomFilter {
	// The adjustment formula calculates the appropriate number of bits and the number of hash functions
	// formula Optimization：m = -n*ln(p)/(ln(2)²)
	m := uint(math.Ceil(-float64(expectedItems) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2))))
	// formula Optimization：k = (m/n)*ln(2)
	k := uint(math.Max(1, math.Round(float64(m)/float64(expectedItems)*math.Log(2))))

	return &BloomFilter{
		bits: make([]byte, (m+7)/8), // 向上取整到最接近的字节
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
	// Use two different hash functions to enhance distribution
	h1 := uint(0x9e3779b9)
	h2 := uint(0x85ebca6b)

	for _, b := range key {
		h1 = ((h1 << 5) + h1) ^ uint(b) // similar FNV-1a
		h2 = ((h2<<5)+h2)*33 + uint(b)  // mixed multiplicative hashing
	}

	// use different seeds to generate different hashes
	return h1 + seed*h2
}
