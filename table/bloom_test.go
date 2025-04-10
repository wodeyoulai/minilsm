package table

import (
	"crypto/rand"
	"testing"
)

// TestNewBloomFilter verifies the constructor creates filters with proper parameters
func TestNewBloomFilter(t *testing.T) {
	tests := []struct {
		name              string
		expectedItems     int
		falsePositiveRate float64
		expectedBitsMin   int // Minimum expected bits based on formula
	}{
		{"small filter", 100, 0.01, 950},        // ~959 bits for 100 items at 1% FP rate
		{"medium filter", 1000, 0.001, 14300},   // ~14,400 bits for 1000 items at 0.1% FP rate
		{"large filter", 10000, 0.0001, 190000}, // ~191,000 bits for 10k items at 0.01% FP rate
		{"edge case - empty", 0, 0.01, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bf := NewBloomFilter(tt.expectedItems, tt.falsePositiveRate)

			// Verify we have at least the minimum bits required
			if tt.expectedItems > 0 && len(bf.bits)*8 < tt.expectedBitsMin {
				t.Errorf("Expected at least %d bits, got %d", tt.expectedBitsMin, len(bf.bits)*8)
			}

			// Verify hash function count is reasonable
			if tt.expectedItems > 0 && (bf.k < 3 || bf.k > 20) {
				t.Errorf("Expected reasonable hash function count (3-20), got %d", bf.k)
			}
		})
	}
}

// TestBloomFilterAddAndMayContain tests the basic functionality
func TestBloomFilterAddAndMayContain(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Adding keys
	keys := [][]byte{
		[]byte("key1"),
		[]byte("key2"),
		[]byte("this is a longer key"),
		[]byte{0xFF, 0xAA, 0x00, 0x55}, // Binary data
		make([]byte, 100),              // Larger key with zeros
	}

	for _, key := range keys {
		bf.Add(key)
	}

	// Testing that added keys are found
	for _, key := range keys {
		if !bf.MayContain(key) {
			t.Errorf("Key %v was added but not found", key)
		}
	}

	// Testing that non-added keys are not found (negative test)
	nonExistentKeys := [][]byte{
		[]byte("nonexistent"),
		[]byte("key3"),
		[]byte{0xAA, 0xBB, 0xCC},
	}

	for _, key := range nonExistentKeys {
		// Note: This might have false positives, which is expected
		// but we're not testing for that specifically here
		if bf.MayContain(key) {
			t.Logf("Note: False positive detected for %v", key)
		}
	}
}

// TestBloomFilterEmptyKey tests behavior with empty key
func TestBloomFilterEmptyKey(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)

	// Empty key should be handled properly
	emptyKey := []byte{}
	bf.Add(emptyKey)

	if !bf.MayContain(emptyKey) {
		t.Error("Empty key was added but not found")
	}
}

// TestBloomFilterFalsePositiveRate tests the approximate false positive rate
func TestBloomFilterFalsePositiveRate(t *testing.T) {
	expectedItems := 10000
	falsePositiveRate := 0.01 // 1%

	bf := NewBloomFilter(expectedItems, falsePositiveRate)

	// Add expectedItems random items
	for i := 0; i < expectedItems; i++ {
		key := make([]byte, 16)
		rand.Read(key)
		bf.Add(key)
	}

	// Test with 10000 items that were NOT added
	falsePositives := 0
	testCount := 10000

	for i := 0; i < testCount; i++ {
		key := make([]byte, 16)
		rand.Read(key)

		// If the filter says it may contain an item we didn't add, that's a false positive
		if bf.MayContain(key) {
			falsePositives++
		}
	}

	actualRate := float64(falsePositives) / float64(testCount)

	// Allow for some statistical variation - bloom filters are probabilistic
	// The actual rate should be close to the expected rate, but not necessarily exact
	upperBound := falsePositiveRate * 2.0 // Allow up to 2x the expected rate

	if actualRate > upperBound {
		t.Errorf("False positive rate too high: expected <= %f, got %f (%d/%d)",
			upperBound, actualRate, falsePositives, testCount)
	} else {
		t.Logf("False positive rate: %f (%d/%d)", actualRate, falsePositives, testCount)
	}
}

// TestBloomFilterHashDistribution tests the distribution of hash values
func TestBloomFilterHashDistribution(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Generate a histogram of hash values to check distribution
	buckets := make([]int, 10)
	iterations := 10000

	for i := 0; i < iterations; i++ {
		key := make([]byte, 4)
		rand.Read(key)

		// Test first hash function
		h := bf.hash(key, 0) % bf.m
		bucketIdx := int(h * 10 / bf.m)
		buckets[bucketIdx]++
	}

	// Check that buckets are relatively evenly distributed
	// Each bucket should have approximately iterations/10 entries
	expected := iterations / 10
	tolerance := expected / 3 // Allow for 33% deviation

	for i, count := range buckets {
		if count < expected-tolerance || count > expected+tolerance {
			t.Errorf("Hash distribution not uniform. Bucket %d: %d items (expected %d±%d)",
				i, count, expected, tolerance)
		}
	}
}

// TestBloomFilterManyItems verifies behavior with a large number of items
func TestBloomFilterManyItems(t *testing.T) {
	// Skip in short mode as this test is more resource-intensive
	if testing.Short() {
		t.Skip("Skipping large item test in short mode")
	}

	itemCount := 100000
	bf := NewBloomFilter(itemCount, 0.001) // 0.1% false positive rate

	// Insert items with predictable pattern
	items := make([][]byte, itemCount)
	for i := 0; i < itemCount; i++ {
		key := make([]byte, 8)
		rand.Read(key)
		items[i] = key
		bf.Add(key)
	}

	// Verify all items are found
	for i, key := range items {
		if !bf.MayContain(key) {
			t.Errorf("Item %d not found in bloom filter", i)
			// Only report first few failures to keep output manageable
			if i > 10 {
				t.Fatalf("Too many failures, aborting test")
			}
		}
	}
}

// TestBloomFilterConsistency tests that same key always hashes to same positions
func TestBloomFilterConsistency(t *testing.T) {
	bf := NewBloomFilter(100, 0.01)
	testKey := []byte("test-key")

	// Record initial hash positions by adding the key
	bf.Add(testKey)

	// Create a new empty filter with same parameters
	bf2 := NewBloomFilter(100, 0.01)

	// Add the same key to the new filter
	bf2.Add(testKey)

	// Both filters should produce the same result for MayContain
	if bf.MayContain(testKey) != bf2.MayContain(testKey) {
		t.Error("Inconsistent results for same key between two filters")
	}

	// More specific test: we expect the same bits to be set
	// This assumes filter parameters and hash function remain constant
	for i, b := range bf.bits {
		if i < len(bf2.bits) && b != bf2.bits[i] {
			t.Errorf("Different bits set at position %d: %08b vs %08b", i, b, bf2.bits[i])
		}
	}
}
