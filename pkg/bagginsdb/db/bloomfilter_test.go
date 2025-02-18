package db

import (
	"math"
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	expectedItems := 100
	falsePositiveRate := 0.01
	bf := newBloomFilter(expectedItems, falsePositiveRate)

	if bf.m == 0 {
		t.Errorf("Expected m (bit size) to be > 0, got %v", bf.m)
	}
	if bf.k == 0 {
		t.Errorf("Expected k (hash functions) to be > 0, got %v", bf.k)
	}
	if len(bf.bitset) == 0 {
		t.Error("Expected a non-empty bitset")
	}

	// Check if computed size is at least close to the theoretical optimum
	// The formula is: m = - (n * ln(p)) / (ln2^2).
	// m was computed in bits, but stored as bytes in the bitset.
	mFloat := -float64(expectedItems) * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	// Our bf.m is the ceiling of mFloat in bits, so it should be >= mFloat
	if float64(bf.m) < mFloat {
		t.Errorf("BloomFilter m is too small. Got %v, expected at least %v", bf.m, mFloat)
	}

	// Check that k is close to optimum: k ~ (m / n) * ln2
	optimalK := uint32(math.Ceil((float64(bf.m) / float64(expectedItems)) * math.Ln2))
	if bf.k < optimalK-1 || bf.k > optimalK+1 {
		t.Errorf("BloomFilter k is off. Got %v, expected around %v", bf.k, optimalK)
	}
}

// TestBloomFilterAddAndMightContain checks basic functionality for inserted items.
func TestBloomFilterAddAndMightContain(t *testing.T) {
	bf := newBloomFilter(10, 0.001) // small set, low false-positive rate

	keys := [][]byte{
		[]byte("apple"),
		[]byte("banana"),
		[]byte("cherry"),
		[]byte("date"),
	}

	// Add each key to the Bloom filter
	for _, key := range keys {
		bf.Add(key)
	}

	// Ensure that each key might be contained
	for _, key := range keys {
		if !bf.MightContain(key) {
			t.Errorf("Expected key %q to be in Bloom filter, but it was reported absent", key)
		}
	}
}

// TestBloomFilterMightContainForNonExisting checks that non-inserted items
// are typically (though not guaranteed) reported absent.
func TestBloomFilterMightContainForNonExisting(t *testing.T) {
	bf := newBloomFilter(1000, 0.0001) // Large enough to make false positives very unlikely

	// Insert some keys
	inserted := [][]byte{
		[]byte("foo"),
		[]byte("bar"),
		[]byte("baz"),
	}
	for _, key := range inserted {
		bf.Add(key)
	}

	// Check some keys not inserted
	notInserted := [][]byte{
		[]byte("not-foo"),
		[]byte("definitely-not-bar"),
		[]byte("xyzzy"),
		[]byte("asdf"),
	}

	// We expect these to be absent, but since Bloom filters can false-positive,
	// there's a small chance of failure. With a very low falsePositiveRate, it
	// should be extremely rare in a test environment.
	for _, key := range notInserted {
		if bf.MightContain(key) {
			t.Errorf("Key %q was NOT inserted but Bloom filter indicates it might be present (false positive)", key)
		}
	}
}

// TestBloomFilterCount ensures that the bloom filter is counting added items.
func TestBloomFilterCount(t *testing.T) {
	bf := newBloomFilter(50, 0.01)
	if bf.n != 0 {
		t.Errorf("Expected 0 items initially, got %d", bf.n)
	}

	keys := [][]byte{
		[]byte("one"),
		[]byte("two"),
		[]byte("three"),
		[]byte("four"),
	}
	for _, k := range keys {
		bf.Add(k)
	}

	if bf.n != uint64(len(keys)) {
		t.Errorf("Expected bloom filter count to be %d, got %d", len(keys), bf.n)
	}
}

// BenchmarkBloomFilterAdd measures the performance of Add.
func BenchmarkBloomFilterAdd(b *testing.B) {
	bf := newBloomFilter(100000, 0.01)
	key := []byte("benchmark_key")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		bf.Add(key)
	}
}

// BenchmarkBloomFilterMightContain measures the performance of MightContain.
func BenchmarkBloomFilterMightContain(b *testing.B) {
	bf := newBloomFilter(100000, 0.01)
	key := []byte("benchmark_key")
	bf.Add(key)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = bf.MightContain(key)
	}
}
