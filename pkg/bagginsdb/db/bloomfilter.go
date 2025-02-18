package db

import (
	"hash/fnv"
	"math"
)

// This is my implementation of a bloom filter. It is used to check if a key might be in a set.
// The bloom filter is a probabilistic data structure that uses multiple hash functions to
// determine if an element is in a set. It can return false positives, but never false negatives.
type bloomFilter struct {
	m      uint64 // number of bits
	k      uint32 // number of hash functions
	bitset []byte // underlying bitset
	n      uint64 // number of items added
}

// newBloomFilter computes optimal parameters and returns a new bloomFilter.
// m = - (n * ln(p)) / (ln2)^2, rounded up to a whole number of bytes.
func newBloomFilter(expectedItems int, falsePositiveRate float64) *bloomFilter {
	n := float64(expectedItems)
	mFloat := -n * math.Log(falsePositiveRate) / (math.Ln2 * math.Ln2)
	mBytes := uint64(math.Ceil(mFloat / 8.0))
	m := mBytes * 8
	k := uint32(math.Ceil((float64(m) / n) * math.Ln2))
	return &bloomFilter{
		m:      m,
		k:      k,
		bitset: make([]byte, mBytes),
		n:      0,
	}
}

// Add inserts a key into the Bloom filter.
func (bf *bloomFilter) Add(key []byte) {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < bf.k; i++ {
		combined := h1 + uint64(i)*h2
		bit := combined % bf.m
		byteIndex := bit / 8
		bitIndex := bit % 8
		bf.bitset[byteIndex] |= 1 << bitIndex
	}
	bf.n++
}

// MightContain returns true if the key might be in the filter.
func (bf *bloomFilter) MightContain(key []byte) bool {
	h1, h2 := hashKey(key)
	for i := uint32(0); i < bf.k; i++ {
		combined := h1 + uint64(i)*h2
		bit := combined % bf.m
		byteIndex := bit / 8
		bitIndex := bit % 8
		if (bf.bitset[byteIndex] & (1 << bitIndex)) == 0 {
			return false
		}
	}
	return true
}

// hashKey returns two 64–bit hash values for key using FNV–64a.
func hashKey(key []byte) (uint64, uint64) {
	h1 := fnv.New64a()
	h1.Write(key)
	sum1 := h1.Sum64()
	h2 := fnv.New64a()
	revKey := make([]byte, len(key))
	for i, b := range key {
		revKey[len(key)-1-i] = b
	}
	h2.Write(revKey)
	sum2 := h2.Sum64()
	return sum1, sum2
}
