package db

import (
	"bytes"
)

// memtable is an in-memory data structure that stores wide-column entries.
// It is an abstraction over an underlying data-structure. In our case a skiplist.
// Usually this is a self-balancing tree like a red-black tree or AVL tree.
// We use a skiplist because it is simpler to implement and has good performance characteristics.
// + I've never used one before so I was excited to learn about it.
type memtable struct {
	skiplist *skipList
}

// NewMemtable creates a new wide memtable.
func newMemtable() *memtable {
	return &memtable{
		skiplist: newSkipList(),
	}
}

// Len returns the number of entries in the skiplist.
func (m *memtable) Len() int {
	return m.skiplist.Len()
}

// Entries returns a slice of all (key, Value) pairs. You can parse them
// into wide-column entries if needed. This is used by the flush code.
func (m *memtable) Entries() []struct {
	Key []byte
	Val Value
} {
	return m.skiplist.Entries()
}

// buildCompositeKey builds a composite key from the partition key, clustering keys and column name.
// We use this all over the place for searching in the skiplist and for building SSTable keys.
// The format is:
// partitionKey + 0x00 + clusteringKey1 + 0x01 + clusteringKey2 + 0x01 + ... + columnName + 0x02
func buildCompositeKey(part []byte, clustering [][]byte, col []byte) []byte {
	var buf bytes.Buffer
	// partition key + 0x00
	buf.Write(part)
	buf.WriteByte(0x00)

	// clustering values + 0x01 delimiter
	for _, ck := range clustering {
		buf.Write(ck)
		buf.WriteByte(0x01)
	}

	// write 0x02, then column name
	buf.WriteByte(0x02)
	buf.Write(col)

	return buf.Bytes()
}

// Put Builds a composite key from the partition key, clustering keys and column name.
// Then inserts the key and value into the skiplist.
func (m *memtable) Put(entry Cell) {
	// build composite key
	composite := buildCompositeKey(entry.PartitionKey, entry.ClusteringValues, entry.ColumnName)
	v := Value{
		Data:      entry.Value,
		Timestamp: entry.Timestamp, // We keep track of the timestamp to resolve conflicts.
	}
	m.skiplist.Set(composite, v)
}

// Get similarly builds a composite key and then looks up the value in the skiplist.
func (m *memtable) Get(pk []byte, clustering [][]byte, colName []byte) []byte {
	composite := buildCompositeKey(pk, clustering, colName)
	v := m.skiplist.Get(composite)
	if v == nil || len(v.Data) == 0 {
		return nil
	}

	return v.Data
}

// Convert all skiplist entries back into wide ColumnEntries for flush
func (m *memtable) ToColumnEntries() []Cell {
	raw := m.skiplist.Entries()
	var result []Cell
	for _, kv := range raw {
		// parseCompositeKey is your function that splits composite back into pk, clustering, col
		pk, cks, cname := parseCompositeKey(kv.Key)
		result = append(result, Cell{
			PartitionKey:     pk,
			ClusteringValues: cks,
			ColumnName:       cname,
			Value:            kv.Val.Data,
			Timestamp:        kv.Val.Timestamp,
		})
	}
	return result
}

func parseCompositeKey(key []byte) (partitionKey []byte, ClusteringValues [][]byte, columnName []byte) {
	// 1) Find the first occurrence of 0x00 => partitionKey is everything before it
	idx00 := bytes.IndexByte(key, 0x00)
	if idx00 < 0 {
		// No 0x00? Return empty or handle error
		return nil, nil, nil
	}
	partitionKey = key[:idx00]

	// Everything after idx00+1 is [ck data + columnName + 0x02]
	remainder := key[idx00+1:]
	if len(remainder) == 0 {
		// No data after partition key
		return partitionKey, nil, nil
	}

	// 2) Find the last occurrence of 0x02 => everything after that is the columnName
	idx02 := bytes.LastIndexByte(remainder, 0x02)
	if idx02 < 0 {
		// No 0x02? Then maybe there's no column name. Or handle error if that's unexpected.
		return partitionKey, nil, nil
	}
	columnName = remainder[idx02+1:]
	if len(columnName) == 0 {
		// If there's nothing after 0x02, columnName is empty
		columnName = nil
	}

	// 3) Everything before idx02 are the clustering keys, separated by 0x01
	ckRaw := remainder[:idx02]
	if len(ckRaw) > 0 {
		parts := bytes.Split(ckRaw, []byte{0x01})
		// Some splits may be empty if you have trailing 0x01, so filter those out
		for _, p := range parts {
			if len(p) > 0 {
				ClusteringValues = append(ClusteringValues, p)
			}
		}
	}

	return partitionKey, ClusteringValues, columnName
}
