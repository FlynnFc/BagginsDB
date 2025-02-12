package database

import (
	"bytes"
	"encoding/binary"
	"io"
)

// Cell represents a single cell in a wide–column store.
type Cell struct {
	PartitionKey     []byte
	ClusteringValues [][]byte
	ColumnName       []byte
	Value            []byte
	Timestamp        int64
}

// CompositeKey returns a byte slice that uniquely identifies the cell by
// concatenating its PartitionKey, ClusteringValues and ColumnName.
// It uses a length–prefixed encoding (with BigEndian for the length) so that the
// natural lexicographic order of the composite key matches the order of the fields.
func (c *Cell) CompositeKey() []byte {
	var buf bytes.Buffer
	// Write PartitionKey with length prefix.
	writeBytesWithPrefix(&buf, c.PartitionKey)
	// Write the count of clustering values (4 bytes, BigEndian).
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(c.ClusteringValues)))
	// Write each clustering value with its length.
	for _, cv := range c.ClusteringValues {
		writeBytesWithPrefix(&buf, cv)
	}
	// Write ColumnName with length prefix.
	writeBytesWithPrefix(&buf, c.ColumnName)
	return buf.Bytes()
}

// writeBytesWithPrefix writes a uint32 length (BigEndian) followed by the byte slice.
func writeBytesWithPrefix(w io.Writer, b []byte) error {
	if err := binary.Write(w, binary.BigEndian, uint32(len(b))); err != nil {
		return err
	}
	_, err := w.Write(b)
	return err
}
