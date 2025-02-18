package db

import (
	"bytes"
	"encoding/binary"
	"path/filepath"
	"reflect"
	"testing"
)

// --------------------
// Test for readCell / writeCell in-memory
// --------------------

func TestReadWriteCell(t *testing.T) {
	// Create a buffer to simulate writing to a file
	var buf bytes.Buffer

	// Construct a sample Cell
	original := Cell{
		PartitionKey:     []byte("myPartition"),
		ClusteringValues: [][]byte{[]byte("ck1"), []byte("ck2")},
		ColumnName:       []byte("myColumn"),
		Value:            []byte("myValue"),
	}

	// Write the cell to buf
	if err := writeCell(&buf, &original); err != nil {
		t.Fatalf("writeCell failed: %v", err)
	}

	// Read the cell back
	readBack, err := readCell(&buf)
	if err != nil {
		t.Fatalf("readCell failed: %v", err)
	}

	// Compare fields
	if !bytes.Equal(readBack.PartitionKey, original.PartitionKey) {
		t.Errorf("PartitionKey mismatch. Got %q, want %q", readBack.PartitionKey, original.PartitionKey)
	}
	if !reflect.DeepEqual(readBack.ClusteringValues, original.ClusteringValues) {
		t.Errorf("ClusteringValues mismatch. Got %q, want %q", readBack.ClusteringValues, original.ClusteringValues)
	}
	if !bytes.Equal(readBack.ColumnName, original.ColumnName) {
		t.Errorf("ColumnName mismatch. Got %q, want %q", readBack.ColumnName, original.ColumnName)
	}
	if !bytes.Equal(readBack.Value, original.Value) {
		t.Errorf("Value mismatch. Got %q, want %q", readBack.Value, original.Value)
	}
}

// --------------------
// Tests for writeSSTable / loadSSTable
// --------------------

func TestWriteSSTableAndLoadSSTable(t *testing.T) {
	// Create a temp directory for test files
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_sstable.db")

	// Construct some sorted Cells (sorted by CompositeKey).
	// In real usage, ensure your composite keys are sorted in the order your store expects.
	cells := []Cell{
		{
			PartitionKey: []byte("a-part"),
			ClusteringValues: [][]byte{
				[]byte("ck1"),
			},
			ColumnName: []byte("colA"),
			Value:      []byte("valA"),
		},
		{
			PartitionKey: []byte("a-part"),
			ClusteringValues: [][]byte{
				[]byte("ck1"),
				[]byte("ck2"),
			},
			ColumnName: []byte("colB"),
			Value:      []byte("valB"),
		},
		{
			PartitionKey:     []byte("b-part"),
			ClusteringValues: [][]byte{[]byte("ck1")},
			ColumnName:       []byte("colC"),
			Value:            []byte("valC"),
		},
	}

	// Write SSTable
	sparseInterval := 2
	expectedItems := len(cells)
	falsePositiveRate := 0.01

	sst, err := writeSSTable(filePath, cells, sparseInterval, expectedItems, falsePositiveRate)
	if err != nil {
		t.Fatalf("writeSSTable failed: %v", err)
	}

	// Basic checks on in-memory sstable structure
	if sst.indexOffset <= 0 {
		t.Errorf("Expected a positive indexOffset, got %d", sst.indexOffset)
	}
	if sst.bfOffset <= 0 {
		t.Errorf("Expected a positive bfOffset, got %d", sst.bfOffset)
	}
	if len(sst.index) == 0 {
		t.Error("Expected at least one index entry, got none")
	}
	if sst.bloom == nil {
		t.Error("Expected a non-nil Bloom filter")
	}

	// Load the SSTable back from disk
	loaded, err := loadSSTable(filePath)
	if err != nil {
		t.Fatalf("loadSSTable failed: %v", err)
	}

	// Check loaded sstable metadata
	if loaded.filePath != filePath {
		t.Errorf("Expected filePath=%q, got %q", filePath, loaded.filePath)
	}
	if len(loaded.index) != len(sst.index) {
		t.Errorf("Index length mismatch: wrote %d entries, loaded %d",
			len(sst.index), len(loaded.index))
	}
	if loaded.dataOffset != sst.dataOffset {
		t.Errorf("dataOffset mismatch: wrote %d, loaded %d", sst.dataOffset, loaded.dataOffset)
	}
	if loaded.indexOffset != sst.indexOffset {
		t.Errorf("indexOffset mismatch: wrote %d, loaded %d", sst.indexOffset, loaded.indexOffset)
	}
	if loaded.bfOffset != sst.bfOffset {
		t.Errorf("bfOffset mismatch: wrote %d, loaded %d", sst.bfOffset, loaded.bfOffset)
	}
	if loaded.bloom == nil {
		t.Fatal("Loaded Bloom filter is nil")
	}
}

// --------------------
// Test sstable.Get
// --------------------

func TestSSTableGet(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_sstable_get.db")

	// Create a few cells with distinct composite keys
	cells := []Cell{
		{
			PartitionKey:     []byte("pk1"),
			ClusteringValues: [][]byte{[]byte("ck1")},
			ColumnName:       []byte("colA"),
			Value:            []byte("valA"),
		},
		{
			PartitionKey:     []byte("pk1"),
			ClusteringValues: [][]byte{[]byte("ck2")},
			ColumnName:       []byte("colB"),
			Value:            []byte("valB"),
		},
		{
			PartitionKey:     []byte("pk2"),
			ClusteringValues: [][]byte{[]byte("ck1")},
			ColumnName:       []byte("colX"),
			Value:            []byte("valX"),
		},
	}

	// Sort cells by composite key (if not already).
	// For demonstration, let's assume they're already sorted. If not, you'd sort them:
	// sort.Slice(cells, func(i, j int) bool {
	// 	return bytes.Compare(cells[i].CompositeKey(), cells[j].CompositeKey()) < 0
	// })

	// Write an SSTable
	sst, err := writeSSTable(filePath, cells, 2, len(cells), 0.01)
	if err != nil {
		t.Fatalf("writeSSTable failed: %v", err)
	}

	// Reload to ensure we read from disk
	sst, err = loadSSTable(filePath)
	if err != nil {
		t.Fatalf("loadSSTable failed: %v", err)
	}

	// Attempt to retrieve each cell
	for _, c := range cells {
		got, err := sst.Get(c.PartitionKey, c.ColumnName, c.ClusteringValues...)
		if err != nil {
			t.Errorf("Get failed for %q: %v", c.CompositeKey(), err)
			continue
		}
		if !bytes.Equal(got.Value, c.Value) {
			t.Errorf("Value mismatch. Got %q, want %q", got.Value, c.Value)
		}
	}

	// Attempt to retrieve a cell that doesn't exist
	missing, err := sst.Get([]byte("pkMissing"), []byte("colMissing"), []byte("ckMissing"))
	if err == nil {
		t.Errorf("Expected error for missing key, got a cell: %v", missing)
	}
}

// --------------------
// Test sstable.ReadAllCells
// --------------------

func TestSSTableReadAllCells(t *testing.T) {
	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "test_sstable_readall.sst")

	cells := []Cell{
		{
			PartitionKey:     []byte("partA"),
			ClusteringValues: [][]byte{[]byte("ck1"), []byte("ck2")},
			ColumnName:       []byte("col1"),
			Value:            []byte("val1"),
		},
		{
			PartitionKey:     []byte("partA"),
			ClusteringValues: [][]byte{[]byte("ck2")},
			ColumnName:       []byte("col2"),
			Value:            []byte("val2"),
		},
		{
			PartitionKey: []byte("partB"),
			ClusteringValues: [][]byte{
				[]byte("ckX"),
			},
			ColumnName: []byte("colX"),
			Value:      []byte("valX"),
		},
	}

	// Sort by composite key if needed
	// For this example, assume they're already in sorted order.

	_, err := writeSSTable(filePath, cells, 2, len(cells), 0.01)
	if err != nil {
		t.Fatalf("writeSSTable failed: %v", err)
	}

	sst, err := loadSSTable(filePath)
	if err != nil {
		t.Fatalf("loadSSTable failed: %v", err)
	}

	readCells, err := sst.ReadAllCells()
	if err != nil {
		t.Fatalf("ReadAllCells failed: %v", err)
	}

	if len(readCells) != len(cells) {
		t.Fatalf("Expected %d cells, got %d", len(cells), len(readCells))
	}

	// Verify that each cell is in the original list (order may differ if you rely on ordering).
	for _, rc := range readCells {
		found := false
		for _, original := range cells {
			same := bytes.Equal(rc.PartitionKey, original.PartitionKey) &&
				bytes.Equal(rc.ColumnName, original.ColumnName) &&
				bytes.Equal(rc.Value, original.Value) &&
				reflect.DeepEqual(rc.ClusteringValues, original.ClusteringValues)
			if same {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("ReadAllCells returned a cell not in original set: %+v", rc)
		}
	}
}

// --------------------
// (Optional) If you'd like to explicitly test readBytesWithPrefix:
// --------------------

func TestReadBytesWithPrefix(t *testing.T) {
	var buf bytes.Buffer

	// Write length (4 bytes) + data
	data := []byte("hello")
	if err := binary.Write(&buf, binary.BigEndian, uint32(len(data))); err != nil {
		t.Fatalf("failed writing length prefix: %v", err)
	}
	if _, err := buf.Write(data); err != nil {
		t.Fatalf("failed writing data: %v", err)
	}

	// Now read it back
	out, err := readBytesWithPrefix(&buf)
	if err != nil {
		t.Fatalf("readBytesWithPrefix error: %v", err)
	}
	if !bytes.Equal(out, data) {
		t.Errorf("Expected %q, got %q", data, out)
	}

	// Zero-length prefix
	buf.Reset()
	if err := binary.Write(&buf, binary.BigEndian, uint32(0)); err != nil {
		t.Fatalf("failed writing zero-length prefix: %v", err)
	}
	out, err = readBytesWithPrefix(&buf)
	if err != nil {
		t.Errorf("Expected no error for zero-length data, got %v", err)
	}
	if len(out) != 0 {
		t.Errorf("Expected empty slice, got %q", out)
	}
}
