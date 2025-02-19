package db

import (
	"bytes"
	"testing"
	"time"

	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/truetime"
)

func compareIntervals(a, b truetime.Interval) bool {
	return a.Earliest.Equal(b.Earliest) && a.Latest.Equal(b.Latest)
}

// TestNewMemtable verifies that newMemtable creates a memtable with a non-nil skiplist.
func TestNewMemtable(t *testing.T) {
	mt := newMemtable()
	if mt == nil {
		t.Fatal("Expected new memtable to be non-nil")
	}
	if mt.skiplist == nil {
		t.Fatal("Expected memtable.skiplist to be non-nil")
	}
	if mt.Len() != 0 {
		t.Errorf("Expected empty memtable, but Len() = %d", mt.Len())
	}
}

// TestMemtablePutAndGet checks that Put stores cells and Get retrieves them correctly.
func TestMemtablePutAndGet(t *testing.T) {
	mt := newMemtable()

	// Create a small interval around a base time.
	base := time.Unix(12345, 0)
	cell := Cell{
		PartitionKey:     []byte("pk1"),
		ClusteringValues: [][]byte{[]byte("ck1")},
		ColumnName:       []byte("col1"),
		Value:            []byte("value1"),
		Timestamp: truetime.Interval{
			Earliest: base,
			Latest:   base.Add(time.Millisecond * 10),
		},
	}

	mt.Put(cell)

	got := mt.Get([]byte("pk1"), [][]byte{[]byte("ck1")}, []byte("col1"))
	if !bytes.Equal(got, []byte("value1")) {
		t.Errorf("Get returned %q, expected %q", got, cell.Value)
	}

	// Check with non-existent key
	got2 := mt.Get([]byte("pkX"), [][]byte{[]byte("ck1")}, []byte("col1"))
	if got2 != nil {
		t.Errorf("Expected nil for non-existent key, got %q", got2)
	}
}

// TestMemtableMultipleCells tests that multiple cells can be inserted and retrieved.
func TestMemtableMultipleCells(t *testing.T) {
	mt := newMemtable()

	cells := []Cell{
		{
			PartitionKey:     []byte("pk1"),
			ClusteringValues: [][]byte{[]byte("ck1")},
			ColumnName:       []byte("colA"),
			Value:            []byte("A1"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(100, 0),
				Latest:   time.Unix(100, 0).Add(time.Second),
			},
		},
		{
			PartitionKey:     []byte("pk1"),
			ClusteringValues: [][]byte{[]byte("ck2")},
			ColumnName:       []byte("colB"),
			Value:            []byte("B1"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(101, 0),
				Latest:   time.Unix(101, 0).Add(time.Second),
			},
		},
		{
			PartitionKey:     []byte("pk2"),
			ClusteringValues: [][]byte{[]byte("ck1"), []byte("ck2")},
			ColumnName:       []byte("colX"),
			Value:            []byte("X1"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(200, 0),
				Latest:   time.Unix(200, 0).Add(time.Second),
			},
		},
	}

	// Put all cells
	for _, c := range cells {
		mt.Put(c)
	}

	// Check retrieval
	gotA := mt.Get([]byte("pk1"), [][]byte{[]byte("ck1")}, []byte("colA"))
	if !bytes.Equal(gotA, []byte("A1")) {
		t.Errorf("Expected 'A1' for pk1:ck1:colA, got %q", gotA)
	}

	gotB := mt.Get([]byte("pk1"), [][]byte{[]byte("ck2")}, []byte("colB"))
	if !bytes.Equal(gotB, []byte("B1")) {
		t.Errorf("Expected 'B1' for pk1:ck2:colB, got %q", gotB)
	}

	gotX := mt.Get([]byte("pk2"), [][]byte{[]byte("ck1"), []byte("ck2")}, []byte("colX"))
	if !bytes.Equal(gotX, []byte("X1")) {
		t.Errorf("Expected 'X1' for pk2:ck1:ck2:colX, got %q", gotX)
	}

	// Check something that doesn't exist
	gotNil := mt.Get([]byte("pk2"), [][]byte{[]byte("ckXYZ")}, []byte("colX"))
	if gotNil != nil {
		t.Errorf("Expected nil for non-existent combination, got %q", gotNil)
	}
}

// TestMemtableEntries verifies we can retrieve all entries in the skiplist as raw Key/Value pairs.
func TestMemtableEntries(t *testing.T) {
	mt := newMemtable()
	cells := []Cell{
		{
			PartitionKey:     []byte("pkA"),
			ClusteringValues: [][]byte{[]byte("ck1")},
			ColumnName:       []byte("col1"),
			Value:            []byte("v1"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(111, 0),
				Latest:   time.Unix(111, 0).Add(time.Millisecond),
			},
		},
		{
			PartitionKey:     []byte("pkB"),
			ClusteringValues: [][]byte{[]byte("ck2")},
			ColumnName:       []byte("col2"),
			Value:            []byte("v2"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(222, 0),
				Latest:   time.Unix(222, 0).Add(time.Millisecond),
			},
		},
	}

	for _, c := range cells {
		mt.Put(c)
	}

	entries := mt.Entries()
	if len(entries) != len(cells) {
		t.Fatalf("Expected %d entries, got %d", len(cells), len(entries))
	}

	// Quick check that keys/values align with what's expected by matching Value.Data and Value.Interval.
	for _, kv := range entries {
		found := false
		for _, c := range cells {
			if bytes.Equal(kv.Val.Data, c.Value) && compareIntervals(kv.Val.Timestamp, c.Timestamp) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Unexpected entry: key=%v, val=%v", kv.Key, kv.Val)
		}
	}
}

// TestMemtableToColumnEntries checks we can convert all skiplist entries back into wide cells.
func TestMemtableToColumnEntries(t *testing.T) {
	mt := newMemtable()

	// Insert some cells with multiple clustering values
	cells := []Cell{
		{
			PartitionKey:     []byte("pk1"),
			ClusteringValues: [][]byte{[]byte("ck1"), []byte("ck2")},
			ColumnName:       []byte("colA"),
			Value:            []byte("valA"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(101, 0),
				Latest:   time.Unix(101, 0).Add(time.Millisecond),
			},
		},
		{
			PartitionKey:     []byte("pk2"),
			ClusteringValues: [][]byte{[]byte("ckX")},
			ColumnName:       []byte("colB"),
			Value:            []byte("valB"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(202, 0),
				Latest:   time.Unix(202, 0).Add(time.Millisecond),
			},
		},
	}

	for _, c := range cells {
		mt.Put(c)
	}

	columnEntries := mt.ToColumnEntries()
	if len(columnEntries) != len(cells) {
		t.Fatalf("Expected %d column entries, got %d", len(cells), len(columnEntries))
	}

	// Verify round-trip equivalence
	for _, ce := range columnEntries {
		found := false
		for _, original := range cells {
			if bytes.Equal(ce.PartitionKey, original.PartitionKey) &&
				compareClustering(ce.ClusteringValues, original.ClusteringValues) &&
				bytes.Equal(ce.ColumnName, original.ColumnName) &&
				bytes.Equal(ce.Value, original.Value) &&
				compareIntervals(ce.Timestamp, original.Timestamp) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("Returned cell not found in original set: %+v", ce)
		}
	}
}

// TestBuildAndParseCompositeKey ensures buildCompositeKey and parseCompositeKey produce compatible results.
func TestBuildAndParseCompositeKey(t *testing.T) {
	partitionKey := []byte("partition")
	clusteringVals := [][]byte{
		[]byte("ck1"),
		[]byte("ck2"),
		[]byte("ck3"),
	}
	columnName := []byte("colName")

	composite := buildCompositeKey(partitionKey, clusteringVals, columnName)
	pk, cks, cname := parseCompositeKey(composite)

	if !bytes.Equal(pk, partitionKey) {
		t.Errorf("Parsed partition key %q != original %q", pk, partitionKey)
	}
	if !compareClustering(cks, clusteringVals) {
		t.Errorf("Parsed clustering %q != original %q", cks, clusteringVals)
	}
	if !bytes.Equal(cname, columnName) {
		t.Errorf("Parsed column name %q != original %q", cname, columnName)
	}
}

// TestParseCompositeKeyEdgeCases checks parsing edge cases like missing delimiters.
func TestParseCompositeKeyEdgeCases(t *testing.T) {
	// No 0x00 => parseCompositeKey returns nil, nil, nil
	noDelim := []byte("noDelimiterAtAll")
	pk, cks, cname := parseCompositeKey(noDelim)
	if pk != nil || cks != nil || cname != nil {
		t.Errorf("Expected all nil when no 0x00 delimiter found. Got pk=%v, cks=%v, cname=%v", pk, cks, cname)
	}

	// Partition key only, no 0x02 => we get a partition key, but no column name
	pkOnly := append([]byte("partition"), 0x00)
	pk, cks, cname = parseCompositeKey(pkOnly)
	if !bytes.Equal(pk, []byte("partition")) {
		t.Errorf("Expected partition key 'partition', got %q", pk)
	}
	if cks != nil || cname != nil {
		t.Errorf("Expected cks=nil, cname=nil, got cks=%v, cname=%v", cks, cname)
	}
}

// compareClustering is a helper function to compare slices of byte slices.
func compareClustering(a, b [][]byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !bytes.Equal(a[i], b[i]) {
			return false
		}
	}
	return true
}
