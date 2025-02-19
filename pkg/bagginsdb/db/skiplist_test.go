package db

import (
	"bytes"
	"sync"
	"testing"
	"time"

	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/truetime"
	// Adjust the import path for truetime if needed.
	// If truetime.Interval is defined in this package, you can omit this import.
	// "yourmodule/truetime"
)

// TestNewSkipList checks the basic properties of a newly created skiplist.
func TestNewSkipList(t *testing.T) {
	sl := newSkipList()
	if sl == nil {
		t.Fatal("Expected non-nil skiplist")
	}
	if sl.level != 1 {
		t.Errorf("Expected initial level=1, got %d", sl.level)
	}
	if sl.Len() != 0 {
		t.Errorf("Expected empty skiplist, got Len()=%d", sl.Len())
	}
	if sl.head == nil {
		t.Error("Expected non-nil head node")
	}
	if len(sl.head.next) == 0 {
		t.Error("Expected head node to have next pointers")
	}
}

// TestSkipListSetAndGetSingle verifies that a single key-value pair can be set and retrieved.
func TestSkipListSetAndGetSingle(t *testing.T) {
	sl := newSkipList()

	// Create a fixed trusted interval.
	earliest := time.Unix(100, 0)
	latest := earliest.Add(1 * time.Second)
	val := Value{
		Data: []byte("bar"),
		Timestamp: truetime.Interval{
			Earliest: earliest,
			Latest:   latest,
		},
	}

	key := []byte("foo")
	sl.Set(key, val)

	retrieved := sl.Get(key)
	if retrieved == nil {
		t.Fatalf("Expected value for key %q, got nil", key)
	}
	if !bytes.Equal(retrieved.Data, val.Data) {
		t.Errorf("For key %q, expected Data %q, got %q", key, val.Data, retrieved.Data)
	}
	if !retrieved.Timestamp.Earliest.Equal(val.Timestamp.Earliest) {
		t.Errorf("For key %q, expected Earliest %v, got %v", key, val.Timestamp.Earliest, retrieved.Timestamp.Earliest)
	}
	if !retrieved.Timestamp.Latest.Equal(val.Timestamp.Latest) {
		t.Errorf("For key %q, expected Latest %v, got %v", key, val.Timestamp.Latest, retrieved.Timestamp.Latest)
	}
}

// TestSkipListSetAndGetMultiple verifies insertion and retrieval of multiple key-value pairs.
func TestSkipListSetAndGetMultiple(t *testing.T) {
	sl := newSkipList()

	keys := [][]byte{
		[]byte("alpha"),
		[]byte("bravo"),
		[]byte("charlie"),
		[]byte("delta"),
	}
	values := []Value{
		{
			Data: []byte("A"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(1, 0),
				Latest:   time.Unix(1, 0).Add(time.Second),
			},
		},
		{
			Data: []byte("B"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(2, 0),
				Latest:   time.Unix(2, 0).Add(time.Second),
			},
		},
		{
			Data: []byte("C"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(3, 0),
				Latest:   time.Unix(3, 0).Add(time.Second),
			},
		},
		{
			Data: []byte("D"),
			Timestamp: truetime.Interval{
				Earliest: time.Unix(4, 0),
				Latest:   time.Unix(4, 0).Add(time.Second),
			},
		},
	}

	// Insert each key-value pair.
	for i, k := range keys {
		sl.Set(k, values[i])
	}

	// Verify each key-value pair is retrievable.
	for i, k := range keys {
		got := sl.Get(k)
		if got == nil {
			t.Fatalf("Expected value for key %q, got nil", k)
		}
		if !bytes.Equal(got.Data, values[i].Data) {
			t.Errorf("For key %q, expected Data %q, got %q", k, values[i].Data, got.Data)
		}
		if !got.Timestamp.Earliest.Equal(values[i].Timestamp.Earliest) {
			t.Errorf("For key %q, expected Earliest %v, got %v", k, values[i].Timestamp.Earliest, got.Timestamp.Earliest)
		}
		if !got.Timestamp.Latest.Equal(values[i].Timestamp.Latest) {
			t.Errorf("For key %q, expected Latest %v, got %v", k, values[i].Timestamp.Latest, got.Timestamp.Latest)
		}
	}
}

// TestSkipListUpdateTimestamp ensures that updating an existing key with a newer interval overwrites the value.
func TestSkipListUpdateTimestamp(t *testing.T) {
	sl := newSkipList()

	key := []byte("myKey")
	oldEarliest := time.Unix(50, 0)
	oldLatest := oldEarliest.Add(time.Second)
	newEarliest := time.Unix(100, 0)
	newLatest := newEarliest.Add(time.Second)
	oldVal := Value{
		Data: []byte("oldData"),
		Timestamp: truetime.Interval{
			Earliest: oldEarliest,
			Latest:   oldLatest,
		},
	}
	newVal := Value{
		Data: []byte("newData"),
		Timestamp: truetime.Interval{
			Earliest: newEarliest,
			Latest:   newLatest,
		},
	}

	// Insert initial value.
	sl.Set(key, oldVal)

	// Update with a newer interval.
	sl.Set(key, newVal)
	got := sl.Get(key)
	if got == nil {
		t.Fatalf("Expected key %q after update, got nil", key)
	}
	if !bytes.Equal(got.Data, newVal.Data) {
		t.Errorf("For key %q, expected Data %q, got %q", key, newVal.Data, got.Data)
	}
	if !got.Timestamp.Earliest.Equal(newVal.Timestamp.Earliest) {
		t.Errorf("For key %q, expected Earliest %v, got %v", key, newVal.Timestamp.Earliest, got.Timestamp.Earliest)
	}
	if !got.Timestamp.Latest.Equal(newVal.Timestamp.Latest) {
		t.Errorf("For key %q, expected Latest %v, got %v", key, newVal.Timestamp.Latest, got.Timestamp.Latest)
	}

	// Attempt to update with an older interval; it should NOT overwrite.
	olderEarliest := time.Unix(10, 0)
	olderLatest := olderEarliest.Add(time.Second)
	olderVal := Value{
		Data: []byte("olderData"),
		Timestamp: truetime.Interval{
			Earliest: olderEarliest,
			Latest:   olderLatest,
		},
	}
	sl.Set(key, olderVal)
	got2 := sl.Get(key)
	if got2 == nil {
		t.Fatalf("Expected key %q to still exist", key)
	}
	if bytes.Equal(got2.Data, olderVal.Data) || got2.Timestamp.Earliest.Equal(olderVal.Timestamp.Earliest) {
		t.Errorf("Skiplist should not overwrite with an older Timestamp. Got Data %q and Earliest %v", got2.Data, got2.Timestamp.Earliest)
	}
}

// TestSkipListLen verifies that the length of the skiplist is correctly maintained.
func TestSkipListLen(t *testing.T) {
	sl := newSkipList()
	if sl.Len() != 0 {
		t.Errorf("Expected empty skiplist initially, got Len()=%d", sl.Len())
	}

	sl.Set([]byte("k1"), Value{
		Data: []byte("v1"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(1, 0),
			Latest:   time.Unix(1, 0).Add(time.Second),
		},
	})
	sl.Set([]byte("k2"), Value{
		Data: []byte("v2"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(2, 0),
			Latest:   time.Unix(2, 0).Add(time.Second),
		},
	})
	sl.Set([]byte("k3"), Value{
		Data: []byte("v3"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(3, 0),
			Latest:   time.Unix(3, 0).Add(time.Second),
		},
	})

	if sl.Len() != 3 {
		t.Errorf("Expected skiplist length=3, got %d", sl.Len())
	}

	// Updating an existing key should not change the length.
	sl.Set([]byte("k2"), Value{
		Data: []byte("newV2"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(10, 0),
			Latest:   time.Unix(10, 0).Add(time.Second),
		},
	})
	if sl.Len() != 3 {
		t.Errorf("Expected skiplist length to remain 3 after updating an existing key, got %d", sl.Len())
	}
}

// TestSkipListFront checks that Front() returns the first node in sorted order.
func TestSkipListFront(t *testing.T) {
	sl := newSkipList()

	// Insert keys in a non-sorted order.
	sl.Set([]byte("bravo"), Value{
		Data: []byte("B"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(20, 0),
			Latest:   time.Unix(20, 0).Add(time.Second),
		},
	})
	sl.Set([]byte("charlie"), Value{
		Data: []byte("C"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(30, 0),
			Latest:   time.Unix(30, 0).Add(time.Second),
		},
	})
	sl.Set([]byte("alpha"), Value{
		Data: []byte("A"),
		Timestamp: truetime.Interval{
			Earliest: time.Unix(10, 0),
			Latest:   time.Unix(10, 0).Add(time.Second),
		},
	})

	front := sl.Front()
	if front == nil {
		t.Fatal("Expected non-nil front after inserts")
	}
	expectedKey := []byte("alpha")
	if !bytes.Equal(front.key, expectedKey) {
		t.Errorf("Expected front key %q, got %q", expectedKey, front.key)
	}
}

// TestSkipListEntries verifies that all key-value pairs can be retrieved in sorted order.
func TestSkipListEntries(t *testing.T) {
	sl := newSkipList()

	input := []struct {
		key []byte
		val Value
	}{
		{
			key: []byte("cat"),
			val: Value{
				Data: []byte("C"),
				Timestamp: truetime.Interval{
					Earliest: time.Unix(3, 0),
					Latest:   time.Unix(3, 0).Add(time.Second),
				},
			},
		},
		{
			key: []byte("apple"),
			val: Value{
				Data: []byte("A"),
				Timestamp: truetime.Interval{
					Earliest: time.Unix(1, 0),
					Latest:   time.Unix(1, 0).Add(time.Second),
				},
			},
		},
		{
			key: []byte("banana"),
			val: Value{
				Data: []byte("B"),
				Timestamp: truetime.Interval{
					Earliest: time.Unix(2, 0),
					Latest:   time.Unix(2, 0).Add(time.Second),
				},
			},
		},
	}

	for _, kv := range input {
		sl.Set(kv.key, kv.val)
	}

	entries := sl.Entries()
	if len(entries) != len(input) {
		t.Fatalf("Expected %d entries, got %d", len(input), len(entries))
	}

	// Verify that entries are sorted by key.
	var lastKey []byte
	for i, entry := range entries {
		if i > 0 && bytes.Compare(entry.Key, lastKey) <= 0 {
			t.Errorf("Entries not in ascending order: %q came after %q", entry.Key, lastKey)
		}
		lastKey = entry.Key
	}
}

// TestSkipListConcurrentAccess tests thread-safety by concurrently performing Set and Get operations.
func TestSkipListConcurrentAccess(t *testing.T) {
	sl := newSkipList()
	var wg sync.WaitGroup

	numGoroutines := 10
	numKeysPerGoroutine := 100

	// Writer goroutines.
	for gid := 0; gid < numGoroutines; gid++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeysPerGoroutine; k++ {
				key := []byte{byte(gid), byte(k)}
				// Create an interval based on k.
				earliest := time.Unix(int64(k), 0)
				latest := earliest.Add(time.Second)
				val := Value{
					Data: []byte{byte(gid), byte(k)},
					Timestamp: truetime.Interval{
						Earliest: earliest,
						Latest:   latest,
					},
				}
				sl.Set(key, val)
			}
		}(gid)
	}

	// Reader goroutines.
	for gid := 0; gid < numGoroutines; gid++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeysPerGoroutine; k++ {
				key := []byte{byte(gid), byte(k)}
				_ = sl.Get(key)
			}
		}(gid)
	}

	wg.Wait()

	expectedSize := numGoroutines * numKeysPerGoroutine
	if sl.Len() != expectedSize {
		t.Errorf("Expected skiplist size=%d, got %d", expectedSize, sl.Len())
	}
}
