package db

import (
	"bytes"
	"sync"
	"testing"
)

// TestNewSkipList checks the basic properties of a newly created skipList.
func TestNewSkipList(t *testing.T) {
	sl := newSkipList()
	if sl == nil {
		t.Fatal("Expected a non-nil skipList")
	}
	if sl.level != 1 {
		t.Errorf("Expected level=1 for new skipList, got %d", sl.level)
	}
	if sl.Len() != 0 {
		t.Errorf("Expected an empty skipList, got Len()=%d", sl.Len())
	}
	if sl.head == nil {
		t.Errorf("Expected a non-nil head node")
	}
	if len(sl.head.next) == 0 {
		t.Errorf("Expected the head node to have next pointers, found none")
	}
}

// TestSkipListSetAndGetSingle verifies Set() and Get() for a single key-value pair.
func TestSkipListSetAndGetSingle(t *testing.T) {
	sl := newSkipList()

	key := []byte("foo")
	val := Value{Data: []byte("bar"), Timestamp: 100}

	sl.Set(key, val)

	got := sl.Get(key)
	if got == nil {
		t.Fatalf("Expected to get a value for key=%q, got nil", key)
	}
	if !bytes.Equal(got.Data, val.Data) {
		t.Errorf("Got Data=%q, expected %q", got.Data, val.Data)
	}
	if got.Timestamp != val.Timestamp {
		t.Errorf("Got Timestamp=%d, expected %d", got.Timestamp, val.Timestamp)
	}
}

// TestSkipListSetAndGetMultiple checks insertion and retrieval of multiple keys in sorted order.
func TestSkipListSetAndGetMultiple(t *testing.T) {
	sl := newSkipList()

	keys := [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie"), []byte("delta")}
	values := []Value{
		{Data: []byte("A"), Timestamp: 1},
		{Data: []byte("B"), Timestamp: 2},
		{Data: []byte("C"), Timestamp: 3},
		{Data: []byte("D"), Timestamp: 4},
	}

	// Insert the key-value pairs
	for i, k := range keys {
		sl.Set(k, values[i])
	}

	// Verify retrieval
	for i, k := range keys {
		got := sl.Get(k)
		if got == nil {
			t.Fatalf("Expected value for key=%q, got nil", k)
		}
		if !bytes.Equal(got.Data, values[i].Data) {
			t.Errorf("For key=%q, got data=%q, expected %q", k, got.Data, values[i].Data)
		}
		if got.Timestamp != values[i].Timestamp {
			t.Errorf("For key=%q, got timestamp=%d, expected %d", k, got.Timestamp, values[i].Timestamp)
		}
	}
}

// TestSkipListUpdateTimestamp ensures that updating an existing key with a newer timestamp overwrites it.
func TestSkipListUpdateTimestamp(t *testing.T) {
	sl := newSkipList()

	key := []byte("myKey")
	oldVal := Value{Data: []byte("oldData"), Timestamp: 50}
	newVal := Value{Data: []byte("newData"), Timestamp: 100}

	// Insert older value first
	sl.Set(key, oldVal)

	// Try to update with a newer timestamp
	sl.Set(key, newVal)

	got := sl.Get(key)
	if got == nil {
		t.Fatalf("Expected to find key=%q after update, got nil", key)
	}
	if !bytes.Equal(got.Data, newVal.Data) {
		t.Errorf("Expected Data=%q, got %q", newVal.Data, got.Data)
	}
	if got.Timestamp != newVal.Timestamp {
		t.Errorf("Expected Timestamp=%d, got %d", newVal.Timestamp, got.Timestamp)
	}

	// Try to update with an older timestamp, should NOT overwrite
	olderVal := Value{Data: []byte("olderData"), Timestamp: 10}
	sl.Set(key, olderVal)
	got2 := sl.Get(key)
	if got2 == nil {
		t.Fatalf("Expected key=%q to still exist", key)
	}
	if bytes.Equal(got2.Data, olderVal.Data) {
		t.Errorf("Did not expect skipList to overwrite with older timestamp. Got data=%q", got2.Data)
	}
	if got2.Timestamp == olderVal.Timestamp {
		t.Errorf("Did not expect skipList to overwrite with older timestamp. Got timestamp=%d", got2.Timestamp)
	}
}

// TestSkipListLen verifies that the size counter is correct.
func TestSkipListLen(t *testing.T) {
	sl := newSkipList()
	if sl.Len() != 0 {
		t.Errorf("Expected skipList to be empty initially, got Len()=%d", sl.Len())
	}

	sl.Set([]byte("k1"), Value{Data: []byte("v1"), Timestamp: 1})
	sl.Set([]byte("k2"), Value{Data: []byte("v2"), Timestamp: 2})
	sl.Set([]byte("k3"), Value{Data: []byte("v3"), Timestamp: 3})

	if sl.Len() != 3 {
		t.Errorf("Expected skipList length=3, got %d", sl.Len())
	}

	// Insert a duplicate key (should not increment size)
	sl.Set([]byte("k2"), Value{Data: []byte("newV2"), Timestamp: 10})
	if sl.Len() != 3 {
		t.Errorf("Expected skipList length to remain 3 after updating an existing key, got %d", sl.Len())
	}
}

// TestSkipListFront checks that Front() returns the first node in sorted order.
func TestSkipListFront(t *testing.T) {
	sl := newSkipList()

	keys := [][]byte{[]byte("alpha"), []byte("bravo"), []byte("charlie")}
	values := []Value{
		{Data: []byte("A"), Timestamp: 10},
		{Data: []byte("B"), Timestamp: 20},
		{Data: []byte("C"), Timestamp: 30},
	}

	// Insert in a shuffled order to ensure sorting is tested
	sl.Set(keys[1], values[1]) // bravo
	sl.Set(keys[2], values[2]) // charlie
	sl.Set(keys[0], values[0]) // alpha

	front := sl.Front()
	if front == nil {
		t.Fatal("Expected front to be non-nil after inserts")
	}
	// The smallest key in lexical order is "alpha"
	if !bytes.Equal(front.key, []byte("alpha")) {
		t.Errorf("Expected front key=%q, got %q", "alpha", front.key)
	}
}

// TestSkipListEntries verifies we can retrieve all (Key, Val) pairs in ascending order.
func TestSkipListEntries(t *testing.T) {
	sl := newSkipList()

	input := []struct {
		key []byte
		val Value
	}{
		{[]byte("cat"), Value{Data: []byte("C"), Timestamp: 3}},
		{[]byte("apple"), Value{Data: []byte("A"), Timestamp: 1}},
		{[]byte("banana"), Value{Data: []byte("B"), Timestamp: 2}},
	}

	for _, kv := range input {
		sl.Set(kv.key, kv.val)
	}

	entries := sl.Entries()
	if len(entries) != len(input) {
		t.Fatalf("Expected %d entries, got %d", len(input), len(entries))
	}

	// Verify entries are in sorted order by key
	var lastKey []byte
	for i, e := range entries {
		if i > 0 && bytes.Compare(e.Key, lastKey) <= 0 {
			t.Errorf("Entries are not in ascending order: %q came after %q", e.Key, lastKey)
		}
		lastKey = e.Key
	}
}

// Optional: a concurrency test to ensure thread-safety of Set and Get.
func TestSkipListConcurrentAccess(t *testing.T) {
	sl := newSkipList()
	var wg sync.WaitGroup
	numGoroutines := 10
	numKeysPerGoroutine := 100

	// Writer goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeysPerGoroutine; k++ {
				key := []byte{byte(gid), byte(k)}
				val := Value{Data: []byte{byte(gid), byte(k)}, Timestamp: int64(k)}
				sl.Set(key, val)
			}
		}(i)
	}

	// Reader goroutines
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for k := 0; k < numKeysPerGoroutine; k++ {
				key := []byte{byte(gid), byte(k)}
				sl.Get(key) // Just read; we don't check to avoid race with writes
			}
		}(i)
	}

	wg.Wait()
	// Check skipList size is at least the total keys inserted
	// Some might be duplicates if the same (gid,k) combination is used multiple times
	// but in this example, each (gid,k) is unique, so we expect exactly numGoroutines * numKeysPerGoroutine.
	expectedSize := numGoroutines * numKeysPerGoroutine
	if sl.Len() != expectedSize {
		t.Errorf("Expected skipList size=%d, got %d", expectedSize, sl.Len())
	}
}
