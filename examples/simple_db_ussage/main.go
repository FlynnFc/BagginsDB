package main

import (
	"fmt"

	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/pkg/bagginsdb/db"
)

func main() {
	logger := logger.InitLogger("bagginsdb-example")
	// Create a new database instance. This will handle the creation and management of memtables and SSTables.
	db := db.NewDatabase(logger, db.Config{MemTableSize: 1024 * 5})
	// Closing the database will flush the memtable to SSTables and close any SSTables open.
	defer db.Close()
	// Put some data into the database
	// This will be initially stored in the memtable
	// If the database is closed of if the memtable is filled it will write to an SSTable
	db.Put([]byte("key1"), [][]byte{}, []byte("data-col"), []byte("Hello, World!"))

	// Get the data back out
	// Get first searches the memtable. If the data is not found in the memtable it will search the SSTables from newest to oldest.
	// It will only open and search the SSTable if the bloom filter indicates that the key may be in the SSTable.
	// In the case of a bloom filter hit, the SSTable will be opened and the key will be searched for using a sparse index.
	data, err := db.Get([]byte("key1"), [][]byte{}, []byte("data-col"))
	if err != nil {
		println("Failed to get data from database!")
	}
	println(fmt.Sprintf("Data: %s", string(data)))
}
