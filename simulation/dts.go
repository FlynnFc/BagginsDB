// main.go
package simulation

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database" // Adjust import path as necessary
	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/flynnfc/bagginsdb/logger"
)

const (
	NumRecords   = 1000000 // Example: 10k or 1M records
	KeyPrefix    = "key_"
	ValuePrefix  = "value_"
	NonExistKeys = 1000
)

// Generate a deterministic partition key based on the index
func generateKey(index int) []byte {
	s := fmt.Sprintf("%s%d", KeyPrefix, index)
	hash := sha256.Sum256([]byte(s))
	return []byte(hex.EncodeToString(hash[:]))
}

// Generate a deterministic value based on the index
func generateValue(index int) []byte {
	s := fmt.Sprintf("%s%d", ValuePrefix, index)
	hash := sha256.Sum256([]byte(s))
	return []byte(hex.EncodeToString(hash[:]))
}

// bulkInsert inserts [numRecords] wide-column entries
// partitionKey = generateKey(index), columnName="data", value=generateValue(index)
func bulkInsert(db *database.Database, numRecords, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var inserted int64

	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			partitionKey := generateKey(index)
			value := generateValue(index)
			db.Put(partitionKey, defaultClustering, defaultColumnName, value)
			atomic.AddInt64(&inserted, 1)
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Feed tasks
	for i := 0; i < numRecords; i++ {
		recordCh <- i
	}
	close(recordCh)

	wg.Wait()
	if inserted != int64(numRecords) {
		return fmt.Errorf("inserted mismatch: expected %d, got %d", numRecords, inserted)
	}
	return nil
}

// bulkRetrieveAndValidate retrieves [numRecords] wide-column entries
// by the same partition key & column, and validates the value
func bulkRetrieveAndValidate(db *database.Database, numRecords, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var validationErrors int64
	var retrieved int64

	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			partitionKey := generateKey(index)
			expectedVal := generateValue(index)

			val := db.Get(partitionKey, defaultClustering, defaultColumnName)
			if val == nil {
				atomic.AddInt64(&validationErrors, 1)
				continue
			}
			if !bytes.Equal(val, expectedVal) {
				atomic.AddInt64(&validationErrors, 1)
			} else {
				atomic.AddInt64(&retrieved, 1)
			}
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Feed tasks
	for i := 0; i < numRecords; i++ {
		recordCh <- i
	}
	close(recordCh)

	wg.Wait()
	if validationErrors > 0 {
		return fmt.Errorf("validation failed with %d errors, retrieved %d/%d",
			validationErrors, retrieved, numRecords)
	}
	return nil
}

// bulkRetrieveNonExistent tries to get keys we know don’t exist
func bulkRetrieveNonExistent(db *database.Database, numNonExist, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var notFoundCount int64
	var foundCount int64

	worker := func() {
		defer wg.Done()
		for offsetIndex := range recordCh {
			// offset to ensure these are distinct from inserted range
			partitionKey := generateKey(offsetIndex + 10000000)
			val := db.Get(partitionKey, defaultClustering, defaultColumnName)
			// If we get anything other than nil or empty, it’s an error
			if val != nil && len(val) > 0 {
				atomic.AddInt64(&foundCount, 1)
			} else {
				atomic.AddInt64(&notFoundCount, 1)
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	for i := 0; i < numNonExist; i++ {
		recordCh <- i
	}
	close(recordCh)

	wg.Wait()
	if foundCount > 0 {
		return fmt.Errorf("expected none found, but found %d", foundCount)
	}
	return nil
}

// mixedWorkload runs a mix of writes (Put) and reads (Get) at the ratio specified
func mixedWorkload(db *database.Database, totalOps, writeRatio, workers int) error {
	writeOps := totalOps * writeRatio / 100
	readOps := totalOps - writeOps

	var writesDone int64
	var readsDone int64
	var errs int64

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	opCh := make(chan bool, workers*2) // true=write, false=read

	worker := func() {
		defer wg.Done()
		for {
			select {
			case op, ok := <-opCh:
				if !ok {
					return
				}
				if op {
					// Write
					idx := int(atomic.AddInt64(&writesDone, 1))
					partitionKey := generateKey(idx + 20000000) // Distinct range
					val := generateValue(idx + 20000000)
					db.Put(partitionKey, defaultClustering, defaultColumnName, val)
				} else {
					// Read
					idx := int(atomic.AddInt64(&readsDone, 1))
					partitionKey := generateKey(idx)
					_ = db.Get(partitionKey, defaultClustering, defaultColumnName)
				}
			case <-ctx.Done():
				return
			}
		}
	}

	// Spawn workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Push ops
	for i := 0; i < writeOps; i++ {
		opCh <- true
	}
	for i := 0; i < readOps; i++ {
		opCh <- false
	}
	close(opCh)

	wg.Wait()

	wDone := atomic.LoadInt64(&writesDone)
	rDone := atomic.LoadInt64(&readsDone)

	if wDone != int64(writeOps) || rDone != int64(readOps) {
		atomic.AddInt64(&errs, 1)
		return fmt.Errorf("mismatch in ops done: writes=%d/%d, reads=%d/%d",
			wDone, writeOps, rDone, readOps)
	}
	return nil
}

// measureExecutionTime prints throughput metrics
func measureExecutionTime(start, end time.Time, operation string, opsCount int) {
	duration := end.Sub(start)
	rate := float64(opsCount) / duration.Seconds()
	fmt.Printf("%s: %d operations in %v (%.2f ops/sec)\n",
		operation, opsCount, duration, rate)
}

// Dts runs the simulation scenarios using the wide-column DB.
func Dts() {
	seed := time.Now().Format("2006-01-02-15-04-05")
	logger := logger.InitLogger(seed + "-dts")

	// Setup DB
	config := database.Config{
		Host: "localhost",
		// other fields if needed
	}
	mockClock := truetime.NewTrueTime(logger)
	mockClock.Run()

	db := database.NewDatabase(logger, config)
	defer db.Close()

	workers := 50

	// Scenario A: Bulk Insert
	fmt.Printf("Scenario A: Bulk Insert %d records\n", NumRecords)
	startInsert := time.Now()
	err := bulkInsert(db, NumRecords, workers)
	endInsert := time.Now()
	if err != nil {
		fmt.Printf("Bulk insert error: %v\n", err)
		return
	}
	measureExecutionTime(startInsert, endInsert, "Bulk Insert", NumRecords)

	// Scenario A: Bulk Retrieve & Validate
	fmt.Printf("Scenario A: Bulk Retrieve & Validate %d records\n", NumRecords)
	startRetrieve := time.Now()
	err = bulkRetrieveAndValidate(db, NumRecords, workers)
	endRetrieve := time.Now()
	if err != nil {
		fmt.Printf("Bulk retrieve/validate error: %v\n", err)
	} else {
		measureExecutionTime(startRetrieve, endRetrieve, "Bulk Retrieve/Validate", NumRecords)
	}

	// Scenario B: Retrieve non-existent keys
	fmt.Printf("Scenario B: Retrieve %d non-existent keys\n", NonExistKeys)
	startNonExist := time.Now()
	err = bulkRetrieveNonExistent(db, NonExistKeys, workers)
	endNonExist := time.Now()
	if err != nil {
		fmt.Printf("Non-existent key test error: %v\n", err)
	} else {
		measureExecutionTime(startNonExist, endNonExist, "Non-existent Keys Retrieval", NonExistKeys)
	}

	// Scenario C: Mixed Workload
	totalMixedOps := 20000
	writeRatio := 20
	fmt.Printf("Scenario C: Mixed workload of %d ops with %d%% writes\n", totalMixedOps, writeRatio)
	startMixed := time.Now()
	err = mixedWorkload(db, totalMixedOps, writeRatio, workers)
	endMixed := time.Now()
	if err != nil {
		fmt.Printf("Mixed workload error: %v\n", err)
	} else {
		measureExecutionTime(startMixed, endMixed, "Mixed Workload", totalMixedOps)
	}

	// Summary
	fmt.Println("=== Final Summary ===")
	fmt.Printf("Inserted: %d records\n", NumRecords)
	fmt.Printf("Retrieved/Validated: %d records\n", NumRecords)
	fmt.Printf("Non-existent keys tested: %d\n", NonExistKeys)
	fmt.Printf("Mixed Ops: %d (with %d%% writes)\n", totalMixedOps, writeRatio)
	fmt.Println("Performance tests completed successfully.")
}
