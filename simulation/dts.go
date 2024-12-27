// main.go
package simulation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database" // Adjust import as necessary
	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/flynnfc/bagginsdb/logger"
)

const (
	NumRecords   = 1000000 // Example: 10k records for faster testing
	KeyPrefix    = "key_"
	ValuePrefix  = "value_"
	NonExistKeys = 1000 // Number of keys that we know do not exist
)

// Generate a deterministic key based on the index
func generateKey(index int) []byte {
	key := fmt.Sprintf("%s%d", KeyPrefix, index)
	hash := sha256.Sum256([]byte(key))
	return []byte(hex.EncodeToString(hash[:]))
}

// Generate a deterministic value based on the index
func generateValue(index int) []byte {
	value := fmt.Sprintf("%s%d", ValuePrefix, index)
	hash := sha256.Sum256([]byte(value))
	return []byte(hex.EncodeToString(hash[:]))
}

// Bulk insert function using worker pools
func bulkInsert(db *database.Database, numRecords, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var inserted int64

	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			key := generateKey(index)
			value := generateValue(index)
			db.Put(key, value)
			atomic.AddInt64(&inserted, 1)
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	for i := 0; i < numRecords; i++ {
		recordCh <- i
	}
	close(recordCh)

	wg.Wait()
	if inserted != int64(numRecords) {
		return fmt.Errorf("inserted count mismatch: expected %d, got %d", numRecords, inserted)
	}
	return nil
}

// Bulk retrieve and validate function using worker pools
func bulkRetrieveAndValidate(db *database.Database, numRecords, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var validationErrors int64
	var retrieved int64

	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			key := generateKey(index)
			expectedValue := generateValue(index)
			retrievedVal := db.Get(key)
			if retrievedVal == nil {
				atomic.AddInt64(&validationErrors, 1)
				continue
			}
			valBytes, ok := retrievedVal.([]byte)
			if !ok || string(valBytes) != string(expectedValue) {
				atomic.AddInt64(&validationErrors, 1)
			} else {
				atomic.AddInt64(&retrieved, 1)
			}
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

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

// Scenario B: Retrieve keys that are known not to exist
func bulkRetrieveNonExistent(db *database.Database, numNonExist, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var notFoundCount int64
	var foundCount int64

	// Generate non-existent keys by starting from numRecords+1, etc.
	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			nonExistKey := generateKey(index + 1000000) // Offset to ensure non-existence
			val := db.Get(nonExistKey)
			if string(val.([]byte)) == "" {
				atomic.AddInt64(&notFoundCount, 1)
			} else {
				atomic.AddInt64(&foundCount, 1)
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
		return fmt.Errorf("expected no keys found, but found %d", foundCount)
	}
	return nil
}

// Scenario C: Mixed reads and writes concurrently.
func mixedWorkload(db *database.Database, totalOps, writeRatio, workers int) error {
	// writeRatio is a percentage of how many ops are writes vs reads.
	// E.g. 50 means 50% writes and 50% reads.
	writeOps := totalOps * writeRatio / 100
	readOps := totalOps - writeOps

	var writesDone int64
	var readsDone int64
	var errs int64

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	opCh := make(chan bool, workers*2) // true for write, false for read

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
					key := generateKey(idx + 2000000) // Distinct range
					val := generateValue(idx + 2000000)
					db.Put(key, val)
				} else {
					// Read
					idx := int(atomic.AddInt64(&readsDone, 1))
					key := generateKey(idx) // some existing range
					_ = db.Get(key)         // don't validate here, just read
				}
			case <-ctx.Done():
				return
			}
		}
	}

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
		return fmt.Errorf("mismatch in done ops. writes: %d/%d, reads: %d/%d",
			wDone, writeOps, rDone, readOps)
	}

	return nil
}

// Measure execution time and print throughput
func measureExecutionTime(start, end time.Time, operation string, opsCount int) {
	duration := end.Sub(start)
	rate := float64(opsCount) / duration.Seconds()
	fmt.Printf("%s: %d operations in %v (%.2f ops/sec)\n",
		operation, opsCount, duration, rate)
}

func Dts() {
	seed := time.Now().Format("2006-01-02-15-04-05")
	logger := logger.InitLogger(seed + "-dts")

	// Setup database
	config := database.Config{Host: "localhost"}
	mockClock := truetime.NewTrueTime(logger)
	mockClock.Run()
	db := database.NewDatabase(logger, config)
	defer db.Close()

	workers := 50

	// Scenario A: Bulk Insert and Validate
	fmt.Printf("Scenario A: Bulk Insert %d records\n", NumRecords)
	startInsert := time.Now()
	err := bulkInsert(db, NumRecords, workers)
	endInsert := time.Now()
	if err != nil {
		fmt.Printf("Bulk insert error: %v\n", err)
		return
	}
	measureExecutionTime(startInsert, endInsert, "Bulk Insert", NumRecords)

	fmt.Printf("Scenario A: Bulk Retrieve and Validate %d records\n", NumRecords)
	startRetrieve := time.Now()
	err = bulkRetrieveAndValidate(db, NumRecords, workers)
	endRetrieve := time.Now()
	if err != nil {
		fmt.Printf("Bulk retrieve/validate error: %v\n", err)
	} else {
		measureExecutionTime(startRetrieve, endRetrieve, "Bulk Retrieve/Validate", NumRecords)
	}

	// Scenario B: Non-existent Keys
	fmt.Printf("Scenario B: Retrieve %d non-existent keys\n", NonExistKeys)
	startNonExist := time.Now()
	err = bulkRetrieveNonExistent(db, NonExistKeys, workers)
	endNonExist := time.Now()
	if err != nil {
		fmt.Printf("Non-existent key test error: %v\n", err)
	} else {
		measureExecutionTime(startNonExist, endNonExist, "Non-existent Keys Retrieval", NonExistKeys)
	}

	// Scenario C: Mixed Workload (e.g., 20% writes, 80% reads)
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

	// Final Summary
	fmt.Println("=== Final Summary ===")
	fmt.Printf("Inserted: %d records\n", NumRecords)
	fmt.Printf("Retrieved/Validated: %d records\n", NumRecords)
	fmt.Printf("Non-existent keys tested: %d\n", NonExistKeys)
	fmt.Printf("Mixed Ops: %d (with %d%% writes)\n", totalMixedOps, writeRatio)
	fmt.Println("Performance tests completed successfully.")
}
