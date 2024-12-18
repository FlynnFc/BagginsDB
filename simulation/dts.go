// main.go
package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database" // Adjust the import path as necessary
	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/flynnfc/bagginsdb/logger"
)

const (
	NumRecords  = 100000 // 1 million records
	KeyPrefix   = "key_"
	ValuePrefix = "value_"
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
func bulkInsert(db *database.Database, numRecords int, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2) // Buffered channel

	// Worker function
	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			key := generateKey(index)
			value := generateValue(index)
			db.Put(key, value)
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Send record indices to the channel
	for i := 0; i < numRecords; i++ {
		recordCh <- i
	}
	close(recordCh)

	// Wait for all workers to finish
	wg.Wait()
	return nil
}

// Bulk retrieve and validate function using worker pools
func bulkRetrieveAndValidate(db *database.Database, numRecords int, workers int) error {
	var wg sync.WaitGroup
	recordCh := make(chan int, workers*2)
	var validationErrors int64 = 0
	var mu sync.Mutex // Mutex to protect validationErrors

	// Worker function
	worker := func() {
		defer wg.Done()
		for index := range recordCh {
			key := generateKey(index)
			expectedValue := generateValue(index)
			retrieved := db.Get(key)
			if retrieved == nil {
				fmt.Printf("Error: Key %s not found\n", key)
				mu.Lock()
				validationErrors++
				mu.Unlock()
				continue
			}
			retrievedValue, ok := retrieved.([]byte)
			if !ok || string(retrievedValue) != string(expectedValue) {
				fmt.Printf("Error: Value mismatch for key %s. Expected %s, Got %s\n", key, expectedValue, retrievedValue)
				mu.Lock()
				validationErrors++
				mu.Unlock()
				// panic("Validation error")
			}
		}
	}

	// Start workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Send record indices to the channel
	for i := 0; i < numRecords; i++ {
		recordCh <- i
	}
	close(recordCh)

	// Wait for all workers to finish
	wg.Wait()

	if validationErrors > 0 {
		return fmt.Errorf("validation failed with %d errors", validationErrors)
	}
	return nil
}

// Function to measure and print execution time
func measureExecutionTime(start time.Time, end time.Time, operation string, numRecords int) {
	duration := end.Sub(start)
	rate := float64(numRecords) / duration.Seconds()
	fmt.Printf("%s: %d records in %v (%.2f records/sec)\n",
		operation, numRecords, duration, rate)
}

func main() {
	// Initialize Mock Logger
	seed := time.Now().Format("2006-01-02-15-04-05")
	logger := logger.InitLogger(seed + "-dts")

	// Define Database Configuration
	config := database.Config{
		Host: "localhost",
	}

	// Initialize Mock TrueTime
	mockClock := truetime.NewTrueTime(logger)
	mockClock.Run()

	// Create Database Instance
	db := database.NewDatabase(logger, config)

	// Number of Workers (Adjust based on your CPU cores)
	workers := 100

	// Bulk Insert
	fmt.Printf("Starting bulk insert of %d records with %d workers...\n", NumRecords, workers)
	insertStart := time.Now()
	err := bulkInsert(db, NumRecords, workers)
	if err != nil {
		fmt.Printf("Bulk insert encountered an error: %v\n", err)
		return
	}
	insertEnd := time.Now()
	measureExecutionTime(insertStart, insertEnd, "Bulk Insert", NumRecords)

	// Bulk Retrieve and Validate
	fmt.Printf("Starting bulk retrieve and validation of %d records with %d workers...\n", NumRecords, workers)
	retrieveStart := time.Now()
	err = bulkRetrieveAndValidate(db, NumRecords, workers)
	if err != nil {
		fmt.Printf("Bulk retrieve and validation encountered an error: %v\n", err)
		return
	}
	retrieveEnd := time.Now()
	measureExecutionTime(retrieveStart, retrieveEnd, "Bulk Retrieve and Validation", NumRecords)

	fmt.Println("DTS completed successfully.")
}
