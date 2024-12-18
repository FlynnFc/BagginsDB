// realistic_scenario.go
package simulation

import (
	"context"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database" // Adjust import as needed
	"github.com/flynnfc/bagginsdb/internal/truetime"
	"github.com/flynnfc/bagginsdb/logger"
	"go.uber.org/zap"
)

// RealisticCassandraLoad simulates a workload pattern resembling Cassandra usage:
// - Mixed read/write ratio (e.g., 70% reads, 30% writes).
// - Values of varying sizes.
// - Random keys spread across a large keyspace.
// - Multiple rounds of testing over time, simulating long-lived operation.
//
// Tweak parameters to represent realistic loads.
type RealisticCassandraLoad struct {
	DB            *database.Database
	Logger        *zap.Logger
	NumRounds     int           // How many rounds of testing to run
	OpsPerRound   int           // Total operations per round
	ReadRatio     int           // Percentage of ops that are reads (e.g., 70)
	KeySpaceSize  int           // Number of unique keys in the keyspace
	MaxValueSize  int           // Max size of values in bytes
	Workers       int           // Concurrency level
	RoundInterval time.Duration // Pause between rounds
}

// randomBytes generates a random byte slice of the given size.
func randomBytes(size int) []byte {
	b := make([]byte, size)
	_, _ = rand.Read(b)
	return b
}

// randomKey picks a random key number from 0 to KeySpaceSize-1 and returns it as bytes.
func (r *RealisticCassandraLoad) randomKey() []byte {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(r.KeySpaceSize)))
	return []byte(fmt.Sprintf("user_%010d", n.Int64()))
}

// randomValue generates a random value of random length up to MaxValueSize.
func (r *RealisticCassandraLoad) randomValue() []byte {
	sizeN, _ := rand.Int(rand.Reader, big.NewInt(int64(r.MaxValueSize)))
	return randomBytes(int(sizeN.Int64()) + 1)
}

// Run simulates multiple rounds of mixed reads and writes.
func (r *RealisticCassandraLoad) Run() {
	for round := 1; round <= r.NumRounds; round++ {
		r.Logger.Info("Starting round", zap.Int("round", round))

		start := time.Now()

		// Calculate how many writes and reads this round
		writeRatio := 100 - r.ReadRatio
		writeOps := r.OpsPerRound * writeRatio / 100
		readOps := r.OpsPerRound - writeOps

		r.Logger.Info("Round configuration",
			zap.Int("ops_per_round", r.OpsPerRound),
			zap.Int("read_ops", readOps),
			zap.Int("write_ops", writeOps),
			zap.Int("workers", r.Workers),
		)

		err := r.runRound(readOps, writeOps)
		if err != nil {
			r.Logger.Info("Starting round", zap.Int("round", round))
		}

		end := time.Now()
		opsSec := float64(r.OpsPerRound) / end.Sub(start).Seconds()
		r.Logger.Info("Round completed",
			zap.Int("round", round),
			zap.Duration("duration", end.Sub(start)),
			zap.Float64("ops_sec", opsSec),
		)

		if round < r.NumRounds {
			r.Logger.Info("Pausing before next round", zap.Duration("interval", r.RoundInterval))
			time.Sleep(r.RoundInterval)
		}
	}

	r.Logger.Info("All rounds completed successfully")
}

// runRound executes readOps and writeOps in a mixed fashion.
func (r *RealisticCassandraLoad) runRound(readOps, writeOps int) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	var wg sync.WaitGroup
	opCh := make(chan bool, r.Workers*2) // true = write, false = read
	var writeCount int64
	var readCount int64
	var errorsCount int64

	worker := func() {
		defer wg.Done()
		for {
			select {
			case op, ok := <-opCh:
				if !ok {
					return
				}
				if op {
					// Write operation
					key := r.randomKey()
					val := r.randomValue()
					r.DB.Put(key, val)
					atomic.AddInt64(&writeCount, 1)
				} else {
					// Read operation
					key := r.randomKey()
					val := r.DB.Get(key)
					// In a real scenario, some keys may not exist or have expired.
					// Cassandra often sees partial misses. We won't treat this as an error,
					// but if desired, track misses:
					_ = val
					atomic.AddInt64(&readCount, 1)
				}
			case <-ctx.Done():
				return
			}
		}
	}

	// Start workers
	for i := 0; i < r.Workers; i++ {
		wg.Add(1)
		go worker()
	}

	// Queue ops (mix them randomly if desired)
	// For simplicity, we enqueue all writes first, then reads.
	// For a more realistic scenario, shuffle them.
	for i := 0; i < writeOps; i++ {
		opCh <- true
	}
	for i := 0; i < readOps; i++ {
		opCh <- false
	}
	close(opCh)

	// Wait for completion
	wg.Wait()

	// Check final counts
	wDone := atomic.LoadInt64(&writeCount)
	rDone := atomic.LoadInt64(&readCount)
	if wDone != int64(writeOps) || rDone != int64(readOps) {
		atomic.AddInt64(&errorsCount, 1)
		return fmt.Errorf("operation count mismatch (writes: %d/%d, reads: %d/%d)",
			wDone, writeOps, rDone, readOps)
	}
	if errorsCount > 0 {
		return fmt.Errorf("encountered %d errors during round", errorsCount)
	}

	return nil
}

func Load() {
	// Initialize logger, truetime, and database as before
	seed := time.Now().Format("2006-01-02-15-04-05")
	logger := logger.InitLogger(seed + "-realistic")
	mockClock := truetime.NewTrueTime(logger)
	mockClock.Run()

	config := database.Config{Host: "localhost"}
	db := database.NewDatabase(logger, config)

	// Configure the realistic scenario
	// Assume a Cassandra-like load:
	// - Large keyspace (1 million keys)
	// - Majority reads (70%) and some writes (30%)
	// - 100k ops per round, 5 rounds total
	// - Values can be up to 1KB
	// - Pause 5 seconds between rounds
	loadTest := RealisticCassandraLoad{
		DB:            db,
		Logger:        logger,
		NumRounds:     5,
		OpsPerRound:   100000,
		ReadRatio:     70,      // 70% reads, 30% writes
		KeySpaceSize:  1000000, // 1 million keys
		MaxValueSize:  1024,    // up to 1KB
		Workers:       200,     // concurrency level
		RoundInterval: 5 * time.Second,
	}

	loadTest.Run()
}
