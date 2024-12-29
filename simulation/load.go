// realistic_scenario.go
package simulation

import (
	"context"
	"crypto/rand"
	"encoding/csv"
	"fmt"
	"math/big"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/flynnfc/bagginsdb/internal/database" // Adjust import as needed
	"github.com/flynnfc/bagginsdb/logger"
	"go.uber.org/zap"
)

// --------------------------------------------------------------------------------------
// Stats & CSV Helpers
// --------------------------------------------------------------------------------------

// Stats holds latency measurements for a set of operations
type Stats struct {
	latencies []time.Duration
}

// NewStats creates a Stats object
func NewStats() *Stats {
	return &Stats{
		latencies: make([]time.Duration, 0, 1000),
	}
}

// Record adds one measurement (the latency) to the stats
func (s *Stats) Record(d time.Duration) {
	s.latencies = append(s.latencies, d)
}

// Len returns how many measurements we have
func (s *Stats) Len() int {
	return len(s.latencies)
}

// Compute calculates p50, p95, p99, plus overall ops/sec if you provide totalOps & totalTime.
// p50, p95, p99 are returned as time.Duration, but we'll convert them to microseconds below.
func (s *Stats) Compute(totalOps int, totalTime time.Duration) (p50, p95, p99 time.Duration, opsSec float64) {
	n := len(s.latencies)
	if n == 0 {
		return 0, 0, 0, 0
	}

	sort.Slice(s.latencies, func(i, j int) bool {
		return s.latencies[i] < s.latencies[j]
	})

	percentileIndex := func(p float64) int {
		if p <= 0 {
			return 0
		}
		idx := int(float64(n)*p) - 1
		if idx < 0 {
			return 0
		}
		if idx >= n {
			return n - 1
		}
		return idx
	}

	p50 = s.latencies[percentileIndex(0.50)]
	p95 = s.latencies[percentileIndex(0.95)]
	p99 = s.latencies[percentileIndex(0.99)]

	opsSec = float64(totalOps) / totalTime.Seconds()
	return p50, p95, p99, opsSec
}

// ResultRecord holds the stats we want to log for each test run/round.
type ResultRecord struct {
	TestName  string
	RunNumber int
	Round     int
	OpsCount  int
	OpsSec    float64
	P50Us     float64 // p50 in microseconds
	P95Us     float64 // p95 in microseconds
	P99Us     float64 // p99 in microseconds
	TotalTime time.Duration
}

// WriteCSV writes a list of ResultRecords to a CSV file.
func WriteCSV(filename string, records []ResultRecord) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	defer w.Flush()

	// Write header
	header := []string{
		"test_name", "run_number", "round",
		"ops_count", "ops_sec",
		"p50_us", "p95_us", "p99_us",
		"total_time_ms",
	}
	if err := w.Write(header); err != nil {
		return err
	}

	// Write each record
	for _, rec := range records {
		row := []string{
			rec.TestName,
			strconv.Itoa(rec.RunNumber),
			strconv.Itoa(rec.Round),
			strconv.Itoa(rec.OpsCount),
			fmt.Sprintf("%.2f", rec.OpsSec),
			fmt.Sprintf("%.2f", rec.P50Us),
			fmt.Sprintf("%.2f", rec.P95Us),
			fmt.Sprintf("%.2f", rec.P99Us),
			fmt.Sprintf("%d", rec.TotalTime.Milliseconds()),
		}
		if err := w.Write(row); err != nil {
			return err
		}
	}
	return nil
}

// --------------------------------------------------------------------------------------
// Original Realistic Scenario
// --------------------------------------------------------------------------------------

var (
	defaultClustering = [][]byte{}
	defaultColumnName = []byte("data")
)

// RealisticCassandraLoad simulates a workload pattern resembling Cassandra usage
type RealisticCassandraLoad struct {
	DB            *database.Database
	Logger        *zap.Logger
	NumRounds     int           // How many rounds of testing to run
	OpsPerRound   int           // Total operations per round
	ReadRatio     int           // % of ops that are reads (e.g., 70)
	KeySpaceSize  int           // Number of unique keys in the keyspace
	MaxValueSize  int           // Max size of values in bytes
	Workers       int           // Concurrency level
	RoundInterval time.Duration // Pause between rounds

	roundStart time.Time // track round start time
	roundEnd   time.Time // track round end time
}

// randomBytes generates a random byte slice of the given size
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

// randomValue generates a random value of random length up to MaxValueSize
func (r *RealisticCassandraLoad) randomValue() []byte {
	sizeN, _ := rand.Int(rand.Reader, big.NewInt(int64(r.MaxValueSize)))
	return randomBytes(int(sizeN.Int64()) + 1)
}

// Run simulates multiple rounds of mixed reads and writes, capturing stats each round
func (r *RealisticCassandraLoad) Run(runNumber int) ([]ResultRecord, error) {
	var roundResults []ResultRecord

	for round := 1; round <= r.NumRounds; round++ {
		r.Logger.Info("Starting round", zap.Int("round", round))
		r.roundStart = time.Now()

		// Calculate how many writes vs. reads
		writeRatio := 100 - r.ReadRatio
		writeOps := r.OpsPerRound * writeRatio / 100
		readOps := r.OpsPerRound - writeOps

		r.Logger.Info("Round configuration",
			zap.Int("ops_per_round", r.OpsPerRound),
			zap.Int("read_ops", readOps),
			zap.Int("write_ops", writeOps),
			zap.Int("workers", r.Workers),
		)

		// Run one round of read/write mix
		stats, err := r.runRound(readOps, writeOps)
		if err != nil {
			r.Logger.Error("Error during round", zap.Int("round", round), zap.Error(err))
			return roundResults, err
		}

		r.roundEnd = time.Now()
		duration := r.roundEnd.Sub(r.roundStart)

		// Summarize round stats
		totalOps := readOps + writeOps
		p50, p95, p99, opsSec := stats.Compute(totalOps, duration)

		// Convert durations to microseconds
		p50us := float64(p50.Microseconds())
		p95us := float64(p95.Microseconds())
		p99us := float64(p99.Microseconds())

		r.Logger.Info("Round completed",
			zap.Int("round", round),
			zap.Duration("duration", duration),
			zap.Float64("ops_sec", opsSec),
			zap.Float64("p50_us", p50us),
			zap.Float64("p95_us", p95us),
			zap.Float64("p99_us", p99us),
		)

		// Save results to slice
		roundResults = append(roundResults, ResultRecord{
			TestName:  "RealisticCassandraLoad",
			RunNumber: runNumber,
			Round:     round,
			OpsCount:  totalOps,
			OpsSec:    opsSec,
			P50Us:     p50us,
			P95Us:     p95us,
			P99Us:     p99us,
			TotalTime: duration,
		})

		// Pause before next round (unless it's the last)
		if round < r.NumRounds {
			r.Logger.Info("Pausing before next round", zap.Duration("interval", r.RoundInterval))
			time.Sleep(r.RoundInterval)
		}
	}

	r.Logger.Info("All rounds completed successfully")
	return roundResults, nil
}

// runRound executes readOps and writeOps in a mixed fashion, returning Stats for latencies
func (r *RealisticCassandraLoad) runRound(readOps, writeOps int) (*Stats, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	stats := NewStats()

	var wg sync.WaitGroup
	opCh := make(chan bool, r.Workers*2) // true=write, false=read
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
				start := time.Now()
				if op {
					// WRITE
					key := r.randomKey()
					val := r.randomValue()
					r.DB.Put(key, defaultClustering, defaultColumnName, val)
					atomic.AddInt64(&writeCount, 1)
				} else {
					// READ
					key := r.randomKey()
					val := r.DB.Get(key, defaultClustering, defaultColumnName)
					_ = val // ignoring nil checks, as partial misses can be normal
					atomic.AddInt64(&readCount, 1)
				}
				stats.Record(time.Since(start))

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

	// Enqueue operations (basic approach: writeOps first, then readOps)
	for i := 0; i < writeOps; i++ {
		opCh <- true
	}
	for i := 0; i < readOps; i++ {
		opCh <- false
	}
	close(opCh)

	// Wait for completion
	wg.Wait()

	wDone := atomic.LoadInt64(&writeCount)
	rDone := atomic.LoadInt64(&readCount)
	if wDone != int64(writeOps) || rDone != int64(readOps) {
		atomic.AddInt64(&errorsCount, 1)
		return stats, fmt.Errorf("op mismatch (writes: %d/%d, reads: %d/%d)",
			wDone, writeOps, rDone, readOps)
	}
	if errorsCount > 0 {
		return stats, fmt.Errorf("encountered %d errors during round", errorsCount)
	}

	return stats, nil
}

// --------------------------------------------------------------------------------------
// Top-Level Load Function: runs the RealisticCassandraLoad 3 times with cleanup & CSV output
// --------------------------------------------------------------------------------------

// Load sets up the environment, runs the RealisticCassandraLoad scenario multiple times, writes CSV
func Load() {
	var allResults []ResultRecord

	for runNumber := 1; runNumber <= 3; runNumber++ {
		fmt.Printf("=== RealisticCassandraLoad: Run %d/3 ===\n", runNumber)

		// (1) Cleanup / re-init data between runs
		cleanupDatabase() // Implement this function in your codebase

		// (2) Initialize logger and DB
		seed := time.Now().Format("2006-01-02-15-04-05")
		log := logger.InitLogger(seed + fmt.Sprintf("-realistic-run%d", runNumber))
		config := database.Config{Host: "localhost"}
		db := database.NewDatabase(log, config)

		// (3) Configure our scenario
		loadTest := RealisticCassandraLoad{
			DB:            db,
			Logger:        log,
			NumRounds:     3,       // e.g., 3 rounds
			OpsPerRound:   1000000, // e.g., 50k ops/round
			ReadRatio:     50,      // 70% reads, 30% writes
			KeySpaceSize:  1000000, // ~1 million keys
			MaxValueSize:  1024,    // up to 1KB
			Workers:       200,     // concurrency level
			RoundInterval: 5 * time.Second,
		}

		// (4) Run the scenario
		start := time.Now()
		roundRecords, err := loadTest.Run(runNumber)
		end := time.Now()
		if err != nil {
			log.Error("Realistic load scenario error", zap.Error(err))
			db.Close()
			continue
		}

		// Summarize entire run
		totalOpsAcrossAllRounds := loadTest.NumRounds * loadTest.OpsPerRound
		totalDuration := end.Sub(start)
		totalOpsSec := float64(totalOpsAcrossAllRounds) / totalDuration.Seconds()
		log.Info("Entire scenario run completed",
			zap.Float64("total_ops_sec", totalOpsSec),
			zap.Duration("total_duration", totalDuration),
		)

		// (5) Append round-by-round records
		allResults = append(allResults, roundRecords...)

		db.Close()
	}

	// (6) Finally, write out a CSV for your 3 runs (and each round within them)
	csvFileName := fmt.Sprintf("load-results-%s.csv", time.Now().Format("2006-01-02-15-04-05"))
	err := WriteCSV(csvFileName, allResults)
	if err != nil {
		fmt.Printf("Error writing CSV: %v\n", err)
	} else {
		fmt.Printf("Results written to %s\n", csvFileName)
	}

	fmt.Println("=== Realistic Cassandra Load Tests Completed ===")
}

// cleanupDatabase is a placeholder; implement however you wish.
func cleanupDatabase() {
	// e.g., drop data directories, or call db.TruncateAll() on a fresh DB, etc.
}
