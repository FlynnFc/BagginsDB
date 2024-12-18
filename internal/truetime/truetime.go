package truetime

import (
	"sync"
	"time"

	"go.uber.org/zap"
)

// Timestamp represents a point in time with uncertainty bounds.
type Timestamp struct {
	Earliest time.Time
	Latest   time.Time
}

// TrueTime provides time with bounded uncertainty.
type TrueTime struct {
	uncertainty time.Duration
	mu          sync.RWMutex
	time        Timestamp
	logger      *zap.Logger
	initialized bool
}

// NewTrueTime creates a new TrueTime instance with the given uncertainty and logger.
func NewTrueTime(logger *zap.Logger) *TrueTime {
	tt := &TrueTime{
		logger:      logger,
		initialized: false,
	}
	return tt
}

// Now returns the current time along with uncertainty bounds.
func (tt *TrueTime) Now() Timestamp {
	tt.mu.RLock()
	for !tt.initialized {
		tt.mu.RUnlock()
		time.Sleep(5 * time.Millisecond)
		tt.mu.RLock()
	}
	tt.mu.RUnlock()

	tt.mu.RLock()
	defer tt.mu.RUnlock()

	return tt.time

}

// Run is a method that adjusts the local clock based on the atomic clock api
func (tt *TrueTime) Run() {
	tt.logger.Info("Starting TrueTime")
	go func() {
		for {
			// Simulate checking the atomic clock API
			atomicTime := time.Now().Add(500 * time.Millisecond) // Simulated atomic time
			// Calculate the difference between the atomic clock and the local clock
			latency := 2 * time.Millisecond // Simulated latency
			predictedTime := atomicTime.Add(-latency)
			newTime := time.Now()
			tt.uncertainty = newTime.Sub(predictedTime)
			if tt.uncertainty.Abs() > 10*time.Millisecond {
				tt.logger.Warn("Clock is out of the acceptable sync range", zap.Duration("Clock is out of sync by (ms)", tt.uncertainty))
			}
			// Adjust the local clock
			tt.mu.Lock()
			tt.time.Earliest = newTime.Add(-tt.uncertainty)
			tt.time.Latest = newTime.Add(tt.uncertainty)
			tt.mu.Unlock()
			if !tt.initialized {
				tt.initialized = true
			}
			// Sync system clock to atomic clock

			// Log the adjustment
			tt.logger.Info("Adjusted local clock", zap.Duration("Clock was out of sync by (ms)", tt.uncertainty))

			// Sleep for 30 seconds before the next adjustment
			time.Sleep(30 * time.Second)
		}
	}()
}
