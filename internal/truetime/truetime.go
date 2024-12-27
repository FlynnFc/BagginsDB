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
		initialized: true,
	}
	return tt
}

// Now returns the current time along with uncertainty bounds.
func (tt *TrueTime) Now() Timestamp {
	return Timestamp{Earliest: time.Now(), Latest: time.Now()}
}

// Run is a method that adjusts the local clock based on the atomic clock api
func (tt *TrueTime) Run() {
	tt.logger.Info("Starting TrueTime")
	go func() {
		for {
			// Simulate checking the atomic clock API
			atomicTime, latency := getTime() // Simulated atomic time
			// Calculate the difference between the atomic clock and the local clock
			predictedTime := atomicTime.Add(-latency)
			newTime := time.Now()
			tt.uncertainty = newTime.Sub(predictedTime)

			// Adjust the local clock
			tt.mu.Lock()
			tt.time.Earliest = newTime.Add(-tt.uncertainty)
			tt.time.Latest = newTime.Add(tt.uncertainty)
			tt.mu.Unlock()
			if !tt.initialized {
				tt.initialized = true
			}
			// Sync system clock to atomic clock
			if tt.uncertainty.Abs() > 10*time.Millisecond {
				tt.logger.Warn("Clock is out of the acceptable sync range", zap.Duration("Clock is out of sync by (ms)", tt.uncertainty))
				err := setSystemTime(atomicTime)
				if err != nil {
					tt.logger.Error("Failed to adjust system clock", zap.Error(err))
				}
			}
			// Log the adjustment
			tt.logger.Info("Adjusted local clock", zap.Duration("Clock was out of sync by (ms)", tt.uncertainty))

			// Sleep for 30 seconds before the next adjustment
			time.Sleep(30 * time.Second)
		}
	}()
}
