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
