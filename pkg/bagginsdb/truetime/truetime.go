package truetime

import "time"

// Interval represents the time interval [Earliest, Latest] within which the true time is guaranteed to lie.
type Interval struct {
	Earliest time.Time
	Latest   time.Time
}

// TrueTime defines the interface for a trusted time service.
type TrueTime interface {
	// GetTime returns the current time along with the duration it took to fetch it.
	GetTime() (time.Time, time.Duration)

	// SetTime sets the system time to newTime.
	SetTime(newTime time.Time) error

	// NowInterval returns an interval within which the true time is guaranteed to lie.
	// This interval accounts for clock uncertainty.
	NowInterval() (Interval, error)

	// MaxOffset returns the maximum clock uncertainty.
	MaxOffset() time.Duration

	// WaitUntil blocks until the true time is guaranteed to be after the provided timestamp.
	WaitUntil(t time.Time) error
}
