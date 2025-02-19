package truetime

import (
	"log"
	"os/exec"
	"runtime"
	"time"

	"github.com/beevik/ntp"
)

// NTPTime is an implementation of the TrueTime interface using an NTP server.
type NTPTime struct {
	Server string
}

// NewNTPTime returns a new NTPTime instance.
// If the provided server is empty, it defaults to "time.google.com".
func NewNTPTime(server string) *NTPTime {
	if server == "" {
		server = "time.google.com"
	}
	return &NTPTime{
		Server: server,
	}
}

// GetTime retrieves the current time from the NTP server along with the round-trip duration.
// It uses the simple ntp.Time() helper which internally performs an NTP query.
func (n *NTPTime) GetTime() (time.Time, time.Duration) {
	start := time.Now()
	t, err := ntp.Time(n.Server)
	if err != nil {
		log.Printf("Error querying time from NTP server %s: %v", n.Server, err)
	}
	return t, time.Since(start)
}

// SetTime sets the system time to newTime using platform-specific commands.
func (n *NTPTime) SetTime(newTime time.Time) error {
	var err error
	switch runtime.GOOS {
	case "linux", "darwin":
		// Format time as "YYYY-MM-DD HH:MM:SS"
		timeStr := newTime.Format("2006-01-02 15:04:05")
		cmd := exec.Command("sudo", "date", "-s", timeStr)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("Failed to set time: %s, error: %v", output, err)
		}
	case "windows":
		// Format time as "HH:mm:ss" and date as "MM-dd-yyyy"
		timeStr := newTime.Format("15:04:05")
		dateStr := newTime.Format("01-02-2006")

		// Set the date
		cmdDate := exec.Command("cmd", "/C", "date", dateStr)
		output, err := cmdDate.CombinedOutput()
		if err != nil {
			log.Printf("Failed to set date: %s, error: %v", output, err)
		}

		// Set the time
		cmdTime := exec.Command("cmd", "/C", "time", timeStr)
		output, err = cmdTime.CombinedOutput()
		if err != nil {
			log.Printf("Failed to set time: %s, error: %v", output, err)
		}
	default:
		log.Printf("Unsupported platform: %s", runtime.GOOS)
	}
	return err
}

// NowInterval returns an interval within which the true time is guaranteed to lie.
// It performs an NTP query and estimates the uncertainty as half of the round-trip time (RTT).
func (n *NTPTime) NowInterval() (Interval, error) {
	resp, err := ntp.Query(n.Server)
	if err != nil {
		return Interval{}, err
	}
	// The corrected time is given by adding the estimated clock offset.
	trueTime := resp.Time.Add(resp.ClockOffset)
	// Use half the round-trip delay as the uncertainty.
	// More sophisticated algorithms can be used (e.g., NTP's intersection algorithm).
	// But for our case RTT/2 is a common simple heuristic.
	uncertainty := resp.RTT / 2

	return Interval{
		Earliest: trueTime.Add(-uncertainty),
		Latest:   trueTime.Add(uncertainty),
	}, nil
}

// MaxOffset returns the maximum clock uncertainty as half of the round-trip time from an NTP query.
func (n *NTPTime) MaxOffset() time.Duration {
	resp, err := ntp.Query(n.Server)
	if err != nil {
		log.Printf("Error querying time for MaxOffset from server %s: %v", n.Server, err)
		return 0
	}
	return resp.RTT / 2
}

// WaitUntil blocks until the true time interval is guaranteed to be after the provided timestamp t.
// It repeatedly queries the NTP server until the earliest time in the interval is not before t.
func (n *NTPTime) WaitUntil(t time.Time) error {
	for {
		interval, err := n.NowInterval()
		if err != nil {
			return err
		}
		if !interval.Earliest.Before(t) {
			// The earliest possible true time is at or after t.
			return nil
		}
		waitDuration := t.Sub(interval.Earliest)
		// Ensure we sleep for at least a minimal duration to avoid busy waiting.
		if waitDuration < 10*time.Millisecond {
			waitDuration = 10 * time.Millisecond
		}
		time.Sleep(waitDuration)
	}
}
