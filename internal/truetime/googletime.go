package truetime

import (
	"log"
	"os/exec"
	"runtime"
	"time"

	"github.com/beevik/ntp"
)

var ntpServer = "time.google.com"

func getTime() (time.Time, time.Duration) {
	start := time.Now()
	// Query the current time from the NTP server
	currentTime, err := ntp.Time(ntpServer)
	if err != nil {
		log.Printf("Error querying time from NTP server: %v", err)
	}
	return currentTime, time.Since(start)
}

func setSystemTime(newTime time.Time) error {
	var err error
	switch runtime.GOOS {
	case "linux", "darwin":
		// Format time as "YYYY-MM-DD HH:MM:SS"
		timeStr := newTime.Format("2006-01-02 15:04:05")
		cmd := exec.Command("sudo", "date", "-s", timeStr)
		output, err := cmd.CombinedOutput()
		if err != nil {
			log.Printf("failed to set time: %s, error: %v", output, err)
		}
	case "windows":
		// Format time as "HH:mm:ss" and date as "MM-dd-yyyy"
		timeStr := newTime.Format("15:04:05")
		dateStr := newTime.Format("01-02-2006")

		// Set the date
		cmdDate := exec.Command("cmd", "/C", "date", dateStr)
		output, err := cmdDate.CombinedOutput()
		if err != nil {
			log.Printf("failed to set date: %s, error: %v", output, err)
		}

		// Set the time
		cmdTime := exec.Command("cmd", "/C", "time", timeStr)
		output, err = cmdTime.CombinedOutput()
		if err != nil {
			log.Printf("failed to set time: %s, error: %v", output, err)
		}
	default:
		log.Printf("unsupported platform: %s", runtime.GOOS)
	}
	return err
}
