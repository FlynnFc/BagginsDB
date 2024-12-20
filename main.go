package main

import (
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/flynnfc/bagginsdb/simulation"
)

func main() {
	go func() {
		// Start the pprof server on port 6060
		fmt.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	simulation.Dts()
	simulation.Load()

	// Prevent the program from exiting immediately (keeping it open)
	fmt.Println("Sleeping for 10 minutes to allow profiling...")
	time.Sleep(10 * time.Minute) // Adjust as necessary
}
