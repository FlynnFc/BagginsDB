package logger

import (
	"github.com/tidwall/wal"
)

type WAL struct {
	Log   *wal.Log
	Batch *wal.Batch
	Index uint64
}

func InitWAL(filePath string) WAL {
	// open a new log file
	log, err := wal.Open(filePath, nil)
	if err != nil {
		panic(err)
	}
	batch := new(wal.Batch)

	return WAL{Log: log, Batch: batch}
}
