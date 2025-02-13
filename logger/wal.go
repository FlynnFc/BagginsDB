package logger

import (
	"time"

	wal "github.com/aarthikrao/wal"
	"go.uber.org/zap"
)

func InitWAL(filePath string, log *zap.Logger) *wal.WriteAheadLog {

	wal, err := wal.NewWriteAheadLog(&wal.WALOptions{
		LogDir:            filePath,
		MaxLogSize:        40 * 1024 * 1024, // 40 MB (log rotation size)
		MaxSegments:       2,
		Log:               log,
		MaxWaitBeforeSync: 1 * time.Second,
		SyncMaxBytes:      1000,
	})
	if err != nil {
		panic(err)
	}

	return wal
}
