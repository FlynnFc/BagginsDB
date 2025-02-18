package logger

import (
	"fmt"
	"log"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger *zap.Logger

// InitLogger initializes and returns a Zap logger with Lumberjack for log rotation.
// give it a name to use for the log file
func InitLogger(n string) *zap.Logger {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "logs"
	}
	path := fmt.Sprintf("%s/%s.log", nodeID, n)
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path,
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
		Compress:   true,
	})

	// Create a zap core that writes to lumberjack
	writeSyncer := zapcore.AddSync(w)
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		MessageKey:   "msg",
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // Use JSON for structured logging
		writeSyncer,                           // Write logs to lumberjack
		zapcore.DebugLevel,                    // Log level
	)

	logger := zap.New(core, zap.AddCaller())

	// Redirect the standard log package to use zap
	zapRedirect := logger.WithOptions(zap.AddCallerSkip(1)) // Skip one caller for correct log location
	zap.RedirectStdLog(zapRedirect)
	log.SetFlags(0) // Disable standard log timestamps (handled by zap)

	return logger
}

func InitWALLogger(n string) *zap.Logger {
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "logs"
	}
	path := fmt.Sprintf("%s/%s.log", nodeID, n)
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   path,
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     7, // days
		Compress:   true,
	})

	// Create a zap core that writes to lumberjack
	writeSyncer := zapcore.AddSync(w)
	encoderConfig := zapcore.EncoderConfig{
		TimeKey:      "time",
		LevelKey:     "level",
		MessageKey:   "msg",
		EncodeTime:   zapcore.ISO8601TimeEncoder,
		EncodeLevel:  zapcore.CapitalLevelEncoder,
		EncodeCaller: zapcore.ShortCallerEncoder,
	}

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig), // Use JSON for structured logging
		writeSyncer,                           // Write logs to lumberjack
		zapcore.DebugLevel,                    // Log level
	)

	logger := zap.New(core, zap.AddCaller())

	return logger
}
