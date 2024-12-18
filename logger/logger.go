package logger

import (
	"fmt"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/natefinch/lumberjack.v2"
)

type Logger *zap.Logger

// InitLogger initializes and returns a Zap logger with Lumberjack for log rotation.
// give it a name to use for the log file
func InitLogger(n string) *zap.Logger {
	w := zapcore.AddSync(&lumberjack.Logger{
		Filename:   fmt.Sprintf("logs/%s.log", n),
		MaxSize:    10, // megabytes
		MaxBackups: 3,
		MaxAge:     28, // days
		Compress:   true,
	})

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "timestamp"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		w,
		zap.InfoLevel,
	)

	logger := zap.New(core, zap.AddCaller())
	return logger
}
