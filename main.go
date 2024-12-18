package main

import (
	"github.com/flynnfc/bagginsdb/internal/database"
	"github.com/flynnfc/bagginsdb/logger"
	"go.uber.org/zap"
)

var log *zap.Logger

func init() {
	log = logger.InitLogger("main")
	log.Info("BAGGINSDB SPINNING UP")
}

func main() {
	db := database.NewDatabase(log, database.Config{Host: "localhost"})

	db.Put([]byte("key"), []byte("CHEESE"))

	value, ok := db.Get([]byte("key")).([]byte)
	if !ok {
		log.Error("Failed to convert value to []byte")
		return
	}

	log.Info("Value", zap.String("value", string(value)))
}
