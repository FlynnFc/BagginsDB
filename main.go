package main

import (
	"github.com/flynnfc/bagginsdb/logger"
	"github.com/flynnfc/bagginsdb/simulation"
	"go.uber.org/zap"
)

var log *zap.Logger

func init() {
	log = logger.InitLogger("main")
	log.Info("BAGGINSDB SPINNING UP")
}

func main() {
	simulation.Dts()
	simulation.Load()

}
