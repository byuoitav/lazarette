package main

import (
	"github.com/byuoitav/lazarette/handlers"
	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/labstack/echo"
	"go.uber.org/zap"
)

func main() {
	e := echo.New()

	cache, err := lazarette.Open("/tmp")
	if err != nil {
		log.Fatal("unable to open lazarette: ", zap.Error(err))
	}

	// websocket endpoint
	e.GET("/subscribe/:prefix", handlers.Subscribe(cache))

	e.GET("/key/:key", handlers.GetKey(cache))
	e.PUT("/key/:key", handlers.SetKey(cache))

	err = e.Start(":7777")
	if err != nil {
		log.Fatal("unable to start server: ", zap.Error(err))
	}
}
