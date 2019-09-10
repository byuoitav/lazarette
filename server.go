package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/byuoitav/lazarette/cache"
	"github.com/byuoitav/lazarette/lazarette"
	"github.com/labstack/echo"
)

func main() {
	e := echo.New()

	cash, err := lazarette.Open("/tmp")
	if err != nil {
		log.Fatalf("unable to open lazarette: %s", err)
	}

	// e.GET("/subscribe/:prefix", handlers.Subscribe)
	e.GET("/key/:key", func(c echo.Context) error {
		key := c.Param("key")

		val, err := cash.Get(key)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		c.Response().Header().Add("Last-Modified", val.Timestamp.Format(time.RFC1123))
		return c.Blob(http.StatusOK, "application/json", val.Data)
	})

	e.PUT("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
		bytes, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}

		val := &cache.Value{
			Timestamp: time.Now(),
			Data:      bytes,
		}

		err = cash.Set(key, val)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "updated "+key)
	})

	e.Logger.Fatal(e.Start(":7777"))
}
