package main

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/labstack/echo"
)

func main() {
	e := echo.New()

	cache, err := lazarette.Open("/tmp")
	if err != nil {
		log.Fatalf("unable to open lazarette: %s", err)
	}

	// e.GET("/subscribe/:prefix", handlers.Subscribe)
	e.GET("/key/:key", func(c echo.Context) error {
		key := c.Param("key")

		val, err := cache.Get(key)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.Blob(http.StatusOK, "application/json", val)
	})

	e.PUT("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
		val, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
		}

		err = cache.Put(key, val)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "updated "+key)
	})

	e.Logger.Fatal(e.Start(":7777"))
}
