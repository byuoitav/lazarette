package main

import (
	"io/ioutil"
	"log"
	"net/http"
	"time"

	"github.com/byuoitav/lazarette/cache"
	"github.com/byuoitav/lazarette/lazarette"
	"github.com/golang/protobuf/ptypes"
	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
)

var (
	upgrader = websocket.Upgrader{
		EnableCompression: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

func main() {
	e := echo.New()

	cash, err := lazarette.Open("/tmp")
	if err != nil {
		log.Fatalf("unable to open lazarette: %s", err)
	}

	/*
		e.GET("/subscribe/:prefix", func(c echo.Context) error {
			prefix := c.Param("prefix")
			if len(prefix) == 0 {
				log.Fatalf("wtf")
			}

			ch, unsub := cash.Subscribe(prefix)

			ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
			if err != nil {
				log.Printf("unable to upgrade connection: %w", err)
				return fmt.Errorf("unable to upgrade connection: %w", err)
			}
			defer ws.Close()

			ws.SetCloseHandler(func(code int, text string) error {
				fmt.Printf("close handler: %v, %v\n", code, text)
				unsub()
				return nil
			})

			log.Printf("%v subscribed to %q", c.Request().RemoteAddr, prefix)

			for {
				select {
				case kv := <-ch:
					bytes, err := kv.MarshalBinary()
					if err != nil {
						// TODO
						c.Logger().Error(err)
						continue
					}

					err = ws.WriteMessage(websocket.BinaryMessage, bytes)
					if err != nil {
						c.Logger().Error(err)
						return err
					}
				}
			}
			return nil
		})
	*/

	e.GET("/key/:key", func(c echo.Context) error {
		key := c.Param("key")

		val, err := cash.Get(key)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		if val.GetTimestamp() != nil {
			if t, err := ptypes.Timestamp(val.GetTimestamp()); err != nil {
				c.Response().Header().Add("Last-Modified", t.Format(time.RFC1123))
			}
		}

		return c.Blob(http.StatusOK, "application/json", val.GetData())
	})

	e.PUT("/key/:key", func(c echo.Context) error {
		key := c.Param("key")
		bytes, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}

		val := cache.Value{
			Timestamp: ptypes.TimestampNow(),
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
