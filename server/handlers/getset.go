package handlers

import (
	"io/ioutil"
	"net/http"
	"time"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/golang/protobuf/ptypes"
	"github.com/labstack/echo"
)

// GetKey .
func GetKey(cache *lazarette.Cache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := &lazarette.Key{
			Key: c.Param("key"),
		}

		val, err := cache.Get(c.Request().Context(), key)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		if val.GetTimestamp() != nil {
			if t, err := ptypes.Timestamp(val.GetTimestamp()); err == nil {
				c.Response().Header().Add("Last-Modified", t.Format(time.RFC3339Nano))
			}
		}

		return c.Blob(http.StatusOK, "application/octet-stream", val.GetData())
	}
}

// SetKey .
func SetKey(cache *lazarette.Cache) echo.HandlerFunc {
	return func(c echo.Context) error {
		bytes, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}

		kv := &lazarette.KeyValue{
			Key: &lazarette.Key{
				Key: c.Param("key"),
			},
			Value: &lazarette.Value{
				Timestamp: ptypes.TimestampNow(),
				Data:      bytes,
			},
		}

		_, err = cache.Set(c.Request().Context(), kv)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "updated "+kv.GetKey().GetKey())
	}
}
