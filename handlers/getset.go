package handlers

/*

// GetKey .
func GetKey(cc cache.Cache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := c.Param("key")

		val, err := cc.Get(key)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		if val.GetTimestamp() != nil {
			if t, err := ptypes.Timestamp(val.GetTimestamp()); err != nil {
				c.Response().Header().Add("Last-Modified", t.Format(time.RFC1123))
			}
		}

		return c.Blob(http.StatusOK, "application/json", val.GetData())
	}
}

// SetKey .
func SetKey(cc cache.Cache) echo.HandlerFunc {
	return func(c echo.Context) error {
		key := c.Param("key")

		bytes, err := ioutil.ReadAll(c.Request().Body)
		if err != nil {
			return c.String(http.StatusBadRequest, err.Error())
		}

		val := cache.Value{
			Timestamp: ptypes.TimestampNow(),
			Data:      bytes,
		}

		err = cc.Set(key, val)
		if err != nil {
			return c.String(http.StatusInternalServerError, err.Error())
		}

		return c.String(http.StatusOK, "updated "+key)
	}
}
*/
