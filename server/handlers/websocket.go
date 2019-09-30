package handlers

/*
var (
	upgrader = websocket.Upgrader{
		EnableCompression: true,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
)

// Subscribe .
func Subscribe(cc cache.Cache) echo.HandlerFunc {
	return func(c echo.Context) error {
		prefix := c.Param("prefix")

		ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
		if err != nil {
			return c.String(http.StatusBadRequest, "unable to upgrade connection: "+err.Error())
		}
		defer ws.Close()

		ch, unsub := cc.Subscribe(prefix)
		defer unsub()

		log.Printf("%v subscribed to %q", ws.RemoteAddr(), prefix)

		// create json marshaler
		json := jsonpb.Marshaler{OrigName: false}
		buf := &bytes.Buffer{}

		for {
			select {
			case kv := <-ch:
				err = json.Marshal(buf, &kv)
				if err != nil {
					buf.Reset()
					continue
				}

				err = ws.WriteMessage(websocket.TextMessage, buf.Bytes())
				if err != nil {
					buf.Reset()
					fmt.Printf("failed to write message: %s", err)
					return err
				}

				buf.Reset()
			}
		}
	}
}
*/
