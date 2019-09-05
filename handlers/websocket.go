package handlers

import (
	"fmt"

	"github.com/gorilla/websocket"
	"github.com/labstack/echo"
)

var (
	upgrader = websocket.Upgrader{
		EnableCompression: true,
	}
)

// Subscribe .
func Subscribe(c echo.Context) error {
	ws, err := upgrader.Upgrade(c.Response(), c.Request(), nil)
	if err != nil {
		return fmt.Errorf("unable to upgrade connection: %w", err)
	}
	defer ws.Close()

	for {
	}
}
