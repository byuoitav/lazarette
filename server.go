package main

import (
	"net"
	"net/http"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/memstore"
	"github.com/labstack/echo"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	lis, err := net.Listen("tcp", ":7777")
	if err != nil {
		log.P.Fatal("failed to listen", zap.Error(err))
	}

	store, err := memstore.NewStore()
	if err != nil {
		log.P.Fatal("failed to create store", zap.Error(err))
	}

	cache, err := lazarette.NewCache(store)
	if err != nil {
		log.P.Fatal("failed to create cache", zap.Error(err))
	}

	m := cmux.New(lis)
	grpcLis := m.Match(cmux.HTTP2HeaderField("content-type", "application/grpc"))
	httpLis := m.Match(cmux.Any())

	grpcSrv := grpc.NewServer()
	lazarette.RegisterLazaretteServer(grpcSrv, cache)

	e := echo.New()
	e.Listener = httpLis

	e.GET("/hello", func(c echo.Context) error {
		return c.HTML(http.StatusOK, "<html><script>window.onload = alert('hello!')</script></html>")
	})

	go grpcSrv.Serve(grpcLis)
	go e.Start("")

	log.P.Info("Started server", zap.String("address", lis.Addr().String()))
	err = m.Serve()
	if err != nil {
		log.P.Fatal("failed to start server", zap.Error(err))
	}
}
