package main

import (
	"net"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/memstore"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	// build the in memory store
	store, err := memstore.NewStore()
	if err != nil {
		log.P.Fatal("failed to create in memory store", zap.Error(err))
	}

	// TODO build a persistant store

	// build the cache
	laz, err := lazarette.New(store)
	if err != nil {
		log.P.Fatal("failed to create cache", zap.Error(err))
	}

	// TODO get port from args
	lis, err := net.Listen("tcp", ":7777")
	if err != nil {
		log.P.Fatal("failed to create listener", zap.Error(err))
	}

	// create grpc server
	server := grpc.NewServer()
	lazarette.RegisterLazaretteServer(server, laz)

	// serve!
	if err := server.Serve(lis); err != nil {
		log.P.Fatal("failed to start server", zap.Error(err))
	}
}
