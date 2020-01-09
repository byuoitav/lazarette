package main

import (
	"net"
	"time"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/byuoitav/lazarette/store/memstore"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	// build the in-memory store
	store, err := memstore.NewStore()
	if err != nil {
		log.P.Fatal("failed to create in memory store", zap.Error(err))
	}

	// build the permanant store
	db, err := bolt.Open("/byu/lazarette.db", 0600, nil)
	if err != nil {
		log.P.Fatal("failed to open bolt", zap.Error(err))
	}

	pStore, err := boltstore.NewStore(db)
	if err != nil {
		log.P.Fatal("failed to create persistent store", zap.Error(err))
	}

	// TODO make interval configurable
	// build the cache
	laz, err := lazarette.New(store, lazarette.WithPersistent(pStore, 5*time.Minute))
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
