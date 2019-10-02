package main

import (
	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/server"
	"github.com/byuoitav/lazarette/store/memstore"
	"go.uber.org/zap"
)

func main() {
	store, err := memstore.NewStore()
	if err != nil {
		log.P.Fatal("failed to create store", zap.Error(err))
	}

	cache, err := lazarette.NewCache(store)
	if err != nil {
		log.P.Fatal("failed to create cache", zap.Error(err))
	}

	server := &server.Server{
		Cache: cache,
	}

	err = server.Serve(":7777", ":7778")
	if err != nil {
		log.P.Fatal("failed to serve", zap.Error(err))
	}
}
