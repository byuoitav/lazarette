package main

import (
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/byuoitav/lazarette/store/syncmapstore"
	"github.com/spf13/pflag"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func main() {
	var port int
	var logLevel int
	var persistPath string
	var persistInterval time.Duration

	pflag.IntVarP(&port, "port", "P", 8080, "port to run lazarette on")
	pflag.IntVarP(&logLevel, "log-level", "l", 2, "level of logging wanted. 1=DEBUG, 2=INFO, 3=WARN, 4=ERROR, 5=PANIC")
	pflag.StringVarP(&persistPath, "persist-path", "p", "", "path where persisted cache data should be stored")
	pflag.DurationVarP(&persistInterval, "persist-interval", "i", 5*time.Minute, "interval to persist data to --persist-path")
	pflag.Parse()

	setLog := func(level int) error {
		switch level {
		case 1:
			fmt.Printf("Setting log level to *debug*\n\n")
			log.Config.Level.SetLevel(zap.DebugLevel)
		case 2:
			fmt.Printf("Setting log level to *info*\n\n")
			log.Config.Level.SetLevel(zap.InfoLevel)
		case 3:
			fmt.Printf("Setting log level to *warn*\n\n")
			log.Config.Level.SetLevel(zap.WarnLevel)
		case 4:
			fmt.Printf("Setting log level to *error*\n\n")
			log.Config.Level.SetLevel(zap.ErrorLevel)
		case 5:
			fmt.Printf("Setting log level to *panic*\n\n")
			log.Config.Level.SetLevel(zap.PanicLevel)
		default:
			return errors.New("invalid log level: must be [1-4]")
		}

		return nil
	}

	// set the initial log level
	if err := setLog(logLevel); err != nil {
		log.P.Fatal("unable to set log level", zap.Error(err), zap.Int("got", logLevel))
	}

	// build the in-memory store
	store, err := syncmapstore.NewStore()
	if err != nil {
		log.P.Fatal("failed to create in memory store", zap.Error(err))
	}

	// create all of the options
	var opts []lazarette.Option

	// persist the cache or don't
	if len(persistPath) > 0 {
		log.P.Info("Persisting cache to " + persistPath)

		// TODO add logic to just get a persist directory, and add the database's ourselves
		// build the permanant store
		db, err := bolt.Open(persistPath, 0600, nil)
		if err != nil {
			log.P.Fatal("failed to open permanant store", zap.Error(err))
		}

		pStore, err := boltstore.NewStore(db)
		if err != nil {
			log.P.Fatal("failed to create persistent store", zap.Error(err))
		}

		opts = append(opts, lazarette.WithPersistent(pStore, persistInterval))
	} else {
		log.P.Warn("Running lazarette without a persistant backup")
	}

	// build the lazarette cache
	laz, err := lazarette.New(store, opts...)
	if err != nil {
		log.P.Fatal("failed to create cache", zap.Error(err))
	}

	// bind a listener to the given port
	addr := fmt.Sprintf(":%d", port)
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.P.Fatal("failed to create listener", zap.Error(err))
	}

	// create grpc server
	server := grpc.NewServer()
	lazarette.RegisterLazaretteServer(server, laz)

	log.P.Info("Starting server on " + lis.Addr().String())

	// serve!
	if err := server.Serve(lis); err != nil {
		log.P.Fatal("failed to start server", zap.Error(err))
	}
}
