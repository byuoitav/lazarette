package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/syncmapstore"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

func newCache(tb testing.TB) *lazarette.Cache {
	store, err := syncmapstore.NewStore()
	if err != nil {
		tb.Fatalf("failed to create store: %v", err)
	}

	cache, err := lazarette.NewCache(store)
	if err != nil {
		tb.Fatalf("failed to create cache: %v", err)
	}

	// make sure it's clean
	err = cache.Clean()
	if err != nil {
		tb.Fatalf("failed to clean cache: %v", err)
	}

	return cache
}

func startServer(cache *lazarette.Cache, grpcAddr, httpAddr string) *Server {
	server := &Server{
		Cache: cache,
	}

	go server.Serve(grpcAddr, httpAddr)
	return server
}

func newGRPCClient(tb testing.TB, address string) lazarette.LazaretteClient {
	conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock(), grpc.WithTimeout(2*time.Second))
	if err != nil {
		tb.Fatalf("failed to connect to server: %v", err)
	}

	return lazarette.NewLazaretteClient(conn)
}

func checkValueEqual(tb testing.TB, key *lazarette.Key, expected, actual *lazarette.Value) {
	if !proto.Equal(expected, actual) {
		tb.Fatalf("values don't match for key %q:\n\texpected: %s\n\tactual: %s\n", key.GetKey(), expected.String(), actual.String())
	}
}

func TestMain(m *testing.M) {
	log.Config.Level.SetLevel(zap.PanicLevel)
	os.Exit(m.Run())
}

func TestGRPCServer(t *testing.T) {
	ctx := context.Background()

	server := startServer(newCache(t), ":7777", "")
	client := newGRPCClient(t, "localhost:7777")
	defer server.Stop(ctx)

	kv := &lazarette.KeyValue{
		Key: &lazarette.Key{
			Key: "ITB-1101-CP1",
		},
		Value: &lazarette.Value{
			Timestamp: ptypes.TimestampNow(),
			Data:      []byte(`{"key": "value"}`),
		},
	}

	t.Run("SetAndGet", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		_, err := client.Set(ctx, kv)
		if err != nil {
			t.Fatalf("failed to set %q: %v", kv.GetKey().GetKey(), err)
		}

		nval, err := client.Get(ctx, kv.GetKey())
		if err != nil {
			t.Fatalf("failed to get %q: %v", kv.GetKey().GetKey(), err)
		}

		checkValueEqual(t, kv.GetKey(), kv.GetValue(), nval)
	})
}
