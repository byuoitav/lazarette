package lazarette

import (
	context "context"
	"math/rand"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	grpc "google.golang.org/grpc"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + "!@#$%^&*()-_=+;|/\\{}"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func startServer(tb testing.TB, address string) (*Server, *grpc.Server) {
	laz, err := NewServer(os.TempDir())
	if err != nil {
		tb.Fatalf("failed to start server: %v", err)
	}

	err = laz.Clean()
	if err != nil {
		tb.Fatalf("failed to clean server: %v", err)
	}

	lis, err := net.Listen("tcp", ":7777")
	if err != nil {
		tb.Fatalf("failed to listen: %v", err)
	}

	grpcSrv := grpc.NewServer()
	RegisterLazaretteServer(grpcSrv, laz)

	go grpcSrv.Serve(lis)
	return laz, grpcSrv
}

func closeServer(tb testing.TB, s *Server, grpcSrv *grpc.Server) {
	err := s.Close()
	if err != nil {
		tb.Fatalf("failed to close server: %v", err)
	}

	grpcSrv.GracefulStop()
}

func cleanServer(tb testing.TB, s *Server) {
	err := s.Clean()
	if err != nil {
		tb.Fatalf("failed to clean server: %v", err)
	}
}

func newClient(tb testing.TB, address string) LazaretteClient {
	// conn, err := grpc.Dial(address, grpc.WithInsecure(), grpc.WithBlock())
	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		tb.Fatalf("failed to connect to server: %v", err)
	}

	return NewLazaretteClient(conn)
}

func randKey(tb testing.TB, maxLength int) *Key {
	for {
		b := make([]byte, seededRand.Intn(maxLength))
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}

		if len(string(b)) > 0 {
			return &Key{
				Key: string(b),
			}
		}
	}
}

func randVal(tb testing.TB, maxLength int) *Value {
	buf := make([]byte, seededRand.Intn(maxLength))
	_, err := seededRand.Read(buf)
	if err != nil {
		tb.Fatal(err)
	}

	return &Value{
		Timestamp: ptypes.TimestampNow(),
		Data:      buf,
	}
}

func checkValueEqual(tb testing.TB, key *Key, expected, actual *Value) {
	if !proto.Equal(expected, actual) {
		tb.Fatalf("values don't match for key %q:\n\texpected: %s\n\tactual: %s\n", key.GetKey(), expected.String(), actual.String())
	}
}

func setAndCheck(tb testing.TB, client LazaretteClient, kv *KeyValue) {
	_, err := client.Set(context.Background(), kv)
	if err != nil {
		tb.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
	}

	nval, err := client.Get(context.Background(), kv.GetKey())
	if err != nil {
		tb.Fatalf("failed to get %q: %v", kv.GetKey().GetKey(), err)
	}

	checkValueEqual(tb, kv.GetKey(), kv.GetValue(), nval)
}

func TestSetAndGet(t *testing.T) {
	server, grpcSrv := startServer(t, "localhost:7777")
	client := newClient(t, "localhost:7777")

	kv := &KeyValue{
		Key:   randKey(t, 50),
		Value: randVal(t, 300),
	}

	setAndCheck(t, client, kv)
	closeServer(t, server, grpcSrv)
}

func TestSettingTheSameKey(t *testing.T) {
	server, grpcSrv := startServer(t, "localhost:7777")
	client := newClient(t, "localhost:7777")

	kv := &KeyValue{
		Key:   randKey(t, 50),
		Value: randVal(t, 300),
	}

	for i := 0; i < 10; i++ {
		setAndCheck(t, client, kv)
		kv.Value = randVal(t, 300)
	}

	closeServer(t, server, grpcSrv)
}

func TestConcurrentSettingTheSameKey(t *testing.T) {
	server, grpcSrv := startServer(t, "localhost:7777")

	kv := &KeyValue{
		Key:   randKey(t, 50),
		Value: randVal(t, 300),
	}

	wg := sync.WaitGroup{}
	test := func(tt *testing.T, n int) {
		go func() {
			defer wg.Done()
			client := newClient(tt, "localhost:7777")

			for i := 0; i < n; i++ {
				time.Sleep(time.Duration(seededRand.Intn(500)) * time.Millisecond)
				kv.GetValue().Timestamp = ptypes.TimestampNow()

				_, err := client.Set(context.Background(), kv, grpc.WaitForReady(true))
				if err != nil && !strings.Contains(err.Error(), ErrNotNew.Error()) {
					tt.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
				}
			}
		}()
	}

	t.Run("32Routines", func(tt *testing.T) {
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go test(tt, 8)
		}

		wg.Wait()
	})

	cleanServer(t, server)

	t.Run("64Routines", func(tt *testing.T) {
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go test(tt, 16)
		}

		wg.Wait()
	})

	cleanServer(t, server)

	t.Run("128Routines", func(tt *testing.T) {
		for i := 0; i < 128; i++ {
			wg.Add(1)
			go test(tt, 16)
		}

		wg.Wait()
	})

	closeServer(t, server, grpcSrv)
}

func TestConcurrentSettingRandomKeys(t *testing.T) {
	server, grpcSrv := startServer(t, "localhost:7777")

	wg := sync.WaitGroup{}
	test := func(tt *testing.T, n int) {
		go func() {
			defer wg.Done()
			client := newClient(t, "localhost:7777")

			for i := 0; i < n; i++ {
				time.Sleep(time.Duration(seededRand.Intn(500)) * time.Millisecond)

				kv := &KeyValue{
					Key:   randKey(tt, 50),
					Value: randVal(tt, 300),
				}

				_, err := client.Set(context.Background(), kv, grpc.WaitForReady(true))
				if err != nil && !strings.Contains(err.Error(), ErrNotNew.Error()) {
					tt.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
				}
			}
		}()
	}

	t.Run("32Routines", func(tt *testing.T) {
		for i := 0; i < 32; i++ {
			wg.Add(1)
			go test(tt, 8)
		}

		wg.Wait()
	})

	cleanServer(t, server)

	t.Run("64Routines", func(tt *testing.T) {
		for i := 0; i < 64; i++ {
			wg.Add(1)
			go test(tt, 16)
		}

		wg.Wait()
	})

	cleanServer(t, server)

	t.Run("128Routines", func(tt *testing.T) {
		for i := 0; i < 128; i++ {
			wg.Add(1)
			go test(tt, 16)
		}

		wg.Wait()
	})

	closeServer(t, server, grpcSrv)
}

func BenchmarkSet(b *testing.B) {
	server, grpcSrv := startServer(b, "localhost:7777")
	client := newClient(b, "localhost:7777")

	// generate keys/values
	var keys []*Key
	var vals []*Value

	for i := 0; i < 1000; i++ {
		keys = append(keys, randKey(b, 50))
	}

	for i := 0; i < 1000; i++ {
		vals = append(vals, randVal(b, 300))
	}

	b.Run("UniqueKeys", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			val := vals[0]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, client, &KeyValue{
				Key:   keys[i],
				Value: val,
			})
		}
	})

	b.Run("UniqueVals", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			val := vals[i]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, client, &KeyValue{
				Key:   keys[0],
				Value: val,
			})
		}
	})

	b.Run("UniqueKeysAndVals", func(bb *testing.B) {
		for i := 0; i < bb.N; i++ {
			val := vals[i]
			val.Timestamp = ptypes.TimestampNow()

			setAndCheck(bb, client, &KeyValue{
				Key:   keys[i],
				Value: val,
			})
		}
	})

	closeServer(b, server, grpcSrv)
}
