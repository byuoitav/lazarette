package lazarette

import (
	context "context"
	"errors"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store"
	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/byuoitav/lazarette/store/memstore"
	"github.com/byuoitav/lazarette/store/syncmapstore"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + "!@#$%^&*()-_=+;|/\\{}"

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func newCache(tb testing.TB, store store.Store) *Cache {
	cache, err := NewCache(store)
	if err != nil {
		tb.Fatalf("failed to start Cache: %v", err)
	}

	err = cache.Clean()
	if err != nil {
		tb.Fatalf("failed to clean Cache: %v", err)
	}

	return cache
}

func closeCache(tb testing.TB, s *Cache) {
	err := s.Close()
	if err != nil {
		tb.Fatalf("failed to close Cache: %v", err)
	}
}

func cleanCache(tb testing.TB, s *Cache) {
	err := s.Clean()
	if err != nil {
		tb.Fatalf("failed to clean Cache: %v", err)
	}
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

func setAndCheck(tb testing.TB, cache *Cache, kv *KeyValue) {
	_, err := cache.Set(context.Background(), kv)
	if err != nil {
		tb.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
	}

	nval, err := cache.Get(context.Background(), kv.GetKey())
	if err != nil {
		tb.Fatalf("failed to get %q: %v", kv.GetKey().GetKey(), err)
	}

	checkValueEqual(tb, kv.GetKey(), kv.GetValue(), nval)
}

func TestMain(m *testing.M) {
	log.Config.Level.SetLevel(zap.PanicLevel)
	os.Exit(m.Run())
}

/* TESTS */

func doCacheTest(t *testing.T, cache *Cache) {
	// testing it works
	t.Run("TestSetAndGet", SetAndGet(cache))
	cleanCache(t, cache)

	t.Run("TestSettingTheSameKey", SetAndGet(cache))
	cleanCache(t, cache)

	// testing concurrency
	t.Run("TestConcurrentSettingTheSameKey32Routines/8Times", ConcurrentSettingTheSameKey(cache, 32, 8))
	cleanCache(t, cache)

	t.Run("TestConcurrentSettingTheSameKey64Routines/16Times", ConcurrentSettingTheSameKey(cache, 64, 16))
	cleanCache(t, cache)

	t.Run("TestConcurrentSettingTheSameKey128Routines/16Times", ConcurrentSettingTheSameKey(cache, 128, 16))
	cleanCache(t, cache)

	t.Run("TestConcurrentSettingRandomKeys32Routines/8Times", ConcurrentSettingRandomKeys(cache, 32, 8))
	cleanCache(t, cache)

	t.Run("TestConcurrentSettingRandomKeys64Routines/16Times", ConcurrentSettingRandomKeys(cache, 64, 16))
	cleanCache(t, cache)

	t.Run("TestConcurrentSettingRandomKeys128Routines/16Times", ConcurrentSettingRandomKeys(cache, 128, 16))
	cleanCache(t, cache)

	closeCache(t, cache)
}

func TestMemStore(t *testing.T) {
	store, err := memstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create memstore: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}

	doCacheTest(t, cache)
}

func TestSyncMapStore(t *testing.T) {
	store, err := syncmapstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create syncmap store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}

	doCacheTest(t, cache)
}

func TestBoltStore(t *testing.T) {
	options := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(os.TempDir()+"/lazarette.bolt", 0666, options)
	if err != nil {
		t.Fatalf("failed to open bolt: %v", err)
	}

	store, err := boltstore.NewStore(db)
	if err != nil {
		t.Fatalf("failed to create bolt store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		t.Fatalf("failed to start cache: %v", err)
	}

	doCacheTest(t, cache)
}

func SetAndGet(cache *Cache) func(t *testing.T) {
	return func(t *testing.T) {
		kv := &KeyValue{
			Key:   randKey(t, 50),
			Value: randVal(t, 300),
		}

		setAndCheck(t, cache, kv)
	}
}

func SettingTheSameKey(cache *Cache) func(t *testing.T) {
	return func(t *testing.T) {
		kv := &KeyValue{
			Key:   randKey(t, 50),
			Value: randVal(t, 300),
		}

		for i := 0; i < 10; i++ {
			setAndCheck(t, cache, kv)
			kv.Value = randVal(t, 300)
		}
	}
}

func ConcurrentSettingTheSameKey(cache *Cache, routines, n int) func(t *testing.T) {
	return func(t *testing.T) {
		key := randKey(t, 50)

		wg := sync.WaitGroup{}
		wg.Add(routines)

		for i := 0; i < routines; i++ {
			go func() {
				defer wg.Done()

				for i := 0; i < n; i++ {
					time.Sleep(time.Duration(seededRand.Intn(500)) * time.Millisecond)
					kv := &KeyValue{
						Key:   key,
						Value: randVal(t, 300),
					}

					_, err := cache.Set(context.Background(), kv)
					if err != nil && !errors.Is(err, ErrNotNew) {
						t.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
					}
				}
			}()
		}

		wg.Wait()
	}
}

func ConcurrentSettingRandomKeys(cache *Cache, routines, n int) func(t *testing.T) {
	return func(t *testing.T) {
		wg := sync.WaitGroup{}
		wg.Add(routines)

		for i := 0; i < routines; i++ {
			go func() {
				defer wg.Done()

				for i := 0; i < n; i++ {
					time.Sleep(time.Duration(seededRand.Intn(500)) * time.Millisecond)
					kv := &KeyValue{
						Key:   randKey(t, 50),
						Value: randVal(t, 300),
					}

					_, err := cache.Set(context.Background(), kv)
					if err != nil && !errors.Is(err, ErrNotNew) {
						t.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey().GetKey(), err, kv.GetValue().GetData())
					}
				}
			}()
		}

		wg.Wait()
	}
}

/* BENCHMARKS */

func doBenchmarks(b *testing.B, cache *Cache) {
	// generate keys/values
	var keys []*Key
	var vals []*Value

	for i := 0; i < 10000000; i++ {
		keys = append(keys, randKey(b, 32))
	}

	for i := 0; i < 10000000; i++ {
		vals = append(vals, randVal(b, 512))
	}

	b.Run("BenchmarkUniqueKeys", BUniqueKeys(cache, keys, vals))
	cleanCache(b, cache)

	b.Run("BenchmarkUniqueVals", BUniqueVals(cache, keys, vals))
	cleanCache(b, cache)

	b.Run("BenchmarkUniqueKeysAndVals", BUniqueKeysAndVals(cache, keys, vals))
	cleanCache(b, cache)

	closeCache(b, cache)
}

func BenchmarkMemStore(b *testing.B) {
	store, err := memstore.NewStore()
	if err != nil {
		b.Fatalf("failed to create memstore: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		b.Fatalf("failed to start cache: %v", err)
	}

	doBenchmarks(b, cache)
}

func BenchmarkSyncMapStore(b *testing.B) {
	store, err := syncmapstore.NewStore()
	if err != nil {
		b.Fatalf("failed to create syncmap store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		b.Fatalf("failed to start cache: %v", err)
	}

	doBenchmarks(b, cache)
}

func BenchmarkBoltStore(b *testing.B) {
	options := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(os.TempDir()+"/lazarette.bolt", 0666, options)
	if err != nil {
		b.Fatalf("failed to open bolt: %v", err)
	}

	store, err := boltstore.NewStore(db)
	if err != nil {
		b.Fatalf("failed to create bolt store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		b.Fatalf("failed to start cache: %v", err)
	}

	doBenchmarks(b, cache)
}

func BUniqueKeys(cache *Cache, ks []*Key, vs []*Value) func(b *testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val := vs[0]
			val.Timestamp = ptypes.TimestampNow()

			_, err := cache.Set(context.Background(), &KeyValue{
				Key:   ks[i],
				Value: val,
			})
			if err != nil {
				b.Fatalf("failed to set: %v", err)
			}
		}
	}
}

func BUniqueVals(cache *Cache, ks []*Key, vs []*Value) func(b *testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val := vs[i]
			val.Timestamp = ptypes.TimestampNow()

			_, err := cache.Set(context.Background(), &KeyValue{
				Key:   ks[0],
				Value: val,
			})
			if err != nil {
				b.Fatalf("failed to set: %v", err)
			}
		}
	}
}

func BUniqueKeysAndVals(cache *Cache, ks []*Key, vs []*Value) func(b *testing.B) {
	return func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			val := vs[i]
			val.Timestamp = ptypes.TimestampNow()

			_, err := cache.Set(context.Background(), &KeyValue{
				Key:   ks[i],
				Value: val,
			})
			if err != nil {
				b.Fatalf("failed to set: %v", err)
			}
		}
	}
}
