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

func newMemCache(tb testing.TB) *Cache {
	store, err := memstore.NewStore()
	if err != nil {
		tb.Fatalf("failed to create memstore: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		tb.Fatalf("failed to start cache: %v", err)
	}

	err = cache.Clean()
	if err != nil {
		tb.Fatalf("failed to clean cache: %s", err)
	}

	return cache
}

func newSyncMapCache(tb testing.TB) *Cache {
	store, err := syncmapstore.NewStore()
	if err != nil {
		tb.Fatalf("failed to create syncmap store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		tb.Fatalf("failed to start cache: %v", err)
	}

	err = cache.Clean()
	if err != nil {
		tb.Fatalf("failed to clean cache: %s", err)
	}

	return cache
}

func newBoltCache(tb testing.TB) *Cache {
	options := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(os.TempDir()+"/lazarette.bolt", 0666, options)
	if err != nil {
		tb.Fatalf("failed to open bolt: %v", err)
	}

	store, err := boltstore.NewStore(db)
	if err != nil {
		tb.Fatalf("failed to create bolt store: %v", err)
	}

	cache, err := NewCache(store)
	if err != nil {
		tb.Fatalf("failed to start cache: %v", err)
	}

	err = cache.Clean()
	if err != nil {
		tb.Fatalf("failed to clean cache: %s", err)
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

	// ok with this long of a delay
	time.Sleep(10 * time.Millisecond)

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
	t.Helper()

	// testing it works
	t.Run("TestSetAndGet", SetAndGet(cache))
	cleanCache(t, cache)

	t.Run("TestSettingTheSameKey", SettingTheSameKey(cache))
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

	t.Run("TestSubscriptionChanMatch", SubscriptionChanMatchTest(cache))
	cleanCache(t, cache)

	t.Run("TestSubscriptionChanNoMatch", SubscriptionChanNoMatchTest(cache))
	cleanCache(t, cache)

	t.Run("TestUnsubscribe", UnsubscribeTest(cache))
	cleanCache(t, cache)

	closeCache(t, cache)
}

func TestMemStore(t *testing.T) {
	doCacheTest(t, newMemCache(t))
}

func TestSyncMapStore(t *testing.T) {
	doCacheTest(t, newSyncMapCache(t))
}

func TestBoltStore(t *testing.T) {
	doCacheTest(t, newBoltCache(t))
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

		for r := 0; r < routines; r++ {
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

		for r := 0; r < routines; r++ {
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

func SubscriptionChanMatchTest(cache *Cache) func(t *testing.T) {
	return func(t *testing.T) {
		ch, unsub := cache.SubscribeChan("ITB")
		defer unsub()

		kv := &KeyValue{
			Key: &Key{
				Key: "ITB-1101-CP1",
			},
			Value: &Value{
				Timestamp: ptypes.TimestampNow(),
				Data:      []byte(`{"test": "value"}`),
			},
		}

		_, err := cache.Set(context.Background(), kv)
		if err != nil {
			t.Fatalf("unable to set key: %s", err)
		}

		if len(ch) != 1 {
			t.Fatalf("channel didn't get new value")
		}

		checkValueEqual(t, kv.GetKey(), kv.GetValue(), (<-ch).GetValue())
	}
}

func SubscriptionChanNoMatchTest(cache *Cache) func(*testing.T) {
	return func(t *testing.T) {
		kv := &KeyValue{
			Key: &Key{
				Key: "ITC-1101-CP1", // prefix doesn't match
			},
			Value: &Value{
				Timestamp: ptypes.TimestampNow(),
				Data:      []byte(`{"test": "value"}`),
			},
		}

		ch, unsub := cache.SubscribeChan("ITB")
		defer unsub()

		_, err := cache.Set(context.Background(), kv)
		if err != nil {
			t.Fatalf("unable to set key: %s", err)
		}

		if len(ch) > 0 {
			t.Fatalf("channel received value when prefix didn't match")
		}
	}
}

func UnsubscribeTest(cache *Cache) func(*testing.T) {
	return func(t *testing.T) {
		kv := &KeyValue{
			Key: &Key{
				Key: "ITB-1101-CP1",
			},
			Value: &Value{
				Timestamp: ptypes.TimestampNow(),
				Data:      []byte(`{"test": "value"}`),
			},
		}

		ch, unsub := cache.SubscribeChan("ITB")
		unsub()

		_, err := cache.Set(context.Background(), kv)
		if err != nil {
			t.Fatalf("unable to set key: %s", err)
		}

		if len(ch) > 0 {
			t.Fatalf("channel got a new value after unsubscribe")
		}
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

	b.Run("BenchmarkConcurrentUniqueKeys16Routines", BConcurrentUniqueKeys(cache, keys, vals, 16))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueKeys32Routines", BConcurrentUniqueKeys(cache, keys, vals, 32))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueKeys64Routines", BConcurrentUniqueKeys(cache, keys, vals, 64))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueVals16Routines", BConcurrentUniqueVals(cache, keys, vals, 16))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueVals32Routines", BConcurrentUniqueVals(cache, keys, vals, 32))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueVals64Routines", BConcurrentUniqueVals(cache, keys, vals, 64))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueKeysAndVals16Routines", BConcurrentUniqueKeysAndVals(cache, keys, vals, 16))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueKeysAndVals32Routines", BConcurrentUniqueKeysAndVals(cache, keys, vals, 32))
	cleanCache(b, cache)

	b.Run("BenchmarkConcurrentUniqueKeysAndVals64Routines", BConcurrentUniqueKeysAndVals(cache, keys, vals, 64))
	cleanCache(b, cache)

	closeCache(b, cache)
}

func BenchmarkMemStore(b *testing.B) {
	doBenchmarks(b, newMemCache(b))
}

func BenchmarkSyncMapStore(b *testing.B) {
	doBenchmarks(b, newSyncMapCache(b))
}

func BenchmarkBoltStore(b *testing.B) {
	doBenchmarks(b, newBoltCache(b))
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

func BConcurrentUniqueKeys(cache *Cache, ks []*Key, vs []*Value, routines int) func(b *testing.B) {
	return func(b *testing.B) {
		wg := sync.WaitGroup{}
		wg.Add(routines)

		for r := 0; r < routines; r++ {
			go func() {
				defer wg.Done()

				for i := 0; i < b.N; i++ {
					val := vs[0]
					val.Timestamp = ptypes.TimestampNow()

					_, err := cache.Set(context.Background(), &KeyValue{
						Key:   ks[i],
						Value: val,
					})
					if err != nil && !errors.Is(err, ErrNotNew) {
						b.Fatalf("failed to set: %v", err)
					}
				}
			}()
		}

		wg.Wait()
	}
}

func BConcurrentUniqueVals(cache *Cache, ks []*Key, vs []*Value, routines int) func(b *testing.B) {
	return func(b *testing.B) {
		wg := sync.WaitGroup{}
		wg.Add(routines)

		for r := 0; r < routines; r++ {
			go func() {
				defer wg.Done()

				for i := 0; i < b.N; i++ {
					val := vs[i]
					val.Timestamp = ptypes.TimestampNow()

					_, err := cache.Set(context.Background(), &KeyValue{
						Key:   ks[0],
						Value: val,
					})
					if err != nil && !errors.Is(err, ErrNotNew) {
						b.Fatalf("failed to set: %v", err)
					}
				}
			}()
		}

		wg.Wait()
	}
}

func BConcurrentUniqueKeysAndVals(cache *Cache, ks []*Key, vs []*Value, routines int) func(b *testing.B) {
	return func(b *testing.B) {
		wg := sync.WaitGroup{}
		wg.Add(routines)

		for r := 0; r < routines; r++ {
			go func() {
				defer wg.Done()

				for i := 0; i < b.N; i++ {
					val := vs[i]
					val.Timestamp = ptypes.TimestampNow()

					_, err := cache.Set(context.Background(), &KeyValue{
						Key:   ks[i],
						Value: val,
					})
					if err != nil && !errors.Is(err, ErrNotNew) {
						b.Fatalf("failed to set: %v", err)
					}
				}
			}()
		}

		wg.Wait()
	}
}
