package lazarette

import (
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store/syncmapstore"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	// disable logging for tests
	log.Config.Level.SetLevel(zap.PanicLevel)
	os.Exit(m.Run())
}

const charset = "abcdefghijklmnopqrstuvwxyz" +
	"ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789" + "!@#$%^&*()-_=+;|/\\{}"

var (
	seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))
	randMu     sync.Mutex
)

func newSyncMapCache(tb testing.TB) *Cache {
	store, err := syncmapstore.NewStore()
	if err != nil {
		tb.Fatalf("failed to create syncmap store: %v", err)
	}

	cache, err := New(store)
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

func randKey(tb testing.TB, maxLength int) string {
	randMu.Lock()
	defer randMu.Unlock()

	for {
		b := make([]byte, seededRand.Intn(maxLength))
		for i := range b {
			b[i] = charset[seededRand.Intn(len(charset))]
		}

		if len(string(b)) > 0 {
			return string(b)
		}
	}
}

func randVal(tb testing.TB, maxLength int) *Value {
	randMu.Lock()
	defer randMu.Unlock()

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

func randKV(tb testing.TB, maxKey, maxVal int) *KeyValue {
	return &KeyValue{
		Key:       randKey(tb, maxKey),
		Data:      randData(tb, maxVal),
		Timestamp: ptypes.TimestampNow(),
	}
}

func randData(tb testing.TB, maxLength int) []byte {
	randMu.Lock()
	defer randMu.Unlock()

	buf := make([]byte, seededRand.Intn(maxLength))
	_, err := seededRand.Read(buf)
	if err != nil {
		tb.Fatal(err)
	}

	return buf
}

func checkValueEqual(tb testing.TB, key string, expected, actual *Value) {
	if !proto.Equal(expected, actual) {
		tb.Fatalf("values don't match for key %q:\n\texpected: %s\n\tactual: %s\n", key, expected.String(), actual.String())
	}
}

/*
TESTS
func doCacheTest(t *testing.T, cache *Cache) {
	t.Log("SubscriptionChanMatch")
	t.Run("TestSubscriptionChanMatch", SubscriptionChanMatchTest(cache))
	cleanCache(t, cache)

	t.Log("SubscriptionChanNoMatch")
	t.Run("TestSubscriptionChanNoMatch", SubscriptionChanNoMatchTest(cache))
	cleanCache(t, cache)

	t.Log("Unsubscribe")
	t.Run("TestUnsubscribe", UnsubscribeTest(cache))
	cleanCache(t, cache)

	closeCache(t, cache)
}

func SubscriptionChanMatchTest(cache *Cache) func(t *testing.T) {
	return func(t *testing.T) {
		ch, unsub := cache.SubscribeChan("ITB")
		defer unsub()

		kv := &KeyValue{
			Key:       "ITB-1101-CP1",
			Timestamp: ptypes.TimestampNow(),
			Data:      []byte(`{"test": "value"}`),
		}

		_, err := cache.Set(context.Background(), kv)
		if err != nil {
			t.Fatalf("unable to set key: %s", err)
		}

		if len(ch) != 1 {
			t.Fatalf("channel didn't get new value")
		}

		nkv := <-ch

		checkValueEqual(t, kv.GetKey(), &Value{Timestamp: kv.GetTimestamp(), Data: kv.GetData()}, &Value{Timestamp: nkv.GetTimestamp(), Data: nkv.GetData()})
	}
}

func SubscriptionChanNoMatchTest(cache *Cache) func(*testing.T) {
	return func(t *testing.T) {
		kv := &KeyValue{
			Key:       "ITC-1101-CP1", // prefix doesn't match
			Timestamp: ptypes.TimestampNow(),
			Data:      []byte(`{"test": "value"}`),
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
			Key:       "ITB-1101-CP1",
			Timestamp: ptypes.TimestampNow(),
			Data:      []byte(`{"test": "value"}`),
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
*/
