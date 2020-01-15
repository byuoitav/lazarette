package lazarette

import (
	context "context"
	"errors"
	"sync"
	"testing"

	"github.com/golang/protobuf/ptypes"
)

func TestSetAndGet1024Random(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	for i := 0; i < 1024; i++ {
		kv := randKV(t, 50, 300)

		_, err := cache.Set(context.Background(), kv)
		if err != nil {
			t.Fatalf("failed to set: %s", err)
		}

		nval, err := cache.Get(context.Background(), &Key{Key: kv.GetKey()})
		if err != nil {
			t.Fatalf("failed to get %q: %s", kv.GetKey(), err)
		}

		checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)
	}
}

func TestSetttingTheSameKey(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      []byte(`{"initial": "value"}`),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err := cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set initial value: %s", err)
	}

	// update kv
	kv.Data = []byte(`{"something": "different"}`)
	kv.Timestamp = ptypes.TimestampNow()

	// try setting it again
	_, err = cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to update value: %s", err)
	}

	// get it an make sure the updated one is stored
	nval, err := cache.Get(context.Background(), &Key{Key: kv.GetKey()})
	if err != nil {
		t.Fatalf("failed to get %q: %s", kv.GetKey(), err)
	}

	checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)
}

func TestConcurrentSettingRandomKeys(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	wg := sync.WaitGroup{}
	wg.Add(128)

	for r := 0; r < 128; r++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 64; i++ {
				_, err := cache.Set(context.Background(), randKV(t, 50, 300))
				if err != nil && !errors.Is(err, ErrNotNew) {
					// ErrNotNew is an acceptable error in this test
					t.Fatalf("failed to set: %s", err)
				}
			}
		}()
	}

	wg.Wait()
}

func TestConcurrentSettingTheSameKey(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	wg := sync.WaitGroup{}
	wg.Add(128)

	key := "ITB-1101-CP1"

	for r := 0; r < 128; r++ {
		go func() {
			defer wg.Done()

			for i := 0; i < 64; i++ {
				kv := &KeyValue{
					Key:       key,
					Data:      randData(t, 300),
					Timestamp: ptypes.TimestampNow(),
				}

				_, err := cache.Set(context.Background(), kv)
				if err != nil && !errors.Is(err, ErrNotNew) {
					// ErrNotNew is an acceptable error in this test
					t.Fatalf("failed to set: %s", err)
				}
			}
		}()
	}

	wg.Wait()
}
