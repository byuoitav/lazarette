package lazarette

import (
	context "context"
	"testing"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
)

func TestCreatingSubscriptionCloseCacheFirst(t *testing.T) {
	cache := newSyncMapCache(t)

	// create a subscription
	s, err := cache.NewSubscription("ITB-1101")
	if err != nil {
		t.Fatalf("failed to create subscription: %s", err)
	}

	closeCache(t, cache)
	s.Stop()
}

func TestCreatingSubscriptionStopSubFirst(t *testing.T) {
	cache := newSyncMapCache(t)
	// create a subscription
	s, err := cache.NewSubscription("ITB-1101")
	if err != nil {
		t.Fatalf("failed to create subscription: %s", err)
	}

	s.Stop()
	closeCache(t, cache)
}

func TestSubscribeGettingMatches(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	// create a subscription
	s, err := cache.NewSubscription("ITB-1101")
	if err != nil {
		t.Fatalf("failed to create subscription: %s", err)
	}
	defer s.Stop()

	// set something on the cache that matches our prefix
	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err = cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	for {
		select {
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for value")
		case <-s.Done():
			t.Fatalf("subscription stopped before I received any values")
		case nkv := <-s.Changes():
			// should match kv
			checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, &Value{Data: nkv.GetData(), Timestamp: nkv.GetTimestamp()})

			// if we didn't just fail, then it worked
			return
		}
	}
}

func TestSubscribeIgnoring(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	// create a subscription
	s, err := cache.NewSubscription("ITB-1101")
	if err != nil {
		t.Fatalf("failed to create subscription: %s", err)
	}
	defer s.Stop()

	// set something on the cache that *doesn't* match our prefix
	kv := &KeyValue{
		Key:       "EB-2202-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err = cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	for {
		select {
		case <-time.After(1 * time.Second):
			// we shouldn't get anything
			return
		case <-s.Done():
			t.Fatalf("subscription stopped by someone other than me")
		case <-s.Changes():
			// we shouldn't get anything
			t.Fatalf("recevied value that didn't match my prefix")
		}
	}
}

func TestSubscribeGettingDump(t *testing.T) {
	cache := newSyncMapCache(t)
	defer closeCache(t, cache)

	// set some values on the cache that match our prefix
	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err := cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	kv2 := &KeyValue{
		Key:       "ITB-1101-D1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err = cache.Set(context.Background(), kv2)
	if err != nil {
		t.Fatalf("failed to set: %s", err)
	}

	// create a subscription
	s, err := cache.NewSubscription("ITB-1101")
	if err != nil {
		t.Fatalf("failed to create subscription: %s", err)
	}
	defer s.Stop()

	matched1 := false
	matched2 := false

	// make sure we get both of the values above
	for {
		select {
		case <-time.After(1 * time.Second):
			t.Fatalf("timed out waiting for values from subscription")
		case <-s.Done():
			t.Fatalf("subscription stopped by someone other than me")
		case nkv := <-s.Changes():
			if !matched1 {
				if proto.Equal(kv, nkv) {
					matched1 = true
				}
			}

			if !matched2 {
				if proto.Equal(kv, nkv) {
					matched2 = true
				}
			}

			if matched1 && matched2 {
				return
			}
		}
	}
}
