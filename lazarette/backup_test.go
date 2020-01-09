package lazarette

import (
	context "context"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/byuoitav/lazarette/store/memstore"
	bolt "go.etcd.io/bbolt"
)

func TestPersistentStorage(t *testing.T) {
	store, err := memstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create in memory store: %v\n", err)
	}

	db, err := bolt.Open(os.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %v\n", err)
	}

	pStore, err := boltstore.NewStore(db)
	if err != nil {
		t.Fatalf("failed to create persistent store: %v\n", err)
	}

	// build the cache
	cache, err := New(store, WithPersistent(pStore, 3*time.Second))
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	kv := randKV(t, 100)

	_, err = cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey(), err, kv.GetData())
	}

	//PAUSE TO DUMP
	time.Sleep(4 * time.Second)

	//Close the cache
	if err = cache.Close(); err != nil {
		t.Fatalf("failed to close cache")
	}

	nStore, err := memstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create in memory store the second time: %v\n", err)
	}

	nDB, err := bolt.Open(os.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %v\n", err)
	}

	nPStore, err := boltstore.NewStore(nDB)
	if err != nil {
		t.Fatalf("failed to create persistent store the second time: %v\n", err)
	}

	// build the cache again
	nCache, err := New(nStore, WithPersistent(nPStore, 3*time.Second))
	if err != nil {
		t.Fatalf("failed to create cache the second time: %v", err)
	}

	nval, err := nCache.Get(context.Background(), &Key{Key: kv.GetKey()})
	if err != nil {
		t.Fatalf("failed to get %q: %v", kv.GetKey(), err)
	}

	checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)

	if err = nCache.Close(); err != nil {
		t.Fatalf("failed to close nCache")
	}
}

func TestPersistentStorageFail(t *testing.T) {
	store, err := memstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create in memory store: %v\n", err)
	}

	db, err := bolt.Open(os.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %v\n", err)
	}

	pStore, err := boltstore.NewStore(db)
	if err != nil {
		t.Fatalf("failed to create persistent store: %v\n", err)
	}

	// build the cache
	cache, err := New(store, WithPersistent(pStore, 5*time.Second))
	if err != nil {
		t.Fatalf("failed to create cache: %v", err)
	}

	kv := randKV(t, 100)

	_, err = cache.Set(context.Background(), kv)
	if err != nil {
		t.Fatalf("failed to set %q: %v. buf was 0x%x", kv.GetKey(), err, kv.GetData())
	}

	//PAUSE TO DUMP
	time.Sleep(1 * time.Second)

	//Close the cache
	if err = cache.Close(); err != nil {
		t.Fatalf("failed to close cache")
	}

	nStore, err := memstore.NewStore()
	if err != nil {
		t.Fatalf("failed to create in memory store the second time: %v\n", err)
	}

	nDB, err := bolt.Open(os.TempDir()+"/test.db", 0600, nil)
	if err != nil {
		t.Fatalf("failed to open bolt: %v\n", err)
	}

	nPStore, err := boltstore.NewStore(nDB)
	if err != nil {
		t.Fatalf("failed to create persistent store the second time: %v\n", err)
	}

	// build the cache again
	nCache, err := New(nStore, WithPersistent(nPStore, 3*time.Second))
	if err != nil {
		t.Fatalf("failed to create cache the second time: %v", err)
	}

	_, err = nCache.Get(context.Background(), &Key{Key: kv.GetKey()})
	if err != nil {
		if !errors.Is(err, ErrKeyNotFound) {
			t.Fatalf("error should be ErrKeyNotFound: %v", err)
		}
	} else {
		t.Fatal("error should not be nil")
	}

	if err = nCache.Close(); err != nil {
		t.Fatalf("failed to close nCache")
	}
}
