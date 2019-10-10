package syncmapstore

import (
	"bytes"
	"testing"
)

func newStore(tb testing.TB) *syncmapstore {
	tb.Helper()
	store, err := NewStore()
	if err != nil {
		tb.Fatalf("failed to create memstore: %v", err)
	}

	err = store.Clean()
	if err != nil {
		tb.Fatalf("failed to clean memstore: %v", err)
	}

	return store.(*syncmapstore)
}

func testEqual(tb testing.TB, expected, actual []byte) {
	tb.Helper()

	if !bytes.Equal(expected, actual) {
		tb.Fatalf("values didn't match:\nexpected: 0x%x\nactual: 0x%x", expected, actual)
	}
}

func TestSet(t *testing.T) {
	store := newStore(t)
	defer store.Close()

	key := []byte("hello")
	val := []byte(`{"test": "value"}`)

	err := store.Set(key, val)
	if err != nil {
		t.Fatalf("failed to set %q: %v", key, err)
	}

	nval, err := store.Get(key)
	if err != nil {
		t.Fatalf("failed to get key: %v", err)
	}

	testEqual(t, val, nval)
}

func TestGetInvalidKey(t *testing.T) {
	store := newStore(t)
	defer store.Close()

	key := []byte("keythatdoesntexist")

	nval, err := store.Get(key)
	switch {
	case err != nil:
		t.Fatalf("failed to get key: %v", err)
	case nval != nil:
		t.Fatalf("expected: nil\ngot: 0x%x", nval)
	}
}

func TestClean(t *testing.T) {
	store := newStore(t)
	defer store.Close()

	key := []byte("hello")
	val := []byte(`{"test": "value"}`)

	err := store.Set(key, val)
	if err != nil {
		t.Fatalf("failed to set %q: %v", key, err)
	}

	err = store.Clean()
	if err != nil {
		t.Fatalf("failed to clean store: %v", err)
	}

	nval, err := store.Get(key)
	switch {
	case err != nil:
		t.Fatalf("failed to get key: %v", err)
	case nval != nil:
		t.Fatalf("expected: nil\ngot: 0x%x", nval)
	}
}

func TestGetPrefix(t *testing.T) {
	store := newStore(t)
	defer store.Close()

	key1 := []byte("ITB-1101-CP1")
	val1 := []byte(`{"key1": "val1"}`)

	key2 := []byte("ITB-1101-CP2")
	val2 := []byte(`{"key2": "val2"}`)

	key3 := []byte("ITB-2202-CP3")
	val3 := []byte(`{"key3": "val3"}`)

	err := store.Set(key1, val1)
	if err != nil {
		t.Fatalf("failed to set %q: %s", key1, err)
	}

	err = store.Set(key2, val2)
	if err != nil {
		t.Fatalf("failed to set %q: %s", key2, err)
	}

	err = store.Set(key3, val3)
	if err != nil {
		t.Fatalf("failed to set %q: %s", key3, err)
	}

	prefix := []byte("ITB-1101")
	kvs, err := store.GetPrefix(prefix)
	if err != nil {
		t.Fatalf("failed to get prefix %q: %s", prefix, err)
	}

	if len(kvs) != 2 {
		t.Fatalf("expected %d keys, but got %d\nkvs: %+v", 2, len(kvs), kvs)
	}

	for _, kv := range kvs {
		switch {
		case bytes.Equal(key1, kv.Key):
			if !bytes.Equal(val1, kv.Value) {
				t.Fatalf("in key %q:\nexpected 0x%x\ngot: 0x%x", kv.Key, val1, kv.Value)
			}
		case bytes.Equal(key2, kv.Key):
			if !bytes.Equal(val2, kv.Value) {
				t.Fatalf("in key %q:\nexpected 0x%x\ngot: 0x%x", kv.Key, val2, kv.Value)
			}
		default:
			t.Fatalf("expected %q and %q in response, but got %q", key1, key2, kv.Key)
		}
	}
}
