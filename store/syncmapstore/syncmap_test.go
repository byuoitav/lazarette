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

func TestPut(t *testing.T) {
	store := newStore(t)
	defer store.Close()

	key := []byte("hello")
	val := []byte(`{"test": "value"}`)

	err := store.Put(key, val)
	if err != nil {
		t.Fatalf("failed to put key: %v", err)
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

	err := store.Put(key, val)
	if err != nil {
		t.Fatalf("failed to put key: %v", err)
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
