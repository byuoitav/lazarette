package ristrettostore

import (
	"bytes"
	"testing"
	"time"
)

func newStore(tb testing.TB) *ristrettostore {
	store, err := NewStore(100 << 20) // 100mb
	if err != nil {
		tb.Fatalf("failed to create ristrettostore: %v", err)
	}

	err = store.Clean()
	if err != nil {
		tb.Fatalf("failed to clean ristrettostore: %v", err)
	}

	return store.(*ristrettostore)
}

func testEqual(tb testing.TB, expected, actual []byte) {
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
		t.Fatalf("failed to put key: %v", err)
	}

	time.Sleep(10 * time.Millisecond) // let value pass through buffer

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
	t.Skip("skipping test because clean isn't implemented")

	store := newStore(t)
	defer store.Close()

	key := []byte("hello")
	val := []byte(`{"test": "value"}`)

	err := store.Set(key, val)
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
