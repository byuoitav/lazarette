package lazarette

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes"
	grpc "google.golang.org/grpc"
)

func TestReplicationFromLocalToRemote(t *testing.T) {
	// build a local cache
	local := newSyncMapCache(t)
	defer local.Close()

	// build a "remote" cache
	remote := newSyncMapCache(t)
	defer remote.Close()

	// start grpc server for remote cache
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to bind to a port: %s", err)
	}

	server := grpc.NewServer()
	RegisterLazaretteServer(server, remote)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Fatalf("failed to start remote server: %s", err)
		}
	}()
	defer server.Stop()

	// start replication from local to remote
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		err = local.Replicate(ctx, "ITB-1101", lis.Addr().String())
		if err != nil {
			t.Fatalf("failed to start replication: %s", err)
		}
	}()

	// set something on local
	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err = local.Set(ctx, kv)
	if err != nil {
		t.Fatalf("failed to set on local: %s", err)
	}

	// give it a second to replicate
	time.Sleep(1 * time.Second)

	// make sure it replicated to remote
	nval, err := remote.Get(ctx, &Key{Key: kv.GetKey()})
	if err != nil {
		t.Fatalf("failed to get from remote: %s", err)
	}

	checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)
}

func TestReplicationFromRemoteToLocal(t *testing.T) {
	// build a local cache
	local := newSyncMapCache(t)
	defer local.Close()

	// build a "remote" cache
	remote := newSyncMapCache(t)
	defer remote.Close()

	// start grpc server for remote cache
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to bind to a port: %s", err)
	}

	server := grpc.NewServer()
	RegisterLazaretteServer(server, remote)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Fatalf("failed to start remote server: %s", err)
		}
	}()
	defer server.Stop()

	// start replication from local to remote
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	go func() {
		err = local.Replicate(ctx, "ITB-1101", lis.Addr().String())
		if err != nil {
			t.Fatalf("failed to start replication: %s", err)
		}
	}()

	// set something on remote
	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	_, err = remote.Set(ctx, kv)
	if err != nil {
		t.Fatalf("failed to set on local: %s", err)
	}

	// give it a second to replicate
	time.Sleep(1 * time.Second)

	// make sure it replicated to local
	nval, err := local.Get(ctx, &Key{Key: kv.GetKey()})
	if err != nil {
		t.Fatalf("failed to get from remote: %s", err)
	}

	checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)
}

func TestReplicationGettingDump(t *testing.T) {
	// build a local cache
	local := newSyncMapCache(t)
	defer local.Close()

	// set something on local
	kv := &KeyValue{
		Key:       "ITB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	kv2 := &KeyValue{
		Key:       "ITB-1101-D1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	kv3 := &KeyValue{
		Key:       "HCEB-1101-CP1",
		Data:      randData(t, 300),
		Timestamp: ptypes.TimestampNow(),
	}

	// build ctx
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// set values
	_, err := local.Set(ctx, kv)
	if err != nil {
		t.Fatalf("failed to set on local: %s", err)
	}

	_, err = local.Set(ctx, kv2)
	if err != nil {
		t.Fatalf("failed to set on local: %s", err)
	}

	_, err = local.Set(ctx, kv3)
	if err != nil {
		t.Fatalf("failed to set on local: %s", err)
	}

	// build a "remote" cache
	remote := newSyncMapCache(t)
	defer remote.Close()

	// start grpc server for remote cache
	lis, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatalf("failed to bind to a port: %s", err)
	}

	server := grpc.NewServer()
	RegisterLazaretteServer(server, remote)

	go func() {
		if err := server.Serve(lis); err != nil {
			t.Fatalf("failed to start remote server: %s", err)
		}
	}()
	defer server.Stop()

	// start replication from local to remote
	go func() {
		err = local.Replicate(ctx, "ITB-1101", lis.Addr().String())
		if err != nil {
			t.Fatalf("failed to start replication: %s", err)
		}
	}()

	// give it a second to replicate
	time.Sleep(1 * time.Second)

	// make sure the keys replicated to remote
	nval, err := remote.Get(ctx, &Key{Key: kv.GetKey()})
	if err != nil {
		t.Fatalf("failed to get from remote: %s", err)
	}

	checkValueEqual(t, kv.GetKey(), &Value{Data: kv.GetData(), Timestamp: kv.GetTimestamp()}, nval)

	nval2, err := remote.Get(ctx, &Key{Key: kv2.GetKey()})
	if err != nil {
		t.Fatalf("failed to get from remote: %s", err)
	}

	checkValueEqual(t, kv2.GetKey(), &Value{Data: kv2.GetData(), Timestamp: kv2.GetTimestamp()}, nval2)

	// make sure kv3 didn't replicate
	_, err = remote.Get(ctx, &Key{Key: kv3.GetKey()})
	switch {
	case errors.Is(err, ErrKeyNotFound):
		// yay!
	case err != nil:
		t.Fatalf("failed to get from remote: %s", err)
	default:
		t.Fatalf("key that didn't match the prefix replicated to remote")
	}
}

// TODO
// test taking down one side of connection and it reestablishing
