package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"sync"
	"time"

	pb "github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store"
	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	"github.com/golang/protobuf/ptypes/empty"
	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

//go:generate protoc -I ../lazarette --go_out=plugins=grpc:../lazarette ../lazarette/lazarette.proto

var (
	// ErrKeyNotFound .
	ErrKeyNotFound = errors.New("key not found in cache")

	// ErrNotNew .
	ErrNotNew = errors.New("a newer value was found; not setting")
)

type lazaretteServer struct {
	store store.Store

	subsMu sync.RWMutex
	subs   map[string][]chan *pb.KeyValue
}

// NewLazaretteServer .
func NewLazaretteServer(path string) (pb.LazaretteServer, error) {
	path = filepath.Clean(path)
	options := &bolt.Options{
		Timeout: 2 * time.Second,
	}

	db, err := bolt.Open(path+"/lazarette.bolt", 0666, options)
	if err != nil {
		return nil, fmt.Errorf("unable to open bolt: %v", err)
	}

	store, err := boltstore.NewStore(db)
	if err != nil {
		return nil, fmt.Errorf("unable to create boltstore: %v", err)
	}

	s := &lazaretteServer{
		store: store,
		subs:  make(map[string][]chan *pb.KeyValue),
	}

	return s, nil
}

// Get .
func (s *lazaretteServer) Get(ctx context.Context, key *pb.Key) (*pb.Value, error) {
	if key == nil {
		return nil, errors.New("key must not be nil")
	}

	data, err := s.store.Get([]byte(key.GetKey()))
	switch {
	case err != nil:
		return nil, err
	case data == nil:
		return nil, ErrKeyNotFound
	}

	val := &pb.Value{}
	err = proto.Unmarshal(data, val)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal val: %v", err)
	}

	return val, nil
}

// Set .
func (s *lazaretteServer) Set(ctx context.Context, kv *pb.KeyValue) (*empty.Empty, error) {
	switch {
	case kv == nil:
		return nil, errors.New("kv must not be nil")
	case kv.GetValue() == nil:
		return nil, errors.New("value must not be nil")
	case kv.GetValue().GetTimestamp() == nil:
		return nil, errors.New("timestamp must not be nil")
	case kv.GetKey() == nil:
		return nil, errors.New("key must not be nil")
	}

	// get the current val
	cur, err := s.Get(ctx, kv.GetKey())
	switch {
	case err != nil && !errors.Is(err, ErrKeyNotFound):
		return nil, fmt.Errorf("unable to get current value: %v", err)
	case cur == nil:
		// shouldn't ever happen
	}

	if cur.GetTimestamp() != nil {
		curTime, err := ptypes.Timestamp(cur.GetTimestamp())
		if err != nil {
			return nil, fmt.Errorf("unable to check current timestamp: %v", err)
		}

		valTime, err := ptypes.Timestamp(kv.GetValue().GetTimestamp())
		if err != nil {
			return nil, fmt.Errorf("unable to check timestamp: %v", err)
		}

		if valTime.Before(curTime) {
			return nil, ErrNotNew
		}
	}

	bytes, err := proto.Marshal(kv)
	if err != nil {
		return nil, fmt.Errorf("unable to marshal new value: %v", err)
	}

	err = s.store.Put([]byte(kv.GetKey().GetKey()), bytes)
	if err != nil {
		return nil, fmt.Errorf("unable to put value into store: %v", err)
	}

	/*
		keyval := cache.KeyValue{
			Key:   key,
			Value: &val,
		}

		// notify all subscriptions that this key has updated
		l.subsMu.RLock()
		for prefix, chans := range l.subs {
			if strings.HasPrefix(key, prefix) {
				for i := range chans {
					chans[i] <- keyval
				}
			}
		}
		l.subsMu.RLock()
	*/

	return nil, nil
}

// Subscribe .
func (s *lazaretteServer) Subscribe(prefix *pb.Key, stream pb.Lazarette_SubscribeServer) error {
	return nil
}

func main() {
	port := 7777
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatal("failed to listen", zap.Error(err))
	}

	laz, err := NewLazaretteServer("/tmp")
	if err != nil {
		log.Fatal("failed to start lazarette", zap.Error(err))
	}

	grpcServer := grpc.NewServer()
	pb.RegisterLazaretteServer(grpcServer, laz)
	err = grpcServer.Serve(lis)
	if err != nil {
		log.Fatal("failed to start grpc server", zap.Error(err))
	}
}
