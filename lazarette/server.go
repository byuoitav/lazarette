package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"io"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/lazarette/store"
	"github.com/byuoitav/lazarette/store/boltstore"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	bolt "go.etcd.io/bbolt"
	grpc "google.golang.org/grpc"
)

//go:generate protoc -I ./ --go_out=plugins=grpc:./ ./lazarette.proto

var (
	// ErrKeyNotFound .
	ErrKeyNotFound = errors.New("key not found in cache")

	// ErrNotNew .
	ErrNotNew = errors.New("a newer value was found; not setting")
)

// Server .
type Server struct {
	store  store.Store
	server *grpc.Server

	subsMu sync.RWMutex
	subs   map[string][]chan *KeyValue
}

// Replication .
type Replication struct {
	KeyPrefix  string `json:"key-prefix"`
	RemoteAddr string `json:"remote-addr"`
}

// NewServer .
func NewServer(path string, repls ...Replication) (*Server, error) {
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

	s := &Server{
		store: store,
		subs:  make(map[string][]chan *KeyValue),
	}

	for _, repl := range repls {
		s.AddReplication(context.TODO(), repl)
	}

	return s, nil
}

// StartGRPCServer .
func (s *Server) StartGRPCServer(address string) error {
	lis, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("failed to listen: %v", err)
	}

	s.server = grpc.NewServer()
	RegisterLazaretteServer(s.server, s)
	err = s.server.Serve(lis)
	if err != nil {
		return fmt.Errorf("failed to serve: %v", err)
	}

	return nil
}

// Get .
func (s *Server) Get(ctx context.Context, key *Key) (*Value, error) {
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

	val := &Value{}
	err = proto.Unmarshal(data, val)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal val: %v", err)
	}

	return val, nil
}

// Set .
func (s *Server) Set(ctx context.Context, kv *KeyValue) (*empty.Empty, error) {
	err := s.set(ctx, kv)
	if err != nil {
		return nil, err
	}

	// notify all subscriptions that this key has updated
	s.subsMu.RLock()
	for prefix, chans := range s.subs {
		if strings.HasPrefix(kv.GetKey().GetKey(), prefix) {
			for i := range chans {
				chans[i] <- kv
			}
		}
	}
	s.subsMu.RUnlock()

	return &empty.Empty{}, nil
}

// Subscribe .
func (s *Server) Subscribe(prefix *Key, stream Lazarette_SubscribeServer) error {
	if prefix == nil {
		return errors.New("prefix must not be nil")
	}

	ch := make(chan *KeyValue)
	s.subsMu.Lock()
	s.subs[prefix.GetKey()] = append(s.subs[prefix.GetKey()], ch)
	s.subsMu.Unlock()

	for kv := range ch {
		err := stream.Send(kv)
		if err != nil {
			// TODO print error
		}
	}

	return nil
}

// AddReplication .
func (s *Server) AddReplication(ctx context.Context, repl Replication) error {
	conn, err := grpc.Dial(repl.RemoteAddr)
	if err != nil {
		return fmt.Errorf("unable to open connection: %v", err)
	}
	defer conn.Close()

	client := NewLazaretteClient(conn)
	prefix := &Key{
		Key: repl.KeyPrefix,
	}

	stream, err := client.Subscribe(ctx, prefix)
	if err != nil {
		return fmt.Errorf("unable to subscribe: %v", err)
	}

	for {
		kv, err := stream.Recv()
		switch {
		case kv == nil:
			continue
		case err == io.EOF:
			break
		case err != nil:
			return fmt.Errorf("unable to recv message from stream: %v", err)
		}

		err = s.set(ctx, kv)
		if err != nil && !errors.Is(err, ErrNotNew) {
			// log something?
		}
	}
}

// Close .
func (s *Server) Close() error {
	if s.server != nil {
		s.server.GracefulStop()
	}

	return s.store.Close()
}

// Clean .
func (s *Server) Clean() error {
	return s.store.Clean()
}

func (s *Server) set(ctx context.Context, kv *KeyValue) error {
	switch {
	case kv == nil:
		return errors.New("kv must not be nil")
	case kv.GetKey() == nil:
		return errors.New("key must not be nil")
	case kv.GetValue() == nil:
		return errors.New("value must not be nil")
	case kv.GetValue().GetTimestamp() == nil:
		return errors.New("timestamp must not be nil")
	}

	// get the current val
	cur, err := s.Get(ctx, kv.GetKey())
	switch {
	case err != nil && !errors.Is(err, ErrKeyNotFound):
		return fmt.Errorf("unable to get current value: %v", err)
	case cur == nil:
		// shouldn't ever happen
	}

	if cur.GetTimestamp() != nil {
		curTime, err := ptypes.Timestamp(cur.GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check current timestamp: %v", err)
		}

		valTime, err := ptypes.Timestamp(kv.GetValue().GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check timestamp: %v", err)
		}

		if valTime.Before(curTime) {
			return ErrNotNew
		}
	}

	bytes, err := proto.Marshal(kv.GetValue())
	if err != nil {
		return fmt.Errorf("unable to marshal new value: %v", err)
	}

	err = s.store.Put([]byte(kv.GetKey().GetKey()), bytes)
	if err != nil {
		return fmt.Errorf("unable to put value into store: %v", err)
	}

	return nil
}
