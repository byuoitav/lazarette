package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"io"
	"strings"
	"sync"

	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/store"
	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

//go:generate protoc -I ./ --go_out=plugins=grpc:./ ./lazarette.proto

var (
	// ErrKeyNotFound .
	ErrKeyNotFound = errors.New("key not found in cache")

	// ErrNotNew .
	ErrNotNew = errors.New("a newer value was found; not setting")
)

// Cache .
type Cache struct {
	store store.Store

	subsMu sync.RWMutex
	subs   map[string][]chan *KeyValue
}

// Replication .
type Replication struct {
	KeyPrefix  string `json:"key-prefix"`
	RemoteAddr string `json:"remote-addr"`
}

// UnsubscribeFunc .
type UnsubscribeFunc func()

// NewCache .
func NewCache(store store.Store, repls ...Replication) (*Cache, error) {
	s := &Cache{
		store: store,
		subs:  make(map[string][]chan *KeyValue),
	}

	for _, repl := range repls {
		// TODO error codes for each replication?
		err := s.ReplicateFrom(context.TODO(), repl)
		if err != nil {
			return nil, err
		}
	}

	return s, nil
}

// Get .
func (s *Cache) Get(ctx context.Context, key *Key) (*Value, error) {
	if key == nil {
		return nil, errors.New("key must not be nil")
	}

	log.P.Info("Getting", zap.String("key", key.GetKey()))

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

	log.P.Debug("Successfully got", zap.String("key", key.GetKey()), zap.ByteString("value", val.GetData()))
	return val, nil
}

// Set .
func (s *Cache) Set(ctx context.Context, kv *KeyValue) (*empty.Empty, error) {
	err := s.set(ctx, kv)
	if err != nil {
		log.P.Warn("failed to set", zap.Error(err))
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
func (s *Cache) Subscribe(prefix *Key, stream Lazarette_SubscribeServer) error {
	if prefix == nil {
		return errors.New("prefix must not be nil")
	}

	ch, unsub := s.SubscribeChan(prefix.GetKey())
	for kv := range ch {
		err := stream.Send(kv)
		if err != nil {
			log.P.Warn("Failed to send to stream", zap.Error(err), zap.String("key", kv.GetKey().GetKey()))
			unsub()
			return err
		}
	}

	return nil
}

// SubscribeChan .
func (s *Cache) SubscribeChan(prefix string) (chan *KeyValue, UnsubscribeFunc) {
	ch := make(chan *KeyValue, 16)
	log.P.Info("Subscribing to", zap.String("prefix", prefix))

	unsubscribe := func() {
		log.P.Info("Unsubscribing from", zap.String("prefix", prefix))
		s.subsMu.Lock()
		defer s.subsMu.Unlock()

		if chs, ok := s.subs[prefix]; ok {
			s.subs[prefix] = nil
			for i := range chs {
				if ch != chs[i] {
					s.subs[prefix] = append(s.subs[prefix], chs[i])
				}
			}
		}

		close(ch)
	}

	s.subsMu.Lock()
	s.subs[prefix] = append(s.subs[prefix], ch)
	s.subsMu.Unlock()

	return ch, unsubscribe
}

// ReplicateFrom .
func (s *Cache) ReplicateFrom(ctx context.Context, repl Replication) error {
	// TODO make the connection retry if it disconnects
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

		if kv.GetKey() != nil {
			log.P.Info("Received value", zap.String("from", repl.RemoteAddr), zap.String("key", kv.GetKey().GetKey()))
		}

		err = s.set(ctx, kv)
		if err != nil && !errors.Is(err, ErrNotNew) {
			log.P.Warn("unable to set key", zap.String("from", repl.RemoteAddr), zap.Error(err))
		}
	}
}

// Close .
func (s *Cache) Close() error {
	log.P.Info("Closing lazarette Cache")
	return s.store.Close()
}

// Clean .
func (s *Cache) Clean() error {
	log.P.Info("Cleaning lazarette Cache")
	return s.store.Clean()
}

func (s *Cache) set(ctx context.Context, kv *KeyValue) error {
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

	log.P.Info("Setting", zap.String("key", kv.GetKey().GetKey()), zap.ByteString("value", kv.GetValue().GetData()))

	// get the current val
	cur, err := s.Get(ctx, kv.GetKey())
	switch {
	case errors.Is(err, ErrKeyNotFound):
	case err != nil:
		return fmt.Errorf("unable to get current value: %v", err)
	case cur == nil:
		return fmt.Errorf("weird. current value AND error were nil")
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

	log.P.Debug("Successfully set", zap.String("key", kv.GetKey().GetKey()))
	return nil
}
