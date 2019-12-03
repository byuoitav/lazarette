package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"io"
	"strings"
	"sync"

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

	replsMu sync.RWMutex
	repls   []*replication

	*zap.Logger
}

type replication struct {
	*Replication
	LazaretteClient
	*zap.Logger
}

// UnsubscribeFunc .
type UnsubscribeFunc func()

// NewCache .
func NewCache(store store.Store, logger *zap.Logger) (*Cache, error) {
	s := &Cache{
		store:  store,
		subs:   make(map[string][]chan *KeyValue),
		Logger: logger,
	}

	return s, nil
}

// Get .
func (s *Cache) Get(ctx context.Context, key *Key) (*Value, error) {
	if key == nil {
		return nil, errors.New("key must not be nil")
	}

	s.Info("Getting", zap.String("key", key.GetKey()))

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
		return nil, fmt.Errorf("unable to unmarshal val: %w", err)
	}

	s.Debug("Successfully got", zap.String("key", key.GetKey()), zap.ByteString("value", val.GetData()))
	return val, nil
}

// Set .
func (s *Cache) Set(ctx context.Context, kv *KeyValue) (*empty.Empty, error) {
	err := s.set(ctx, kv)
	if err != nil {
		s.Warn("failed to set", zap.Error(err))
		return nil, err
	}

	// notify all subscriptions that this key has updated
	// TODO add some timeout so that one slow connection doesn't hold things up?
	s.subsMu.RLock()
	for prefix, chans := range s.subs {
		if strings.HasPrefix(kv.GetKey(), prefix) {
			for i := range chans {
				chans[i] <- kv
			}
		}
	}
	s.subsMu.RUnlock()

	/*
		// notify everyone i'm replicating with
		go func() {
			s.replsMu.RLock()
			defer s.replsMu.RUnlock()

			for _, repl := range s.repls {
				nctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

				_, err := repl.Set(nctx, kv)
				if err != nil {
					s.Warn("unable to set updated key on replication", zap.String("repl", repl.RemoteAddr), zap.Error(err))
				} else {
					s.Info("updated key on replication", zap.String("repl", repl.RemoteAddr), zap.String("key", kv.GetKey()))
				}

				// make sure context doesn't leak
				cancel()
			}
		}()
	*/

	return &empty.Empty{}, nil
}

// Subscribe .
func (s *Cache) Subscribe(prefix *Key, stream Lazarette_SubscribeServer) error {
	if prefix == nil {
		return errors.New("prefix must not be nil")
	}

	ch, unsub := s.SubscribeChan(prefix.GetKey())
	for kv := range ch {
		s.Info("[subscribe] Sending key " + kv.GetKey())
		err := stream.Send(kv)
		if err != nil { // TODO should probably switch on the error
			s.Warn("failed to send key to stream", zap.Error(err), zap.String("key", kv.GetKey()))
			unsub()
			return err
		}
	}

	return nil
}

// SubscribeChan .
func (s *Cache) SubscribeChan(prefix string) (chan *KeyValue, UnsubscribeFunc) {
	ch := make(chan *KeyValue, 16)
	s.Info("Subscribing to", zap.String("prefix", prefix))

	go func() {
		// get a dump of the cache, send that down the stream
		kvs, err := s.store.GetPrefix([]byte(prefix))
		if err != nil {
			s.Warn("unable to get prefix dump", zap.String("prefix", prefix), zap.Error(err))
			return
		}

		for i := range kvs {
			var v Value
			if err := proto.Unmarshal(kvs[i].Value, &v); err != nil {
				s.Warn("unable to unmarshal value", zap.ByteString("key", kvs[i].Key), zap.Error(err))
				continue
			}

			ch <- &KeyValue{
				Key:       string(kvs[i].Key),
				Timestamp: v.GetTimestamp(),
				Data:      v.GetData(),
			}
		}
	}()

	unsubscribe := func() {
		s.Info("Unsubscribing from", zap.String("prefix", prefix))
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

// ReplicateWith .
func (s *Cache) ReplicateWith(ctx context.Context, repl *Replication) (*empty.Empty, error) {
	// TODO make the connection retry if it disconnects
	// TODO a way to stop a replication?
	conn, err := grpc.Dial(repl.GetRemoteAddr(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("unable to open connection: %w", err)
	}

	r := &replication{
		Replication:     repl,
		LazaretteClient: NewLazaretteClient(conn),
		Logger:          s.Named("repl-" + repl.GetRemoteAddr()),
	}

	r.Info("Started replication")

	s.replsMu.Lock()
	s.repls = append(s.repls, r)
	s.replsMu.Unlock()

	defer func() {
		s.replsMu.Lock()
		defer s.replsMu.Unlock()

		// TODO remove from the repls list
		conn.Close()
	}()

	// get all changes from the remote address
	prefix := &Key{
		Key: repl.GetPrefix(),
	}

	stream, err := r.Subscribe(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("unable to subscribe: %w", err)
	}

	go func() {
		for {
			kv, err := stream.Recv()
			switch {
			case kv == nil:
				continue
			case err == io.EOF:
				break
			case err != nil:
				// TODO handle error
				// return fmt.Errorf("unable to recv message from stream: %w", err)
			}

			r.Info("Received value", zap.String("key", kv.GetKey()))

			err = s.set(ctx, kv)
			if err != nil && !errors.Is(err, ErrNotNew) {
				s.Warn("unable to set key", zap.Error(err))
			}
		}
	}()

	// send all of our changes (and current state) to the remote address
	changes, unsub := s.SubscribeChan(r.GetPrefix())
	defer unsub()

	for change := range changes {
		_, err := r.Set(ctx, change)
		if err != nil {
			r.Warn("unable to update remote lazarette", zap.String("key", change.GetKey()))
		}
	}

	return &empty.Empty{}, nil
}

// Close .
func (s *Cache) Close() error {
	s.Info("Closing lazarette Cache")
	return s.store.Close()
}

// Clean .
func (s *Cache) Clean() error {
	s.Info("Cleaning lazarette Cache")
	return s.store.Clean()
}

func (s *Cache) set(ctx context.Context, kv *KeyValue) error {
	switch {
	case kv == nil:
		return errors.New("kv must not be nil")
	case kv.GetTimestamp() == nil:
		return errors.New("timestamp must not be nil")
	}

	s.Info("Setting", zap.String("key", kv.GetKey()), zap.ByteString("value", kv.GetData()))

	// get the current val
	cur, err := s.Get(ctx, &Key{Key: kv.GetKey()})
	switch {
	case errors.Is(err, ErrKeyNotFound):
	case err != nil:
		return fmt.Errorf("unable to get current value: %w", err)
	case cur == nil:
		return fmt.Errorf("weird. current value AND error were nil")
	}

	if cur.GetTimestamp() != nil {
		curTime, err := ptypes.Timestamp(cur.GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check current timestamp: %w", err)
		}

		valTime, err := ptypes.Timestamp(kv.GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check timestamp: %w", err)
		}

		if valTime.Before(curTime) {
			return ErrNotNew
		}
	}

	bytes, err := proto.Marshal(&Value{Timestamp: kv.GetTimestamp(), Data: kv.GetData()})
	if err != nil {
		return fmt.Errorf("unable to marshal new value: %w", err)
	}

	err = s.store.Set([]byte(kv.GetKey()), bytes)
	if err != nil {
		return fmt.Errorf("unable to put value into store: %w", err)
	}

	s.Debug("Successfully set", zap.String("key", kv.GetKey()))
	return nil
}
