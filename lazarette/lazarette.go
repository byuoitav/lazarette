package lazarette

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/byuoitav/lazarette/cache"
	"github.com/byuoitav/lazarette/store"
	"github.com/byuoitav/lazarette/store/boltstore"
	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	bolt "go.etcd.io/bbolt"
)

type lazarette struct {
	store store.Store

	subsMu sync.RWMutex
	subs   map[string][]chan cache.KeyValue
}

// Open .
func Open(path string) (cache.Cache, error) {
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

	c := &lazarette{
		store: store,
		subs:  make(map[string][]chan cache.KeyValue),
	}

	return c, nil
}

func (l *lazarette) Close() error {
	return l.store.Close()
}

func (l *lazarette) Clean() error {
	return l.store.Clean()
}

// Get .
func (l *lazarette) Get(key string) (cache.Value, error) {
	val := cache.Value{}

	data, err := l.store.Get([]byte(key))
	switch {
	case err != nil:
		return val, err
	case data == nil:
		return val, cache.ErrKeyNotFound
	}

	err = proto.Unmarshal(data, &val)
	if err != nil {
		return val, fmt.Errorf("unable to unmarshal val: %v", err)
	}

	return val, nil
}

// Set .
func (l *lazarette) Set(key string, val cache.Value) error {
	switch {
	case val.GetData() == nil:
		return errors.New("data must not be nil")
	case val.GetTimestamp() == nil:
		return errors.New("timestamp must not be nil")
	default:
	}

	// get the current val
	cur, err := l.Get(key)
	if err != nil && !errors.Is(err, cache.ErrKeyNotFound) {
		return fmt.Errorf("unable to get current value: %v", err)
	}

	if cur.GetTimestamp() != nil {
		curTime, err := ptypes.Timestamp(cur.GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check current timestamp: %v", err)
		}

		valTime, err := ptypes.Timestamp(val.GetTimestamp())
		if err != nil {
			return fmt.Errorf("unable to check timestamp: %v", err)
		}

		if valTime.Before(curTime) {
			return cache.ErrNotNew
		}
	}

	bytes, err := proto.Marshal(&val)
	if err != nil {
		return fmt.Errorf("unable to marshal new value: %v", err)
	}

	err = l.store.Put([]byte(key), bytes)
	if err != nil {
		return fmt.Errorf("unable to put value into store: %v", err)
	}

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

	return nil
}

// Subscribe .
func (l *lazarette) Subscribe(prefix string) (ch chan cache.KeyValue, unsubscribe func()) {
	ch = make(chan cache.KeyValue)

	unsubscribe = func() {
		l.subsMu.Lock()
		defer l.subsMu.Unlock()

		if chs, ok := l.subs[prefix]; ok {
			l.subs[prefix] = nil
			for i := range chs {
				if ch != chs[i] {
					l.subs[prefix] = append(l.subs[prefix], chs[i])
				}
			}
		}
	}

	l.subsMu.Lock()
	defer l.subsMu.Unlock()

	l.subs[prefix] = append(l.subs[prefix], ch)
	return ch, unsubscribe
}
