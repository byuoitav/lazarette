package lazarette

import (
	"errors"
	fmt "fmt"
	"sync"

	"github.com/byuoitav/lazarette/store"
	proto "github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func (c *Cache) Subscribe(prefix *Key, stream Lazarette_SubscribeServer) error {
	if prefix == nil {
		return errors.New("prefix must not be nil")
	}

	s, err := c.NewSubscription(prefix.GetKey())
	if err != nil {
		return fmt.Errorf("unable to create subscription: %w", err)
	}
	defer s.Stop()

	for {
		select {
		case <-s.Done():
			break
		case kv := <-s.Changes():
			if err := stream.Send(kv); err != nil { // TODO should probably switch on the error
				return fmt.Errorf("unable to send %q to stream", kv.GetKey())
			}
		}
	}

	return nil
}

type Subscription struct {
	cache *Cache

	prefix  string
	changes chan *KeyValue
	once    sync.Once
	kill    chan struct{}
}

func (c *Cache) NewSubscription(prefix string) (*Subscription, error) {
	c.log.Info("Creating new subscription", zap.String("prefix", prefix))

	s := &Subscription{
		cache:   c,
		prefix:  prefix,
		changes: make(chan *KeyValue, 1),
		kill:    make(chan struct{}),
	}

	curKvs, err := c.store.GetPrefix([]byte(s.prefix))
	if err != nil {
		return nil, fmt.Errorf("unable to dump keys matching given prefix: %w", err)
	}

	// add this subscription to my subs map
	buddies, ok := c.subs.Load(s.prefix)
	if !ok {
		buddies = make([]*Subscription, 0)
	}

	buds := buddies.([]*Subscription)
	buds = append(buds, s)
	c.subs.Store(s.prefix, buds)

	// send all current keys to subscriber
	go s.sendBulk(curKvs)

	return s, nil
}

func (s *Subscription) sendBulk(kvs []store.KeyValue) {
	for i := range kvs {
		var v Value
		if err := proto.Unmarshal(kvs[i].Value, &v); err != nil {
			s.cache.log.Warn("unable to unmarshal value", zap.ByteString("key", kvs[i].Key), zap.Error(err))
			continue
		}

		// send value to subscriber
		s.send(&KeyValue{
			Key:       string(kvs[i].Key),
			Timestamp: v.GetTimestamp(),
			Data:      v.GetData(),
		})
	}
}

func (s *Subscription) send(kv *KeyValue) {
	select {
	case <-s.kill:
		return
	default:
		s.changes <- kv
	}
}

func (s *Subscription) Changes() chan *KeyValue {
	return s.changes
}

func (s *Subscription) Done() chan struct{} {
	return s.kill
}

func (s *Subscription) Stop() {
	s.once.Do(func() {
		buddies, ok := s.cache.subs.Load(s.prefix)
		if ok {
			buds := buddies.([]*Subscription)

			// remove myself from the slice
			var newBuds []*Subscription
			for i := range buds {
				if s == buds[i] {
					continue
				}

				newBuds = append(newBuds, buds[i])
			}

			if len(newBuds) > 0 {
				// put back all of my friends
				s.cache.subs.Store(s.prefix, newBuds)
			} else {
				s.cache.subs.Delete(s.prefix)
			}
		}

		// stop that goroutine's spawned by me
		close(s.kill)
	})
}
