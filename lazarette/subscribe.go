package lazarette

import (
	"errors"
	fmt "fmt"

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

	for kv := range s.Changes() {
		if err := stream.Send(kv); err != nil { // TODO should probably switch on the error
			return fmt.Errorf("unable to send %q to stream", kv.GetKey())
		}
	}

	return nil
}

type Subscription struct {
	cache  *Cache
	prefix string
	kvs    chan *KeyValue
}

func (c *Cache) NewSubscription(prefix string) (*Subscription, error) {
	c.log.Info("Creating new subscription", zap.String("prefix", prefix))

	s := &Subscription{
		cache:  c,
		prefix: prefix,
		kvs:    make(chan *KeyValue, 1),
	}

	curKvs, err := c.store.GetPrefix([]byte(s.prefix))
	if err != nil {
		return nil, fmt.Errorf("unable to dump keys matching given prefix: %w", err)
	}

	// add this subscription to my subs map
	buddies, ok := c.subs.Load(s.prefix)
	if !ok {
		buddies = make([]*Subscription, 1)
	}

	buds := buddies.([]*Subscription)
	buds = append(buds, s)
	c.subs.Store(s.prefix, buds)

	// send all current keys to subscriber
	go func() {
		for i := range curKvs {
			var v Value
			if err := proto.Unmarshal(curKvs[i].Value, &v); err != nil {
				c.log.Warn("unable to unmarshal value", zap.ByteString("key", curKvs[i].Key), zap.Error(err))
				continue
			}

			// TODO make sure we haven't closed yet

			// send value to subscriber
			s.kvs <- &KeyValue{
				Key:       string(curKvs[i].Key),
				Timestamp: v.GetTimestamp(),
				Data:      v.GetData(),
			}
		}
	}()

	return s, nil
}

func (s *Subscription) Changes() chan *KeyValue {
	return s.kvs
}

func (s *Subscription) Stop() error {
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

		// put back all of my friends
		s.cache.subs.Store(s.prefix, newBuds)
	}

	// TODO stop that one goroutine

	return nil
}
