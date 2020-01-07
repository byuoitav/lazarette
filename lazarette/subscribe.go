package lazarette

import (
	"errors"

	proto "github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

func (c *Cache) Subscribe(prefix *Key, stream Lazarette_SubscribeServer) error {
	if prefix == nil {
		return errors.New("prefix must not be nil")
	}

	ch, unsub := c.SubscribeChan(prefix.GetKey())
	for kv := range ch {
		c.log.Info("[subscribe] Sending key " + kv.GetKey())
		err := stream.Send(kv)
		if err != nil { // TODO should probably switch on the error
			c.log.Warn("failed to send key to stream", zap.Error(err), zap.String("key", kv.GetKey()))
			unsub()
			return err
		}
	}

	return nil
}

// SubscribeChan .
func (c *Cache) SubscribeChan(prefix string) (chan *KeyValue, UnsubscribeFunc) {
	ch := make(chan *KeyValue, 16)
	c.log.Info("Subscribing to", zap.String("prefix", prefix))

	go func() {
		// get a dump of the cache, send that down the stream
		kvs, err := c.store.GetPrefix([]byte(prefix))
		if err != nil {
			c.log.Warn("unable to get prefix dump", zap.String("prefix", prefix), zap.Error(err))
			return
		}

		for i := range kvs {
			var v Value
			if err := proto.Unmarshal(kvs[i].Value, &v); err != nil {
				c.log.Warn("unable to unmarshal value", zap.ByteString("key", kvs[i].Key), zap.Error(err))
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
		c.log.Info("Unsubscribing from", zap.String("prefix", prefix))
		c.subsMu.Lock()
		defer c.subsMu.Unlock()

		if chs, ok := c.subs[prefix]; ok {
			c.subs[prefix] = nil
			for i := range chs {
				if ch != chs[i] {
					c.subs[prefix] = append(c.subs[prefix], chs[i])
				}
			}
		}

		close(ch)
	}

	c.subsMu.Lock()
	c.subs[prefix] = append(c.subs[prefix], ch)
	c.subsMu.Unlock()

	return ch, unsubscribe
}
