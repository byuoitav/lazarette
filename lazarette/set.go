package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"strings"

	proto "github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes"
	empty "github.com/golang/protobuf/ptypes/empty"
	"go.uber.org/zap"
)

func (c *Cache) Set(ctx context.Context, kv *KeyValue) (*empty.Empty, error) {
	err := c.set(ctx, kv)
	if err != nil {
		c.log.Warn("unable to set", zap.Error(err))
		return nil, err
	}

	// let *all* subscriptions know that this key was updated
	c.subs.Range(func(key, value interface{}) bool {
		prefix := key.(string)

		if strings.HasPrefix(kv.GetKey(), prefix) {
			// send it to all of these subscribers
			subs := value.([]*Subscription)

			for i := range subs {
				// TODO make sure this doesn't block
				subs[i].send(kv)
			}
		}

		return true
	})

	return &empty.Empty{}, nil
}

func (c *Cache) setFromReplication(ctx context.Context, kv *KeyValue, skip *Subscription) error {
	err := c.set(ctx, kv)
	if err != nil {
		return err
	}

	// let subscriptions know that this key was updated
	// but we have to skip the replication that sent this key to us
	c.subs.Range(func(key, value interface{}) bool {
		prefix := key.(string)

		if strings.HasPrefix(kv.GetKey(), prefix) {
			// send it to all of these subscribers
			subs := value.([]*Subscription)

			for i := range subs {
				if skip == subs[i] {
					continue
				}

				// TODO make sure this doesn't block
				subs[i].send(kv)
			}
		}

		return true
	})

	return nil
}

func (c *Cache) set(ctx context.Context, kv *KeyValue) error {
	switch {
	case kv == nil:
		return errors.New("kv must not be nil")
	case kv.GetTimestamp() == nil:
		return errors.New("timestamp must not be nil")
	}

	c.log.Info("Setting", zap.String("key", kv.GetKey()), zap.ByteString("value", kv.GetData()))

	// get the current val
	cur, err := c.Get(ctx, &Key{Key: kv.GetKey()})
	switch {
	case errors.Is(err, ErrKeyNotFound):
	case err != nil:
		return fmt.Errorf("unable to get current value: %w", err)
	}

	if cur != nil && cur.GetTimestamp() != nil {
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

	err = c.store.Set([]byte(kv.GetKey()), bytes)
	if err != nil {
		return fmt.Errorf("unable to put value into store: %w", err)
	}

	c.log.Debug("Successfully set", zap.String("key", kv.GetKey()))
	return nil
}
