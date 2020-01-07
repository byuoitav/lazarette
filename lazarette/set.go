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
		c.log.Warn("failed to set", zap.Error(err))
		return nil, err
	}

	// notify all subscriptions that this key has updated
	// TODO add some timeout so that one slow connection doesn't hold things up?
	c.subsMu.RLock()
	for prefix, chans := range c.subs {
		if strings.HasPrefix(kv.GetKey(), prefix) {
			for i := range chans {
				chans[i] <- kv
			}
		}
	}
	c.subsMu.RUnlock()

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

	err = c.store.Set([]byte(kv.GetKey()), bytes)
	if err != nil {
		return fmt.Errorf("unable to put value into store: %w", err)
	}

	c.log.Debug("Successfully set", zap.String("key", kv.GetKey()))
	return nil
}
