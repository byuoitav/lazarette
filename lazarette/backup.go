package lazarette

import (
	fmt "fmt"
	"time"

	"go.uber.org/zap"
)

func (c *Cache) persist() {
	t := time.NewTicker(c.interval)
	defer t.Stop()

	for {
		select {
		case <-t.C:
			c.log.Info("Backing up cache to persistant store")

			kvs, err := c.store.Dump()
			if err != nil {
				c.log.Warn("failed to dump from memory store", zap.Error(err))
				continue
			}

			if err = c.pStore.Clean(); err != nil {
				c.log.Warn("failed to clear persistent store", zap.Error(err))
				continue
			}

			for _, kv := range kvs {
				if err = c.pStore.Set(kv.Key, kv.Value); err != nil {
					c.log.Warn("unable to backup item", zap.ByteString("key", kv.Key), zap.Error(err))
					continue
				}
			}
		case <-c.kill:
			return
		}
	}
}

func (c *Cache) restore() error {
	c.log.Info("Restoring cache from persistent storage")

	kvs, err := c.pStore.Dump()
	if err != nil {
		return fmt.Errorf("unable to get persistant store dump: %w", err)
	}

	if err = c.store.Clean(); err != nil {
		return fmt.Errorf("unable to get clear store: %w", err)
	}

	errCount := 0

	for _, kv := range kvs {
		if err = c.store.Set(kv.Key, kv.Value); err != nil {
			c.log.Warn("unable to restore item", zap.ByteString("key", kv.Key), zap.Error(err))
			errCount++
			continue
		}
	}

	if errCount > 0 {
		return fmt.Errorf("encountered %v error(s) restoring data from persistent storage", errCount)
	}

	return nil
}
