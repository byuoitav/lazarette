package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"

	proto "github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

var ErrKeyNotFound = errors.New("key not found in cache")

// Get .
func (c *Cache) Get(ctx context.Context, key *Key) (*Value, error) {
	if key == nil {
		return nil, errors.New("key must not be nil")
	}

	c.log.Info("Getting", zap.String("key", key.GetKey()))

	data, err := c.store.Get([]byte(key.GetKey()))
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

	c.log.Debug("Successfully got", zap.String("key", key.GetKey()), zap.ByteString("value", val.GetData()))
	return val, nil
}
