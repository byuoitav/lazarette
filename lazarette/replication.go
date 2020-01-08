package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"io"

	empty "github.com/golang/protobuf/ptypes/empty"
	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

type replication struct {
	*Replication
	LazaretteClient
	*zap.Logger
}

// UnsubscribeFunc .
type UnsubscribeFunc func()

// ReplicateWith .
func (c *Cache) ReplicateWith(ctx context.Context, repl *Replication) (*empty.Empty, error) {
	// TODO make the connection retry if it disconnects
	// TODO a way to stop a replication?
	conn, err := grpc.Dial(repl.GetRemoteAddr(), grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("unable to open connection: %w", err)
	}

	r := &replication{
		Replication:     repl,
		LazaretteClient: NewLazaretteClient(conn),
		Logger:          c.log.Named("repl-" + repl.GetRemoteAddr()),
	}

	r.Info("Started replication")

	c.replsMu.Lock()
	c.repls = append(c.repls, r)
	c.replsMu.Unlock()

	defer func() {
		c.replsMu.Lock()
		defer c.replsMu.Unlock()

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

			err = c.set(ctx, kv)
			if err != nil && !errors.Is(err, ErrNotNew) {
				c.log.Warn("unable to set key", zap.Error(err))
			}
		}
	}()

	// send all of our changes (and current state) to the remote address
	changes, unsub := c.SubscribeChan(r.GetPrefix())
	defer unsub()

	for change := range changes {
		_, err := r.Set(ctx, change)
		if err != nil {
			r.Warn("unable to update remote lazarette", zap.String("key", change.GetKey()))
		}
	}

	return &empty.Empty{}, nil
}
