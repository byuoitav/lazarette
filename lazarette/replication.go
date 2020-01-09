package lazarette

import (
	context "context"
	"errors"
	fmt "fmt"
	"io"
	"sync"

	"go.uber.org/zap"
	grpc "google.golang.org/grpc"
)

func (c *Cache) Replicate(ctx context.Context, prefix, addr string) error {
	// TODO make the connection retry if it disconnects

	// connect to remote cache
	conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		return fmt.Errorf("unable to connect to remote lazarette: %w", err)
	}
	defer conn.Close()

	remote := NewLazaretteClient(conn)
	rlog := c.log.Named(fmt.Sprintf("%s-repl", addr))

	// get all changes from remote cache
	rChanges, err := remote.Subscribe(ctx, &Key{Key: prefix})
	if err != nil {
		return fmt.Errorf("unable to subscribe to remote lazarette: %w", err)
	}

	rlog.Info("Replicating prefix", zap.String("prefix", prefix))

	// start read/write routines
	wg := sync.WaitGroup{}
	wg.Add(2)

	kill := make(chan struct{})

	// get changes coming from remote
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				fallthrough
			case <-kill:
				return
			default:
				kv, err := rChanges.Recv()
				switch {
				case err == io.EOF:
					return
				case err != nil:
					rlog.Warn("something went wrong receiving change from remote", zap.Error(err))
				case kv == nil:
					continue
				}

				if err := c.set(ctx, kv); err != nil && !errors.Is(err, ErrNotNew) {
					rlog.Warn("unable to set key on local cache", zap.Error(err))
				}
			}
		}
	}()

	// send my changes to remote
	go func() {
		defer wg.Done()

		// subscribe to my changes
		lChanges, unsub := c.SubscribeChan(prefix)
		defer unsub()

		for {
			select {
			case <-ctx.Done():
				return
			case change, ok := <-lChanges:
				if !ok {
					// time to stop
					rlog.Info("Stopping replication")
					return
				}

				if _, err := remote.Set(ctx, change); err != nil {
					rlog.Warn("unable to set key on remote cache", zap.Error(err))
				}
			}
		}
	}()

	wg.Wait()
	return nil
}
