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

// TODO there will be a loop if a remote cache replicates a local one,
// and the local one is also replicating the remote one

// TODO should this return a struct that you could stop the replication with?

// Replicate .
func (c *Cache) Replicate(ctx context.Context, prefix, addr string) error {
	// TODO make the connection retry if it disconnects

	// connect to remote cache
	conn, err := grpc.DialContext(ctx, addr, grpc.WithInsecure(), grpc.WithBlock())
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

	// subscribe to local changes
	s, err := c.NewSubscription(prefix)
	if err != nil {
		return fmt.Errorf("unable to subscribe to local lazarette: %w", err)
	}
	defer s.Stop()

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
				return
			case <-kill:
				return
			default:
				kv, err := rChanges.Recv()
				switch {
				case err == io.EOF:
					return
				case err != nil:
					rlog.Warn("something went wrong receiving change from remote", zap.Error(err))
					continue
				case kv == nil:
					continue
				}

				if err := c.setFromReplication(ctx, kv, s); err != nil && errors.Is(err, ErrNotNew) {
					rlog.Warn("unable to set key on local lazarette", zap.Error(err))
				}
			}
		}
	}()

	// send my changes to remote
	go func() {
		defer wg.Done()

		for {
			select {
			case <-ctx.Done():
				break
			case kv, ok := <-s.Changes():
				if !ok {
					// time to stop
					rlog.Info("Stopping replication")
					break
				}

				if _, err := remote.Set(ctx, kv); err != nil {
					rlog.Warn("unable to set key on remote cache", zap.Error(err))
				}
			}
		}
	}()

	wg.Wait()
	return nil
}
