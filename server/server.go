package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"

	"github.com/byuoitav/lazarette/lazarette"
	"github.com/byuoitav/lazarette/log"
	"github.com/byuoitav/lazarette/server/handlers"
	"github.com/labstack/echo"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// Server .
type Server struct {
	Cache *lazarette.Cache

	grpc *grpc.Server
	echo *echo.Echo
}

// Serve .
func (s *Server) Serve(grpcAddr string, httpAddr string) error {
	if len(grpcAddr) == 0 && len(httpAddr) == 0 {
		return errors.New("must pass at least one address to bind to")
	}

	wg := &sync.WaitGroup{}

	if len(grpcAddr) > 0 {
		grpcLis, err := net.Listen("tcp", grpcAddr)
		if err != nil {
			return fmt.Errorf("unable to bind grpc listener: %v", err)
		}

		wg.Add(1)
		go s.serveGRPC(grpcLis, wg)

		log.P.Info("Started grpc server", zap.String("address", grpcLis.Addr().String()))
	}

	if len(httpAddr) > 0 {
		httpLis, err := net.Listen("tcp", httpAddr)
		if err != nil {
			return fmt.Errorf("unable to bind http listener: %v", err)
		}

		wg.Add(1)
		go s.serveHTTP(httpLis, wg)

		log.P.Info("Started http server", zap.String("address", httpLis.Addr().String()))
	}

	wg.Wait()
	log.P.Info("Lazarette server shut down")

	return nil
}

func (s *Server) serveGRPC(l net.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	s.grpc = grpc.NewServer()
	lazarette.RegisterLazaretteServer(s.grpc, s.Cache)

	if err := s.grpc.Serve(l); err != nil {
		log.P.Fatal("failed to serve grpc", zap.Error(err))
	}
}

func (s *Server) serveHTTP(l net.Listener, wg *sync.WaitGroup) {
	defer wg.Done()

	s.echo = echo.New()
	s.echo.HideBanner = true
	s.echo.HidePort = true
	s.echo.Listener = l

	s.echo.GET("/cache/:key", handlers.GetKey(s.Cache))
	s.echo.PUT("/cache/:key", handlers.SetKey(s.Cache))

	if err := s.echo.Start(""); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.P.Fatal("failed to serve http", zap.Error(err))
	}
}

// Stop .
func (s *Server) Stop(ctx context.Context) error {
	if s.grpc != nil {
		s.grpc.Stop()
	}

	if s.echo != nil {
		err := s.echo.Shutdown(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}
