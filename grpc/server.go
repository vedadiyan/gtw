package grpc

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"sync"

	"github.com/vedadiyan/gtw/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	GrpcServer struct {
		server      *grpc.Server
		listener    net.Listener
		handlers    map[string]gtw.MessageHandler
		isRunning   bool
		mut         sync.Mutex
		address     string
		serviceName string
	}
)

func (g *GrpcServer) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		g.mut.Lock()
		messageHandler, ok := g.handlers[info.FullMethod]
		g.mut.Unlock()

		if !ok {
			return nil, fmt.Errorf("no handler registered for method: %s", info.FullMethod)
		}

		md, _ := metadata.FromIncomingContext(ctx)

		payload, ok := req.([]byte)
		if !ok {
			return nil, fmt.Errorf("expected []byte payload, got %T", req)
		}

		grpcMsg := &gtw.GrpcMsg{
			Metadata: md,
			Payload:  payload,
		}

		msg, err := gtw.Import(grpcMsg)
		if err != nil {
			log.Printf("failed to import gRPC message: %v", err)
			return nil, err
		}

		res, err := messageHandler(msg)
		if err != nil {
			log.Printf("handler error: %v", err)
			return nil, err
		}

		if res == nil {
			log.Println("handler returned nil response")
			return nil, fmt.Errorf("handler returned nil response")
		}

		grpcRes, err := gtw.Export[gtw.GrpcMsg](res)
		if err != nil {
			log.Printf("failed to export response: %v", err)
			return nil, err
		}

		if len(grpcRes.Metadata) > 0 {
			if err := grpc.SendHeader(ctx, grpcRes.Metadata); err != nil {
				log.Printf("failed to send header: %v", err)
			}
		}

		return grpcRes.Payload, nil
	}
}

func (g *GrpcServer) streamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		g.mut.Lock()
		messageHandler, ok := g.handlers[info.FullMethod]
		g.mut.Unlock()

		if !ok {
			return fmt.Errorf("no handler registered for method: %s", info.FullMethod)
		}

		md, _ := metadata.FromIncomingContext(ss.Context())

		for {
			var payload []byte
			if err := ss.RecvMsg(&payload); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			grpcMsg := &gtw.GrpcMsg{
				Metadata: md,
				Payload:  payload,
			}

			msg, err := gtw.Import(grpcMsg)
			if err != nil {
				log.Printf("failed to import gRPC message: %v", err)
				return err
			}

			res, err := messageHandler(msg)
			if err != nil {
				log.Printf("handler error: %v", err)
				return err
			}

			if res == nil {
				return fmt.Errorf("handler returned nil response")
			}

			grpcRes, err := gtw.Export[gtw.GrpcMsg](res)
			if err != nil {
				log.Printf("failed to export response: %v", err)
				return err
			}

			if err := ss.SendMsg(grpcRes.Payload); err != nil {
				return err
			}
		}

		return nil
	}
}

func New(address string, serviceName string) (*GrpcServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	gs := &GrpcServer{
		listener:    listener,
		handlers:    make(map[string]gtw.MessageHandler),
		address:     address,
		serviceName: serviceName,
		isRunning:   false,
	}

	opts := []grpc.ServerOption{
		grpc.UnaryInterceptor(gs.unaryInterceptor()),
		grpc.StreamInterceptor(gs.streamInterceptor()),
	}

	gs.server = grpc.NewServer(opts...)

	return gs, nil
}

func (g *GrpcServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	fullMethod := fmt.Sprintf("/%s/%s", g.serviceName, p.Pattern())

	if _, exists := g.handlers[fullMethod]; exists {
		return fmt.Errorf("handler already registered for method: %s", fullMethod)
	}

	g.handlers[fullMethod] = fn
	return nil
}

func (g *GrpcServer) Start() error {
	g.mut.Lock()

	if g.isRunning {
		g.mut.Unlock()
		return gtw.ErrServerAlreadyRunning
	}

	if g.server == nil {
		g.mut.Unlock()
		return fmt.Errorf("gRPC server is not initialized")
	}

	if len(g.handlers) == 0 {
		g.mut.Unlock()
		return fmt.Errorf("no handlers registered")
	}

	g.isRunning = true
	g.mut.Unlock()

	if err := g.server.Serve(g.listener); err != nil {
		g.mut.Lock()
		g.isRunning = false
		g.mut.Unlock()
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func (g *GrpcServer) Stop(ctx context.Context) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if !g.isRunning {
		return gtw.ErrServerNotStarted
	}

	stopped := make(chan struct{})
	go func() {
		g.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		g.server.Stop()
		return fmt.Errorf("graceful shutdown timed out, forced stop")
	case <-stopped:
	}

	g.isRunning = false
	return nil
}
