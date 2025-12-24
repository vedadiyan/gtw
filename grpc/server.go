package grpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync"

	"github.com/vedadiyan/gtw/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type (
	GrpcServer struct {
		server      *grpc.Server
		listener    net.Listener
		handlers    *http.ServeMux
		isRunning   bool
		mut         sync.RWMutex
		address     string
		serviceName string
		grpcOptions []grpc.ServerOption
	}

	GrpcResponseWriter struct {
		header     http.Header
		statusCode int
		data       []byte
		method     string
	}

	Option func(*GrpcServer)
)

func NewGrpcResponseWriter(method string) *GrpcResponseWriter {
	return &GrpcResponseWriter{
		header:     make(http.Header),
		statusCode: http.StatusOK,
		method:     method,
	}
}

func (w *GrpcResponseWriter) Header() http.Header {
	return w.header
}

func (w *GrpcResponseWriter) Write(data []byte) (int, error) {
	w.data = append(w.data, data...)
	return len(data), nil
}

func (w *GrpcResponseWriter) WriteHeader(statusCode int) {
	if w.statusCode == 0 {
		w.statusCode = statusCode
	}
}

func (w *GrpcResponseWriter) GetStatusCode() int {
	return w.statusCode
}

func (w *GrpcResponseWriter) GetData() []byte {
	return w.data
}

func (w *GrpcResponseWriter) GetMethod() string {
	return w.method
}

func WithServerServiceName(serviceName string) Option {
	return func(gs *GrpcServer) {
		if serviceName != "" {
			gs.serviceName = serviceName
		}
	}
}

func WithGrpcOptions(opts ...grpc.ServerOption) Option {
	return func(gs *GrpcServer) {
		gs.grpcOptions = append(gs.grpcOptions, opts...)
	}
}

func (g *GrpcServer) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if info == nil {
			return nil, fmt.Errorf("server info is nil")
		}

		md, _ := metadata.FromIncomingContext(ctx)

		var grpcMsg gtw.GrpcMsg
		switch v := req.(type) {
		case []byte:
			if err := json.Unmarshal(v, &grpcMsg); err != nil {
				log.Printf("failed to unmarshal gRPC message: %v", err)
				return nil, fmt.Errorf("failed to unmarshal message: %w", err)
			}
		default:
			return nil, fmt.Errorf("expected []byte payload, got %T", req)
		}

		httpReq, err := http.NewRequest(http.MethodPost, info.FullMethod, nil)
		if err != nil {
			log.Printf("failed to create request: %v", err)
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Convert gRPC metadata to HTTP headers
		for key, values := range md {
			for _, value := range values {
				httpReq.Header.Add(key, value)
			}
		}

		httpHandler, _ := g.handlers.Handler(httpReq)
		if httpHandler == nil {
			log.Printf("no handler found for method: %s", info.FullMethod)
			return nil, fmt.Errorf("no handler registered for method: %s", info.FullMethod)
		}

		gtwReq, err := gtw.Import((*gtw.HttpRequest)(httpReq))
		if err != nil {
			log.Printf("failed to import request: %v", err)
			return nil, fmt.Errorf("failed to import request: %w", err)
		}

		if gtwReq == nil {
			log.Println("import returned nil request")
			return nil, fmt.Errorf("import returned nil request")
		}

		writer := NewGrpcResponseWriter(info.FullMethod)
		httpHandler.ServeHTTP(writer, httpReq)

		// Convert HTTP headers back to gRPC metadata
		if len(writer.Header()) > 0 {
			outMd := metadata.New(nil)
			for key, values := range writer.Header() {
				outMd.Set(key, values...)
			}
			if err := grpc.SendHeader(ctx, outMd); err != nil {
				log.Printf("failed to send header: %v", err)
			}
		}

		return writer.GetData(), nil
	}
}

func (g *GrpcServer) streamInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		if ss == nil {
			return fmt.Errorf("server stream is nil")
		}

		if info == nil {
			return fmt.Errorf("stream server info is nil")
		}

		md, _ := metadata.FromIncomingContext(ss.Context())

		httpReq, err := http.NewRequest(http.MethodPost, info.FullMethod, nil)
		if err != nil {
			log.Printf("failed to create request: %v", err)
			return fmt.Errorf("failed to create request: %w", err)
		}

		// Convert gRPC metadata to HTTP headers
		for key, values := range md {
			for _, value := range values {
				httpReq.Header.Add(key, value)
			}
		}

		httpHandler, _ := g.handlers.Handler(httpReq)
		if httpHandler == nil {
			log.Printf("no handler found for method: %s", info.FullMethod)
			return fmt.Errorf("no handler registered for method: %s", info.FullMethod)
		}

		for {
			var payload []byte
			if err := ss.RecvMsg(&payload); err != nil {
				if err == io.EOF {
					break
				}
				return err
			}

			var grpcMsg gtw.GrpcMsg
			if err := json.Unmarshal(payload, &grpcMsg); err != nil {
				log.Printf("failed to unmarshal gRPC message: %v", err)
				return fmt.Errorf("failed to unmarshal message: %w", err)
			}

			gtwReq, err := gtw.Import((*gtw.HttpRequest)(httpReq))
			if err != nil {
				log.Printf("failed to import request: %v", err)
				return fmt.Errorf("failed to import request: %w", err)
			}

			if gtwReq == nil {
				log.Println("import returned nil request")
				return fmt.Errorf("import returned nil request")
			}

			writer := NewGrpcResponseWriter(info.FullMethod)
			httpHandler.ServeHTTP(writer, httpReq)

			if err := ss.SendMsg(writer.GetData()); err != nil {
				return err
			}
		}

		return nil
	}
}

func New(address string, opts ...Option) (*GrpcServer, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, fmt.Errorf("failed to listen on %s: %w", address, err)
	}

	gs := &GrpcServer{
		listener:    listener,
		handlers:    http.NewServeMux(),
		address:     address,
		serviceName: "default",
		isRunning:   false,
		grpcOptions: make([]grpc.ServerOption, 0),
	}

	// Apply options
	for _, opt := range opts {
		opt(gs)
	}

	// Add interceptors to gRPC options
	serverOpts := append(gs.grpcOptions,
		grpc.UnaryInterceptor(gs.unaryInterceptor()),
		grpc.StreamInterceptor(gs.streamInterceptor()),
	)

	gs.server = grpc.NewServer(serverOpts...)

	return gs, nil
}

func (g *GrpcServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	g.mut.Lock()
	defer g.mut.Unlock()

	if g.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	method := p.Pattern()
	if method == "" {
		return fmt.Errorf("pattern cannot be empty")
	}

	if fn == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	fullMethod := fmt.Sprintf("/%s/%s", g.serviceName, method)

	g.handlers.HandleFunc(fullMethod, func(w http.ResponseWriter, r *http.Request) {
		if r == nil {
			log.Println("received nil request")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		req, err := gtw.Import((*gtw.HttpRequest)(r))
		if err != nil {
			log.Printf("failed to import request: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if req == nil {
			log.Println("import returned nil request")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		res, err := fn(req)
		if err != nil {
			log.Printf("handler error: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if res == nil {
			log.Println("handler returned nil response")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if res.Header() != nil {
			for key, values := range res.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
		}

		if res.Status() != 0 {
			w.WriteHeader(res.Status())
		}

		if _, err := res.WriteTo(w); err != nil {
			log.Printf("failed to write response: %v", err)
		}
	})

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

	if g.listener == nil {
		g.mut.Unlock()
		return fmt.Errorf("listener is not initialized")
	}

	g.isRunning = true
	g.mut.Unlock()

	log.Printf("gRPC server started on %s with service name '%s'", g.address, g.serviceName)

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

	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	stopped := make(chan struct{})
	go func() {
		g.server.GracefulStop()
		close(stopped)
	}()

	select {
	case <-ctx.Done():
		g.server.Stop()
		log.Println("gRPC graceful shutdown timed out, forced stop")
		return fmt.Errorf("graceful shutdown timed out, forced stop")
	case <-stopped:
		log.Println("gRPC server stopped gracefully")
	}

	g.isRunning = false
	return nil
}

func (g *GrpcServer) GetServiceName() string {
	g.mut.RLock()
	defer g.mut.RUnlock()
	return g.serviceName
}

func (g *GrpcServer) GetAddress() string {
	g.mut.RLock()
	defer g.mut.RUnlock()
	return g.address
}
