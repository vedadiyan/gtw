package grpc

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/vedadiyan/gtw/v2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type (
	GrpcClient struct {
		conn         *grpc.ClientConn
		timeout      time.Duration
		serviceName  string
		useStreaming bool
	}

	GrpcClientOption func(*GrpcClient) error
)

// Client Options

func WithTimeout(timeout time.Duration) GrpcClientOption {
	return func(c *GrpcClient) error {
		c.timeout = timeout
		return nil
	}
}

func WithClientServiceName(serviceName string) GrpcClientOption {
	return func(c *GrpcClient) error {
		c.serviceName = serviceName
		return nil
	}
}

func WithStreaming() GrpcClientOption {
	return func(c *GrpcClient) error {
		c.useStreaming = true
		return nil
	}
}

func WithUnary() GrpcClientOption {
	return func(c *GrpcClient) error {
		c.useStreaming = false
		return nil
	}
}

func WithGrpcDialOptions(opts ...grpc.DialOption) GrpcClientOption {
	return func(c *GrpcClient) error {
		// Close existing connection if any
		if c.conn != nil {
			c.conn.Close()
		}

		// Extract target from existing connection or use default
		target := c.conn.Target()
		if target == "" {
			return fmt.Errorf("no target address available")
		}

		conn, err := grpc.NewClient(target, opts...)
		if err != nil {
			return fmt.Errorf("failed to create gRPC connection: %w", err)
		}
		c.conn = conn
		return nil
	}
}

func WithInsecure() GrpcClientOption {
	return func(c *GrpcClient) error {
		return nil // Will be applied during dial
	}
}

func NewClient(target string, opts ...GrpcClientOption) (*GrpcClient, error) {
	client := &GrpcClient{
		timeout:      5 * time.Second,
		serviceName:  "default",
		useStreaming: false,
	}

	// Apply options first to get configuration
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	// Create connection with insecure credentials by default
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to create gRPC connection: %w", err)
	}
	client.conn = conn

	return client, nil
}

func (gc *GrpcClient) Call(p gtw.Pattern, msg gtw.Message) (gtw.Message, error) {
	// Export message to gRPC format
	grpcMsg, err := gtw.Export[gtw.GrpcMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	// Construct full method name: /ServiceName/MethodName
	method := fmt.Sprintf("/%s/%s", gc.serviceName, p.Pattern())

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), gc.timeout)
	defer cancel()

	// Add metadata to context
	if len(grpcMsg.Metadata) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, grpcMsg.Metadata)
	}

	if gc.useStreaming {
		// Use streaming RPC
		return gc.callStreaming(ctx, method, grpcMsg)
	}

	// Use unary RPC
	return gc.callUnary(ctx, method, grpcMsg)
}

func (gc *GrpcClient) callUnary(ctx context.Context, method string, grpcMsg *gtw.GrpcMsg) (gtw.Message, error) {
	var response []byte

	err := gc.conn.Invoke(ctx, method, grpcMsg.Payload, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to invoke method %s: %w", method, err)
	}

	// Extract response metadata
	var responseMD metadata.MD
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		responseMD = md
	}

	// Create response GrpcMsg
	respGrpcMsg := &gtw.GrpcMsg{
		Metadata: responseMD,
		Payload:  response,
	}

	// Import back to Message
	return gtw.Import(respGrpcMsg)
}

func (gc *GrpcClient) callStreaming(ctx context.Context, method string, grpcMsg *gtw.GrpcMsg) (gtw.Message, error) {
	// Create stream descriptor
	desc := &grpc.StreamDesc{
		StreamName:    method,
		ClientStreams: true,
		ServerStreams: true,
	}

	// Create bidirectional stream
	stream, err := gc.conn.NewStream(ctx, desc, method)
	if err != nil {
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	// Send request
	if err := stream.SendMsg(grpcMsg.Payload); err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	// Close send direction
	if err := stream.CloseSend(); err != nil {
		return nil, fmt.Errorf("failed to close send: %w", err)
	}

	// Receive response
	var response []byte
	if err := stream.RecvMsg(&response); err != nil {
		if err == io.EOF {
			return nil, fmt.Errorf("stream closed without response")
		}
		return nil, fmt.Errorf("failed to receive message: %w", err)
	}

	// Extract response metadata
	responseMD, err := stream.Header()
	if err != nil {
		responseMD = metadata.MD{}
	}

	// Create response GrpcMsg
	respGrpcMsg := &gtw.GrpcMsg{
		Metadata: responseMD,
		Payload:  response,
	}

	// Import back to Message
	return gtw.Import(respGrpcMsg)
}

// UnaryCall performs a unary RPC call
func (gc *GrpcClient) UnaryCall(method string, msg gtw.Message) (gtw.Message, error) {
	grpcMsg, err := gtw.Export[gtw.GrpcMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	fullMethod := fmt.Sprintf("/%s/%s", gc.serviceName, method)

	ctx, cancel := context.WithTimeout(context.Background(), gc.timeout)
	defer cancel()

	if len(grpcMsg.Metadata) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, grpcMsg.Metadata)
	}

	return gc.callUnary(ctx, fullMethod, grpcMsg)
}

// StreamingCall performs a streaming RPC call
func (gc *GrpcClient) StreamingCall(method string, msg gtw.Message) (gtw.Message, error) {
	grpcMsg, err := gtw.Export[gtw.GrpcMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	fullMethod := fmt.Sprintf("/%s/%s", gc.serviceName, method)

	ctx, cancel := context.WithTimeout(context.Background(), gc.timeout)
	defer cancel()

	if len(grpcMsg.Metadata) > 0 {
		ctx = metadata.NewOutgoingContext(ctx, grpcMsg.Metadata)
	}

	return gc.callStreaming(ctx, fullMethod, grpcMsg)
}

// Close closes the gRPC connection
func (gc *GrpcClient) Close() error {
	if gc.conn != nil {
		return gc.conn.Close()
	}
	return nil
}

// GetConnection returns the underlying gRPC connection
func (gc *GrpcClient) GetConnection() *grpc.ClientConn {
	return gc.conn
}

// IsConnected checks if the client is connected
func (gc *GrpcClient) IsConnected() bool {
	if gc.conn == nil {
		return false
	}
	state := gc.conn.GetState()
	return state == connectivity.Ready || state == connectivity.Connecting
}
