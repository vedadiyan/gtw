package nats

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/vedadiyan/gtw/v2"
)

type (
	NatsClient struct {
		conn        *nats.Conn
		timeout     time.Duration
		publishOnly bool
	}

	NatsClientOption func(*NatsClient) error
)

// Client Options

func WithTimeout(timeout time.Duration) NatsClientOption {
	return func(c *NatsClient) error {
		c.timeout = timeout
		return nil
	}
}

func WithPublishOnly() NatsClientOption {
	return func(c *NatsClient) error {
		c.publishOnly = true
		return nil
	}
}

func WithRequestReply() NatsClientOption {
	return func(c *NatsClient) error {
		c.publishOnly = false
		return nil
	}
}

func WithNatsClientOptions(opts ...nats.Option) NatsClientOption {
	return func(c *NatsClient) error {
		conn, err := nats.Connect(c.conn.Opts.Url, opts...)
		if err != nil {
			return fmt.Errorf("failed to connect to NATS: %w", err)
		}
		// Close old connection if exists
		if c.conn != nil {
			c.conn.Close()
		}
		c.conn = conn
		return nil
	}
}

func NewClient(url string, opts ...NatsClientOption) (*NatsClient, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	client := &NatsClient{
		conn:        conn,
		timeout:     5 * time.Second, // Default timeout
		publishOnly: false,           // Default to request-reply
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(client); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return client, nil
}

func (nc *NatsClient) Call(p gtw.Pattern, msg *gtw.Message) (*gtw.Message, error) {
	// Export message to NATS format
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	// Get subject from pattern
	subject := p.Pattern()

	// Create request message
	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	// Handle based on client mode
	if nc.publishOnly {
		// Publish mode: fire-and-forget
		if err := nc.conn.PublishMsg(reqMsg); err != nil {
			return nil, fmt.Errorf("failed to publish: %w", err)
		}
		// Return empty message for publish-only mode
		return &gtw.Message{}, nil
	}

	// Request-Reply mode: wait for response
	respMsg, err := nc.conn.RequestMsg(reqMsg, nc.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Import response back to Message
	return gtw.Import((*gtw.NatsMsg)(respMsg))
}

// Publish sends a message without expecting a response (fire-and-forget)
func (nc *NatsClient) Publish(p gtw.Pattern, msg *gtw.Message) error {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	subject := p.Pattern()

	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	if err := nc.conn.PublishMsg(reqMsg); err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

// Close closes the NATS connection
func (nc *NatsClient) Close() error {
	if nc.conn != nil {
		nc.conn.Close()
	}
	return nil
}

// Drain drains the connection gracefully
func (nc *NatsClient) Drain() error {
	if nc.conn != nil {
		return nc.conn.Drain()
	}
	return nil
}

// IsConnected checks if the client is connected
func (nc *NatsClient) IsConnected() bool {
	return nc.conn != nil && nc.conn.IsConnected()
}
