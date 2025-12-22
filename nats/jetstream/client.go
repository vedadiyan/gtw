package jetstream

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vedadiyan/gtw/v2"
)

type (
	NatsJetStreamClient struct {
		conn        *nats.Conn
		js          jetstream.JetStream
		timeout     time.Duration
		publishOnly bool
		useAsync    bool
	}

	NatsJetStreamClientOption func(*NatsJetStreamClient) error
)

func WithTimeout(timeout time.Duration) NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		c.timeout = timeout
		return nil
	}
}

func WithPublishOnly() NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		c.publishOnly = true
		return nil
	}
}

func WithRequestReply() NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		c.publishOnly = false
		return nil
	}
}

func WithAsyncPublish() NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		c.useAsync = true
		return nil
	}
}

func WithSyncPublish() NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		c.useAsync = false
		return nil
	}
}

func WithNatsJetStreamClientOptions(opts ...nats.Option) NatsJetStreamClientOption {
	return func(c *NatsJetStreamClient) error {
		conn, err := nats.Connect(c.conn.Opts.Url, opts...)
		if err != nil {
			return fmt.Errorf("failed to connect to NATS: %w", err)
		}
		if c.conn != nil {
			c.conn.Close()
		}
		c.conn = conn

		js, err := jetstream.New(conn)
		if err != nil {
			conn.Close()
			return fmt.Errorf("failed to create JetStream context: %w", err)
		}
		c.js = js
		return nil
	}
}

func NewClient(url string, opts ...NatsJetStreamClientOption) (*NatsJetStreamClient, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}

	client := &NatsJetStreamClient{
		conn:        conn,
		js:          js,
		timeout:     5 * time.Second,
		publishOnly: false,
		useAsync:    false,
	}

	for _, opt := range opts {
		if err := opt(client); err != nil {
			conn.Close()
			return nil, err
		}
	}

	return client, nil
}

func (jc *NatsJetStreamClient) Call(p gtw.Pattern, msg gtw.Message) (gtw.Message, error) {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	subject := p.Pattern()

	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	if jc.publishOnly {
		if jc.useAsync {
			ack, err := jc.js.PublishMsgAsync(reqMsg)
			if err != nil {
				return nil, fmt.Errorf("failed to publish async: %w", err)
			}
			if err := <-ack.Err(); err != nil {
				return nil, fmt.Errorf("failed to publish async: %w", err)
			}
		} else {
			_, err := jc.js.PublishMsg(context.Background(), reqMsg)
			if err != nil {
				return nil, fmt.Errorf("failed to publish: %w", err)
			}
		}
		return &gtw.GenericMessage{}, nil
	}

	respMsg, err := jc.conn.RequestMsg(reqMsg, jc.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return gtw.Import((*gtw.NatsMsg)(respMsg))
}

func (jc *NatsJetStreamClient) Publish(p gtw.Pattern, msg gtw.Message) (*jetstream.PubAck, error) {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	subject := p.Pattern()

	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	ack, err := jc.js.PublishMsg(context.Background(), reqMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish: %w", err)
	}

	return ack, nil
}

func (jc *NatsJetStreamClient) PublishAsync(p gtw.Pattern, msg gtw.Message) (jetstream.PubAckFuture, error) {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	subject := p.Pattern()

	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	future, err := jc.js.PublishMsgAsync(reqMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish async: %w", err)
	}

	return future, nil
}

func (jc *NatsJetStreamClient) Request(p gtw.Pattern, msg gtw.Message) (gtw.Message, error) {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	subject := p.Pattern()

	reqMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	respMsg, err := jc.conn.RequestMsg(reqMsg, jc.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	return gtw.Import((*gtw.NatsMsg)(respMsg))
}

func (jc *NatsJetStreamClient) Close() error {
	if jc.conn != nil {
		jc.conn.Close()
	}
	return nil
}

func (jc *NatsJetStreamClient) Drain() error {
	if jc.conn != nil {
		return jc.conn.Drain()
	}
	return nil
}

func (jc *NatsJetStreamClient) IsConnected() bool {
	return jc.conn != nil && jc.conn.IsConnected()
}
