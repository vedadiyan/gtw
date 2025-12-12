package jetstream

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vedadiyan/gtw/v2"
)

type (
	NatsJetStreamServer struct {
		conn          *nats.Conn
		js            jetstream.JetStream
		consumers     map[string]jetstream.ConsumeContext
		streams       map[string]jetstream.Stream
		isRunning     bool
		mut           sync.Mutex
		url           string
		streamConfig  *jetstream.StreamConfig
		consumerGroup string
	}

	JetStreamConfig struct {
		StreamName    string
		Subjects      []string
		StorageType   jetstream.StorageType
		Retention     jetstream.RetentionPolicy
		MaxMsgs       int64
		MaxBytes      int64
		MaxAge        time.Duration
		Replicas      int
		ConsumerGroup string
	}

	// Option pattern for server configuration
	ServerOption func(*NatsJetStreamServer) error

	// Option pattern for consumer configuration
	ConsumerOption func(*consumerOptions)

	consumerOptions struct {
		name       string
		durable    string
		ackPolicy  jetstream.AckPolicy
		maxDeliver int
		ackWait    time.Duration
	}
)

// Default configuration
func DefaultJetStreamConfig(streamName string) *JetStreamConfig {
	return &JetStreamConfig{
		StreamName:    streamName,
		Subjects:      []string{streamName + ".*"},
		StorageType:   jetstream.FileStorage,
		Retention:     jetstream.WorkQueuePolicy,
		MaxMsgs:       1000000,
		MaxBytes:      1024 * 1024 * 1024, // 1GB
		MaxAge:        24 * time.Hour,
		Replicas:      1,
		ConsumerGroup: "default",
	}
}

// Server Options

func WithNatsOptions(opts ...nats.Option) ServerOption {
	return func(s *NatsJetStreamServer) error {
		conn, err := nats.Connect(s.url, opts...)
		if err != nil {
			return fmt.Errorf("failed to connect to NATS: %w", err)
		}
		s.conn = conn
		return nil
	}
}

func WithStreamName(name string) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.Name = name
		}
		return nil
	}
}

func WithStreamSubjects(subjects []string) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.Subjects = subjects
		}
		return nil
	}
}

func WithStorageType(storageType jetstream.StorageType) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.Storage = storageType
		}
		return nil
	}
}

func WithRetentionPolicy(retention jetstream.RetentionPolicy) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.Retention = retention
		}
		return nil
	}
}

func WithMaxMsgs(maxMsgs int64) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.MaxMsgs = maxMsgs
		}
		return nil
	}
}

func WithMaxBytes(maxBytes int64) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.MaxBytes = maxBytes
		}
		return nil
	}
}

func WithMaxAge(maxAge time.Duration) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.MaxAge = maxAge
		}
		return nil
	}
}

func WithReplicas(replicas int) ServerOption {
	return func(s *NatsJetStreamServer) error {
		if s.streamConfig != nil {
			s.streamConfig.Replicas = replicas
		}
		return nil
	}
}

func WithConsumerGroup(group string) ServerOption {
	return func(s *NatsJetStreamServer) error {
		s.consumerGroup = group
		return nil
	}
}

// Consumer Options

func WithConsumerName(name string) ConsumerOption {
	return func(o *consumerOptions) {
		o.name = name
	}
}

func WithDurableName(durable string) ConsumerOption {
	return func(o *consumerOptions) {
		o.durable = durable
	}
}

func WithAckPolicy(policy jetstream.AckPolicy) ConsumerOption {
	return func(o *consumerOptions) {
		o.ackPolicy = policy
	}
}

func WithMaxDeliver(maxDeliver int) ConsumerOption {
	return func(o *consumerOptions) {
		o.maxDeliver = maxDeliver
	}
}

func WithAckWait(ackWait time.Duration) ConsumerOption {
	return func(o *consumerOptions) {
		o.ackWait = ackWait
	}
}

func defaultConsumerOptions(subject, group string) *consumerOptions {
	return &consumerOptions{
		name:       fmt.Sprintf("%s_%s_consumer", group, subject),
		durable:    fmt.Sprintf("%s_%s_durable", group, subject),
		ackPolicy:  jetstream.AckExplicitPolicy,
		maxDeliver: 3,
		ackWait:    30 * time.Second,
	}
}

func New(url string, config *JetStreamConfig, opts ...ServerOption) (*NatsJetStreamServer, error) {
	if config == nil {
		config = DefaultJetStreamConfig("DEFAULT_STREAM")
	}

	streamConfig := jetstream.StreamConfig{
		Name:      config.StreamName,
		Subjects:  config.Subjects,
		Storage:   config.StorageType,
		Retention: config.Retention,
		MaxMsgs:   config.MaxMsgs,
		MaxBytes:  config.MaxBytes,
		MaxAge:    config.MaxAge,
		Replicas:  config.Replicas,
	}

	ns := &NatsJetStreamServer{
		consumers:     make(map[string]jetstream.ConsumeContext),
		streams:       make(map[string]jetstream.Stream),
		url:           url,
		consumerGroup: config.ConsumerGroup,
		streamConfig:  &streamConfig,
		isRunning:     false,
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(ns); err != nil {
			return nil, err
		}
	}

	// Connect if not already connected via options
	if ns.conn == nil {
		conn, err := nats.Connect(url)
		if err != nil {
			return nil, fmt.Errorf("failed to connect to NATS: %w", err)
		}
		ns.conn = conn
	}

	// Create JetStream context
	js, err := jetstream.New(ns.conn)
	if err != nil {
		ns.conn.Close()
		return nil, fmt.Errorf("failed to create JetStream context: %w", err)
	}
	ns.js = js

	// Create or update stream
	stream, err := js.CreateOrUpdateStream(context.Background(), *ns.streamConfig)
	if err != nil {
		ns.conn.Close()
		return nil, fmt.Errorf("failed to create stream: %w", err)
	}

	ns.streams[ns.streamConfig.Name] = stream

	return ns, nil
}

func (n *NatsJetStreamServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if n.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	subject := p.Pattern()

	if _, exists := n.consumers[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	return nil
}

func (n *NatsJetStreamServer) Start() error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if n.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	if n.conn == nil || !n.conn.IsConnected() {
		return fmt.Errorf("NATS connection is not established")
	}

	if n.js == nil {
		return fmt.Errorf("JetStream context is not initialized")
	}

	n.isRunning = true
	return nil
}

func (n *NatsJetStreamServer) CreateConsumer(subject string, fn gtw.MessageHandler, opts ...ConsumerOption) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if _, exists := n.consumers[subject]; exists {
		return fmt.Errorf("consumer already exists for subject: %s", subject)
	}

	var stream jetstream.Stream
	for _, s := range n.streams {
		stream = s
		break
	}

	if stream == nil {
		return fmt.Errorf("no stream available")
	}

	// Start with defaults and apply options
	consumerOpts := defaultConsumerOptions(subject, n.consumerGroup)
	for _, opt := range opts {
		opt(consumerOpts)
	}

	consumerConfig := jetstream.ConsumerConfig{
		Name:          consumerOpts.name,
		Durable:       consumerOpts.durable,
		FilterSubject: subject,
		AckPolicy:     consumerOpts.ackPolicy,
		MaxDeliver:    consumerOpts.maxDeliver,
		AckWait:       consumerOpts.ackWait,
	}

	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	consumeCtx, err := consumer.Consume(func(msg jetstream.Msg) {
		natsMsg := &nats.Msg{
			Subject: msg.Subject(),
			Reply:   msg.Reply(),
			Header:  msg.Headers(),
			Data:    msg.Data(),
		}

		req, err := gtw.Import((*gtw.NatsMsg)(natsMsg))
		if err != nil {
			log.Printf("failed to import message: %v", err)
			_ = msg.Nak()
			return
		}

		res, err := fn(req)
		if err != nil {
			log.Printf("handler error: %v", err)
			_ = msg.Nak()
			return
		}

		if res == nil {
			log.Println("handler returned nil response")
			_ = msg.Nak()
			return
		}

		if err := msg.Ack(); err != nil {
			log.Printf("failed to ack message: %v", err)
			return
		}

		if natsMsg.Reply != "" {
			natsRes, err := gtw.Export[gtw.NatsMsg](res)
			if err != nil {
				log.Printf("failed to export response: %v", err)
				return
			}

			err = n.conn.PublishMsg(&nats.Msg{
				Subject: natsMsg.Reply,
				Header:  natsRes.Header,
				Data:    natsRes.Data,
			})
			if err != nil {
				log.Printf("failed to send response: %v", err)
			}
		}
	})

	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	n.consumers[subject] = consumeCtx
	return nil
}

func (n *NatsJetStreamServer) Publish(subject string, msg *gtw.Message) error {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	jsMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	_, err = n.js.PublishMsg(context.Background(), jsMsg)
	if err != nil {
		return fmt.Errorf("failed to publish: %w", err)
	}

	return nil
}

func (n *NatsJetStreamServer) PublishAsync(subject string, msg *gtw.Message) (jetstream.PubAckFuture, error) {
	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	jsMsg := &nats.Msg{
		Subject: subject,
		Header:  natsMsg.Header,
		Data:    natsMsg.Data,
	}

	ackFuture, err := n.js.PublishMsgAsync(jsMsg)
	if err != nil {
		return nil, fmt.Errorf("failed to publish async: %w", err)
	}

	return ackFuture, nil
}

func (n *NatsJetStreamServer) GetStreamInfo() (*jetstream.StreamInfo, error) {
	n.mut.Lock()
	defer n.mut.Unlock()

	for _, stream := range n.streams {
		info, err := stream.Info(context.Background())
		if err != nil {
			return nil, err
		}
		return info, nil
	}

	return nil, fmt.Errorf("no stream available")
}

func (n *NatsJetStreamServer) DeleteConsumer(subject string) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	consumeCtx, exists := n.consumers[subject]
	if !exists {
		return fmt.Errorf("consumer not found for subject: %s", subject)
	}

	consumeCtx.Stop()
	delete(n.consumers, subject)

	return nil
}

func (n *NatsJetStreamServer) Stop(ctx context.Context) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if !n.isRunning {
		return gtw.ErrServerNotStarted
	}

	for subject, consumeCtx := range n.consumers {
		consumeCtx.Stop()
		log.Printf("stopped consumer for subject: %s", subject)
	}

	if err := n.conn.Drain(); err != nil {
		log.Printf("failed to drain connection: %v", err)
	}

	n.conn.Close()

	n.isRunning = false
	n.consumers = make(map[string]jetstream.ConsumeContext)

	return nil
}
