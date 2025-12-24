package jetstream

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
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
		handlers      *http.ServeMux
		isRunning     bool
		mut           sync.RWMutex
		url           string
		streamConfig  *jetstream.StreamConfig
		consumerGroup string
		subjectPrefix string
		natsOptions   []nats.Option
	}

	JetStreamResponseWriter struct {
		header     http.Header
		statusCode int
		data       []byte
		subject    string
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

	ServerOption func(*NatsJetStreamServer) error

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
		s.natsOptions = append(s.natsOptions, opts...)
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
		if group != "" {
			s.consumerGroup = group
		}
		return nil
	}
}

func WithSubjectPrefix(prefix string) ServerOption {
	return func(s *NatsJetStreamServer) error {
		s.subjectPrefix = prefix
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

func NewJetStreamResponseWriter(subject string) *JetStreamResponseWriter {
	return &JetStreamResponseWriter{
		header:     make(http.Header),
		statusCode: http.StatusOK,
		subject:    subject,
	}
}

func (w *JetStreamResponseWriter) Header() http.Header {
	return w.header
}

func (w *JetStreamResponseWriter) Write(data []byte) (int, error) {
	w.data = append(w.data, data...)
	return len(data), nil
}

func (w *JetStreamResponseWriter) WriteHeader(statusCode int) {

}

func (w *JetStreamResponseWriter) GetData() []byte {
	return w.data
}

func (w *JetStreamResponseWriter) GetSubject() string {
	return w.subject
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
		handlers:      http.NewServeMux(),
		url:           url,
		consumerGroup: config.ConsumerGroup,
		streamConfig:  &streamConfig,
		isRunning:     false,
		natsOptions:   make([]nats.Option, 0),
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(ns); err != nil {
			return nil, err
		}
	}

	// Connect with collected options
	conn, err := nats.Connect(url, ns.natsOptions...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}
	ns.conn = conn

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

func (n *NatsJetStreamServer) handleJetStreamMessage(msg jetstream.Msg) {
	if msg == nil {
		log.Println("received nil JetStream message")
		return
	}

	var jsReq gtw.NatsMsg
	if err := json.Unmarshal(msg.Data(), &jsReq); err != nil {
		log.Printf("failed to unmarshal JetStream message: %v", err)
		_ = msg.Nak()
		return
	}

	req, err := http.NewRequest(http.MethodPost, jsReq.Subject, nil)
	if err != nil {
		log.Printf("failed to create request: %v", err)
		_ = msg.Nak()
		return
	}

	if jsReq.Header != nil {
		req.Header = http.Header(jsReq.Header)
	}

	handler, _ := n.handlers.Handler(req)
	if handler == nil {
		log.Printf("no handler found for subject: %s", jsReq.Subject)
		_ = msg.Nak()
		return
	}

	gtwReq, err := gtw.Import((*gtw.HttpRequest)(req))
	if err != nil {
		log.Printf("failed to import request: %v", err)
		_ = msg.Nak()
		return
	}

	if gtwReq == nil {
		log.Println("import returned nil request")
		_ = msg.Nak()
		return
	}

	writer := NewJetStreamResponseWriter(jsReq.Subject)
	handler.ServeHTTP(writer, req)

	if err := msg.Ack(); err != nil {
		log.Printf("failed to ack message: %v", err)
		return
	}

	if msg.Reply() != "" {
		res := &gtw.NatsMsg{
			Header:  nats.Header(writer.Header()),
			Subject: writer.GetSubject(),
			Data:    writer.GetData(),
		}

		resData, err := json.Marshal(res)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			return
		}

		if err := n.conn.Publish(msg.Reply(), resData); err != nil {
			log.Printf("failed to send response: %v", err)
		}
	}
}

func (n *NatsJetStreamServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if n.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	subject := p.Pattern()
	if subject == "" {
		return fmt.Errorf("pattern cannot be empty")
	}

	if fn == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	n.handlers.HandleFunc(gtw.ToGoRouteTemplate(p.Pattern()), func(w http.ResponseWriter, r *http.Request) {
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

	// Determine filter subject based on prefix
	filterSubject := ">"
	if n.subjectPrefix != "" {
		filterSubject = fmt.Sprintf("%s>", n.subjectPrefix)
	}

	// Get the stream
	var stream jetstream.Stream
	for _, s := range n.streams {
		stream = s
		break
	}

	if stream == nil {
		return fmt.Errorf("no stream available")
	}

	// Create consumer with filter
	consumerOpts := defaultConsumerOptions(filterSubject, n.consumerGroup)

	consumerConfig := jetstream.ConsumerConfig{
		Name:          consumerOpts.name,
		Durable:       consumerOpts.durable,
		FilterSubject: filterSubject,
		AckPolicy:     consumerOpts.ackPolicy,
		MaxDeliver:    consumerOpts.maxDeliver,
		AckWait:       consumerOpts.ackWait,
	}

	consumer, err := stream.CreateOrUpdateConsumer(context.Background(), consumerConfig)
	if err != nil {
		return fmt.Errorf("failed to create consumer: %w", err)
	}

	consumeCtx, err := consumer.Consume(n.handleJetStreamMessage)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	n.consumers[filterSubject] = consumeCtx
	n.isRunning = true

	log.Printf("JetStream server started, listening to '%s' with consumer group '%s'", filterSubject, n.consumerGroup)
	return nil
}

func (n *NatsJetStreamServer) CreateConsumer(subject string, opts ...ConsumerOption) error {
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

	consumeCtx, err := consumer.Consume(n.handleJetStreamMessage)
	if err != nil {
		return fmt.Errorf("failed to start consuming: %w", err)
	}

	n.consumers[subject] = consumeCtx
	return nil
}

func (n *NatsJetStreamServer) Publish(subject string, msg gtw.Message) error {
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	if natsMsg == nil {
		return fmt.Errorf("exported message is nil")
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

func (n *NatsJetStreamServer) PublishAsync(subject string, msg gtw.Message) (jetstream.PubAckFuture, error) {
	if msg == nil {
		return nil, fmt.Errorf("message cannot be nil")
	}

	natsMsg, err := gtw.Export[gtw.NatsMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	if natsMsg == nil {
		return nil, fmt.Errorf("exported message is nil")
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
	n.mut.RLock()
	defer n.mut.RUnlock()

	for _, stream := range n.streams {
		if stream == nil {
			continue
		}
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

	if consumeCtx != nil {
		consumeCtx.Stop()
	}
	delete(n.consumers, subject)

	return nil
}

func (n *NatsJetStreamServer) Stop(ctx context.Context) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if !n.isRunning {
		return gtw.ErrServerNotStarted
	}

	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	for subject, consumeCtx := range n.consumers {
		if consumeCtx != nil {
			consumeCtx.Stop()
			log.Printf("stopped consumer for subject: %s", subject)
		}
	}

	if err := n.conn.Drain(); err != nil {
		log.Printf("failed to drain connection: %v", err)
	}

	n.conn.Close()

	n.isRunning = false
	n.consumers = make(map[string]jetstream.ConsumeContext)

	log.Println("JetStream server stopped")
	return nil
}

func (n *NatsJetStreamServer) GetConsumerGroup() string {
	n.mut.RLock()
	defer n.mut.RUnlock()
	return n.consumerGroup
}

func (n *NatsJetStreamServer) GetSubjectPrefix() string {
	n.mut.RLock()
	defer n.mut.RUnlock()
	return n.subjectPrefix
}
