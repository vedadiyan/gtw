package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vedadiyan/gtw/v2"
)

type (
	NatsServer struct {
		conn          *nats.Conn
		subscription  *nats.Subscription
		handlers      *http.ServeMux
		isRunning     bool
		mut           sync.RWMutex
		url           string
		queueGroup    string
		subjectPrefix string
		options       []nats.Option
	}

	NatsResponseWriter struct {
		header     http.Header
		statusCode int
		data       []byte
		subject    string
	}

	Option func(*NatsServer)
)

const defaultQueueGroup = "gtw"

func New(url string, opts ...Option) (*NatsServer, error) {
	ns := &NatsServer{
		handlers:      http.NewServeMux(),
		url:           url,
		queueGroup:    defaultQueueGroup,
		subjectPrefix: "",
		isRunning:     false,
		options:       make([]nats.Option, 0),
	}

	for _, opt := range opts {
		opt(ns)
	}

	conn, err := nats.Connect(url, ns.options...)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	ns.conn = conn

	return ns, nil
}

func WithQueueGroup(queueGroup string) Option {
	return func(ns *NatsServer) {
		if queueGroup != "" {
			ns.queueGroup = queueGroup
		}
	}
}

func WithSubjectPrefix(prefix string) Option {
	return func(ns *NatsServer) {
		ns.subjectPrefix = prefix
	}
}

func WithNatsOptions(opts ...nats.Option) Option {
	return func(ns *NatsServer) {
		ns.options = append(ns.options, opts...)
	}
}

func NewNatsResponseWriter(subject string) *NatsResponseWriter {
	return &NatsResponseWriter{
		header:     make(http.Header),
		statusCode: http.StatusOK,
		subject:    subject,
	}
}

func (w *NatsResponseWriter) Header() http.Header {
	return w.header
}

func (w *NatsResponseWriter) Write(data []byte) (int, error) {
	w.data = append(w.data, data...)
	return len(data), nil
}

func (w *NatsResponseWriter) WriteHeader(statusCode int) {

}

func (w *NatsResponseWriter) GetData() []byte {
	return w.data
}

func (w *NatsResponseWriter) GetSubject() string {
	return w.subject
}

func (n *NatsServer) handleNatsMessage(msg *nats.Msg) {
	if msg == nil {
		log.Println("received nil NATS message")
		return
	}

	var natsReq gtw.NatsMsg
	if err := json.Unmarshal(msg.Data, &natsReq); err != nil {
		log.Printf("failed to unmarshal NATS message: %v", err)
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"error":"failed to unmarshal message"}`))
		}
		return
	}

	req, err := http.NewRequest(http.MethodPost, natsReq.Subject, nil)
	if err != nil {
		log.Printf("failed to create request: %v", err)
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"error":"failed to create request"}`))
		}
		return
	}

	if natsReq.Header != nil {
		req.Header = http.Header(natsReq.Header)
	}

	handler, _ := n.handlers.Handler(req)
	if handler == nil {
		log.Printf("no handler found for subject: %s", natsReq.Subject)
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"error":"no handler found"}`))
		}
		return
	}

	gtwReq, err := gtw.Import((*gtw.HttpRequest)(req))
	if err != nil {
		log.Printf("failed to import request: %v", err)
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"error":"failed to import request"}`))
		}
		return
	}

	if gtwReq == nil {
		log.Println("import returned nil request")
		if msg.Reply != "" {
			_ = msg.Respond([]byte(`{"error":"nil request"}`))
		}
		return
	}

	writer := NewNatsResponseWriter(natsReq.Subject)
	handler.ServeHTTP(writer, req)

	if msg.Reply != "" {
		res := &gtw.NatsMsg{
			Header:  nats.Header(writer.Header()),
			Subject: writer.GetSubject(),
			Data:    writer.GetData(),
		}

		resData, err := json.Marshal(res)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			_ = msg.Respond([]byte(`{"error":"failed to marshal response"}`))
			return
		}

		if err := msg.Respond(resData); err != nil {
			log.Printf("failed to send response: %v", err)
		}
	}
}

func (n *NatsServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
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

	n.handlers.HandleFunc(gtw.ToGoRouteTemplate(subject), func(w http.ResponseWriter, r *http.Request) {
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

func (n *NatsServer) Start() error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if n.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	if n.conn == nil || !n.conn.IsConnected() {
		return fmt.Errorf("NATS connection is not established")
	}

	// Determine subscription subject based on prefix
	subject := ">"
	if n.subjectPrefix != "" {
		subject = fmt.Sprintf("%s>", n.subjectPrefix)
	}

	// Subscribe to subjects with queue group
	sub, err := n.conn.QueueSubscribe(subject, n.queueGroup, n.handleNatsMessage)
	if err != nil {
		return fmt.Errorf("failed to subscribe to subject '%s': %w", subject, err)
	}

	n.subscription = sub
	n.isRunning = true

	log.Printf("NATS server started, listening to '%s' with queue group '%s'", subject, n.queueGroup)
	return nil
}

func (n *NatsServer) Stop(ctx context.Context) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if !n.isRunning {
		return gtw.ErrServerNotStarted
	}

	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	if n.subscription != nil {
		if err := n.subscription.Unsubscribe(); err != nil {
			log.Printf("failed to unsubscribe: %v", err)
		}
		n.subscription = nil
	}

	if err := n.conn.Drain(); err != nil {
		log.Printf("failed to drain connection: %v", err)
	}

	n.conn.Close()

	n.isRunning = false

	log.Println("NATS server stopped")
	return nil
}

func (n *NatsServer) GetQueueGroup() string {
	n.mut.RLock()
	defer n.mut.RUnlock()
	return n.queueGroup
}

func (n *NatsServer) GetSubjectPrefix() string {
	n.mut.RLock()
	defer n.mut.RUnlock()
	return n.subjectPrefix
}
