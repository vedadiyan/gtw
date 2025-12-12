package nats

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/nats-io/nats.go"
	"github.com/vedadiyan/gtw/v2"
)

type (
	NatsServer struct {
		conn          *nats.Conn
		subscriptions map[string]*nats.Subscription
		isRunning     bool
		mut           sync.Mutex
		url           string
	}
)

func New(url string) (*NatsServer, error) {
	conn, err := nats.Connect(url)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to NATS: %w", err)
	}

	return &NatsServer{
		conn:          conn,
		subscriptions: make(map[string]*nats.Subscription),
		url:           url,
		isRunning:     false,
	}, nil
}

func (n *NatsServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if n.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	subject := p.Pattern()

	if _, exists := n.subscriptions[subject]; exists {
		return fmt.Errorf("already subscribed to subject: %s", subject)
	}

	sub, err := n.conn.Subscribe(subject, func(msg *nats.Msg) {
		req, err := gtw.Import((*gtw.NatsMsg)(msg))
		if err != nil {
			log.Printf("failed to import NATS message: %v", err)
			if msg.Reply != "" {
				_ = msg.Respond([]byte("internal error"))
			}
			return
		}

		res, err := fn(req)
		if err != nil {
			log.Printf("handler error: %v", err)
			if msg.Reply != "" {
				_ = msg.Respond([]byte("handler error"))
			}
			return
		}

		if res == nil {
			log.Println("handler returned nil response")
			if msg.Reply != "" {
				_ = msg.Respond([]byte("nil response"))
			}
			return
		}

		natsRes, err := gtw.Export[gtw.NatsMsg](res)
		if err != nil {
			log.Printf("failed to export response: %v", err)
			if msg.Reply != "" {
				_ = msg.Respond([]byte("export error"))
			}
			return
		}

		if msg.Reply != "" {
			err = msg.RespondMsg(&nats.Msg{
				Subject: msg.Reply,
				Header:  natsRes.Header,
				Data:    natsRes.Data,
			})
			if err != nil {
				log.Printf("failed to send response: %v", err)
			}
		}
	})

	if err != nil {
		return fmt.Errorf("failed to subscribe to subject %s: %w", subject, err)
	}

	n.subscriptions[subject] = sub
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

	if len(n.subscriptions) == 0 {
		return fmt.Errorf("no subscriptions registered")
	}

	n.isRunning = true
	return nil
}

func (n *NatsServer) Stop(ctx context.Context) error {
	n.mut.Lock()
	defer n.mut.Unlock()

	if !n.isRunning {
		return gtw.ErrServerNotStarted
	}

	if err := n.conn.Drain(); err != nil {
		log.Printf("failed to drain connection: %v", err)
	}

	n.conn.Close()

	n.isRunning = false
	n.subscriptions = make(map[string]*nats.Subscription)

	return nil
}
