package websocket

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/vedadiyan/gtw/v2"
)

type (
	WebSocketServer struct {
		server      *http.Server
		upgrader    websocket.Upgrader
		handlers    map[string]gtw.MessageHandler
		isRunning   bool
		mut         sync.Mutex
		address     string
		connections map[*websocket.Conn]bool
		connMut     sync.Mutex
	}
	Option func(*websocket.Upgrader)
)

var defaultUpgrader = websocket.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func NewWebSocketServer(address string, opts ...Option) *WebSocketServer {
	upgrader := defaultUpgrader
	for _, opt := range opts {
		opt(&upgrader)
	}

	ws := &WebSocketServer{
		upgrader:    upgrader,
		handlers:    make(map[string]gtw.MessageHandler),
		address:     address,
		connections: make(map[*websocket.Conn]bool),
		isRunning:   false,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", ws.handleWebSocket)

	ws.server = &http.Server{
		Addr:    address,
		Handler: mux,
	}

	return ws
}

func (ws *WebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := ws.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("failed to upgrade connection: %v", err)
		return
	}

	ws.connMut.Lock()
	ws.connections[conn] = true
	ws.connMut.Unlock()

	defer func() {
		ws.connMut.Lock()
		delete(ws.connections, conn)
		ws.connMut.Unlock()
		conn.Close()
	}()

	for {
		messageType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("websocket error: %v", err)
			}
			break
		}

		var wsReq gtw.WebSocketMsg
		if err := json.Unmarshal(data, &wsReq); err != nil {
			log.Printf("failed to unmarshal message: %v", err)
			continue
		}

		ws.mut.Lock()
		fn, ok := ws.handlers[wsReq.Subject]
		ws.mut.Unlock()

		if !ok {
			log.Printf("no handler for subject: %s", wsReq.Subject)
			continue
		}

		msg, err := gtw.Import(&wsReq)
		if err != nil {
			log.Printf("failed to import WebSocket message: %v", err)
			continue
		}

		res, err := fn(msg)
		if err != nil {
			log.Printf("handler error: %v", err)
			continue
		}

		if res == nil {
			log.Println("handler returned nil response")
			continue
		}

		wsRes, err := gtw.Export[gtw.WebSocketMsg](res)
		if err != nil {
			log.Printf("failed to export response: %v", err)
			continue
		}

		err = conn.WriteMessage(messageType, wsRes.Data)
		if err != nil {
			log.Printf("failed to write message: %v", err)
			break
		}
	}
}

func (ws *WebSocketServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	ws.mut.Lock()
	defer ws.mut.Unlock()

	if ws.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	subject := p.Pattern()
	if subject == "" {
		return fmt.Errorf("pattern cannot be empty")
	}

	if _, exists := ws.handlers[subject]; exists {
		return fmt.Errorf("handler already registered for subject: %s", subject)
	}

	ws.handlers[subject] = fn
	return nil
}

func (ws *WebSocketServer) Start() error {
	ws.mut.Lock()

	if ws.isRunning {
		ws.mut.Unlock()
		return gtw.ErrServerAlreadyRunning
	}

	if ws.server == nil {
		ws.mut.Unlock()
		return fmt.Errorf("WebSocket server is not initialized")
	}

	if len(ws.handlers) == 0 {
		ws.mut.Unlock()
		return fmt.Errorf("no handlers registered")
	}

	ws.isRunning = true
	ws.mut.Unlock()

	if err := ws.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		ws.mut.Lock()
		ws.isRunning = false
		ws.mut.Unlock()
		return fmt.Errorf("failed to serve: %w", err)
	}

	return nil
}

func (ws *WebSocketServer) Stop(ctx context.Context) error {
	ws.mut.Lock()
	defer ws.mut.Unlock()

	if !ws.isRunning {
		return gtw.ErrServerNotStarted
	}

	ws.connMut.Lock()
	for conn := range ws.connections {
		err := conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutting down"))
		if err != nil {
			log.Printf("failed to send close message: %v", err)
		}
		conn.Close()
	}
	ws.connections = make(map[*websocket.Conn]bool)
	ws.connMut.Unlock()

	if err := ws.server.Shutdown(ctx); err != nil {
		return fmt.Errorf("failed to shutdown server: %w", err)
	}

	ws.isRunning = false
	return nil
}

func (ws *WebSocketServer) Broadcast(messageType int, msg gtw.Message) error {
	ws.connMut.Lock()
	defer ws.connMut.Unlock()

	wsMsg, err := gtw.Export[gtw.WebSocketMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	for conn := range ws.connections {
		err := conn.WriteMessage(messageType, wsMsg.Data)
		if err != nil {
			log.Printf("failed to broadcast to connection: %v", err)
		}
	}

	return nil
}

func (ws *WebSocketServer) ConnectionCount() int {
	ws.connMut.Lock()
	defer ws.connMut.Unlock()
	return len(ws.connections)
}
