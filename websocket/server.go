package websocket

import (
	"context"
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

func New(address string, opts ...Option) *WebSocketServer {
	defaultUpgrader := &websocket.Upgrader{
		ReadBufferSize:  1024,
		WriteBufferSize: 1024,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	for _, opt := range opts {
		opt(defaultUpgrader)
	}
	ws := &WebSocketServer{
		upgrader:    *defaultUpgrader,
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
	ws.mut.Lock()
	handler, ok := ws.handlers[r.URL.Path]
	ws.mut.Unlock()

	if !ok {
		http.Error(w, "No handler registered for path", http.StatusNotFound)
		return
	}

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

		wsMsg := &gtw.WebSocketMsg{
			MessageType: messageType,
			Data:        data,
			Headers:     r.Header,
		}

		msg, err := gtw.Import(wsMsg)
		if err != nil {
			log.Printf("failed to import WebSocket message: %v", err)
			_ = conn.WriteMessage(websocket.TextMessage, []byte("failed to process message"))
			continue
		}

		res, err := handler(msg)
		if err != nil {
			log.Printf("handler error: %v", err)
			_ = conn.WriteMessage(websocket.TextMessage, []byte("handler error"))
			continue
		}

		if res == nil {
			log.Println("handler returned nil response")
			_ = conn.WriteMessage(websocket.TextMessage, []byte("nil response"))
			continue
		}

		wsRes, err := gtw.Export[gtw.WebSocketMsg](res)
		if err != nil {
			log.Printf("failed to export response: %v", err)
			_ = conn.WriteMessage(websocket.TextMessage, []byte("failed to export response"))
			continue
		}

		err = conn.WriteMessage(wsRes.MessageType, wsRes.Data)
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

	path := p.Pattern()
	if path == "" {
		path = "/"
	}

	if _, exists := ws.handlers[path]; exists {
		return fmt.Errorf("handler already registered for path: %s", path)
	}

	ws.handlers[path] = fn
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

func (ws *WebSocketServer) Broadcast(msg *gtw.Message) error {
	ws.connMut.Lock()
	defer ws.connMut.Unlock()

	wsMsg, err := gtw.Export[gtw.WebSocketMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	for conn := range ws.connections {
		err := conn.WriteMessage(wsMsg.MessageType, wsMsg.Data)
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
