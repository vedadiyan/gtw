package websocket

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"maps"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
	"github.com/vedadiyan/gtw/v2"
)

type (
	WebSocketServer struct {
		server      *http.Server
		upgrader    websocket.Upgrader
		handlers    *http.ServeMux
		isRunning   bool
		mut         sync.Mutex
		address     string
		connections map[*websocket.Conn]bool
		connMut     sync.RWMutex
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
		handlers:    http.NewServeMux(),
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

		req, err := http.NewRequest(http.MethodPost, wsReq.Subject, bytes.NewBuffer(wsReq.Data))
		if err != nil {
			log.Printf("failed to create request: %v", err)
			continue
		}
		req.Header = wsReq.Headers

		handler, _ := ws.handlers.Handler(req)
		if handler == nil {
			log.Printf("no handler found for subject: %s", wsReq.Subject)
			continue
		}

		res := &gtw.WebSocketMsg{
			Headers: make(http.Header),
			Subject: wsReq.Subject,
		}

		handler.ServeHTTP(res, req)

		resData, err := json.Marshal(res)
		if err != nil {
			log.Printf("failed to marshal response: %v", err)
			continue
		}

		if err := conn.WriteMessage(messageType, resData); err != nil {
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

	if fn == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	ws.handlers.HandleFunc(gtw.ToGoRouteTemplate(subject), func(w http.ResponseWriter, r *http.Request) {
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
			maps.Copy(w.Header(), res.Header())
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

	if ctx == nil {
		return fmt.Errorf("context cannot be nil")
	}

	ws.connMut.Lock()
	for conn := range ws.connections {
		if conn == nil {
			continue
		}
		if err := conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "server shutting down"),
		); err != nil {
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
	if msg == nil {
		return fmt.Errorf("message cannot be nil")
	}

	ws.connMut.RLock()
	defer ws.connMut.RUnlock()

	wsMsg, err := gtw.Export[gtw.WebSocketMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	if wsMsg == nil {
		return fmt.Errorf("exported message is nil")
	}

	var broadcastErr error
	for conn := range ws.connections {
		if conn == nil {
			continue
		}
		if err := conn.WriteMessage(messageType, wsMsg.Data); err != nil {
			log.Printf("failed to broadcast to connection: %v", err)
			if broadcastErr == nil {
				broadcastErr = err
			}
		}
	}

	return broadcastErr
}

func (ws *WebSocketServer) ConnectionCount() int {
	ws.connMut.RLock()
	defer ws.connMut.RUnlock()
	return len(ws.connections)
}
