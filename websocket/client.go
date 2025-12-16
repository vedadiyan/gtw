package websocket

import (
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/vedadiyan/gtw/v2"
)

type (
	WebSocketClient struct {
		conn          *websocket.Conn
		url           string
		timeout       time.Duration
		messageType   int
		fireAndForget bool
		dialer        *websocket.Dialer
		headers       http.Header
		mut           sync.Mutex
		responseChan  chan *gtw.Message
		errorChan     chan error
	}

	WebSocketClientOption func(*WebSocketClient) error
)

// Client Options

func WithTimeout(timeout time.Duration) WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.timeout = timeout
		return nil
	}
}

func WithMessageType(messageType int) WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.messageType = messageType
		return nil
	}
}

func WithTextMessages() WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.messageType = websocket.TextMessage
		return nil
	}
}

func WithBinaryMessages() WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.messageType = websocket.BinaryMessage
		return nil
	}
}

func WithFireAndForget() WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.fireAndForget = true
		return nil
	}
}

func WithRequestReply() WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.fireAndForget = false
		return nil
	}
}

func WithHeaders(headers http.Header) WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.headers = headers
		return nil
	}
}

func WithDialer(dialer *websocket.Dialer) WebSocketClientOption {
	return func(c *WebSocketClient) error {
		c.dialer = dialer
		return nil
	}
}

func WithHandshakeTimeout(timeout time.Duration) WebSocketClientOption {
	return func(c *WebSocketClient) error {
		if c.dialer != nil {
			c.dialer.HandshakeTimeout = timeout
		}
		return nil
	}
}

func NewClient(urlStr string, opts ...WebSocketClientOption) (*WebSocketClient, error) {
	// Validate URL
	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Ensure ws:// or wss:// scheme
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return nil, fmt.Errorf("invalid scheme: must be ws:// or wss://")
	}

	client := &WebSocketClient{
		url:           urlStr,
		timeout:       5 * time.Second,
		messageType:   websocket.TextMessage,
		fireAndForget: false,
		dialer:        websocket.DefaultDialer,
		headers:       http.Header{},
		responseChan:  make(chan *gtw.Message, 1),
		errorChan:     make(chan error, 1),
	}

	// Apply options
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	// Connect to WebSocket
	conn, _, err := client.dialer.Dial(urlStr, client.headers)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to WebSocket: %w", err)
	}
	client.conn = conn

	// Start read loop for responses if not fire-and-forget
	if !client.fireAndForget {
		go client.readLoop()
	}

	return client, nil
}

func (wc *WebSocketClient) readLoop() {
	for {
		messageType, data, err := wc.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				wc.errorChan <- fmt.Errorf("websocket error: %w", err)
			}
			return
		}

		// Convert to Message
		wsMsg := &gtw.WebSocketMsg{
			MessageType: messageType,
			Data:        data,
			Headers:     http.Header{},
		}

		msg, err := gtw.Import(wsMsg)
		if err != nil {
			wc.errorChan <- fmt.Errorf("failed to import message: %w", err)
			continue
		}

		// Send to response channel (non-blocking)
		select {
		case wc.responseChan <- msg:
		default:
			// Channel full, skip
		}
	}
}

func (wc *WebSocketClient) Call(p gtw.Pattern, msg *gtw.Message) (*gtw.Message, error) {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return nil, fmt.Errorf("websocket connection is not established")
	}

	// Export message to WebSocket format
	wsMsg, err := gtw.Export[gtw.WebSocketMsg](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	// Use configured message type if wsMsg.MessageType is default/zero
	messageType := wsMsg.MessageType
	if messageType == 0 {
		messageType = wc.messageType
	}

	// Send message
	err = wc.conn.WriteMessage(messageType, wsMsg.Data)
	if err != nil {
		return nil, fmt.Errorf("failed to send message: %w", err)
	}

	// If fire-and-forget, return empty message
	if wc.fireAndForget {
		return &gtw.Message{}, nil
	}

	// Wait for response with timeout
	select {
	case response := <-wc.responseChan:
		return response, nil
	case err := <-wc.errorChan:
		return nil, fmt.Errorf("error receiving response: %w", err)
	case <-time.After(wc.timeout):
		return nil, fmt.Errorf("timeout waiting for response")
	}
}

// Send sends a message without expecting a response
func (wc *WebSocketClient) Send(msg *gtw.Message) error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	wsMsg, err := gtw.Export[gtw.WebSocketMsg](msg)
	if err != nil {
		return fmt.Errorf("failed to export message: %w", err)
	}

	messageType := wsMsg.MessageType
	if messageType == 0 {
		messageType = wc.messageType
	}

	return wc.conn.WriteMessage(messageType, wsMsg.Data)
}

// Receive waits for and returns the next message
func (wc *WebSocketClient) Receive() (*gtw.Message, error) {
	if wc.fireAndForget {
		return nil, fmt.Errorf("cannot receive in fire-and-forget mode")
	}

	select {
	case response := <-wc.responseChan:
		return response, nil
	case err := <-wc.errorChan:
		return nil, err
	case <-time.After(wc.timeout):
		return nil, fmt.Errorf("timeout waiting for message")
	}
}

// SendText sends a text message
func (wc *WebSocketClient) SendText(data []byte) error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	return wc.conn.WriteMessage(websocket.TextMessage, data)
}

// SendBinary sends a binary message
func (wc *WebSocketClient) SendBinary(data []byte) error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	return wc.conn.WriteMessage(websocket.BinaryMessage, data)
}

// Ping sends a ping message
func (wc *WebSocketClient) Ping() error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return fmt.Errorf("websocket connection is not established")
	}

	return wc.conn.WriteMessage(websocket.PingMessage, []byte{})
}

// Close closes the WebSocket connection
func (wc *WebSocketClient) Close() error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	if wc.conn == nil {
		return nil
	}

	// Send close message
	err := wc.conn.WriteMessage(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""),
	)
	if err != nil {
		// Ignore error and close anyway
		wc.conn.Close()
		return err
	}

	// Close connection
	return wc.conn.Close()
}

// IsConnected checks if the client is connected
func (wc *WebSocketClient) IsConnected() bool {
	wc.mut.Lock()
	defer wc.mut.Unlock()
	return wc.conn != nil
}

// Reconnect reconnects to the WebSocket server
func (wc *WebSocketClient) Reconnect() error {
	wc.mut.Lock()
	defer wc.mut.Unlock()

	// Close existing connection
	if wc.conn != nil {
		wc.conn.Close()
	}

	// Reconnect
	conn, _, err := wc.dialer.Dial(wc.url, wc.headers)
	if err != nil {
		return fmt.Errorf("failed to reconnect: %w", err)
	}
	wc.conn = conn

	// Restart read loop if not fire-and-forget
	if !wc.fireAndForget {
		go wc.readLoop()
	}

	return nil
}
