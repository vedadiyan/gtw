package gtw

import (
	"bytes"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"

	"github.com/gorilla/websocket"
	"github.com/nats-io/nats.go"
	"google.golang.org/grpc/metadata"
)

type (
	HttpRequest  http.Request
	HttpResponse http.Response
	NatsMsg      nats.Msg
	GrpcMsg      struct {
		Metadata metadata.MD
		Payload  []byte
	}
	WebSocketMsg struct {
		MessageType int
		Data        []byte
		Headers     http.Header
	}
	MessageConstraint interface {
		HttpRequest | HttpResponse | NatsMsg | GrpcMsg | WebSocketMsg
	}
	MessageType string
	Header      http.Header
	Message     struct {
		msgType    MessageType
		Data       io.ReadCloser
		Headers    Header
		StatusCode int
		protocol   any
	}
)

const (
	TypeHttpRequest  MessageType = "HttpRequest"
	TypeHttpResponse MessageType = "HttpResponse"
	TypeNatsMsg      MessageType = "NatsMsg"
	TypeGrpcMsg      MessageType = "GrpcMsg"
	TypeWebSocketMsg MessageType = "WebSocketMsg"
)

var (
	ErrNilInput        = errors.New("input cannot be nil")
	ErrInvalidProtocol = errors.New("invalid protocol type")
)

func (m *Message) GetType() MessageType {
	return m.msgType
}

func (m *Message) GetStatusCode() int {
	return m.StatusCode
}

func Export[T MessageConstraint](msg *Message) (*T, error) {
	if msg == nil {
		return nil, ErrNilInput
	}

	out, ok := msg.protocol.(*T)
	if ok {
		return out, nil
	}

	return convertToProtocol[T](msg)
}

func convertToProtocol[T MessageConstraint](msg *Message) (*T, error) {
	var zero T
	switch any(zero).(type) {
	case HttpRequest:
		return any(exportHttpRequest(msg)).(*T), nil
	case HttpResponse:
		return any(exportHttpResponse(msg)).(*T), nil
	case NatsMsg:
		return any(exportNatsMsg(msg)).(*T), nil
	case GrpcMsg:
		return any(exportGrpcMsg(msg)).(*T), nil
	case WebSocketMsg:
		return any(exportWebSocketMsg(msg)).(*T), nil
	default:
		return nil, ErrInvalidProtocol
	}
}

func exportHttpRequest(msg *Message) *HttpRequest {
	return &HttpRequest{
		Header: http.Header(msg.Headers),
		Body:   msg.Data,
	}
}

func exportHttpResponse(msg *Message) *HttpResponse {
	return &HttpResponse{
		StatusCode: msg.StatusCode,
		Header:     http.Header(msg.Headers),
		Body:       msg.Data,
	}
}

func exportNatsMsg(msg *Message) *NatsMsg {
	natsMsg := &NatsMsg{
		Header: convertToNatsHeaders(msg.Headers),
		Data:   readDataOrEmpty(msg.Data),
	}
	return natsMsg
}

func exportGrpcMsg(msg *Message) *GrpcMsg {
	grpcMsg := &GrpcMsg{
		Metadata: convertToGrpcMetadata(msg.Headers),
		Payload:  readDataOrEmpty(msg.Data),
	}
	return grpcMsg
}

func exportWebSocketMsg(msg *Message) *WebSocketMsg {
	wsMsg := &WebSocketMsg{
		MessageType: websocket.TextMessage,
		Data:        readDataOrEmpty(msg.Data),
		Headers:     http.Header(msg.Headers),
	}
	return wsMsg
}

func convertToNatsHeaders(headers Header) nats.Header {
	natsHeaders := make(nats.Header)
	maps.Copy(natsHeaders, headers)
	return natsHeaders
}

func convertToGrpcMetadata(headers Header) metadata.MD {
	md := make(metadata.MD)
	maps.Copy(md, headers)
	return md
}

func readDataOrEmpty(data io.ReadCloser) []byte {
	if data == nil {
		return []byte{}
	}

	bytes, err := io.ReadAll(data)
	if err != nil {
		return []byte{}
	}
	return bytes
}

func Import[T MessageConstraint](in *T) (*Message, error) {
	if in == nil {
		return nil, ErrNilInput
	}

	switch v := any(in).(type) {
	case *HttpRequest:
		return importHttpRequest(v), nil
	case *HttpResponse:
		return importHttpResponse(v), nil
	case *NatsMsg:
		return importNatsMsg(v), nil
	case *GrpcMsg:
		return importGrpcMsg(v), nil
	case *WebSocketMsg:
		return importWebSocketMsg(v), nil
	default:
		return nil, ErrInvalidProtocol
	}
}

func importHttpRequest(req *HttpRequest) *Message {
	return &Message{
		msgType:    TypeHttpRequest,
		Headers:    Header(req.Header),
		Data:       getBodyOrEmpty(req.Body),
		StatusCode: 0, // Requests don't have status codes
		protocol:   req,
	}
}

func importHttpResponse(resp *HttpResponse) *Message {
	return &Message{
		msgType:    TypeHttpResponse,
		Headers:    Header(resp.Header),
		Data:       getBodyOrEmpty(resp.Body),
		StatusCode: resp.StatusCode,
		protocol:   resp,
	}
}

func importNatsMsg(msg *NatsMsg) *Message {
	headers := make(Header)
	if msg.Header != nil {
		maps.Copy(headers, msg.Header)
	}

	status := msg.Header.Get("Status")
	statusCode := 0
	if len(status) != 0 {
		if value, err := strconv.Atoi(status); err == nil {
			statusCode = value
		}
	}

	return &Message{
		msgType:    TypeNatsMsg,
		Headers:    headers,
		Data:       io.NopCloser(bytes.NewReader(msg.Data)),
		StatusCode: statusCode,
		protocol:   msg,
	}
}

func importGrpcMsg(msg *GrpcMsg) *Message {
	headers := make(Header)
	if msg.Metadata != nil {
		maps.Copy(headers, msg.Metadata)
	}

	return &Message{
		msgType:    TypeGrpcMsg,
		Headers:    headers,
		Data:       io.NopCloser(bytes.NewReader(msg.Payload)),
		StatusCode: 0, // gRPC uses status in metadata/trailers
		protocol:   msg,
	}
}

func importWebSocketMsg(msg *WebSocketMsg) *Message {
	headers := make(Header)
	if msg.Headers != nil {
		maps.Copy(headers, msg.Headers)
	}

	return &Message{
		msgType:    TypeWebSocketMsg,
		Headers:    headers,
		Data:       io.NopCloser(bytes.NewReader(msg.Data)),
		StatusCode: 0, // WebSocket messages don't have status codes
		protocol:   msg,
	}
}

func getBodyOrEmpty(body io.ReadCloser) io.ReadCloser {
	if body != nil {
		return body
	}
	return io.NopCloser(bytes.NewReader([]byte{}))
}
