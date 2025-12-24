package gtw

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"maps"
	"net/http"
	"strconv"
	"sync"

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
		Subject     string
		Data        []byte
		Headers     http.Header
	}
	MessageConstraint interface {
		HttpRequest | HttpResponse | NatsMsg | GrpcMsg | WebSocketMsg
	}
	MessageType string
	Header      = http.Header

	Message interface {
		io.Reader
		io.Writer
		io.Closer
		Header() Header
		Status() int
		Protocol() any
		WriteTo(io.Writer) (int64, error)
	}

	GenericMessage struct {
		msgType     MessageType
		r           io.ReadCloser
		buffer      *bytes.Buffer
		header      Header
		protocol    any
		StatusCode  int
		once        sync.Once
		initialized bool
	}
)

func NewHttpMessage[T MessageConstraint](protocol T) *GenericMessage {
	out := &GenericMessage{
		header:      make(Header),
		buffer:      bytes.NewBuffer([]byte{}),
		protocol:    protocol,
		initialized: true,
	}
	out.r = io.NopCloser(out.buffer)

	// Set message type based on protocol
	switch any(protocol).(type) {
	case HttpRequest:
		out.msgType = TypeHttpRequest
	case HttpResponse:
		out.msgType = TypeHttpResponse
	case NatsMsg:
		out.msgType = TypeNatsMsg
	case GrpcMsg:
		out.msgType = TypeGrpcMsg
	case WebSocketMsg:
		out.msgType = TypeWebSocketMsg
	}

	return out
}

func (m *GenericMessage) Status() int {
	return m.StatusCode
}

func (m *GenericMessage) Read(p []byte) (int, error) {
	return m.r.Read(p)
}

func (m *GenericMessage) Write(p []byte) (int, error) {
	return m.buffer.Write(p)
}

func (m *GenericMessage) Close() error {
	m.init()
	var err error
	if m.r != nil {
		err = m.r.Close()
	}
	m.buffer = nil
	return err
}

func (m *GenericMessage) Header() Header {
	m.init()
	return m.header
}

func (m *GenericMessage) AddReadCloser(r io.ReadCloser) {
	m.init()
	m.r = MultipleReadCloser(r, m.r)
}

func (m *GenericMessage) Protocol() any {
	return m.protocol
}

func (m *GenericMessage) GetType() MessageType {
	return m.msgType
}

func (m *GenericMessage) GetStatusCode() int {
	return m.StatusCode
}

func (m *GenericMessage) WriteTo(w io.Writer) (int64, error) {
	if m.r == nil {
		return 0, nil
	}
	return io.Copy(w, m.r)
}

func (m *GenericMessage) init() {
	if m.initialized {
		return
	}
	m.once.Do(func() {
		if m.initialized {
			return
		}
		m.buffer = bytes.NewBuffer([]byte{})
		m.r = io.NopCloser(m.buffer)
		m.header = make(Header)
		m.initialized = true
	})
}

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

func Export[T MessageConstraint](msg Message) (*T, error) {
	if msg == nil {
		return nil, ErrNilInput
	}

	gm, ok := msg.(*GenericMessage)
	if !ok {
		return nil, ErrInvalidProtocol
	}

	out, ok := gm.protocol.(*T)
	if ok {
		return out, nil
	}

	return convertToProtocol[T](gm)
}

func convertToProtocol[T MessageConstraint](msg *GenericMessage) (*T, error) {
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
		{
			msg, err := exportWebSocketMsg(msg)
			if err != nil {
				return nil, err
			}
			return any(msg).(*T), nil
		}
	default:
		return nil, ErrInvalidProtocol
	}
}

func exportHttpRequest(msg *GenericMessage) *HttpRequest {
	return &HttpRequest{
		Header: http.Header(msg.header),
		Body:   msg.r,
	}
}

func exportHttpResponse(msg *GenericMessage) *HttpResponse {
	return &HttpResponse{
		StatusCode: msg.StatusCode,
		Header:     http.Header(msg.header),
		Body:       msg.r,
	}
}

func exportNatsMsg(msg *GenericMessage) *NatsMsg {
	natsMsg := &NatsMsg{
		Header: convertToNatsHeaders(msg.header),
		Data:   readDataOrEmpty(msg.r),
	}
	return natsMsg
}

func exportGrpcMsg(msg *GenericMessage) *GrpcMsg {
	grpcMsg := &GrpcMsg{
		Metadata: convertToGrpcMetadata(msg.header),
		Payload:  readDataOrEmpty(msg.r),
	}
	return grpcMsg
}

func exportWebSocketMsg(msg *GenericMessage) (*WebSocketMsg, error) {
	var out WebSocketMsg
	data, err := io.ReadAll(msg.r)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return &out, nil
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

func Import[T MessageConstraint](in *T) (Message, error) {
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
		return importWebSocketMsg(v)
	default:
		return nil, ErrInvalidProtocol
	}
}

func importHttpRequest(req *HttpRequest) *GenericMessage {
	gm := NewHttpMessage(*req)
	if req.Header != nil {
		gm.header = Header(req.Header)
	}
	gm.AddReadCloser(getBodyOrEmpty(req.Body))
	gm.StatusCode = 0 // Requests don't have status codes
	return gm
}

func importHttpResponse(resp *HttpResponse) *GenericMessage {
	gm := NewHttpMessage(*resp)
	if resp.Header != nil {
		gm.header = Header(resp.Header)
	}
	gm.AddReadCloser(getBodyOrEmpty(resp.Body))
	gm.StatusCode = resp.StatusCode
	return gm
}

func importNatsMsg(msg *NatsMsg) *GenericMessage {
	gm := NewHttpMessage(*msg)

	headers := make(Header)
	if msg.Header != nil {
		maps.Copy(headers, msg.Header)
	}
	gm.header = headers

	status := msg.Header.Get("Status")
	statusCode := 0
	if len(status) != 0 {
		if value, err := strconv.Atoi(status); err == nil {
			statusCode = value
		}
	}
	gm.StatusCode = statusCode

	gm.AddReadCloser(io.NopCloser(bytes.NewReader(msg.Data)))
	return gm
}

func importGrpcMsg(msg *GrpcMsg) *GenericMessage {
	gm := NewHttpMessage(*msg)

	headers := make(Header)
	if msg.Metadata != nil {
		maps.Copy(headers, msg.Metadata)
	}
	gm.header = headers
	gm.StatusCode = 0 // gRPC uses status in metadata/trailers

	gm.AddReadCloser(io.NopCloser(bytes.NewReader(msg.Payload)))
	return gm
}

func importWebSocketMsg(msg *WebSocketMsg) (*GenericMessage, error) {
	gm := NewHttpMessage(*msg)

	headers := make(Header)
	if msg.Headers != nil {
		maps.Copy(headers, msg.Headers)
	}
	gm.header = headers
	gm.StatusCode = 0 // WebSocket messages don't have status codes

	json, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	gm.AddReadCloser(io.NopCloser(bytes.NewReader(json)))
	return gm, nil
}

func getBodyOrEmpty(body io.ReadCloser) io.ReadCloser {
	if body != nil {
		return body
	}
	return io.NopCloser(bytes.NewReader([]byte{}))
}
