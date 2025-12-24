package gtw

import (
	"net/http"
)

type (
	responseOptions struct {
		data    []byte
		headers Header
	}
	ResponseOption func(*responseOptions)
)

func Text(data []byte) ResponseOption {
	return contentType(data, "text/plain; charset=utf-8")
}

func HTML(data []byte) ResponseOption {
	return contentType(data, "text/html; charset=utf-8")
}

func JSON(data []byte) ResponseOption {
	return contentType(data, "application/json; charset=utf-8")
}

func XML(data []byte) ResponseOption {
	return contentType(data, "application/xml; charset=utf-8")
}

func CSS(data []byte) ResponseOption {
	return contentType(data, "text/css; charset=utf-8")
}

func JavaScript(data []byte) ResponseOption {
	return contentType(data, "application/javascript; charset=utf-8")
}

func JPEG(data []byte) ResponseOption {
	return contentType(data, "image/jpeg")
}

func PNG(data []byte) ResponseOption {
	return contentType(data, "image/png")
}

func GIF(data []byte) ResponseOption {
	return contentType(data, "image/gif")
}

func SVG(data []byte) ResponseOption {
	return contentType(data, "image/svg+xml; charset=utf-8")
}

func WebP(data []byte) ResponseOption {
	return contentType(data, "image/webp")
}

func ICO(data []byte) ResponseOption {
	return contentType(data, "image/x-icon")
}

func PDF(data []byte) ResponseOption {
	return contentType(data, "application/pdf")
}

func ZIP(data []byte) ResponseOption {
	return contentType(data, "application/zip")
}

func GZIP(data []byte) ResponseOption {
	return contentType(data, "application/gzip")
}

func TAR(data []byte) ResponseOption {
	return contentType(data, "application/x-tar")
}

func OctetStream(data []byte) ResponseOption {
	return contentType(data, "application/octet-stream")
}

func FormURLEncoded(data []byte) ResponseOption {
	return contentType(data, "application/x-www-form-urlencoded")
}

func MultipartFormData(data []byte) ResponseOption {
	return contentType(data, "multipart/form-data")
}

func MP3(data []byte) ResponseOption {
	return contentType(data, "audio/mpeg")
}

func MP4(data []byte) ResponseOption {
	return contentType(data, "video/mp4")
}

func WebM(data []byte) ResponseOption {
	return contentType(data, "video/webm")
}

func OGG(data []byte) ResponseOption {
	return contentType(data, "audio/ogg")
}

func WAV(data []byte) ResponseOption {
	return contentType(data, "audio/wav")
}

func WOFF(data []byte) ResponseOption {
	return contentType(data, "font/woff")
}

func WOFF2(data []byte) ResponseOption {
	return contentType(data, "font/woff2")
}

func TTF(data []byte) ResponseOption {
	return contentType(data, "font/ttf")
}

func EOT(data []byte) ResponseOption {
	return contentType(data, "application/vnd.ms-fontobject")
}

func EventStream(data []byte) ResponseOption {
	return contentType(data, "text/event-stream")
}

func Markdown(data []byte) ResponseOption {
	return contentType(data, "text/markdown; charset=utf-8")
}

func YAML(data []byte) ResponseOption {
	return contentType(data, "application/x-yaml; charset=utf-8")
}

func TOML(data []byte) ResponseOption {
	return contentType(data, "application/toml; charset=utf-8")
}

func CSV(data []byte) ResponseOption {
	return contentType(data, "text/csv; charset=utf-8")
}

func JSONLD(data []byte) ResponseOption {
	return contentType(data, "application/ld+json; charset=utf-8")
}

func MessagePack(data []byte) ResponseOption {
	return contentType(data, "application/msgpack")
}

func Protobuf(data []byte) ResponseOption {
	return contentType(data, "application/protobuf")
}

func AVIF(data []byte) ResponseOption {
	return contentType(data, "image/avif")
}

func contentType(data []byte, mimeType string) ResponseOption {
	return func(ro *responseOptions) {
		ro.data = data
		if ro.headers == nil {
			ro.headers = make(Header)
		}
		ro.headers.Set("Content-Type", mimeType)
	}
}

func createMessage(opts ...ResponseOption) *GenericMessage {
	options := new(responseOptions)
	options.headers = make(http.Header)
	for _, opt := range opts {
		opt(options)
	}
	msg := new(GenericMessage)
	msg.init()
	if options.headers != nil {
		header := msg.Header()
		for key, values := range options.headers {
			for _, value := range values {
				header.Add(key, value)
			}
		}
	}
	if options.data != nil {
		msg.Write(options.data)
	}
	return msg
}

// 1xx Informational
func Continue(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusContinue
	return msg
}

func SwitchingProtocols(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusSwitchingProtocols
	return msg
}

func Processing(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusProcessing
	return msg
}

func EarlyHints(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusEarlyHints
	return msg
}

// 2xx Success
func Ok(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusOK
	return msg
}

func Created(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusCreated
	return msg
}

func Accepted(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusAccepted
	return msg
}

func NonAuthoritativeInfo(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNonAuthoritativeInfo
	return msg
}

func NoContent(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNoContent
	return msg
}

func ResetContent(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusResetContent
	return msg
}

func PartialContent(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusPartialContent
	return msg
}

func MultiStatus(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusMultiStatus
	return msg
}

func AlreadyReported(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusAlreadyReported
	return msg
}

func IMUsed(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusIMUsed
	return msg
}

// 3xx Redirection
func MultipleChoices(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusMultipleChoices
	return msg
}

func MovedPermanently(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusMovedPermanently
	return msg
}

func Found(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusFound
	return msg
}

func SeeOther(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusSeeOther
	return msg
}

func NotModified(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNotModified
	return msg
}

func UseProxy(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUseProxy
	return msg
}

func TemporaryRedirect(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusTemporaryRedirect
	return msg
}

func PermanentRedirect(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusPermanentRedirect
	return msg
}

// 4xx Client Errors
func BadRequest(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusBadRequest
	return msg
}

func Unauthorized(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUnauthorized
	return msg
}

func PaymentRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusPaymentRequired
	return msg
}

func Forbidden(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusForbidden
	return msg
}

func NotFound(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNotFound
	return msg
}

func MethodNotAllowed(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusMethodNotAllowed
	return msg
}

func NotAcceptable(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNotAcceptable
	return msg
}

func ProxyAuthRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusProxyAuthRequired
	return msg
}

func RequestTimeout(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusRequestTimeout
	return msg
}

func Conflict(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusConflict
	return msg
}

func Gone(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusGone
	return msg
}

func LengthRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusLengthRequired
	return msg
}

func PreconditionFailed(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusPreconditionFailed
	return msg
}

func RequestEntityTooLarge(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusRequestEntityTooLarge
	return msg
}

func RequestURITooLong(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusRequestURITooLong
	return msg
}

func UnsupportedMediaType(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUnsupportedMediaType
	return msg
}

func RequestedRangeNotSatisfiable(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusRequestedRangeNotSatisfiable
	return msg
}

func ExpectationFailed(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusExpectationFailed
	return msg
}

func Teapot(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusTeapot
	return msg
}

func MisdirectedRequest(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusMisdirectedRequest
	return msg
}

func UnprocessableEntity(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUnprocessableEntity
	return msg
}

func Locked(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusLocked
	return msg
}

func FailedDependency(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusFailedDependency
	return msg
}

func TooEarly(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusTooEarly
	return msg
}

func UpgradeRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUpgradeRequired
	return msg
}

func PreconditionRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusPreconditionRequired
	return msg
}

func TooManyRequests(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusTooManyRequests
	return msg
}

func RequestHeaderFieldsTooLarge(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusRequestHeaderFieldsTooLarge
	return msg
}

func UnavailableForLegalReasons(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusUnavailableForLegalReasons
	return msg
}

// 5xx Server Errors
func InternalServerError(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusInternalServerError
	return msg
}

func NotImplemented(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNotImplemented
	return msg
}

func BadGateway(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusBadGateway
	return msg
}

func ServiceUnavailable(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusServiceUnavailable
	return msg
}

func GatewayTimeout(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusGatewayTimeout
	return msg
}

func HTTPVersionNotSupported(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusHTTPVersionNotSupported
	return msg
}

func VariantAlsoNegotiates(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusVariantAlsoNegotiates
	return msg
}

func InsufficientStorage(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusInsufficientStorage
	return msg
}

func LoopDetected(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusLoopDetected
	return msg
}

func NotExtended(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNotExtended
	return msg
}

func NetworkAuthenticationRequired(opts ...ResponseOption) Message {
	msg := createMessage(opts...)
	msg.StatusCode = http.StatusNetworkAuthenticationRequired
	return msg
}
