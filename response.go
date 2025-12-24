package gtw

import (
	"net/http"
)

type (
	responseOptions struct {
		data    []byte
		headers Header
		err     error
	}
	ResponseOption func(*responseOptions) error
)

func createMessage(opts ...ResponseOption) (*GenericMessage, error) {
	options := new(responseOptions)
	for _, opt := range opts {
		if err := opt(options); err != nil {
			return nil, err
		}
	}

	if options.err != nil {
		return nil, options.err
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
	return msg, nil
}

// 1xx Informational
func Continue(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusContinue
	return msg, nil
}

func SwitchingProtocols(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusSwitchingProtocols
	return msg, nil
}

func Processing(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusProcessing
	return msg, nil
}

func EarlyHints(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusEarlyHints
	return msg, nil
}

// 2xx Success
func Ok(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusOK
	return msg, nil
}

func Created(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusCreated
	return msg, nil
}

func Accepted(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusAccepted
	return msg, nil
}

func NonAuthoritativeInfo(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNonAuthoritativeInfo
	return msg, nil
}

func NoContent(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNoContent
	return msg, nil
}

func ResetContent(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusResetContent
	return msg, nil
}

func PartialContent(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusPartialContent
	return msg, nil
}

func MultiStatus(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusMultiStatus
	return msg, nil
}

func AlreadyReported(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusAlreadyReported
	return msg, nil
}

func IMUsed(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusIMUsed
	return msg, nil
}

// 3xx Redirection
func MultipleChoices(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusMultipleChoices
	return msg, nil
}

func MovedPermanently(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusMovedPermanently
	return msg, nil
}

func Found(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusFound
	return msg, nil
}

func SeeOther(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusSeeOther
	return msg, nil
}

func NotModified(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNotModified
	return msg, nil
}

func UseProxy(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUseProxy
	return msg, nil
}

func TemporaryRedirect(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusTemporaryRedirect
	return msg, nil
}

func PermanentRedirect(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusPermanentRedirect
	return msg, nil
}

// 4xx Client Errors
func BadRequest(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusBadRequest
	return msg, nil
}

func Unauthorized(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUnauthorized
	return msg, nil
}

func PaymentRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusPaymentRequired
	return msg, nil
}

func Forbidden(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusForbidden
	return msg, nil
}

func NotFound(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNotFound
	return msg, nil
}

func MethodNotAllowed(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusMethodNotAllowed
	return msg, nil
}

func NotAcceptable(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNotAcceptable
	return msg, nil
}

func ProxyAuthRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusProxyAuthRequired
	return msg, nil
}

func RequestTimeout(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusRequestTimeout
	return msg, nil
}

func Conflict(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusConflict
	return msg, nil
}

func Gone(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusGone
	return msg, nil
}

func LengthRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusLengthRequired
	return msg, nil
}

func PreconditionFailed(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusPreconditionFailed
	return msg, nil
}

func RequestEntityTooLarge(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusRequestEntityTooLarge
	return msg, nil
}

func RequestURITooLong(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusRequestURITooLong
	return msg, nil
}

func UnsupportedMediaType(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUnsupportedMediaType
	return msg, nil
}

func RequestedRangeNotSatisfiable(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusRequestedRangeNotSatisfiable
	return msg, nil
}

func ExpectationFailed(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusExpectationFailed
	return msg, nil
}

func Teapot(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusTeapot
	return msg, nil
}

func MisdirectedRequest(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusMisdirectedRequest
	return msg, nil
}

func UnprocessableEntity(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUnprocessableEntity
	return msg, nil
}

func Locked(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusLocked
	return msg, nil
}

func FailedDependency(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusFailedDependency
	return msg, nil
}

func TooEarly(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusTooEarly
	return msg, nil
}

func UpgradeRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUpgradeRequired
	return msg, nil
}

func PreconditionRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusPreconditionRequired
	return msg, nil
}

func TooManyRequests(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusTooManyRequests
	return msg, nil
}

func RequestHeaderFieldsTooLarge(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusRequestHeaderFieldsTooLarge
	return msg, nil
}

func UnavailableForLegalReasons(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusUnavailableForLegalReasons
	return msg, nil
}

// 5xx Server Errors
func InternalServerError(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusInternalServerError
	return msg, nil
}

func NotImplemented(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNotImplemented
	return msg, nil
}

func BadGateway(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusBadGateway
	return msg, nil
}

func ServiceUnavailable(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusServiceUnavailable
	return msg, nil
}

func GatewayTimeout(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusGatewayTimeout
	return msg, nil
}

func HTTPVersionNotSupported(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusHTTPVersionNotSupported
	return msg, nil
}

func VariantAlsoNegotiates(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusVariantAlsoNegotiates
	return msg, nil
}

func InsufficientStorage(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusInsufficientStorage
	return msg, nil
}

func LoopDetected(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusLoopDetected
	return msg, nil
}

func NotExtended(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNotExtended
	return msg, nil
}

func NetworkAuthenticationRequired(opts ...ResponseOption) (Message, error) {
	msg, err := createMessage(opts...)
	if err != nil {
		return nil, err
	}
	msg.StatusCode = http.StatusNetworkAuthenticationRequired
	return msg, nil
}

func Error(err error) (Message, error) {
	return nil, err
}

// Content Type Options
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

// contentType is a helper function that creates a ResponseOption with data and Content-Type header
func contentType(data []byte, mimeType string) ResponseOption {
	return func(ro *responseOptions) error {
		ro.data = data
		if ro.headers == nil {
			ro.headers = make(Header)
		}
		ro.headers.Set("Content-Type", mimeType)
		return nil
	}
}
