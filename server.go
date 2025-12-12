package gtw

import (
	"context"
	"errors"
)

type (
	Method  string
	Pattern struct {
		pattern string
		method  Method
	}
	Server interface {
		Start() error
		Stop(context.Context) error
		HandleMessage(Pattern, MessageHandler) error
	}
	MessageHandler func(*Message) (*Message, error)
)

var (
	ErrServerNotStarted     = errors.New("server not started")
	ErrServerAlreadyRunning = errors.New("server already running")
)

const (
	MethodHead   Method = "HEAD"
	MethodGet    Method = "GET"
	MethodPost   Method = "POST"
	MethodPut    Method = "PUT"
	MethodPatch  Method = "PATCH"
	MethodDelete Method = "DELETE"
)

func Url(pattern string) Pattern {
	return Pattern{
		pattern: pattern,
		method:  MethodHead,
	}
}

func UrlWithMethod(pattern string, method Method) Pattern {
	return Pattern{
		pattern: pattern,
		method:  method,
	}
}

func (p Pattern) Pattern() string {
	return p.pattern
}

func (p Pattern) Method() Method {
	return p.method
}
