package http

import (
	"net/http"
	"net/url"

	"github.com/vedadiyan/gtw/v2"
)

type (
	HttpClient struct {
		httpClient *http.Client
	}
)

func NewClient() *HttpClient {
	out := &HttpClient{
		httpClient: http.DefaultClient,
	}
	return out
}

func (hc *HttpClient) Call(p gtw.Pattern, msg *gtw.Message) (*gtw.Message, error) {
	req, err := gtw.Export[gtw.HttpRequest](msg)
	if err != nil {
		return nil, err
	}
	url, err := url.Parse(p.Pattern())
	if err != nil {
		return nil, err
	}
	req.Host = url.Host
	req.URL = url
	req.Method = string(p.Method())
	res, err := hc.httpClient.Do((*http.Request)(req))
	if err != nil {
		return nil, err
	}
	return gtw.Import((*gtw.HttpResponse)(res))
}
