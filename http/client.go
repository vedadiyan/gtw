package http

import (
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/vedadiyan/gtw/v2"
)

type (
	HttpClient struct {
		httpClient      *http.Client
		baseURL         string
		defaultHeaders  http.Header
		followRedirects bool
	}

	HttpClientOption func(*HttpClient) error
)

func WithTimeout(timeout time.Duration) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.Timeout = timeout
		return nil
	}
}

func WithBaseURL(baseURL string) HttpClientOption {
	return func(c *HttpClient) error {
		_, err := url.Parse(baseURL)
		if err != nil {
			return fmt.Errorf("invalid base URL: %w", err)
		}
		c.baseURL = baseURL
		return nil
	}
}

func WithDefaultHeaders(headers http.Header) HttpClientOption {
	return func(c *HttpClient) error {
		c.defaultHeaders = headers
		return nil
	}
}

func WithHTTPClient(client *http.Client) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient = client
		return nil
	}
}

func WithTransport(transport http.RoundTripper) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.Transport = transport
		return nil
	}
}

func WithFollowRedirects(follow bool) HttpClientOption {
	return func(c *HttpClient) error {
		c.followRedirects = follow
		if !follow {
			c.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
				return http.ErrUseLastResponse
			}
		} else {
			c.httpClient.CheckRedirect = nil
		}
		return nil
	}
}

func WithMaxRedirects(max int) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			if len(via) >= max {
				return fmt.Errorf("stopped after %d redirects", max)
			}
			return nil
		}
		return nil
	}
}

func WithCookieJar(jar http.CookieJar) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.Jar = jar
		return nil
	}
}

func WithTLSConfig(transport *http.Transport) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.Transport = transport
		return nil
	}
}

func NewClient(opts ...HttpClientOption) (*HttpClient, error) {
	client := &HttpClient{
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
		defaultHeaders:  http.Header{},
		followRedirects: true,
	}

	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (hc *HttpClient) Call(p gtw.Pattern, msg *gtw.Message) (*gtw.Message, error) {
	req, err := gtw.Export[gtw.HttpRequest](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	targetURL, err := url.Parse(p.Pattern())
	if err != nil {
		return nil, fmt.Errorf("invalid URL pattern: %w", err)
	}

	if hc.baseURL != "" {
		baseURL, err := url.Parse(hc.baseURL)
		if err != nil {
			return nil, fmt.Errorf("invalid base URL: %w", err)
		}
		targetURL = baseURL.ResolveReference(targetURL)
	}

	req.URL = targetURL
	req.Method = string(p.Method())

	if targetURL.Host != "" {
		req.Host = targetURL.Host
	}

	if len(hc.defaultHeaders) > 0 {
		for key, values := range hc.defaultHeaders {
			if req.Header.Get(key) == "" {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		}
	}

	resp, err := hc.httpClient.Do((*http.Request)(req))
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	return gtw.Import((*gtw.HttpResponse)(resp))
}

func (hc *HttpClient) GetHTTPClient() *http.Client {
	return hc.httpClient
}

func (hc *HttpClient) SetDefaultHeader(key, value string) {
	hc.defaultHeaders.Set(key, value)
}

func (hc *HttpClient) AddDefaultHeader(key, value string) {
	hc.defaultHeaders.Add(key, value)
}

func (hc *HttpClient) RemoveDefaultHeader(key string) {
	hc.defaultHeaders.Del(key)
}
