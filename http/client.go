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

// Client Options

func WithTimeout(timeout time.Duration) HttpClientOption {
	return func(c *HttpClient) error {
		c.httpClient.Timeout = timeout
		return nil
	}
}

func WithBaseURL(baseURL string) HttpClientOption {
	return func(c *HttpClient) error {
		// Validate URL
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

	// Apply options
	for _, opt := range opts {
		if err := opt(client); err != nil {
			return nil, err
		}
	}

	return client, nil
}

func (hc *HttpClient) Call(p gtw.Pattern, msg *gtw.Message) (*gtw.Message, error) {
	// Export message to HTTP request
	req, err := gtw.Export[gtw.HttpRequest](msg)
	if err != nil {
		return nil, fmt.Errorf("failed to export message: %w", err)
	}

	// Parse pattern as URL
	targetURL, err := url.Parse(p.Pattern())
	if err != nil {
		return nil, fmt.Errorf("invalid URL pattern: %w", err)
	}

	// If baseURL is set, resolve relative URLs
	if hc.baseURL != "" {
		baseURL, err := url.Parse(hc.baseURL)
		if err != nil {
			return nil, fmt.Errorf("invalid base URL: %w", err)
		}
		targetURL = baseURL.ResolveReference(targetURL)
	}

	// Set URL and method
	req.URL = targetURL
	req.Method = string(p.Method())

	// Set Host header
	if targetURL.Host != "" {
		req.Host = targetURL.Host
	}

	// Apply default headers
	if len(hc.defaultHeaders) > 0 {
		for key, values := range hc.defaultHeaders {
			// Only set if not already present in request
			if req.Header.Get(key) == "" {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		}
	}

	// Execute request
	resp, err := hc.httpClient.Do((*http.Request)(req))
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	// Convert response to Message
	return gtw.Import((*gtw.HttpResponse)(resp))
}

// Get performs a GET request
func (hc *HttpClient) Get(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodGet), msg)
}

// Post performs a POST request
func (hc *HttpClient) Post(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodPost), msg)
}

// Put performs a PUT request
func (hc *HttpClient) Put(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodPut), msg)
}

// Patch performs a PATCH request
func (hc *HttpClient) Patch(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodPatch), msg)
}

// Delete performs a DELETE request
func (hc *HttpClient) Delete(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodDelete), msg)
}

// Head performs a HEAD request
func (hc *HttpClient) Head(urlStr string, msg *gtw.Message) (*gtw.Message, error) {
	return hc.Call(gtw.UrlWithMethod(urlStr, gtw.MethodHead), msg)
}

// GetHTTPClient returns the underlying http.Client
func (hc *HttpClient) GetHTTPClient() *http.Client {
	return hc.httpClient
}

// SetDefaultHeader sets a default header for all requests
func (hc *HttpClient) SetDefaultHeader(key, value string) {
	hc.defaultHeaders.Set(key, value)
}

// AddDefaultHeader adds a default header for all requests
func (hc *HttpClient) AddDefaultHeader(key, value string) {
	hc.defaultHeaders.Add(key, value)
}

// RemoveDefaultHeader removes a default header
func (hc *HttpClient) RemoveDefaultHeader(key string) {
	hc.defaultHeaders.Del(key)
}

// GetBaseURL returns the base URL
func (hc *HttpClient) GetBaseURL() string {
	return hc.baseURL
}

// SetBaseURL sets the base URL
func (hc *HttpClient) SetBaseURL(baseURL string) error {
	_, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("invalid base URL: %w", err)
	}
	hc.baseURL = baseURL
	return nil
}
