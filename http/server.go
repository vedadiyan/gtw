package http

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/vedadiyan/gtw/v2"
)

type (
	HttpServer struct {
		routes    map[gtw.Method]*http.ServeMux
		server    *http.Server
		isRunning bool
		mut       sync.Mutex
	}
)

func New(address string) *HttpServer {
	mux := http.NewServeMux()
	srv := &http.Server{
		Addr:    address,
		Handler: mux,
	}
	out := &HttpServer{
		routes: make(map[gtw.Method]*http.ServeMux),
		server: srv,
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		route, ok := out.routes[gtw.Method(strings.ToUpper(r.Method))]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		handler, p := route.Handler(r)
		_ = p
		handler.ServeHTTP(w, r)
	})
	return out
}

func (h *HttpServer) HandleMessage(p gtw.Pattern, fn gtw.MessageHandler) error {
	h.mut.Lock()
	defer h.mut.Unlock()

	if h.isRunning {
		return gtw.ErrServerAlreadyRunning
	}

	subject := p.Pattern()
	if subject == "" {
		return fmt.Errorf("pattern cannot be empty")
	}

	if fn == nil {
		return fmt.Errorf("handler cannot be nil")
	}

	h.routes[p.Method()].HandleFunc(gtw.ToGoRouteTemplate(p.Pattern()), func(w http.ResponseWriter, r *http.Request) {
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
			for key, values := range res.Header() {
				for _, value := range values {
					w.Header().Add(key, value)
				}
			}
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

func (h *HttpServer) Start() error {
	h.mut.Lock()
	h.isRunning = true
	h.mut.Unlock()

	if err := h.server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		h.mut.Lock()
		h.isRunning = false
		h.mut.Unlock()
		return err
	}
	return nil
}

func (h *HttpServer) Stop(ctx context.Context) error {
	h.mut.Lock()
	defer h.mut.Unlock()
	if err := h.server.Shutdown(ctx); err != nil {
		return err
	}
	h.isRunning = false
	return nil
}
