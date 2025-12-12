package http

import (
	"context"
	"fmt"
	"io"
	"log"
	"maps"
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
		return fmt.Errorf("server is already running")
	}
	if _, ok := h.routes[p.Method()]; !ok {
		h.routes[p.Method()] = http.NewServeMux()
	}
	h.routes[p.Method()].HandleFunc(toGoRouteTemplate(p.Pattern()), func(w http.ResponseWriter, r *http.Request) {
		req, err := gtw.Import((*gtw.HttpRequest)(r))
		if err != nil {
			log.Println(err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		res, err := fn(req)
		if err != nil {
			log.Println(err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		if res == nil {
			log.Println("handler returned nil response")
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}

		if res.Headers != nil {
			maps.Copy(w.Header(), res.Headers)
			if res.StatusCode != 0 {
				w.WriteHeader(res.StatusCode)
			}
		}
		if res.Data != nil {
			defer res.Data.Close()
			_, _ = io.Copy(w, res.Data)
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
