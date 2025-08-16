package gtw

import (
	"fmt"
	"net/http"
	"net/url"
	"reflect"
	"strings"
	"unsafe"
)

type (
	Cors struct {
		AllowedOrigins string
		AllowedMethods string
		AllowedHeaders string
		ExposedHeaders string
		MaxAge         string
	}
	Server struct {
		mux                   *http.ServeMux
		routeTable            *RouteTable
		corsHandler           http.HandlerFunc
		defaultResponseHeader http.Header
	}
)

var (
	_defaultServer *Server
)

func init() {
	_defaultServer = New()
}

func DefaultServer() *Server {
	return _defaultServer
}

func New() *Server {
	mux := new(http.ServeMux)
	server := new(Server)
	server.mux = mux
	server.routeTable = NewRouteTable()
	server.defaultResponseHeader = http.Header{}
	server.corsHandler = func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodOptions {
			_, err := server.routeTable.Find(r.URL, "*")
			if err != nil {
				http.NotFound(w, r)
				return
			}
			server.corsHandler(w, r)
			return
		}
		route, err := server.routeTable.Find(r.URL, r.Method)
		if err != nil {
			http.NotFound(w, r)
			return
		}
		for key := range server.defaultResponseHeader {
			w.Header().Add(key, server.defaultResponseHeader.Get(key))
		}
		route.ServeHTTP(w, r)
	})
	return server
}

func (srv *Server) Handle(route string, method string, handlerFunc Handler) error {
	url, err := url.Parse(route)
	if err != nil {
		return err
	}
	srv.routeTable.Register(url, method, handlerFunc)
	return nil
}

func (srv *Server) ListenAndServe(server *http.Server) error {
	server.Handler = srv.mux
	return server.ListenAndServe()
}

func (srv *Server) Register(v any) error {
	t := reflect.TypeOf(v)
	if t.Kind() != reflect.Pointer {
		return fmt.Errorf("expected pointer buy found value")
	}
	if t.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("expected struct but found %T", v)
	}
	val := reflect.ValueOf(v)
	handlerType := reflect.TypeOf(func(*HttpCtx) (Status, Response) { return 0, nil })
	metadataType := reflect.TypeOf(Metadata(0))
	lenOfFields := t.Elem().NumField()
	prefix := t.Elem().Name()
	for i := 0; i < lenOfFields; i++ {
		field := t.Elem().Field(i)
		if field.Name == "Metadata" && field.Type.AssignableTo(metadataType) {
			prefix = strings.TrimPrefix(field.Tag.Get("prefix"), "/")
			continue
		}
		if field.Type.AssignableTo(handlerType) {
			route, ok := field.Tag.Lookup("route")
			if !ok {
				route = field.Name
			}
			httpMethod, ok := field.Tag.Lookup("method")
			if !ok {
				httpMethod = "GET"
			}
			methodName := fmt.Sprintf("%sHandler", field.Name)
			_, ok = t.MethodByName(methodName)
			if !ok {
				continue
			}
			method := val.MethodByName(methodName).Interface().(func(*HttpCtx) (Status, Response))
			r := fmt.Sprintf("/%s/%s", strings.TrimSuffix(prefix, "/"), strings.TrimPrefix(route, "/"))
			r = strings.TrimLeft(r, "/")
			srv.Handle(fmt.Sprintf("/%s", r), httpMethod, method)
			continue
		}
		if strings.HasPrefix(field.Type.Name(), "Service[") && field.Type.PkgPath() == "github.com/vedadiyan/gtw" {
			rf := val.Elem().Field(i)
			name, ok := field.Tag.Lookup("name")
			if ok {
				f := rf.FieldByName("name")
				f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
				f.Set(reflect.ValueOf(name))
			}
		}
	}
	return nil
}

func (s *Server) Cors(c *Cors) *Server {
	s.defaultResponseHeader.Add("Access-Control-Expose-Headers", c.ExposedHeaders)
	s.corsHandler = func(w http.ResponseWriter, r *http.Request) {
		_, err := s.routeTable.Find(r.URL, "*")
		if err != nil {
			http.NotFound(w, r)
			return
		}
		w.Header().Add("access-control-allow-origin", c.AllowedOrigins)
		w.Header().Add("access-control-allow-headers", c.AllowedHeaders)
		w.Header().Add("access-control-max-age", c.MaxAge)
		w.Header().Add("access-control-allow-methods", c.AllowedMethods)
		w.WriteHeader(http.StatusOK)
	}
	return s
}

func CorsAllowAll() *Cors {
	return &Cors{
		AllowedOrigins: "*",
		AllowedMethods: "GET,POST,PUT,PATCH,DELETE,OPTIONS",
		AllowedHeaders: "*",
		ExposedHeaders: "*",
		MaxAge:         "3628800",
	}
}

func Register[T any](server *Server) {
	server.Register(new(T))
}
