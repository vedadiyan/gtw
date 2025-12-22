package gtw

import (
	"fmt"
	"reflect"
	"strings"
	"unsafe"
)

type (
	Metadata byte
)

var (
	controllers  []func(Server) error
	handlerType  reflect.Type
	metadataType reflect.Type
)

func init() {
	handlerType = reflect.TypeOf(func(Message) (Message, error) { return &GenericMessage{StatusCode: 200}, nil })
	metadataType = reflect.TypeOf(Metadata(0))
}

func Register(v any) {
	controllers = append(controllers, register(v))
}

func register(v any) func(Server) error {
	return func(srv Server) error {
		t := reflect.TypeOf(v)
		if t.Kind() != reflect.Pointer {
			return fmt.Errorf("expected pointer buy found value")
		}
		if t.Elem().Kind() != reflect.Struct {
			return fmt.Errorf("expected struct but found %T", v)
		}
		val := reflect.ValueOf(v)
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
				handler := val.MethodByName(methodName).Interface().(func(Message) (Message, error))
				r := fmt.Sprintf("/%s/%s", strings.TrimSuffix(prefix, "/"), strings.TrimPrefix(route, "/"))
				r = strings.TrimLeft(r, "/")
				if err := srv.HandleMessage(UrlWithMethod(fmt.Sprintf("/%s", r), Method(strings.ToUpper(httpMethod))), handler); err != nil {
					return err
				}
				continue
			}
			if strings.HasPrefix(field.Type.Name(), "Service[") && field.Type.PkgPath() == "github.com/vedadiyan/gtw/v2" {
				rf := val.Elem().Field(i)
				name, ok := field.Tag.Lookup("name")
				if ok {
					f := rf.FieldByName("name")
					// #nosec G103
					f = reflect.NewAt(f.Type(), unsafe.Pointer(f.UnsafeAddr())).Elem()
					f.Set(reflect.ValueOf(name))
				}
			}
		}
		return nil
	}
}

func ListenAndServer(srv Server) error {
	for _, controller := range controllers {
		if err := controller(srv); err != nil {
			return err
		}
	}
	return srv.Start()
}
