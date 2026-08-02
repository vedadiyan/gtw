package gtw

import (
	"fmt"
	"reflect"
	"sync"

	"github.com/vedadiyan/vedio"
)

type (
	Service[T any] struct {
		name string
	}

	ServiceProxy[T any] struct {
		name    string
		builder func(scope Scope, args ...any) ([]any, error)
		once    sync.Once
	}

	Scope = vedio.Scoped
)

func AddSingleton[T any]() error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.SINGLETON))
}

func AddSingletonWithName[T any](name string) error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.SINGLETON), vedio.WithName(name))
}

func AddTransient[T any]() error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.TRANSIENT))
}

func AddTransientWithName[T any](name string) error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.TRANSIENT), vedio.WithName(name))
}

func AddScoped[T any]() error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.SCOPED))
}

func AddScopedWithName[T any](name string) error {
	return vedio.Register[T](vedio.WithLifeCycle(vedio.TRANSIENT), vedio.WithName(name))
}

func (i *Service[T]) Value(scope Scope) (T, error) {
	name := vedio.Default
	if len(i.name) != 0 {
		name = i.name
	}
	val, err := vedio.ResolveNamed[T](name, vedio.WithScope(scope))
	if err != nil {
		var zero T
		return zero, err
	}
	return val, nil
}

func (i *Service[T]) ValueOrZero(scope Scope) T {
	val, _ := i.Value(scope)
	return val
}

func (i *ServiceProxy[T]) build(scope Scope) error {
	name := vedio.Default
	if len(i.name) != 0 {
		name = i.name
	}
	service, err := vedio.ResolveNamed[any](name, vedio.WithScope(scope))
	if err != nil {
		return err
	}
	typeOfService := reflect.TypeOf(service)
	typeOfProxy := reflect.TypeFor[T]()
	targetMethodName := typeOfProxy.Method(0).Name
	targetMethod, ok := typeOfService.MethodByName(targetMethodName)
	if !ok {
		return fmt.Errorf("")
	}

	sourceInputs := targetMethod.Type.NumIn()
	sourceOutputs := targetMethod.Type.NumOut()

	i.builder = func(scope Scope, args ...any) ([]any, error) {
		in := make([]reflect.Value, sourceInputs)
		service, err := vedio.ResolveNamed[any](name, vedio.WithScope(scope))
		if err != nil {
			return nil, err
		}
		in[0] = reflect.ValueOf(service)
		for i, arg := range args {
			if i > sourceInputs {
				break
			}
			index := i + 1
			aVal := reflect.ValueOf(arg)
			if aVal.Type().AssignableTo(targetMethod.Type.In(index)) {
				in[index] = aVal
				continue
			}
			in[index] = reflect.New(targetMethod.Type.In(index)).Elem()
		}
		res := targetMethod.Func.Call(in)
		out := make([]any, sourceOutputs)
		for i, r := range res {
			out[i] = r.Interface()
		}
		return out, nil
	}
	return nil
}

func (i *ServiceProxy[T]) Proxy(scope Scope, args ...any) ([]any, error) {
	i.once.Do(func() {
		i.build(scope)
	})

	return i.builder(scope, args...)
}

func NewScope() Scope {
	return vedio.NewScope()
}
