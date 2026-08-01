package gtw

import (
	"github.com/vedadiyan/vedio"
)

type (
	Service[T any] struct {
		name string
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

func NewScope() Scope {
	return vedio.NewScope()
}
