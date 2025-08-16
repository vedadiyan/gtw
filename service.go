package gtw

import (
	"time"

	"github.com/vedadiyan/gtw/internal/di"
)

type (
	Service[T any] struct {
		name     string
		hasScope bool
		scopeId  uint64
		ttl      time.Duration
	}
)

func AddSingleton[T any](fn func() (instance *T, err error)) {
	di.AddSinleton(fn)
}

func AddSingletonWithName[T any](name string, fn func() (instance *T, err error)) {
	di.AddSinletonWithName(name, fn)
}

func AddTransient[T any](fn func() (instance *T, err error)) {
	di.AddTransient(fn)
}

func AddTransientWithName[T any](name string, fn func() (instance *T, err error)) {
	di.AddTransientWithName(name, fn)
}

func AddScoped[T any](fn func() (instance *T, err error)) {
	di.AddScoped(fn)
}

func AddScopedWithName[T any](name string, fn func() (instance *T, err error)) {
	di.AddScopedWithName(name, fn)
}

func (i *Service[T]) Value() *T {
	var options *di.Options
	if i.hasScope {
		options = di.NewOptions(i.scopeId, i.ttl)
	}
	if len(i.name) == 0 {
		return di.ResolveOrPanic[T](options)
	}
	return di.ResolveWithNameOrPanic[T](i.name, options)
}

func (i *Service[T]) ValueOrNil() *T {
	var options *di.Options
	if i.hasScope {
		options = di.NewOptions(i.scopeId, i.ttl)
	}
	if len(i.name) == 0 {
		return di.ResolveOrNil[T](options)
	}
	if inst, err := di.ResolveWithName[T](i.name, options); err == nil {
		return inst
	}
	return nil
}

func (i *Service[T]) Scope(scopeId uint64, ttl time.Duration) *Service[T] {
	copy := *i
	copy.hasScope = true
	copy.scopeId = scopeId
	copy.ttl = ttl
	return &copy
}
