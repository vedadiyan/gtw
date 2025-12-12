/*
	Extracted from github.com/vedadiyan/goal/pkg/structutil
	DO NOT EDIT
*/

package di

import (
	"reflect"
	"sync"
	"time"
)

type Events int

type LifeCycles int

const (
	SINGLETON LifeCycles = iota
	TRANSIENT
	SCOPED
)

const (
	REFRESHED Events = iota
)

var (
	_refresh       sync.Map
	_contextTypes  sync.Map
	_context       sync.Map
	_scopedContext sync.Map

	_refreshMute sync.Mutex
)

type Options struct {
	scopeId uint64
	ttl     time.Duration
}

type singleton[T any] struct {
	ig       func() (instance *T, err error)
	created  bool
	instance *T
	err      error
	once     sync.Once
}

func (s *singleton[T]) getInstance() (instance *T, err error) {
	s.once.Do(func() {
		value, err := s.ig()
		s.instance = value
		s.err = err
		s.created = true
	})
	return s.instance, s.err
}

func NewOptions(scopeId uint64, ttl time.Duration) *Options {
	return &Options{scopeId, ttl}
}

func AddSinleton[T any](service func() (instance *T, err error)) error {
	name := nameOf[T]()
	return AddSinletonWithName(name, service)
}

func AddSinletonWithName[T any](name string, service func() (instance *T, err error)) error {
	singleton := singleton[T]{
		ig: service,
	}
	if _, ok := _context.LoadOrStore(name, &singleton); ok {
		return objectAlreadyExistsError(name)
	}
	_contextTypes.Store(name, SINGLETON)
	return nil
}

func RefreshSinleton[T any](service func(current *T) (instance *T, err error)) (*T, error) {
	name := nameOf[T]()
	return RefreshSinletonWithName(name, service)
}

func RefreshSinletonWithName[T any](name string, newService func(current *T) (instance *T, err error)) (*T, error) {
	_refreshMute.Lock()
	defer _refreshMute.Unlock()
	old, err := ResolveWithName[T](name, nil)
	if err != nil {
		return nil, err
	}
	new, e := newService(old)
	singleton := singleton[T]{
		ig: func() (instance *T, err error) {
			return new, e
		},
	}
	_context.Store(name, &singleton)
	values, ok := _refresh.Load(name)
	if ok {
		for _, value := range values.([]func(Events)) {
			value(REFRESHED)
		}
	}
	return old, nil
}

func OnRefresh[T any](cb func(Events)) {
	name := nameOf[T]()
	OnRefreshWithName(name, cb)
}

func OnRefreshWithName(name string, cb func(Events)) {
	value, ok := _refresh.Load(name)
	if !ok {
		value = make([]func(Events), 0)
	}
	value = append(value.([]func(Events)), cb)
	_refresh.Store(name, value)
}

func AddTransient[T any](service func() (instance *T, err error)) error {
	name := nameOf[T]()
	return AddTransientWithName(name, service)
}

func AddTransientWithName[T any](name string, service func() (instance *T, err error)) error {
	if _, ok := _context.Load(name); ok {
		return objectAlreadyExistsError(name)
	}
	_context.Store(name, service)
	_contextTypes.Store(name, TRANSIENT)
	return nil
}

func RefreshTransient[T any](service func() (instance *T, err error)) error {
	name := nameOf[T]()
	return RefreshTransientWithName(name, service)
}

func RefreshTransientWithName[T any](name string, service func() (instance *T, err error)) error {
	_refreshMute.Lock()
	defer _refreshMute.Unlock()
	_context.Store(name, service)
	_contextTypes.Store(name, TRANSIENT)
	values, ok := _refresh.Load(name)
	if ok {
		for _, value := range values.([]func(Events)) {
			value(REFRESHED)
		}
	}
	return nil
}

func AddScoped[T any](service func() (instance *T, err error)) error {
	name := nameOf[T]()
	return AddScopedWithName(name, service)
}

func AddScopedWithName[T any](name string, service func() (instance *T, err error)) error {
	if _, ok := _context.LoadOrStore(name, service); ok {
		return objectAlreadyExistsError(name)
	}
	_contextTypes.Store(name, SCOPED)
	return nil
}

func RefreshScoped[T any](service func() (instance *T, err error)) error {
	name := nameOf[T]()
	return RefreshScopedWithName(name, service)
}

func RefreshScopedWithName[T any](name string, service func() (instance *T, err error)) error {
	_refreshMute.Lock()
	defer _refreshMute.Unlock()
	_contextTypes.Store(name, SCOPED)
	values, ok := _refresh.Load(name)
	if ok {
		for _, value := range values.([]func(Events)) {
			value(REFRESHED)
		}
	}
	return nil
}

func Has[T any]() bool {
	name := nameOf[T]()
	_, ok := _context.Load(name)
	return ok
}

func HasWithName(name string) bool {
	_, ok := _context.Load(name)
	return ok
}

func ResolveOrPanic[T any](options *Options) *T {
	value, err := Resolve[T](options)
	if err != nil {
		panic(err)
	}
	return value
}

func ResolveWithNameOrPanic[T any](name string, options *Options) *T {
	value, err := ResolveWithName[T](name, options)
	if err != nil {
		panic(err)
	}
	return value
}

func ResolveOrNil[T any](options *Options) *T {
	value, _ := Resolve[T](options)
	return value
}

func Resolve[T any](options *Options) (instance *T, err error) {
	name := nameOf[T]()
	return ResolveWithName[T](name, options)
}

func ResolveWithName[T any](name string, options *Options) (instance *T, err error) {
	lifeCycle, ok := _contextTypes.Load(name)
	if !ok {
		return nil, objectNotFoundError(name)
	}
	object, ok := _context.Load(name)
	if !ok {
		return nil, objectNotFoundError(name)
	}
	switch lifeCycle {
	case SINGLETON:
		return resolveSingleton[T](object, name)
	case TRANSIENT:
		return resolveTransient[T](object, name)
	case SCOPED:
		return resolveScoped[T](options, object, name)
	default:
		return nil, nil
	}
}

func CloseScope(option Options) {
	_scopedContext.Delete(option.scopeId)
}

func resolveSingleton[T any](object any, name string) (instance *T, err error) {
	value, ok := object.(*singleton[T])
	if !ok {
		return nil, invalidCastError(name)
	}
	inst, err := value.getInstance()
	return inst, err
}

func resolveTransient[T any](object any, name string) (instance *T, err error) {
	value, ok := object.(func() (instance *T, err error))
	if !ok {
		return nil, invalidCastError(name)
	}
	inst, err := value()
	return inst, err
}

func resolveScoped[T any](options *Options, object any, name string) (instance *T, err error) {
	if options == nil {
		return nil, missingRequiredParameter("Options")
	}
	scopedValue, ok := _scopedContext.Load(options.scopeId)
	if ok {
		if value, ok := scopedValue.(*T); ok {
			return value, nil
		}
		return nil, invalidCastError(name)
	}
	value, ok := object.(func() (instance *T, err error))
	if !ok {
		return nil, invalidCastError(name)
	}
	inst, err := value()
	_scopedContext.Store(options.scopeId, inst)
	time.AfterFunc(options.ttl, func() {
		_scopedContext.Delete(options.scopeId)
	})
	return inst, err
}

func nameOf[T any]() string {
	var typeOfT *T
	return reflect.TypeOf(typeOfT).Elem().String()
}

func init() {
	_contextTypes = sync.Map{}
	_context = sync.Map{}
	_scopedContext = sync.Map{}
}
