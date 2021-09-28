package dep

import (
	"fmt"
	"reflect"
	"sync"
)

type Injector struct {
	vals map[string]interface{}
	lock *sync.Mutex
}

func NewInjector() *Injector {
	return &Injector{make(map[string]interface{}), &sync.Mutex{}}
}

func (i *Injector) Register(name string, val interface{}) {
	i.lock.Lock()
	defer i.lock.Unlock()
	i.vals[name] = val
}

func (i *Injector) Assign(name string, ptr interface{}) {
	i.lock.Lock()
	defer i.lock.Unlock()
	val, ok := i.vals[name]
	if !ok {
		panic(fmt.Sprintf("Missing dependency [%v]", name))
	}

	source := reflect.ValueOf(val)
	target := reflect.ValueOf(ptr)
	if target.Kind() != reflect.Ptr {
		panic(fmt.Sprintf("Dependency [%v] cannot be assigned to target of kind [%v]", name, target.Kind()))
	}

	target = target.Elem()
	if !source.Type().AssignableTo(target.Type()) {
		panic(fmt.Sprintf("Dependency [%v] cannot be assigned to target of type [%v]", name, target.Type()))
	}

	target.Set(source)
}
