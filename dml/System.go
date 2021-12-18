package dml

import (
	"github.com/dop251/goja"
)

/* The general system, exposing Methods */
type System interface {
	MethodHandler

	ExposedToJS() bool
	GetJSObject() *goja.Object

	CanHandleEvent(string) bool   //events to be handled by the behaviour type
	CanHandleKeyword(string) bool //WAMP call keywords to be handled by the behaviour type

	BeforeOperation() error //called before a WAMP operation is processed
	AfterOperation() error  //called after a WAMP operation is Processed
}

type SystemCreatorFunc func(*Runtime) (System, error)

/*Type to handle multiple Systems. As we use this only in runtime, and not to define other
interfaces, we do not a interface for this type*/
type SystemHandler struct {
	systems map[string]System
}

func newSystemHandler() SystemHandler {
	return SystemHandler{make(map[string]System, 0)}
}

// creates the system, panics if it fails
func (self *SystemHandler) RegisterSystem(rntm *Runtime, name string, creator SystemCreatorFunc) {

	if _, has := self.systems[name]; has {
		panic("Manager already registered")
	}

	sys, err := creator(rntm)
	if err != nil {
		panic(err.Error())
	}

	if sys.ExposedToJS() {
		rntm.jsvm.Set(name, sys.GetJSObject())
	}

	self.systems[name] = sys
}

func (self *SystemHandler) HasSystem(name string) bool {
	_, has := self.systems[name]
	return has
}

func (self SystemHandler) GetSystem(name string) System {
	manager, has := self.systems[name]
	if !has {
		return nil
	}
	return manager
}

func (self SystemHandler) GetEventBehaviours(event string) []string {

	result := make([]string, 0)
	for name, manager := range self.systems {
		if manager.CanHandleEvent(event) {
			result = append(result, name)
		}
	}
	return result
}
