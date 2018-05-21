package dml

import "github.com/dop251/goja"

//Defines the default Property interface under which different data types can be stored.
//It uses a getter setter interface for better interactibility between dml, js and go
type Property interface {
	EventHandler
	MethodHandler

	Type() DataType

	SetValue(value interface{}) error
	GetValue() interface{}
}

//Property handler, which defines a interface for holding and using multiple properties
type PropertyHandler interface {
	HasProperty(name string) bool
	AddProperty(name string, dtype DataType) error
	GetProperty(name string) Property

	SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error
}

func NewPropertyHandler() propertyHandler {
	return propertyHandler{make(map[string]Property, 0)}
}

//Implements PropertyHandler interface except the AddProperty function: this must be
//done by the respective implementation, as it must define which kind of property
//shall be used
type propertyHandler struct {
	properties map[string]Property
}

func (self *propertyHandler) HasProperty(name string) bool {

	_, ok := self.properties[name]
	return ok
}

func (self *propertyHandler) GetProperty(name string) Property {

	return self.properties[name]
}

func (self *propertyHandler) SetupJSProperties(vm *goja.Runtime, obj *goja.Object) error {

	for name, prop := range self.properties {

		jsprop := vm.NewObject()
		obj.Set(name, jsprop)
		var propname string = name
		getter := vm.ToValue(func(call goja.FunctionCall) goja.Value {
			return vm.ToValue(self.GetProperty(propname).GetValue())
		})
		setter := vm.ToValue(func(call goja.FunctionCall) (ret goja.Value) {
			err := self.GetProperty(propname).SetValue(call.Argument(0).Export())
			if err != nil {
				panic(vm.ToValue(err))
			}
			return
		})

		err := jsprop.DefineAccessorProperty("value", getter, setter, goja.FLAG_TRUE, goja.FLAG_TRUE)
		if err != nil {
			return err
		}

		prop.SetupJSMethods(vm, jsprop)

	}
	return nil
}
