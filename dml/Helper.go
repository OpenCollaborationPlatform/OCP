package dml

import (
	"github.com/dop251/goja"
)

//should be implemented by everythign that is exposed to JS
type JSObject interface {
	GetJSObject() *goja.Object
	GetJSRuntime() *goja.Runtime
}

//user type to store data about a user
type User string

func (self User) Data() []byte {
	return []byte(self)
}

func UserFromData(data []byte) (User, error) {
	return User(data), nil
}
