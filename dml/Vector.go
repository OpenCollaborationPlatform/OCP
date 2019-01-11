// Vector
package dml

type vector struct {
	*data
}

func NewVector(name string, parent identifier, rntm *Runtime) Object {

	vec := &vector{NewDataBaseClass(name, "Vector", parent, rntm)}

	//add properties
	vec.AddProperty("type", Type, "int", true)

	//add methods

	return vec
}

func (self *vector) get() {

}
