//parser for the datastructure markup language
package dml

import "fmt"

//the file format
type DML struct {

	//single object is allowed: root must be unambigious
	Object *astObject `{ @@ }`
}

/* object
 * - Must have a identifier
 * - Can have multiple existing property assignments
 * - Can have multiple subobjects
 */
type astObject struct {
	Identifier  string           `@Ident "{"`
	Assignments []*astAssignment `{ @@ `
	Properties  []*astProperty   `| @@`
	Events      []*astEvent      `| @@`
	Functions   []*astFunction   `| @@`
	Objects     []*astObject     `| @@ } "}"`
}

func (self astObject) Print() {
	fmt.Printf("%v object:\nAssignments:\n", self.Identifier)

	for _, ass := range self.Assignments {
		fmt.Printf("\t")
		ass.Print()
	}
}

type astAssignment struct {
	Key      []string     `"." @Ident {"." @Ident} ":"`
	Value    *astValue    `(@@`
	Function *astFunction `| @@)`
}

func (self astAssignment) Print() {
	fmt.Printf("%v: ", self.Key)
	if self.Value != nil {
		fmt.Printf("%v\n", self.Value)
	} else {
		fmt.Println("function()")
	}
}

type astProperty struct {
	Const   string       `(@("const" "property") |`
	Normal  string       `@"property")`
	Type    *astDataType `@@`
	Key     string       `@Ident`
	Default *astValue    `[":" @@]`
}

type astEvent struct {
	Key     string         `"event" @Ident`
	Params  []*astDataType `"(" { @@ [","] } ")"`
	Default *astFunction   `[":" @@]`
}

type astFunction struct {
	Name       *string  `Function [@Ident]`
	Parameters []string `"(" {@Ident [","]} ")"`
	Body       string   `@FunctionBody`
}

//matching all supported DML datatypes. Note that a astObject is also a datatype.
//one special datatype is type whichs value is again a DataType
type astDataType struct {
	Pod    string     `@("string" | "bool" | "int" | "float" | "type")`
	Object *astObject `| @@`
}

//special type to make bool handling easier
type Boolean bool

func (b *Boolean) Capture(v []string) error { *b = v[0] == "true"; return nil }

//a value for all possible datatypes
type astValue struct {
	String *string      `  @(String|Char)`
	Number *float64     `| @Float`
	Int    *int64       `| @Int`
	Bool   *Boolean     `| @("true"|"false")`
	Type   *astDataType `| @@`
}
