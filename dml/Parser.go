//parser for the datastructure markup language
package dml

import (
	"fmt"

	"github.com/alecthomas/participle"
)

func init() {
	participle.UseLookahead(2)
}

//the file format
type DML struct {

	//next to imports only a single object is allowed: root must be unambigious
	Imports []*astImport `{ @@ }`
	Object  *astObject   `{ @@ }`
}

/* object
 * - Must have a identifier
 * - Can have multiple existing property assignments
 * - Can have multiple subobjects
 */
type astObject struct {
	Identifier  string           `@Ident "{"`
	Assignments []*astAssignment `( @@ `
	Properties  []*astProperty   `| @@`
	Events      []*astEvent      `| @@`
	Functions   []*astFunction   `| @@`
	Objects     []*astObject     `| @@ )* "}"`
}

func (self astObject) Print() {
	fmt.Printf("%v object:\nAssignments:\n", self.Identifier)

	for _, ass := range self.Assignments {
		fmt.Printf("\t")
		ass.Print()
	}
}

type astImport struct {
	File  string `"import" @String`
	Alias string `( "as" @Ident )?`
}

type astAssignment struct {
	Key      []string     `"." @Ident ("." @Ident)* ":"`
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
	Const   string       `@("const")? "property"`
	Type    *astDataType `@@`
	Key     string       `@Ident`
	Default *astValue    `(":" @@)?`
}

type astEvent struct {
	Key     string       `"event" @Ident`
	Default *astFunction `(":" @@)?`
}

type astFunction struct {
	Const      string   `@("const")? Function `
	Name       *string  `@Ident?`
	Parameters []string `"(" (@Ident ","?)* ")"`
	Body       string   `@FunctionBody`
}

//matching all supported DML datatypes. Note that a astObject is also a datatype.
//one special datatype is type whichs value is again a DataType
type astDataType struct {
	Pod    string     `@("string" | "bool" | "int" | "float" | "type" | "none" | "raw" | "var" | "key")`
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
