//parser for the datastructure markup language
package dml

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
	Functions   []string         `| @Func`
	Objects     []*astObject     `| @@ } "}"`
}

type astAssignment struct {
	Key      string    `"." @Ident {@"." @Ident} ":"`
	Value    *astValue `  @@`
	Function string    `| @AnymFunc`
}

type astProperty struct {
	Key     string    `"property" @Ident`
	Type    string    `@String`
	Default *astValue `[":" @@]`
}

type astEvent struct {
	Key     string   `"event" @Ident`
	Params  []string `"(" "string" | "bool" | "int" | "float" ")"`
	Default string   `[":" @AnymFunc]`
}

type astValue struct {
	String *string  `  @String`
	Number *float64 `| @Float`
	Int    *int64   `| @Int`
	Bool   *string  `| @( "true" | "false" )`
}
