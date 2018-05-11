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
	Functions   []*astFunction   `| @@`
	Objects     []*astObject     `| @@ } "}"`
}

type astAssignment struct {
	Key      []string     `"." @Ident {"." @Ident} ":"`
	Value    *astValue    `(@@`
	Function *astFunction `| @@)`
}

type astProperty struct {
	Type    *astType  `"property" @@`
	Key     string    `@Ident`
	Default *astValue `[":" @@]`
}

type astEvent struct {
	Key     string       `"event" @Ident`
	Params  []*astType   `"(" { @@ [","] } ")"`
	Default *astFunction `[":" @@]`
}

type astFunction struct {
	Name       *string  `Function [@Ident]`
	Parameters []string `"(" {@Ident [","]} ")"`
	Body       string   `@FunctionBody`
}

//special type to make bool handling easier
type Boolean bool

func (b *Boolean) Capture(v []string) error { *b = v[0] == "true"; return nil }

type astValue struct {
	String *string  `  @String`
	Number *float64 `| @Float`
	Int    *int64   `| @Int`
	Bool   *Boolean `| @('true'|'false')`
}

type astType struct {
	Type string `@("string" | "bool" | "int" | "float")`
}
