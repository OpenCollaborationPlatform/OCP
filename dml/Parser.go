//parser for the datastructure markup language
package dml

//the file format
type DML struct {

	//single object is allowed: root must be unambigious
	Object *Object `{ @@ }`
}

/* Object
 * - Must have a identifier
 * - Can have multiple existing property assignments
 * - Can have multiple subobjects
 */
type Object struct {
	Identifier string      `@Ident "{"`
	Properties []*Property `{ @@ }`
	Objects    []*Object   `{ @@ } "}"`
}

type Property struct {
	Key   string `"." @Ident {@"." @Ident} ":"`
	Value *Value `@@`
}

type Value struct {
	String *string  `  @String`
	Number *float64 `| @Float`
	Int    *int64   `| @Int`
	Bool   *string  `| @( "true" | "false" )`
}
