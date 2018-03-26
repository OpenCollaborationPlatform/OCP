package dml

import (
	"bytes"
	"io"
	"io/ioutil"
	"regexp"
	"strconv"
	"unicode/utf8"

	"github.com/alecthomas/participle/lexer"
)

var types []string
var expressions map[string]*regexp.Regexp
var symbols map[string]rune

func init() {

	//all types in important order of parsing
	types = make([]string, 0)
	types = append(types, `EOF`, `Char`, `Func`, `AnymFunc`, `Ident`, `String`, `Float`, `Int`)

	//build all required expressions for stuff we want to do with regexp
	expressions = make(map[string]*regexp.Regexp, 0)

	expressions[`Char`] = regexp.MustCompile(`^[^0-9a-zA-Z_"]`)
	expressions[`Ident`] = regexp.MustCompile(`^[A-Za-z][0-9a-zA-Z_]*`)
	expressions[`String`] = regexp.MustCompile(`^"([^"\\]|\\.)*"`)
	expressions[`Float`] = regexp.MustCompile(`^[+-]?([0-9]+\.[0-9]*|\.[0-9]+)`)
	expressions[`Int`] = regexp.MustCompile(`-?[0-9]+`)

	//build the symbols for the expressions
	symbols = make(map[string]rune, 0)
	for i, name := range types {
		symbols[name] = -rune(-1*i - 1)
	}
}

type dmlDefinition struct{}

func (d *dmlDefinition) Lex(r io.Reader) lexer.Lexer {
	b, err := ioutil.ReadAll(r)
	if err != nil {
		panic(err)
	}
	return &dmlLexer{
		cursor: lexer.Position{
			Filename: lexer.NameOfReader(r),
			Line:     1,
			Column:   1,
			Offset:   0,
		},
		buf:          b,
		patternCache: make(map[string]*regexp.Regexp, 0),
	}
}

func (d *dmlDefinition) Symbols() map[string]rune {
	return symbols
}

//Lexer that handles some special dml needs
// - ignores all comments
// - has a special token for JavaScript code
type dmlLexer struct {
	cursor       lexer.Position
	buf          []byte
	patternCache map[string]*regexp.Regexp
	peek         *lexer.Token
}

func (self *dmlLexer) Peek() lexer.Token {
	if self.peek != nil {
		return *self.peek
	}

	if len(self.buf) > self.cursor.Offset {
		self.skipUnneeded()

		//catch non-regex stuff
		tok, ok := self.matchString()
		if ok {
			self.peek = &tok
			return tok
		}

		tok, ok = self.matchJSFunction()
		if ok {
			self.peek = &tok
			return tok
		}

		//not returned? search the remaining regexp tokens!
		for _, name := range types {

			expr, ok := expressions[name]
			if !ok {
				continue
			}

			tok, ok := self.match(expr)
			if ok {
				tok.Type = symbols[name]

				self.peek = &tok
				return tok
			}
		}
	}

	eof := lexer.EOFToken
	eof.Pos = self.cursor
	return eof
}

func (self *dmlLexer) Next() lexer.Token {
	token := self.Peek()
	self.peek = nil
	return token
}

//*****************************
// 		internal functions
//*****************************

// Match a pattern. Note: Type is not set, as unknown!
func (self *dmlLexer) match(pattern *regexp.Regexp) (lexer.Token, bool) {

	idx := pattern.FindIndex(self.buf[self.cursor.Offset:])
	if idx != nil && len(idx) != 0 {

		//build the token
		match := self.buf[self.cursor.Offset:(self.cursor.Offset + idx[1])]
		token := lexer.Token{
			Pos:   self.cursor,
			Value: string(match),
		}

		//update the lexer
		self.cursor.Offset += idx[1]
		lines := bytes.Count(match, []byte("\n"))
		self.cursor.Line += lines
		if lines == 0 {
			self.cursor.Column += utf8.RuneCount(match)
		} else {
			self.cursor.Column = utf8.RuneCount(match[bytes.LastIndex(match, []byte("\n")):])
		}

		//return the result
		return token, true
	}

	//not found
	return lexer.Token{}, false
}

// Match a string. TODO: use runes and charachters, not bytes on iterating
func (self *dmlLexer) matchJSFunction() (lexer.Token, bool) {

	//everything begins with function keyword. We can use match, as if this is found
	//we never need to go back, it can only  be a function
	_, ok := self.match(self.compile(`^function`))
	if !ok {
		return lexer.Token{}, false
	}
	self.skipUnneeded()

	//maybe there is a identifier, otherwise it is annonymous
	id, named := self.match(expressions["Ident"])
	self.skipUnneeded()

	//now the parameters must come
	param, ok := self.match(self.compile(`\(.*?\)`))
	if !ok {
		lexer.Panic(self.cursor, "expected function parameters(...)")
	}
	self.skipUnneeded()

	//now we will have a arbitrarily nested function body, let's get it!
	if self.buf[self.cursor.Offset] != '{' {
		lexer.Panicf(self.cursor, "expected function body opening '{' instead of %s", string(self.buf[self.cursor.Offset]))
	}
	exp := self.compile(`(\{)|(\})`)
	add := 1
	for cnt := 1; cnt != 0; {

		//little safety: never go over the end of the buffer!
		if len(self.buf) <= self.cursor.Offset+add {
			lexer.Panic(self.cursor, "expected '}' at end of function body")
		}

		//match opeing or closing
		idx := exp.FindSubmatchIndex(self.buf[(self.cursor.Offset + add):])
		if idx == nil || idx[0] < 0 {
			lexer.Panic(self.cursor, "expected '}' at end of function body")
		}
		//and adopt accordingly
		if idx[3] >= 0 {
			cnt++
			add += idx[3]
		} else {
			cnt--
			add += idx[5]
		}
	}

	//we found our body, let's build the token
	tok := lexer.Token{Pos: self.cursor, Type: symbols["AnymFunc"]}

	match := "function "
	if named {
		match += string(id.Value)
		tok.Type = symbols["Func"]
	}
	match += string(param.Value) + " " + string(self.buf[self.cursor.Offset:(self.cursor.Offset+add)])

	tok.Value = match

	//update the lexer
	self.cursor.Offset += add
	bmatch := []byte(match)
	lines := bytes.Count(bmatch, []byte("\n"))
	self.cursor.Line += lines
	if lines == 0 {
		self.cursor.Column += utf8.RuneCount(bmatch)
	} else {
		self.cursor.Column = utf8.RuneCount(bmatch[bytes.LastIndex([]byte(match), []byte("\n")):])
	}
	return tok, true
}

// Match a javascript function, either normal or annonymous
func (self *dmlLexer) matchString() (lexer.Token, bool) {

	//first charachter must be string opening
	if string(self.buf[self.cursor.Offset]) != `"` {
		return lexer.Token{}, false
	}

	//now we get everything till string closing
	old := '"'
	for i := 1; i < (len(self.buf) - self.cursor.Offset); i++ {
		if self.buf[self.cursor.Offset+i] == '"' && old != '\\' {

			//build the token
			match := self.buf[(self.cursor.Offset):(self.cursor.Offset + i + 1)]
			interpreted, _ := strconv.Unquote(string(match)) //unquote and interprete all escapes
			token := lexer.Token{
				Pos:   self.cursor,
				Value: interpreted,
				Type:  symbols["String"],
			}

			//update the lexer
			self.cursor.Offset += i + 1
			lines := bytes.Count(match, []byte("\n"))
			self.cursor.Line += lines
			if lines == 0 {
				self.cursor.Column += utf8.RuneCount(match)
			} else {
				self.cursor.Column = utf8.RuneCount(match[bytes.LastIndex(match, []byte("\n")):])
			}

			return token, true
		}
		old = rune(self.buf[self.cursor.Offset+i])
	}
	return lexer.Token{}, false
}

//remove whitespace
func (self *dmlLexer) skipUnneeded() {
	//remove whitespace and comment as long as available
	exp := self.compile(`^(([ \t\r\n]+)|(\/\/.*)|(\/\*(?s).*?\*\/))`)
	ok := true
	for ok {
		_, ok = self.match(exp)
	}
}

//pattern cache
func (self *dmlLexer) compile(pattern string) *regexp.Regexp {
	regc, ok := self.patternCache[pattern]
	if !ok {
		regc = regexp.MustCompile(pattern)
		self.patternCache[pattern] = regc
	}
	return regc
}
