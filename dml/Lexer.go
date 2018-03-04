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
	types = append(types, `EOF`, `Char`, `Ident`, `String`, `Float`, `Int`)

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
		buf:       b,
		wspattern: regexp.MustCompile(`^[ \t\r\n]+`),
	}
}

func (d *dmlDefinition) Symbols() map[string]rune {
	return symbols
}

//Lexer that handles some special dml needs
// - ignores all comments
// - has a special token for JavaScript code
type dmlLexer struct {
	cursor    lexer.Position
	buf       []byte
	wspattern *regexp.Regexp
	peek      *lexer.Token
}

func (self *dmlLexer) Peek() lexer.Token {
	if self.peek != nil {
		return *self.peek
	}

	if len(self.buf) > self.cursor.Offset {
		self.skipWS()

		//catch non-regex stuff
		tok, ok := self.matchString()
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

// Match a string
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
			interpreted, _ := strconv.Unquote(string(match))
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
func (self *dmlLexer) skipWS() {
	self.match(self.wspattern)
}
