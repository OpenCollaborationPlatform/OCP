//parser for the datastructure markup language
package dml

import (
	"encoding/json"
	"testing"

	"github.com/alecthomas/participle"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSimpleObject(t *testing.T) {

	Convey("Parsing a simple toplevel dml object,", t, func() {

		var text = `
		/**multiline comments
		 * should be fine
		 */
		Test{
			.name: 1 //with comment

			//and even more comments
			.name: "my funny \" string"

			property int 		myprop: 	 1 
			property float 		myseco: 	 1.1
			const property int 	myconst:	 2
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
		So(perr, ShouldBeNil)
		err := parser.ParseString(text, dml)
		So(err, ShouldBeNil)

		Convey("the result should match the input", func() {
			So(dml.Object, ShouldNotBeNil)
			obj := dml.Object
			So(obj.Identifier, ShouldEqual, "Test")
			So(len(obj.Assignments), ShouldEqual, 2)

			prop := obj.Assignments[0]
			So(len(prop.Key), ShouldEqual, 1)
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.Int, ShouldEqual, 1)

			prop = obj.Assignments[1]
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.String, ShouldEqual, `my funny " string`)
		})
		Convey("and the properties should be created correctly,", func() {

			obj := dml.Object
			So(len(obj.Properties), ShouldEqual, 3)
			newprop := obj.Properties[0]
			So(newprop.Type.Pod, ShouldEqual, "int")
			So(newprop.Key, ShouldEqual, "myprop")
			So(*newprop.Default.Int, ShouldEqual, 1)
			So(newprop.Const, ShouldEqual, "")
		})
		Convey("with const value being read only.", func() {

			obj := dml.Object
			newprop := obj.Properties[2]
			So(newprop.Type.Pod, ShouldEqual, "int")
			So(newprop.Key, ShouldEqual, "myconst")
			So(*newprop.Default.Int, ShouldEqual, 2)
			So(newprop.Const, ShouldNotEqual, "")
		})
	})
}

func TestNestedObject(t *testing.T) {

	Convey("Parsing a nested dml object,", t, func() {

		var text = `
		Test{
			.name: 1
			.name: "my funny \" string"

			SubObject {
				.name: 1.1
				.value: false

				SubSubObject1 {
					.name: "Who cares"
				}
				SubSubObject2 {

				}
			}
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
		So(perr, ShouldBeNil)

		err := parser.ParseString(text, dml)
		So(err, ShouldBeNil)

		Convey("the result should match the input", func() {
			So(dml.Object, ShouldNotBeNil)
			obj := dml.Object
			So(obj.Identifier, ShouldEqual, "Test")
			So(len(obj.Assignments), ShouldEqual, 2)
			So(len(obj.Objects), ShouldEqual, 1)

			obj = obj.Objects[0]
			So(obj.Identifier, ShouldEqual, "SubObject")
			So(len(obj.Assignments), ShouldEqual, 2)
			prop := obj.Assignments[0]
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
			prop = obj.Assignments[1]
			So(prop.Key[0], ShouldEqual, "value")
			So(bool(*prop.Value.Bool), ShouldBeFalse)
			So(len(obj.Objects), ShouldEqual, 2)
		})

		Convey("and shall also be serializable", func() {

			obj := dml.Object
			data, err := json.Marshal(obj)
			So(err, ShouldBeNil)

			var reObj *astObject
			err = json.Unmarshal(data, &reObj)
			So(err, ShouldBeNil)

			//retest everything from above with the new object
			So(len(reObj.Assignments), ShouldEqual, 2)
			So(len(reObj.Objects), ShouldEqual, 1)

			reObj = reObj.Objects[0]
			So(reObj.Identifier, ShouldEqual, "SubObject")
			So(len(reObj.Assignments), ShouldEqual, 2)
			prop := reObj.Assignments[0]
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
			prop = reObj.Assignments[1]
			So(prop.Key[0], ShouldEqual, "value")
			So(bool(*prop.Value.Bool), ShouldBeFalse)
			So(len(reObj.Objects), ShouldEqual, 2)
		})
	})
}

func TestJavascriptFunctions(t *testing.T) {

	Convey("Parsing a dml object with js functions,", t, func() {

		var text = `
		Test{
			.name: "my funny \" string"
			
			SubObject {
				.name: 1.1

				function MySubFunc(vara, varb) {
					could be annything
				}
			}

			function MyFunc(vara, varb) {
				could be annything
			}
			
			const function MyConstFunc() {
				could be annything
			}
			
			.name: 1
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, participle.Lexer(&dmlDefinition{}))
		So(perr, ShouldBeNil)
		err := parser.ParseString(text, dml)
		So(err, ShouldBeNil)

		Convey("the result should match the input", func() {
			So(dml.Object, ShouldNotBeNil)
			obj := dml.Object

			So(obj.Identifier, ShouldEqual, "Test")
			So(len(obj.Assignments), ShouldEqual, 2)
			So(len(obj.Objects), ShouldEqual, 1)
			So(len(obj.Functions), ShouldEqual, 2)

			//check function constness
			So(obj.Functions[0].Const, ShouldEqual, "")
			So(obj.Functions[1].Const, ShouldEqual, "const")

			//check subobject
			obj = obj.Objects[0]
			So(obj.Identifier, ShouldEqual, "SubObject")
			So(len(obj.Assignments), ShouldEqual, 1)
			So(len(obj.Functions), ShouldEqual, 1)
			prop := obj.Assignments[0]
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
		})
	})
}
