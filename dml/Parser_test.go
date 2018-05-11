//parser for the datastructure markup language
package dml

import (
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
			.id: 1 //with comment

			//and even more comments
			.name: "my funny \" string"

			property int 	myprop: 1
			property float 	myseco: 1.1
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, &dmlDefinition{})
		err := parser.ParseString(text, dml)
		So(perr, ShouldBeNil)
		So(err, ShouldBeNil)

		Convey("the result should match the input", func() {
			So(dml.Object, ShouldNotBeNil)
			obj := dml.Object
			So(obj.Identifier, ShouldEqual, "Test")
			So(len(obj.Assignments), ShouldEqual, 2)

			prop := obj.Assignments[0]
			So(len(prop.Key), ShouldEqual, 1)
			So(prop.Key[0], ShouldEqual, "id")
			So(*prop.Value.Int, ShouldEqual, 1)

			prop = obj.Assignments[1]
			So(prop.Key[0], ShouldEqual, "name")
			So(*prop.Value.String, ShouldEqual, `my funny " string`)
		})
		Convey("and the properties should be created correctly", func() {

			obj := dml.Object
			So(len(obj.Properties), ShouldEqual, 2)
			newprop := obj.Properties[0]
			So(newprop.Type.Type, ShouldEqual, "int")
			So(newprop.Key, ShouldEqual, "myprop")
			So(*newprop.Default.Int, ShouldEqual, 1)
		})
	})
}

func TestNestedObject(t *testing.T) {

	Convey("Parsing a nested dml object,", t, func() {

		var text = `
		Test{
			.id: 1
			.name: "my funny \" string"

			SubObject {
				.id: 1.1
				.value: false

				SubSubObject1 {
					.id: "Who cares"
				}
				SubSubObject2 {

				}
			}
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, &dmlDefinition{})
		err := parser.ParseString(text, dml)

		So(perr, ShouldBeNil)
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
			So(prop.Key[0], ShouldEqual, "id")
			So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
			prop = obj.Assignments[1]
			So(prop.Key[0], ShouldEqual, "value")
			So(bool(*prop.Value.Bool), ShouldBeFalse)
			So(len(obj.Objects), ShouldEqual, 2)
		})
	})
}

func TestJavascriptFunctions(t *testing.T) {

	Convey("Parsing a dml object with js functions,", t, func() {

		var text = `
		Test{
			.name: "my funny \" string"
			
			SubObject {
				.id: 1.1

				function MySubFunc(vara, varb) {
					could be annything
				}
			}

			function MyFunc(vara, varb) {
				could be annything
			}
			
			.id: 1
		}`

		dml := &DML{}
		parser, perr := participle.Build(&DML{}, &dmlDefinition{})
		err := parser.ParseString(text, dml)
		So(perr, ShouldBeNil)
		So(err, ShouldBeNil)

		Convey("the result should match the input", func() {
			So(dml.Object, ShouldNotBeNil)
			obj := dml.Object

			So(obj.Identifier, ShouldEqual, "Test")
			So(len(obj.Assignments), ShouldEqual, 2)
			So(len(obj.Objects), ShouldEqual, 1)

			obj = obj.Objects[0]
			So(obj.Identifier, ShouldEqual, "SubObject")
			So(len(obj.Assignments), ShouldEqual, 1)
			prop := obj.Assignments[0]
			So(prop.Key[0], ShouldEqual, "id")
			So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
		})
	})
}
