//parser for the datastructure markup language
package dml

import (
	"testing"

	"github.com/alecthomas/participle"
	. "github.com/smartystreets/goconvey/convey"
)

func TestSimpleObject(t *testing.T) {

	Convey("Given some simple toplevel dml object,", t, func() {

		var text = `
		Test{ 
			.id: 1
			.name: "my funny \" sting"
		}`

		Convey("and parsing it with the object parser", func() {

			dml := &DML{}
			parser, perr := participle.Build(&DML{}, &dmlDefinition{})
			err := parser.ParseString(text, dml)

			Convey("The parsing should not throw an error", func() {
				So(perr, ShouldBeNil)
				So(err, ShouldBeNil)
			})
			Convey("and the result should match the input", func() {
				So(dml.Object, ShouldNotBeNil)
				obj := dml.Object
				So(obj.Identifier, ShouldEqual, "Test")
				So(len(obj.Properties), ShouldEqual, 2)

				prop := obj.Properties[0]
				So(prop.Key, ShouldEqual, "id")
				So(*prop.Value.Int, ShouldEqual, 1)

				prop = obj.Properties[1]
				So(prop.Key, ShouldEqual, "name")
				So(*prop.Value.String, ShouldEqual, `my funny " sting`)
			})
		})
	})
}

func TestNestedObject(t *testing.T) {

	Convey("Given some nested dml object,", t, func() {

		var text = `
		Test{ 
			.id: 1
			.name: "my funny \" sting" 
			
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

		Convey("and parsing it with the object parser", func() {

			dml := &DML{}
			parser, perr := participle.Build(&DML{}, &dmlDefinition{})
			err := parser.ParseString(text, dml)

			Convey("The parsing should not throw an error", func() {
				So(perr, ShouldBeNil)
				So(err, ShouldBeNil)
			})
			Convey("and the result should match the input", func() {
				So(dml.Object, ShouldNotBeNil)
				obj := dml.Object
				So(obj.Identifier, ShouldEqual, "Test")
				So(len(obj.Properties), ShouldEqual, 2)
				So(len(obj.Objects), ShouldEqual, 1)

				obj = obj.Objects[0]
				So(obj.Identifier, ShouldEqual, "SubObject")
				So(len(obj.Properties), ShouldEqual, 2)
				prop := obj.Properties[0]
				So(prop.Key, ShouldEqual, "id")
				So(*prop.Value.Number, ShouldAlmostEqual, 1.1)
				prop = obj.Properties[1]
				So(prop.Key, ShouldEqual, "value")
				So(*prop.Value.Bool, ShouldEqual, `false`)
				So(len(obj.Objects), ShouldEqual, 2)
			})
		})
	})
}
