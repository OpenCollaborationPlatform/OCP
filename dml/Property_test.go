package dml

import (
	datastore "CollaborationNode/datastores"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTypeProperty(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Loading dml code into runtime including a type property,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)

		code := `Data {
				.id:"toplevel"
				
				const property type test: Data {
					property int test: 10
				}
				
				Transaction {
					.id: "trans"
					.recursive: true
				}
			}`

		rntm := NewRuntime(store)
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		Convey("The property must be accessbile", func() {
			So(rntm.mainObj.HasProperty("test"), ShouldBeTrue)
			val := rntm.mainObj.GetProperty("test").GetValue()
			str, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(str, ShouldNotBeEmpty)

			val, err := rntm.RunJavaScript("toplevel.test")
			So(err, ShouldBeNil)
			strJs, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(strJs, ShouldEqual, str)
		})

	})
}
