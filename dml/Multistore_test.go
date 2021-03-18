package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMultistore(t *testing.T) {

	Convey("Loading dml code and initializing two stores,", t, func() {

		//make temporary folders for the data
		path1, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path1)
		path2, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path2)

		//create the datastores
		store1, err := datastore.NewDatastore(path1)
		defer store1.Close()
		So(err, ShouldBeNil)

		store2, err := datastore.NewDatastore(path2)
		defer store2.Close()
		So(err, ShouldBeNil)

		code := `Vector {
					.name:"Vector"
					.type: Map {
						.name: "Map"
						.key: string
						.value: Data {
							property int test: 0
						}
					}
				}`

		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)

		err = rntm.InitializeDatastore(store1)
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store2)
		So(err, ShouldBeNil)

		Convey("add to the first store works", func() {

			code := `	Vector.AppendNew()
					 	if (Vector.Length() != 1) {
							throw "length not correct"
						}
					`
			_, err := rntm.RunJavaScript(store1, "user", code)
			So(err, ShouldBeNil)

			Convey("while the second is still empty", func() {

				code := `	if (Vector.Length() != 0) {
								throw "length not correct"
							}
					`
				_, err := rntm.RunJavaScript(store2, "user", code)
				So(err, ShouldBeNil)
			})

			Convey("and is not overridden by adding stuff to the second store", func() {

				code := `	map = Vector.AppendNew()
						 	if (Vector.Length() != 1) {
								throw "length not correct"
							}
							entry = map.New("entry")
							entry.test = 1
						`
				_, err := rntm.RunJavaScript(store2, "user", code)
				So(err, ShouldBeNil)

				code = ` 	if (Vector.Length() != 1) {
								throw "length not correct"
							}
							map = Vector.Get(0)
							if (map.Length() != 0) {
								throw "length not correct"
							}
						`
				_, err = rntm.RunJavaScript(store1, "user", code)
				So(err, ShouldBeNil)
			})
		})

	})
}
