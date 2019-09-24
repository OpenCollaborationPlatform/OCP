package datastructure

import (
	"path/filepath"
	"os"
	"io/ioutil"
	"testing"
	
	"github.com/ickby/CollaborationNode/dml"
	. "github.com/smartystreets/goconvey/convey"
)

const (
	
	dmlContent = 
		  ` Data {
				.id: "Test"

			    property string 	testS: "Hallo"
			    property int 	testI: 1
				property bool 	testB: true
				property float	testF 
	
			}`
)

func TestStateSnapshot(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "datastructure")
	defer os.RemoveAll(path)
	
	//copy the dml file in
	dmlpath := filepath.Join(path, "Dml")
	os.MkdirAll(dmlpath, os.ModePerm)
	ioutil.WriteFile(filepath.Join(dmlpath, "main.dml"), []byte(dmlContent), os.ModePerm)
	
	Convey("Setting up a new state", t, func() {
		
		state, err := newState(path)
		So(err, ShouldBeNil)
		So(state, ShouldNotBeNil)
	
		Convey("A empty snapshot is createable", func() {

			data, err := state.Snapshot() 
			So(err, ShouldBeNil)
			So(len(data), ShouldNotEqual, 0)
			
			s1, _ := state.dml.ReadProperty(dml.User("test"), "/", "testS")
			i1, _ := state.dml.ReadProperty(dml.User("test"), "/", "testI")
			b1, _ :=  state.dml.ReadProperty(dml.User("test"), "/", "testB")
			f1, _ := state.dml.ReadProperty(dml.User("test"), "/", "testF")
			
			Convey("as well as reloadable",func() {
				err := state.LoadSnapshot(data)
				So(err, ShouldBeNil)
				
				s2, _ := state.dml.ReadProperty(dml.User("test"), "/", "testS")
				i2, _ := state.dml.ReadProperty(dml.User("test"), "/", "testI")
				b2, _ :=  state.dml.ReadProperty(dml.User("test"), "/", "testB")
				f2, _ := state.dml.ReadProperty(dml.User("test"), "/", "testF")
			
				So(s1, ShouldEqual, s2)
				So(i1, ShouldEqual, i2)
				So(b1, ShouldEqual, b2)
				So(f1, ShouldEqual, f2)
			})
		})
	})
}
