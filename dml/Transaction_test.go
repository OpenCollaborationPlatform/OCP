//parser for the datastructure markup language
package dml

import (
	datastore "CollaborationNode/datastores"
	"index/suffixarray"
	"io/ioutil"
	"os"
	"regexp"
	"strings"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestBasics(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	Convey("Setting up the basic runtime,", t, func() {

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		rntm := NewRuntime(store)

		Convey("a transaction manager shall be created", func() {

			mngr := rntm.transactions
			So(err, ShouldBeNil)

			Convey("which allows creating transactions", func() {

				rntm.currentUser = "User1"
				So(mngr.IsOpen(), ShouldBeFalse)
				trans1, err := mngr.getOrCreateTransaction()
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeTrue)

				rntm.currentUser = "User2"
				So(mngr.IsOpen(), ShouldBeFalse)
				err = mngr.Open()
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeTrue)
				trans2, err := mngr.getOrCreateTransaction()
				So(err, ShouldBeNil)

				So(trans1.Equal(trans2), ShouldBeFalse)

				rntm.currentUser = "User1"
				annotherTrans1, err := mngr.getOrCreateTransaction()
				So(trans1.Equal(annotherTrans1), ShouldBeTrue)
			})

			Convey("as well as deleting them", func() {

				rntm.currentUser = "User1"
				_, err := mngr.getOrCreateTransaction()
				So(err, ShouldBeNil)
				err = mngr.Close()
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeFalse)
				_, err = mngr.getTransaction()
				So(err, ShouldNotBeNil)
			})

			Convey("and is accessible from Javascript.", func() {

				rntm.currentUser = "User3"
				So(mngr.IsOpen(), ShouldBeFalse)
				res, err := rntm.RunJavaScript("Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeFalse)

				_, err = rntm.RunJavaScript("Transaction.Open()")
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeTrue)
				res, err = rntm.RunJavaScript("Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeTrue)

				_, err = rntm.RunJavaScript("Transaction.Close()")
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeFalse)
				res, err = rntm.RunJavaScript("Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeFalse)
			})
		})
	})
}

func TestTransactionBehaviour(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	//create the runtime
	Convey("Testing transaction events by loading dml code", t, func() {

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.id: "Document"
					
					property string result: ""
					
					Transaction {
						
						.id: "trans"
						
						.onOpen: function() {
							this.parent.result += "o1"
						}
						.onParticipation: function() {this.parent.result += "p1"}
						.onClosing: function() {this.parent.result += "c1"}
						.onFailure: function() {this.parent.result += "f1"}
					}
					
					Data {
						.id: "Child"
						
						Data {
							.id: "ChildChild"
						}
						
						Transaction {
							.id: "ChildTransaction"
							.recursive: true
						
							.onOpen: function() {Document.result += "o2"}
							.onParticipation: function() {Document.result += "p2"}
							.onClosing: function() {Document.result += "c2"}
							.onFailure: function() {Document.result += "f2"}
						}
					}
					
					Data {
						.id: "Child2"
					} // test object without transaction behaviour
					
					Data {
						.id: "Child3"
						
						Transaction{
							.id: "Transaction3"
						} //test default transaction behaviour
					}
							
				}`

		rntm := NewRuntime(store)
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		mngr := rntm.transactions

		Convey("the object structure must be correct", func() {

			val, err := rntm.RunJavaScript("Document.trans.parent.id")
			So(err, ShouldBeNil)
			value, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "Document")
		})

		Convey("On opening a transaction", func() {

			mngr.Open()

			Convey("all objects with transaction behaviour must be called", func() {
				res, err := rntm.ReadProperty("Document", "result")
				So(err, ShouldBeNil)
				str := res.(string)
				//note: ordering of calls is undefined
				r1 := regexp.MustCompile("o1")
				index := suffixarray.New([]byte(str))
				results := index.FindAllIndex(r1, -1)
				So(len(results), ShouldEqual, 1)
				r2 := regexp.MustCompile("o2")
				results = index.FindAllIndex(r2, -1)
				So(len(results), ShouldEqual, 2)
			})
		})

		Convey("Adding the main object to the transaction", func() {

			rntm.currentUser = "User1"
			err := mngr.Open()
			So(err, ShouldBeNil)
			_, err = rntm.RunJavaScript("Document.result = ''")
			So(err, ShouldBeNil)
			err = mngr.Add(rntm.mainObj)
			So(err, ShouldBeNil)

			Convey("only its participation event must have been called", func() {

				res, err := rntm.ReadProperty("Document", "result")
				So(err, ShouldBeNil)
				str := res.(string)
				So(str, ShouldEqual, "p1")
			})

			Convey("and adding it to annother transaction must fail", func() {

				rntm.currentUser = "User2"
				err := mngr.Add(rntm.mainObj)
				So(err, ShouldNotBeNil)
				So(mngr.IsOpen(), ShouldBeFalse)

				err = mngr.Open()
				So(err, ShouldBeNil)
				err = mngr.Add(rntm.mainObj)
				So(err, ShouldNotBeNil)
			})
		})
	})
}
