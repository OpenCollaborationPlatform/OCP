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

				So(mngr.IsOpen(), ShouldBeFalse)
				res, err := rntm.RunJavaScript("User3", "Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeFalse)

				_, err = rntm.RunJavaScript("User3", "Transaction.Open()")
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeTrue)
				res, err = rntm.RunJavaScript("User3", "Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeTrue)

				_, err = rntm.RunJavaScript("User3", "Transaction.Close()")
				So(err, ShouldBeNil)
				So(mngr.IsOpen(), ShouldBeFalse)
				res, err = rntm.RunJavaScript("User3", "Transaction.IsOpen()")
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
					
					Data {
						.id: "result"
						property string value: ""
					}
					
					Transaction {
						
						.id: "trans"
						
						.onOpen: function() {
							this.parent.result.value += "o1"
						}
						.onParticipation: function() {this.parent.result.value += "p1"}
						.onClosing: function() {this.parent.result.value += "c1"}
						.onFailure: function() {this.parent.result.value += "f1"}
					}
					
					Data {
						.id: "Child"
						
						Data {
							.id: "ChildChild"
						}
						
						Transaction {
							.id: "ChildTransaction"
							.recursive: true
						
							.onOpen: function() {Document.result.value += "o2"}
							.onParticipation: function() {Document.result.value += "p2"}
							.onClosing: function() {Document.result.value += "c2"}
							.onFailure: function() {Document.result.value += "f2"}
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

			val, err := rntm.RunJavaScript("User1", "Document.trans.parent.id")
			So(err, ShouldBeNil)
			value, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "Document")
		})

		Convey("On opening a transaction", func() {

			mngr.Open()

			Convey("all objects with transaction behaviour must be called", func() {
				res, err := rntm.ReadProperty("User1", "Document.result", "value")
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
			_, err = rntm.RunJavaScript("User1", "Document.result.value = ''")
			So(err, ShouldBeNil)
			err = mngr.Add(rntm.mainObj)
			So(err, ShouldBeNil)

			Convey("only its participation event must have been called", func() {

				res, err := rntm.ReadProperty("User1", "Document.result", "value")
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

func TestTransactionAbort(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "dml")
	defer os.RemoveAll(path)

	//create the runtime
	Convey("Testing transaction abort by loading dml code and opening an transaction", t, func() {

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.id: "Document"
					
					property  int p: 1
					property bool abort: false
					
					Data {
						.id: "DocumentObject"
							
						property int p: 1
						
						function test() {
							this.p = 10
							this.parent.p = 10	
						}
					}
					
					Data {
						.id: "TransDocumentObject"
							
						property int p: 1
						
						Transaction{
							.id: "trans"
						}
					}
					
					Data {
						.id: "FailTransDocumentObject"
							
						property int p: 1
						
						Transaction{
							.id: "trans"
							
							.onParticipation: function() {	
								throw("Unable to take part in transaction")
							}
						}
					}
					
					Transaction {
						
						.id: "trans"
						
						function CanBeAdded() {
							return !this.parent.abort
						}
					}
				}`

		rntm := NewRuntime(store)
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		So(rntm.transactions.Open(), ShouldBeNil)

		Convey("leads to a single transaction in the manager.", func() {
			mngr := rntm.transactions
			keys, err := mngr.transactions.GetKeys()

			So(err, ShouldBeNil)
			So(len(keys), ShouldEqual, 1)
		})

		Convey("Closing the transaction", func() {

			mngr := rntm.transactions
			mngr.Close()

			Convey("leads to zero transaction in the manager.", func() {
				mngr := rntm.transactions
				keys, err := mngr.transactions.GetKeys()

				So(err, ShouldBeNil)
				So(len(keys), ShouldEqual, 0)
			})
		})

		Convey("Changing the objects properties must work initially", func() {

			code = `Document.p = 2; Document.DocumentObject.p=2;`
			rntm.transactions.Open()
			_, err := rntm.RunJavaScript("User1", code)
			So(err, ShouldBeNil)
			So(rntm.mainObj.GetProperty("p").GetValue(), ShouldEqual, 2)
			do, _ := rntm.mainObj.GetChildByName("DocumentObject")
			So(do.GetProperty("p").GetValue(), ShouldEqual, 2)

			Convey("and lead to an open transaction with the relevant object included", func() {

				mngr := rntm.transactions
				keys, _ := mngr.transactions.GetKeys()
				So(len(keys), ShouldEqual, 1)

				trans, err := mngr.getTransaction()
				So(err, ShouldBeNil)
				user, err := trans.User()
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "User1")
				objs := trans.Objects()
				So(len(objs), ShouldEqual, 1)
				So(objs[0].Id().Name, ShouldEqual, "Document")
			})
		})

		Convey("Setting abort to true", func() {

			rntm.mainObj.GetProperty("abort").SetValue(true)
			rntm.mainObj.FixStateAsVersion()

			Convey("Changing data of non-transactino subobject should work", func() {

				code = `Document.DocumentObject.p=3;`
				_, err := rntm.RunJavaScript("User1", code)

				So(err, ShouldBeNil)
				So(rntm.mainObj.GetProperty("p").GetValue(), ShouldEqual, 2)
				do, _ := rntm.mainObj.GetChildByName("DocumentObject")
				So(do.GetProperty("p").GetValue(), ShouldEqual, 3)
			})

			Convey("but changing data of toplevel should fail", func() {

				code = `Document.p=4;`
				_, err := rntm.RunJavaScript("User1", code)
				So(err, ShouldNotBeNil)
				So(rntm.mainObj.GetProperty("p").GetValue(), ShouldEqual, 2)
				do, _ := rntm.mainObj.GetChildByName("DocumentObject")
				So(do.GetProperty("p").GetValue(), ShouldEqual, 3)

				Convey("And transaction should have no object", func() {
					mngr := rntm.transactions
					trans, _ := mngr.getTransaction()
					obj := trans.Objects()
					So(len(obj), ShouldEqual, 0)
				})
			})

			Convey("Failing data change after successful transaction subobject", func() {

				//initially 0 objects required
				mngr := rntm.transactions
				trans, _ := mngr.getTransaction()
				obj := trans.Objects()
				So(len(obj), ShouldEqual, 0)

				code = `Document.TransDocumentObject.p=5; Document.FailTransDocumentObject.p = 5`
				_, err := rntm.RunJavaScript("User1", code)
				So(err, ShouldNotBeNil)

				Convey("Should not have changed the data", func() {
					So(rntm.mainObj.GetProperty("p").GetValue(), ShouldEqual, 2)
					tdo, _ := rntm.mainObj.GetChildByName("TransDocumentObject")
					So(tdo.GetProperty("p").GetValue(), ShouldEqual, 1)
					tdo, _ = rntm.mainObj.GetChildByName("FailTransDocumentObject")
					So(tdo.GetProperty("p").GetValue(), ShouldEqual, 1)
				})

				Convey("and transaction should have no object", func() {
					obj := trans.Objects()
					So(len(obj), ShouldEqual, 0)
				})
			})

			Convey("Opening a transaction directly bevore failing data change", func() {

				mngr := rntm.transactions

				mngr.Close()
				mngr.Commit()

				_, err := mngr.getTransaction()
				So(err, ShouldNotBeNil)
				keys, _ := mngr.transactions.GetKeys()
				So(len(keys), ShouldEqual, 0)

				code = `Transaction.Open(); Document.FailTransDocument.p = 5`
				_, err = rntm.RunJavaScript("User1", code)

				Convey("Should be an error", func() {
					So(err, ShouldNotBeNil)
				})

				Convey("and should not lead to an open transaction", func() {

					_, err := mngr.getTransaction()
					So(err, ShouldNotBeNil)
					keys, _ := mngr.transactions.GetKeys()
					So(len(keys), ShouldEqual, 0)
				})
			})
		})

	})
}
