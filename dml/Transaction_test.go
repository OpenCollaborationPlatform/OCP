package dml

import (
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/OpenCollaborationPlatform/OCP/utils"

	datastore "github.com/OpenCollaborationPlatform/OCP/datastores"

	. "github.com/smartystreets/goconvey/convey"
)

func TestTransactionBasics(t *testing.T) {

	Convey("Setting up the basic runtime,", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, err := datastore.NewDatastore(path)
		defer store.Close()
		So(err, ShouldBeNil)
		rntm := NewRuntime()
		err = rntm.Parse(strings.NewReader("Data {}"))
		So(err, ShouldBeNil)
		rntm.currentUser = "User1"
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)

		Convey("a transaction manager shall be created", func() {

			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			So(err, ShouldBeNil)

			Convey("which allows creating transactions", func() {

				store.Begin()
				defer store.Commit()

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

				store.Begin()
				defer store.Commit()

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

				store.Begin()
				So(mngr.IsOpen(), ShouldBeFalse)
				store.Rollback()
				res, _, err := rntm.RunJavaScript(store, "User3", "Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeFalse)

				_, _, err = rntm.RunJavaScript(store, "User3", "Transaction.Open()")
				So(err, ShouldBeNil)
				store.Begin()
				So(mngr.IsOpen(), ShouldBeTrue)
				store.Rollback()
				res, _, err = rntm.RunJavaScript(store, "User3", "Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeTrue)

				_, _, err = rntm.RunJavaScript(store, "User3", "Transaction.Close()")
				So(err, ShouldBeNil)
				store.Begin()
				So(mngr.IsOpen(), ShouldBeFalse)
				store.Rollback()
				res, _, err = rntm.RunJavaScript(store, "User3", "Transaction.IsOpen()")
				So(err, ShouldBeNil)
				So(res.(bool), ShouldBeFalse)
			})
		})
	})
}

func TestTransactionBehaviour(t *testing.T) {

	//create the runtime
	Convey("Testing transaction events by loading dml code", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"

					Data {
						.name: "result"
						property string value: ""
					}

					Transaction {

						.name: "trans"

						.onParticipation: function() {this.parent.result.value += "p1"}
						.onClosing: function() {this.parent.result.value += "c1"}
						.onFailure: function() {this.parent.result.value += "f1"}
					}

					Data {
						.name: "Child"

						property int test: 0

						Data {
							.name: "ChildChild"

							property int value: 0
						}

						Map {
							.name: "ChildMap"
							.key: string
							.value: Data {
								.name: "sub"
								property int value: 0
							}
						}

						Transaction {
							.name: "trans"
							.recursive: true

							.onParticipation: function() {Document.result.value += "p2"}
							.onClosing: function() {Document.result.value += "c2"}
							.onFailure: function() {Document.result.value += "f2"}
						}
					}

					Data {
						.name: "Child2"
					} // test object without transaction behaviour

					Data {
						.name: "Child3"

						property bool created: false

						.onCreated: function() {
							if (this.Transaction.InTransaction()) {
								throw "In transaction, but should not"
							}
						}

						Transaction{
							.name: "Transaction"
							.automatic: true
						}
					}

				}`

		rntm := NewRuntime()
		rntm.currentUser = "User1"
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)
		mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)

		Convey("the object structure must be correct", func() {

			val, _, err := rntm.RunJavaScript(store, "User1", "Document.trans.parent.name")
			So(err, ShouldBeNil)
			value, ok := val.(string)
			So(ok, ShouldBeTrue)
			So(value, ShouldEqual, "Document")
		})

		Convey("On opening a transaction", func() {

			store.Begin()
			err := mngr.Open()
			So(err, ShouldBeNil)
			store.Commit()

			Convey("a transaction shall be created", func() {

				store.Begin()
				defer store.Rollback()
				trans, err := mngr.getTransaction()
				So(err, ShouldBeNil)
				user, err := trans.User()
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "User1")
			})

			Convey("Adding the main object to a transaction", func() {

				rntm.currentUser = "User1"
				store.Begin()
				err := mngr.Open()
				store.Commit()
				So(err, ShouldBeNil)

				_, _, err = rntm.RunJavaScript(store, "User1", "Document.result.value = ''")
				So(err, ShouldBeNil)

				store.Begin()
				mset, _ := rntm.getMainObjectSet()
				bhvrSet, _ := mset.obj.(Data).GetBehaviour(mset.id, "Transaction")
				trns := bhvrSet.obj.(*objectTransaction)
				err = trns.add(bhvrSet.id)
				store.Commit()
				So(err, ShouldBeNil)

				Convey("it is listed as part of the transaction ", func() {

					store.Begin()
					defer store.Rollback()
					trans, err := mngr.getTransaction()
					So(err, ShouldBeNil)
					objs, err := trans.Behaviours()
					So(err, ShouldBeNil)
					So(len(objs), ShouldEqual, 1)

					obj, _ := getObjectFromPath(rntm, "Document.trans")
					So(trans.HasBehaviour(obj.id), ShouldBeTrue)
				})

				Convey("and knows itself that it belongs to the transaction", func() {

					store.Begin()
					defer store.Rollback()
					mset, _ := rntm.getMainObjectSet()
					bhvrSet, _ := mset.obj.(Data).GetBehaviour(mset.id, "Transaction")
					trns := bhvrSet.obj.(*objectTransaction)
					ok, err := trns.InTransaction(bhvrSet.id)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
					ok, err = trns.InCurrentTransaction(bhvrSet.id)
					So(err, ShouldBeNil)
					So(ok, ShouldBeTrue)
				})

				Convey("only its participation event must have been called", func() {

					res, _, err := rntm.Call(store, "User1", "Document.result.value")
					So(err, ShouldBeNil)
					str := res.(string)
					So(str, ShouldEqual, "p1")
				})

				Convey("and adding it to annother transaction must fail", func() {

					store.Begin()
					defer store.Commit()

					rntm.currentUser = "User2"
					mset, _ := rntm.getMainObjectSet()
					bhvrSet, _ := mset.obj.(Data).GetBehaviour(mset.id, "Transaction")
					trns := bhvrSet.obj.(*objectTransaction)
					err = trns.add(bhvrSet.id)
					So(err, ShouldNotBeNil)
					So(mngr.IsOpen(), ShouldBeFalse)

					err = mngr.Open()
					So(err, ShouldBeNil)
					err = trns.add(bhvrSet.id)
					So(err, ShouldNotBeNil)
				})

				Convey("and closing the transaction works", func() {

					store.Begin()

					rntm.currentUser = "User1"
					err := mngr.Close()
					open := mngr.IsOpen()
					store.Commit()
					So(err, ShouldBeNil)
					So(open, ShouldBeFalse)

					res, _, err := rntm.Call(store, "User1", "Document.result.value")
					So(err, ShouldBeNil)
					str := res.(string)
					So(str, ShouldEqual, "p1c1")

					Convey("makes the main object transactionless again", func() {

						store.Begin()
						defer store.Rollback()
						mset, _ := rntm.getMainObjectSet()
						bhvrSet, _ := mset.obj.(Data).GetBehaviour(mset.id, "Transaction")
						trns := bhvrSet.obj.(*objectTransaction)
						ok, err := trns.InTransaction(bhvrSet.id)
						So(err, ShouldBeNil)
						So(ok, ShouldBeFalse)
						ok, err = trns.InCurrentTransaction(bhvrSet.id)
						So(err, ShouldBeNil)
						So(ok, ShouldBeFalse)
					})
				})
			})

			Convey("Changing a property on object with transaction behaviour", func() {

				_, _, err := rntm.RunJavaScript(store, "User1", "Document.Child.test = 2")
				So(err, ShouldBeNil)

				Convey("Adds the object to the transaction", func() {

					store.Begin()
					defer store.Rollback()

					set, _ := getObjectFromPath(rntm, "Document.Child.trans")
					trans, err := mngr.getTransaction()
					So(err, ShouldBeNil)
					has := trans.HasBehaviour(set.id)
					So(has, ShouldBeTrue)
				})
			})

			Convey("Changing a object below a recursive transaction", func() {

				_, _, err := rntm.RunJavaScript(store, "User1", "Document.Child.ChildChild.value = 2")
				So(err, ShouldBeNil)

				Convey("Adds the behaviour equiped object to the transaction", func() {

					store.Begin()
					defer store.Rollback()

					set, _ := getObjectFromPath(rntm, "Document.Child.trans")
					trans, err := mngr.getTransaction()
					So(err, ShouldBeNil)
					has := trans.HasBehaviour(set.id)
					So(has, ShouldBeTrue)
				})
			})

			Convey("Creating a new object below a recursive transaction behaviour", func() {

				ret, _, err := rntm.RunJavaScript(store, "User1", "Document.Child.ChildMap.New(\"test\")")
				So(err, ShouldBeNil)
				newMapEntry := ret.(Identifier)

				Convey("adds the behaviour equiped object to the transaction.", func() {

					store.Begin()
					defer store.Rollback()

					set, _ := getObjectFromPath(rntm, "Document.Child.trans")
					trans, err := mngr.getTransaction()
					So(err, ShouldBeNil)
					has := trans.HasBehaviour(set.id)
					So(has, ShouldBeTrue)
				})

				Convey("stores the new object for later processing", func() {

					store.Begin()
					defer store.Rollback()
					bhvSet, _ := getObjectFromPath(rntm, "Document.Child.trans")
					bbhvr, ok := bhvSet.obj.(*objectTransaction)
					So(ok, ShouldBeTrue)
					trans, _ := mngr.getTransaction()
					subs, err := bbhvr.getNewSubobjects(bhvSet.id, trans.identification)
					So(err, ShouldBeNil)
					So(subs, ShouldResemble, []Identifier{newMapEntry})
				})

				Convey("Creating a new transaction and changing the new subobject value", func() {

					store.Begin()
					err = mngr.Open()
					store.Commit()
					So(err, ShouldBeNil)

					_, _, err := rntm.RunJavaScript(store, "User1", "Document.Child.ChildMap.Get(\"test\").value = 5")
					utils.PrintWithStacktrace(err)
					So(err, ShouldBeNil)

					Convey("adds the behaviour equiped object to the transaction.", func() {

						store.Begin()
						defer store.Rollback()

						set, _ := getObjectFromPath(rntm, "Document.Child.trans")
						trans, err := mngr.getTransaction()
						So(err, ShouldBeNil)
						has := trans.HasBehaviour(set.id)
						So(has, ShouldBeTrue)
					})
				})

				Convey("Aborting the transaction", func() {

					code := `Transaction.Abort()`
					_, _, err := rntm.RunJavaScript(store, "User1", code)
					So(err, ShouldBeNil)

					Convey("removes every database entry of the created object", func() {

						store.Begin()
						defer store.Rollback()

						for _, storage := range datastore.StorageTypes {
							has, err := rntm.datastore.HasSet(storage, false, newMapEntry.Hash())
							So(err, ShouldBeNil)
							So(has, ShouldBeFalse)

							has, err = rntm.datastore.HasSet(storage, true, newMapEntry.Hash())
							So(err, ShouldBeNil)
							So(has, ShouldBeFalse)
						}
					})
				})

				Convey("Directly deleting the new object and closing the transaction", func() {

					code := `Document.Child.ChildMap.Remove("test")
							 Transaction.Close()`
					_, _, err := rntm.RunJavaScript(store, "User1", code)
					So(err, ShouldBeNil)

					Convey("removes every database entry of it", func() {

						store.Begin()
						defer store.Rollback()

						for _, storage := range datastore.StorageTypes {
							has, err := rntm.datastore.HasSet(storage, false, newMapEntry.Hash())
							So(err, ShouldBeNil)
							So(has, ShouldBeFalse)

							has, err = rntm.datastore.HasSet(storage, true, newMapEntry.Hash())
							So(err, ShouldBeNil)
							So(has, ShouldBeFalse)
						}

					})
				})
			})
		})
	})
}

func TestTransactionFail(t *testing.T) {

	//create the runtime
	Convey("Testing transaction abort by loading dml code and opening an transaction", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"

					property  int p: 1
					property bool abort: false

					Data {
						.name: "DocumentObject"

						property int p: 1

						function test() {
							this.p = 10
							this.parent.p = 10
						}
					}

					Data {
						.name: "TransDocumentObject"

						property int p: 1

						Transaction{
							.name: "trans"
						}
					}

					Data {
						.name: "FailTransDocumentObject"

						property int p: 1

						Transaction{
							.name: "trans"

							.onParticipation: function() {
								throw("Unable to take part in transaction")
							}
						}
					}

					Transaction {

						.name: "trans"

						function CanBeAdded() {
							return !this.parent.abort
						}
					}

					Data {
						.name: "DepTest"

						Data {
							.name: "Child1"
							Transaction {
								.name: "Transaction"
							}
						}

						Data {
							.name: "Child2"
							property int p: 0

							Transaction {
								.name: "Transaction"
								.automatic: true

								function DependentObjects() {
									return [Document.DepTest.Child1]
								}
							}
						}
					}
				}`

		rntm := NewRuntime()
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)
		rntm.currentUser = "User1"
		store.Begin()
		So(rntm.behaviours.GetManager("Transaction").(*TransactionManager).Open(), ShouldBeNil)
		store.Commit()

		Convey("leads to a single transaction in the manager.", func() {

			store.Begin()
			defer store.Rollback()

			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			transactions, err := mngr.transactionMap()
			So(err, ShouldBeNil)
			keys, err := transactions.GetKeys()

			So(err, ShouldBeNil)
			So(len(keys), ShouldEqual, 1)
		})

		Convey("Closing the transaction", func() {

			store.Begin()
			defer store.Commit()

			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			mngr.Close()

			Convey("leads to zero transaction in the manager.", func() {
				mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
				transactions, err := mngr.transactionMap()
				So(err, ShouldBeNil)
				keys, err := transactions.GetKeys()

				So(err, ShouldBeNil)
				So(len(keys), ShouldEqual, 0)
			})
		})

		Convey("Changing the objects properties must work", func() {

			code = `	Document.p = 2; Document.DocumentObject.p=2;`

			_, _, err := rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)

			store.Begin()
			mset, _ := rntm.getMainObjectSet()
			val, _ := mset.obj.GetProperty("p").GetValue(mset.id)
			So(val, ShouldEqual, 2)
			do, _ := mset.obj.(Data).GetChildByName(mset.id, "DocumentObject")
			val, _ = do.obj.GetProperty("p").GetValue(do.id)
			So(val, ShouldEqual, 2)
			store.Rollback()

			Convey("and lead to an open transaction with the relevant object included", func() {

				store.Begin()
				defer store.Rollback()

				trans, err := rntm.behaviours.GetManager("Transaction").(*TransactionManager).getTransaction()
				So(err, ShouldBeNil)
				user, err := trans.User()
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "User1")
				objs, err := trans.Behaviours()
				So(err, ShouldBeNil)
				So(len(objs), ShouldEqual, 1)
				parent, _ := objs[0].obj.GetParent(objs[0].id)
				So(parent.id.Name, ShouldEqual, "Document")
			})
		})

		Convey("Setting abort to true and opening a new transaction", func() {

			store.Begin()
			mset, _ := rntm.getMainObjectSet()
			err := mset.obj.GetProperty("abort").SetValue(mset.id, true)
			So(err, ShouldBeNil)
			So(rntm.behaviours.GetManager("Transaction").(*TransactionManager).Open(), ShouldBeNil)
			store.Commit()

			Convey("Changing data of non-transaction subobject should work", func() {

				code = `Document.DocumentObject.p=3;`
				_, _, err := rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldBeNil)

				store.Begin()
				mset, _ := rntm.getMainObjectSet()
				val, _ := mset.obj.GetProperty("p").GetValue(mset.id)
				So(val, ShouldEqual, 1)
				do, _ := mset.obj.(Data).GetChildByName(mset.id, "DocumentObject")
				val, _ = do.obj.GetProperty("p").GetValue(do.id)
				So(val, ShouldEqual, 3)
				store.Commit()
			})

			Convey("but changing data of toplevel should fail", func() {

				code = `Document.p=4;`
				_, _, err := rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldNotBeNil)

				store.Begin()
				mset, _ := rntm.getMainObjectSet()
				val, _ := mset.obj.GetProperty("p").GetValue(mset.id)
				So(val, ShouldEqual, 1)
				do, _ := mset.obj.(Data).GetChildByName(mset.id, "DocumentObject")
				val, _ = do.obj.GetProperty("p").GetValue(do.id)
				So(val, ShouldEqual, 1)
				store.Rollback()

				Convey("And transaction should have no object", func() {
					store.Begin()
					mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
					trans, _ := mngr.getTransaction()
					obj, err := trans.Behaviours()
					So(err, ShouldBeNil)
					So(len(obj), ShouldEqual, 0)
					store.Rollback()
				})
			})

			Convey("Failing data change after successful transaction subobject", func() {

				//initially 0 objects required
				store.Begin()
				mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
				trans, _ := mngr.getTransaction()
				obj, err := trans.Behaviours()
				So(err, ShouldBeNil)
				So(len(obj), ShouldEqual, 0)
				store.Commit()

				code = `Document.TransDocumentObject.p=5; Document.FailTransDocumentObject.p = 5`
				_, _, err = rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldNotBeNil)

				Convey("Should not have changed the data", func() {
					store.Begin()
					mset, _ := rntm.getMainObjectSet()
					val, _ := mset.obj.GetProperty("p").GetValue(mset.id)
					So(val, ShouldEqual, 1)
					tdo, _ := mset.obj.(Data).GetChildByName(mset.id, "TransDocumentObject")
					val, _ = tdo.obj.GetProperty("p").GetValue(tdo.id)
					So(val, ShouldEqual, 1)
					tdo, _ = mset.obj.(Data).GetChildByName(mset.id, "FailTransDocumentObject")
					val, _ = tdo.obj.GetProperty("p").GetValue(tdo.id)
					So(val, ShouldEqual, 1)
					store.Rollback()
				})

				Convey("and transaction should have no object", func() {
					store.Begin()
					obj, err := trans.Behaviours()
					So(err, ShouldBeNil)
					So(len(obj), ShouldEqual, 0)
					store.Rollback()
				})
			})

			Convey("Opening a transaction directly bevore failing data change", func() {
				store.Begin()
				mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)

				mngr.Close()

				_, err := mngr.getTransaction()
				So(err, ShouldNotBeNil)
				transactions, err := mngr.transactionMap()
				So(err, ShouldBeNil)
				keys, _ := transactions.GetKeys()
				So(len(keys), ShouldEqual, 0)
				store.Commit()

				code = `Transaction.Open(); Document.FailTransDocument.p = 5`
				_, _, err = rntm.RunJavaScript(store, "User1", code)

				Convey("Should be an error", func() {
					So(err, ShouldNotBeNil)
				})

				Convey("and should not lead to an open transaction", func() {
					store.Begin()
					_, err := mngr.getTransaction()
					So(err, ShouldNotBeNil)
					transactions, err := mngr.transactionMap()
					So(err, ShouldBeNil)
					keys, _ := transactions.GetKeys()
					So(len(keys), ShouldEqual, 0)
					store.Rollback()
				})
			})
		})

		Convey("Changing a object with automatic transaction enabled", func() {

			code = `Transaction.Close(); Document.DepTest.Child2.p = 1`
			_, _, err := rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)

			Convey("should add this object to the transaction", func() {

				store.Begin()
				defer store.Rollback()

				trans, err := rntm.behaviours.GetManager("Transaction").(*TransactionManager).getTransaction()
				So(err, ShouldBeNil)
				user, err := trans.User()
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "User1")
				objs, err := trans.Behaviours()
				So(err, ShouldBeNil)
				parent, _ := objs[0].obj.GetParent(objs[0].id)
				So(parent.id.Name, ShouldEqual, "Child2")
			})

			Convey("and also adds the dependent object to the transaction", func() {

				store.Begin()
				defer store.Rollback()

				trans, err := rntm.behaviours.GetManager("Transaction").(*TransactionManager).getTransaction()
				So(err, ShouldBeNil)
				user, err := trans.User()
				So(err, ShouldBeNil)
				So(user, ShouldEqual, "User1")
				objs, err := trans.Behaviours()
				So(err, ShouldBeNil)
				So(len(objs), ShouldEqual, 2)
				parent, _ := objs[1].obj.GetParent(objs[1].id)
				So(parent.id.Name, ShouldEqual, "Child1")
			})
		})
	})
}

func TestTransactionAbort(t *testing.T) {

	//create the runtime
	Convey("Testing transaction abort by loading dml code", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"

					property  int value: 1

					Data {
						.name: "Child"

						property int value: 1

						function test() {
							this.p = 10
							this.parent.p = 10
						}
					}

					Transaction {

						.name: "trans"
						.recursive: true
						.automatic: true
					}
				}`

		rntm := NewRuntime()
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)
		rntm.currentUser = "User1"

		Convey("Changing some data in the toplevel object is abortable", func() {

			code := `Document.value = 5
					 Transaction.Abort()`
			_, _, err = rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)

			store.Begin()
			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			So(mngr.IsOpen(), ShouldBeFalse)
			set, _ := getObjectFromPath(rntm, "Document")
			value, _ := set.obj.GetProperty("value").GetValue(set.id)
			So(value, ShouldEqual, 1)
		})

		Convey("Changing some data in the recursive object is abortable", func() {

			code := `Document.Child.value = 5
					 Transaction.Abort()`
			_, _, err = rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)

			store.Begin()
			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			So(mngr.IsOpen(), ShouldBeFalse)
			set, _ := getObjectFromPath(rntm, "Document.Child")
			value, _ := set.obj.GetProperty("value").GetValue(set.id)
			So(value, ShouldEqual, 1)
		})
	})
}

func TestPartialTransaction(t *testing.T) {

	//create the runtime
	Convey("Testing partial transaction abort by loading dml code", t, func() {

		//make temporary folder for the data
		path, _ := ioutil.TempDir("", "dml")
		defer os.RemoveAll(path)

		store, _ := datastore.NewDatastore(path)
		defer store.Close()

		var code = `
				Data {
					.name: "Document"

					property int value1: 1
					property int value2: 1
					property int value3: 1
					
					Map {
						.name: "Child"
						
						.key: int
						.value: int
					}
					
					Map {
						.name: "ObjectMap"
						
						.key: string
						.value: Data {
						
							.name: "subobject"
							
							property int data: 0
							
							Data {
								.name: "subsubobject"
								
								property int data: 0
							}
						}
					}

					PartialTransaction {

						.name: "trans"
						.recursive: true
						.automatic: true

						
					}
				}`

		rntm := NewRuntime()
		err := rntm.Parse(strings.NewReader(code))
		So(err, ShouldBeNil)
		err = rntm.InitializeDatastore(store)
		So(err, ShouldBeNil)
		rntm.currentUser = "User1"

		Convey("Changing some data in the toplevel object works", func() {

			code := `Document.value1 = 5
					 Document.value2 = 3
					 Document.trans.CurrentTransactionKeys()`

			keys, _, err := rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)
			So(keys, ShouldResemble, []string{"value1", "value2"})

			store.Begin()
			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			So(mngr.IsOpen(), ShouldBeTrue)
			set, _ := getObjectFromPath(rntm, "Document")
			value, _ := set.obj.GetProperty("value1").GetValue(set.id)
			So(value, ShouldEqual, 5)
			value, _ = set.obj.GetProperty("value2").GetValue(set.id)
			So(value, ShouldEqual, 3)
			store.Rollback()

			Convey("Closing the transaction works and preserves the values", func() {

				code := `Transaction.Close()`
				_, _, err = rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldBeNil)

				store.Begin()
				mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
				So(mngr.IsOpen(), ShouldBeFalse)
				set, _ := getObjectFromPath(rntm, "Document")
				value, _ := set.obj.GetProperty("value1").GetValue(set.id)
				So(value, ShouldEqual, 5)
				value, _ = set.obj.GetProperty("value2").GetValue(set.id)
				So(value, ShouldEqual, 3)
				store.Rollback()

				Convey("And removes the keys correctly", func() {

					code := `Transaction.Open()
							 Document.trans.CurrentTransactionKeys()`
					keys, _, err = rntm.RunJavaScript(store, "User1", code)
					So(err, ShouldBeNil)
					So(keys, ShouldResemble, []string{})
				})
			})

			Convey("Aborting the transaction works and resets both values", func() {

				code := `Transaction.Abort()`
				_, _, err = rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldBeNil)

				store.Begin()
				mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
				So(mngr.IsOpen(), ShouldBeFalse)
				set, _ := getObjectFromPath(rntm, "Document")
				value, _ := set.obj.GetProperty("value1").GetValue(set.id)
				So(value, ShouldEqual, 1)
				value, _ = set.obj.GetProperty("value2").GetValue(set.id)
				So(value, ShouldEqual, 1)
				store.Rollback()

				Convey("And removes the keys correctly", func() {

					code := `Transaction.Open()
							 Document.trans.CurrentTransactionKeys()`
					keys, _, err = rntm.RunJavaScript(store, "User1", code)
					utils.PrintWithStacktrace(err)
					So(err, ShouldBeNil)
					So(keys, ShouldResemble, []string{})
				})
			})

			Convey("Editing other keys by different user works", func() {

				code := `Document.value3 = 7
					 	 Document.trans.CurrentTransactionKeys()`

				keys, _, err = rntm.RunJavaScript(store, "User2", code)
				So(err, ShouldBeNil)
				So(keys, ShouldResemble, []string{"value3"})

				Convey("and closing the first users transaction does not influence the second", func() {

					code := `Transaction.Close()`
					_, _, err = rntm.RunJavaScript(store, "User1", code)
					So(err, ShouldBeNil)

					code = `Document.trans.CurrentTransactionKeys()`
					keys, _, err = rntm.RunJavaScript(store, "User2", code)
					So(err, ShouldBeNil)
					So(keys, ShouldResemble, []string{"value3"})
				})
			})

			Convey("Editing same key by different user fails", func() {

				code := `Document.value1 = 2`

				_, _, err = rntm.RunJavaScript(store, "User2", code)
				So(err, ShouldNotBeNil)
			})
		})

		Convey("Editing the child object map is also supported", func() {

			code := `Document.Child.Set(1,1)
					 Document.Child.Set(2,2)
					 Document.trans.CurrentTransactionKeys()`

			keys, _, err := rntm.RunJavaScript(store, "User1", code)
			utils.PrintWithStacktrace(err)
			So(err, ShouldBeNil)
			So(keys, ShouldResemble, []string{"Child.1", "Child.2"})

			Convey("as well as aborting the changes", func() {

				code := `Transaction.Abort()`
				_, _, err := rntm.RunJavaScript(store, "User1", code)
				utils.PrintWithStacktrace(err)
				So(err, ShouldBeNil)
			})

			Convey("and closing the transaction", func() {

			})
		})

		Convey("Creating child object subobject works", func() {

			code := `Document.ObjectMap.New("first")
					 Document.ObjectMap.New("second")
					 Document.trans.CurrentTransactionKeys()`

			keys, _, err := rntm.RunJavaScript(store, "User1", code)
			So(err, ShouldBeNil)
			So(keys, ShouldResemble, []string{"ObjectMap.first", "ObjectMap.second"})

			store.Begin()
			defer store.Rollback()

			mngr := rntm.behaviours.GetManager("Transaction").(*TransactionManager)
			first, _ := getObjectFromPath(rntm, "Document.ObjectMap.first")
			firstsub, _ := getObjectFromPath(rntm, "Document.ObjectMap.first.subsubobject")
			second, _ := getObjectFromPath(rntm, "Document.ObjectMap.second")
			bhvSet, _ := getObjectFromPath(rntm, "Document.trans")
			bbhvr, ok := bhvSet.obj.(*partialTransaction)
			So(ok, ShouldBeTrue)
			trans, _ := mngr.getTransaction()
			subs, err := bbhvr.getNewSubobjects(bhvSet.id, trans.identification)
			So(err, ShouldBeNil)
			So(len(subs), ShouldEqual, 2)
			So(subs, ShouldContain, first.id)
			So(subs, ShouldContain, second.id)
			store.Rollback()

			Convey("as well as aborting the changes", func() {

				code := `Transaction.Abort()`

				_, _, err := rntm.RunJavaScript(store, "User1", code)
				So(err, ShouldBeNil)

				store.Begin()
				defer store.Rollback()
				objMap, _ := getObjectFromPath(rntm, "Document.ObjectMap")
				keys, err := objMap.obj.(*mapImpl).Keys(objMap.id)
				So(err, ShouldBeNil)
				So(keys, ShouldBeEmpty)

				subs, err := bbhvr.getNewSubobjects(bhvSet.id, trans.identification)
				So(err, ShouldBeNil)
				So(subs, ShouldBeEmpty)

				for _, storage := range datastore.StorageTypes {
					has, err := rntm.datastore.HasSet(storage, false, first.id.Hash())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)

					has, err = rntm.datastore.HasSet(storage, true, first.id.Hash())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
				}

				for _, storage := range datastore.StorageTypes {
					has, err := rntm.datastore.HasSet(storage, false, firstsub.id.Hash())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)

					has, err = rntm.datastore.HasSet(storage, true, firstsub.id.Hash())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
				}
			})
		})

	})
}
