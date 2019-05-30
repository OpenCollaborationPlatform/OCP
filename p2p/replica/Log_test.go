package replica

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"os"
	"testing"
	"time"

	crypto "github.com/libp2p/go-libp2p-crypto"
	. "github.com/smartystreets/goconvey/convey"
)

func TestLog(t *testing.T) {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	priv, pub, _ := crypto.GenerateRSAKeyPair(512, r)
	rsapriv := *priv.(*crypto.RsaPrivateKey)
	rsapub := *pub.(*crypto.RsaPublicKey)

	Convey("A empty log", t, func() {

		log := Log{}

		Convey("should be invalid", func() {
			So(log.IsValid(), ShouldBeFalse)
		})

		Convey("and not singable", func() {
			So(log.Sign(rsapriv), ShouldNotBeNil)
		})

		Convey("but adding data", func() {

			log2 := Log{1, 2, 3, []byte("test"), nil}

			Convey("should make it valid", func() {
				So(log2.IsValid(), ShouldBeTrue)
			})

			Convey("but still not verifiable", func() {
				So(log2.Verify(rsapub), ShouldBeFalse)
			})
		})
	})

	Convey("A log filled filled with data", t, func() {

		log := Log{1, 2, 3, []byte("test"), nil}

		Convey("should be valid", func() {
			So(log.IsValid(), ShouldBeTrue)
		})

		Convey("signing should work", func() {

			So(log.Sign(rsapriv), ShouldBeNil)

			Convey("and make it verifiable", func() {
				So(log.Verify(rsapub), ShouldBeTrue)
			})

			Convey("and resigning", func() {

				oldSign := log.Signature
				So(log.Sign(rsapriv), ShouldBeNil)

				Convey("should come to the same signature", func() {

					So(bytes.Equal(oldSign, log.Signature), ShouldBeTrue)
				})
			})
		})

		Convey("when marshaling", func() {

			marsh, err := log.ToBytes()
			So(err, ShouldBeNil)

			Convey("it can be unmarshald", func() {
				nlog, err := LogFromBytes(marsh)
				So(err, ShouldBeNil)
				So(log.Index, ShouldEqual, nlog.Index)
				So(log.Type, ShouldEqual, nlog.Type)
				So(log.Epoch, ShouldEqual, nlog.Epoch)
				So(bytes.Equal(log.Data, nlog.Data), ShouldBeTrue)
			})

			Convey("and a signature should be kept intact", func() {
				log.Sign(rsapriv)
				marsh, _ := log.ToBytes()
				nlog, _ := LogFromBytes(marsh)
				So(bytes.Equal(log.Signature, nlog.Signature), ShouldBeTrue)
			})
		})
	})
}

func TestLogStore(t *testing.T) {

	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	priv, _, _ := crypto.GenerateRSAKeyPair(512, r)
	rsapriv := *priv.(*crypto.RsaPrivateKey)

	path, _ := ioutil.TempDir("", "logstore")
	defer os.RemoveAll(path)

	Convey("A empty logstore", t, func() {

		store, err := newLogStore(path, "teststore")
		So(err, ShouldBeNil)
		defer store.Close()

		Convey("Should return NoEntry error for index query", func() {
			_, err := store.FirstIndex()
			So(IsNoEntryError(err), ShouldBeTrue)

			_, err = store.LastIndex()
			So(IsNoEntryError(err), ShouldBeTrue)
		})

		Convey("and normal error for log query", func() {

			_, err := store.GetLog(0)
			So(err, ShouldNotBeNil)
			So(IsNoEntryError(err), ShouldBeFalse)
		})

		Convey("Storing a log", func() {

			log := Log{0, 1, 2, []byte("test"), nil}
			log.Sign(rsapriv)

			err := store.StoreLog(log)
			So(err, ShouldBeNil)

			Convey("changes the indexes", func() {
				idx, err := store.FirstIndex()
				So(err, ShouldBeNil)
				So(idx, ShouldEqual, 0)

				idx, err = store.LastIndex()
				So(err, ShouldBeNil)
				So(idx, ShouldEqual, 0)
			})

			Convey("and makes it accessible by index", func() {

				_, err := store.GetLog(0)
				So(err, ShouldBeNil)
			})

			Convey("while preserving all information", func() {

				log, _ := store.GetLog(0)

				So(log.Index, ShouldEqual, 0)
				So(log.Epoch, ShouldEqual, 1)
				So(log.Type, ShouldEqual, 2)
				So(bytes.Equal(log.Data, []byte("test")), ShouldBeTrue)
			})

			Convey("Adding annother log", func() {

				log := Log{1, 2, 3, []byte("test2"), nil}
				log.Sign(rsapriv)

				err := store.StoreLog(log)
				So(err, ShouldBeNil)

				Convey("changes the index queries", func() {
					idx, err := store.FirstIndex()
					So(err, ShouldBeNil)
					So(idx, ShouldEqual, 0)

					idx, err = store.LastIndex()
					So(err, ShouldBeNil)
					So(idx, ShouldEqual, 1)
				})

				Convey("makes the new log accessible", func() {

					log, _ := store.GetLog(1)

					So(log.Index, ShouldEqual, 1)
					So(log.Epoch, ShouldEqual, 2)
					So(log.Type, ShouldEqual, 3)
					So(bytes.Equal(log.Data, []byte("test2")), ShouldBeTrue)
				})

				Convey("while preserving the older information", func() {

					log, _ := store.GetLog(0)

					So(log.Index, ShouldEqual, 0)
					So(log.Epoch, ShouldEqual, 1)
					So(log.Type, ShouldEqual, 2)
					So(bytes.Equal(log.Data, []byte("test")), ShouldBeTrue)
				})
			})
		})

	})

}
