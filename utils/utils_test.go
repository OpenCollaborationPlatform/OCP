package utils

import (
	"bytes"
	"crypto/rand"
	"encoding/gob"
	"testing"

	cid "github.com/ipfs/go-cid"
	mh "github.com/multiformats/go-multihash"
	. "github.com/smartystreets/goconvey/convey"
)

func buildRandCid() Cid {

	data := make([]byte, 10)
	rand.Read(data)
	h, _ := mh.Sum(data, mh.SHA3, 4)

	p2pcid := cid.NewCidV1(7, h)
	return FromP2PCid(p2pcid)
}

func TestCid(t *testing.T) {

	Convey("Cid can be marshalled", t, func() {

		c := buildRandCid()
		var cif interface{} = c

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(&cif)

		So(err, ShouldBeNil)

		Convey("and be marhsalled into a interface again", func() {

			data := buf.Bytes()
			var res interface{}

			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&res)

			So(err, ShouldBeNil)

			rescid, ok := res.(*Cid)
			So(ok, ShouldBeTrue)
			So(rescid.String(), ShouldEqual, c.String())
		})
	})

	Convey("CidUndef can be marshalled", t, func() {

		var c interface{} = CidUndef

		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(&c)

		So(err, ShouldBeNil)

		Convey("but does not result in nil", func() {

			So(buf.Bytes(), ShouldNotBeNil)
			So(len(buf.Bytes()), ShouldNotEqual, 0)
		})

		Convey("and be marhsalled into a interface again", func() {

			data := buf.Bytes()
			var res interface{}

			buf := bytes.NewBuffer(data)
			dec := gob.NewDecoder(buf)
			err := dec.Decode(&res)

			So(err, ShouldBeNil)

			rescid, ok := res.(*Cid)
			So(ok, ShouldBeTrue)
			So(rescid.String(), ShouldEqual, c.(Cid).String())
		})
	})
}
