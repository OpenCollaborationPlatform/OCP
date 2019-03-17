package p2p

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBlockStore(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	Convey("Setting up a blockstore,", t, func() {

		batchingstore, err := NewBitswapStore(path)
		So(err, ShouldBeNil)
		bstore := blockstore.NewBlockstore(batchingstore)
		defer batchingstore.Close()

		Convey("Adding data should work", func() {

			data := repeatableBlock(512)
			has, err := bstore.Has(data.Cid())
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)

			err = bstore.Put(data)
			So(err, ShouldBeNil)

			has, err = bstore.Has(data.Cid())
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)
		})

		Convey("Data should be correctly read", func() {

			data := repeatableBlock(512)
			size, err := bstore.GetSize(data.Cid())
			So(err, ShouldBeNil)
			So(size, ShouldEqual, 512)

			stored, err := bstore.Get(data.Cid())
			So(err, ShouldBeNil)
			So(bytes.Equal(data.RawData(), stored.RawData()), ShouldBeTrue)
		})
	})
}

func TestBitswap(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	//build blockstore
	dstore, err := NewBitswapStore(filepath.Join(path, "store1"))
	if err != nil {
		return
	}
	store1 := blockstore.NewBlockstore(dstore)

	dstore, err = NewBitswapStore(filepath.Join(path, "store2"))
	if err != nil {
		return
	}
	store2 := blockstore.NewBlockstore(dstore)

	Convey("Setting up Bitswaps for two random hosts,", t, func() {

		h1, err := temporaryHost(path)
		So(err, ShouldBeNil)
		bs1, err := NewBitswap(store1, h1)
		So(err, ShouldBeNil)
		defer bs1.Close()
		defer h1.Stop()

		h2, err := temporaryHost(path)
		So(err, ShouldBeNil)
		bs2, err := NewBitswap(store2, h2)
		So(err, ShouldBeNil)
		defer bs2.Close()
		defer h2.Stop()

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Adding/retreiving blocks shall be possible", func() {

			block := randomBlock(187)

			err = bs1.HasBlock(block)
			So(err, ShouldBeNil)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)

			//get block from other host
			retblock, err := bs2.GetBlock(ctx, block.Cid())
			So(err, ShouldBeNil)
			So(block.Cid().Equals(retblock.Cid()), ShouldBeTrue)
			So(bytes.Equal(block.RawData(), retblock.RawData()), ShouldBeTrue)
		})
	})
}
