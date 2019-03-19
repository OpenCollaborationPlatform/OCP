package p2p

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

/*
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

		Convey("and correct errors shall be returned", func() {

			data := randomBlock(10)
			_, err := bstore.Get(data.Cid())
			So(err, ShouldNotBeNil)
			So(err, ShouldEqual, blockstore.ErrNotFound)
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

		h1, err := randomHostWithoutDataSerivce()
		So(err, ShouldBeNil)
		bs1, err := NewBitswap(store1, h1)
		So(err, ShouldBeNil)
		defer bs1.Close()
		defer h1.Stop()

		h2, err := randomHostWithoutDataSerivce()
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
*/
func TestDataService(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	//generate a testfile
	block := repeatableBlock(555)
	testfilepath := filepath.Join(path, "testfile")
	ioutil.WriteFile(testfilepath, block.RawData(), 0644)

	//Setup the hosts
	h1, _ := temporaryHost(path)
	defer h1.Stop()

	h2, _ := temporaryHost(path)
	defer h1.Stop()

	Convey("Setting up two random hosts,", t, func() {

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Adding data to one host should be possible", func() {

			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			res, err := h1.Data.AddFile(ctx, testfilepath)
			So(err, ShouldBeNil)

			reader, err := h1.Data.GetFile(ctx, res)
			So(err, ShouldBeNil)

			data := make([]byte, len(block.RawData()))
			n, err := reader.Read(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 555)
			So(bytes.Equal(data, block.RawData()), ShouldBeTrue)

			Convey("and retreiving from the other shall be possible too", func() {

				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				reader, err := h2.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)
				io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data

				ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
				reader, err = h2.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)

				data := make([]byte, len(block.RawData()))
				n, err := reader.Read(data)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, 555)
				So(bytes.Equal(data, block.RawData()), ShouldBeTrue)
			})

			Convey("Afterwards droping the file from the first host is possible", func() {

				ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
				err := h1.Data.DropFile(ctx, res)
				So(err, ShouldBeNil)

				Convey("while it is still accessing from the second host", func() {
					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					reader, err := h2.Data.GetFile(ctx, res)
					So(err, ShouldBeNil)
					io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data
				})
			})

			Convey("Dropping it from  both hosts", func() {

				ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
				err := h1.Data.DropFile(ctx, res)
				So(err, ShouldBeNil)
				err = h2.Data.DropFile(ctx, res)
				So(err, ShouldBeNil)

				Convey("it should not be accessible anymore", func() {
					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					_, err := h2.Data.GetFile(ctx, res)
					So(err, ShouldNotBeNil)

					_, err = h1.Data.GetFile(ctx, res)
					So(err, ShouldNotBeNil)
				})
			})
		})
	})
}
