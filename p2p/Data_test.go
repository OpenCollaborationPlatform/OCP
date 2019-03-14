package p2p

import (
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	. "github.com/smartystreets/goconvey/convey"
)

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
		bs2, err := NewBitswap(store2, h1)
		So(err, ShouldBeNil)
		defer bs2.Close()
		defer h2.Stop()

		time.Sleep(1 * time.Second)
		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Adding blocks shall be possible", func() {

			block := randomBlock(987)
			store1.Put(block)

			err := bs1.HasBlock(block)
			So(err, ShouldBeNil)
			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			retblock, err := bs2.GetBlock(ctx, block.Cid())
			So(err, ShouldBeNil)
			So(block, ShouldEqual, retblock)
		})
	})
}
