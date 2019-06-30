package p2p

import (
	"bytes"
	"context"

	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"net/http"
	_ "net/http/pprof"

	blockstore "github.com/ipfs/go-ipfs-blockstore"
	. "github.com/smartystreets/goconvey/convey"
)

func TestBlockStore(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	Convey("Setting up a blockstore,", t, func() {

		bstore, err := NewBitswapStore(path)
		So(err, ShouldBeNil)
		defer bstore.Close()

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
			/*	size, err := bstore.GetSize(data.Cid())
				So(err, ShouldBeNil)
				So(size, ShouldEqual, 512)
			*/
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
	store1, err := NewBitswapStore(filepath.Join(path, "store1"))
	if err != nil {
		return
	}

	store2, err := NewBitswapStore(filepath.Join(path, "store2"))
	if err != nil {
		return
	}

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

func TestDataService(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	//defer os.RemoveAll(path)

	//generate a testfile
	block := repeatableBlock(555)
	p2pblock, _ := getP2PBlock(block)
	fileblock := p2pblock.(P2PFileBlock)
	testfilepath := filepath.Join(path, "testfile")
	ioutil.WriteFile(testfilepath, fileblock.Data, 0644)

	//Setup the hosts
	h1, _ := temporaryHost(path)
	defer h1.Stop()

	h2, _ := temporaryHost(path)
	defer h1.Stop()

	Convey("Setting up two random hosts,", t, func() {

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		/*
			Convey("Adding data to one host should be possible", func() {

				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				res, err := h1.Data.Add(testfilepath)
				So(err, ShouldBeNil)

				reader, err := h1.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)

				data := make([]byte, len(block.RawData()))
				n, err := reader.Read(data)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, 555)
				So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)

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
					So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)
				})

				Convey("Afterwards droping the file from the first host is possible", func() {

					err := h1.Data.Drop(res)
					So(err, ShouldBeNil)

					Convey("while it is still accessing from the second host", func() {
						ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
						reader, err := h2.Data.GetFile(ctx, res)
						So(err, ShouldBeNil)
						io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data
					})
				})

				Convey("Dropping it from  both hosts", func() {

					err := h1.Data.Drop(res)
					So(err, ShouldBeNil)
					err = h2.Data.Drop(res)
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

			Convey("Creating a large file (which need splitting up)", func() {

				//generate a testfile
				filesize := int(float32(blocksize) * 4.2)
				block := repeatableBlock(filesize)
				p2pblock, _ := getP2PBlock(block)
				fileblock := p2pblock.(P2PFileBlock)
				testfilepath := filepath.Join(path, "testfile2")
				ioutil.WriteFile(testfilepath, fileblock.Data, 0644)

				Convey("Adding data to one host should be possible", func() {

					ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
					res, err := h1.Data.Add(testfilepath)
					So(err, ShouldBeNil)

					reader, err := h1.Data.GetFile(ctx, res)
					So(err, ShouldBeNil)

					data := make([]byte, filesize)
					n, err := reader.Read(data)
					So(err, ShouldBeNil)
					So(n, ShouldEqual, filesize)
					So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)

					Convey("and retreiving from the other shall be possible too", func() {

						ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
						reader, err := h2.Data.GetFile(ctx, res)
						So(err, ShouldBeNil)
						io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data

						ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
						reader, err = h2.Data.GetFile(ctx, res)
						So(err, ShouldBeNil)

						data := make([]byte, filesize)
						n, err := reader.Read(data)
						So(err, ShouldBeNil)
						So(n, ShouldEqual, filesize)
						So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)
					})
				})
			})*/

		Convey("Creating a directory structure with large and small files", func() {

			//make the first directory
			dirpath1 := filepath.Join(path, "testdir1")
			os.MkdirAll(dirpath1, os.ModePerm)

			//generate testfiles
			largedata := repeatableData(int(float32(blocksize) * 4.2))
			smalldata := repeatableData(int(float32(blocksize) * 0.5))

			testfilepath := filepath.Join(dirpath1, "testfile1")
			ioutil.WriteFile(testfilepath, largedata, os.ModePerm)

			testfilepath = filepath.Join(dirpath1, "testfile2")
			ioutil.WriteFile(testfilepath, smalldata, os.ModePerm)

			//make the subdir
			dirpath2 := filepath.Join(dirpath1, "testdir2")
			os.MkdirAll(dirpath2, os.ModePerm)

			//and write the testfiles
			testfilepath = filepath.Join(dirpath2, "testfile1")
			ioutil.WriteFile(testfilepath, largedata, os.ModePerm)

			testfilepath = filepath.Join(dirpath2, "testfile2")
			ioutil.WriteFile(testfilepath, smalldata, os.ModePerm)

			Convey("Adding a directory with files only should work", func() {

				ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
				res2, err := h1.Data.Add(dirpath2)
				So(err, ShouldBeNil)

				//check if the files were added to the exchange folder
				exchange := filepath.Join(h1.path, "DataExchange")
				files, err := ioutil.ReadDir(exchange)
				So(len(files), ShouldEqual, 3) //2 files + 1 db = 3

				Convey("as well as accessing it from ourself", func() {
					//try write it to the original path
					newpath := filepath.Join(path, "results")
					res2path, err := h1.Data.Write(ctx, res2, newpath)
					defer os.RemoveAll(newpath)

					So(err, ShouldBeNil)
					So(compareDirectories(res2path, dirpath2), ShouldBeTrue)
				})

				Convey("and retreiving from the other shall be possible too", func() {

					err := h2.Data.Fetch(ctx, res2)
					So(err, ShouldBeNil)

					newpath := filepath.Join(path, "results2")
					res2path, err := h2.Data.Write(ctx, res2, newpath)
					defer os.RemoveAll(newpath)

					So(err, ShouldBeNil)
					So(compareDirectories(res2path, dirpath2), ShouldBeTrue)
				})

				Convey("Afterwards adding the parent directory from the other node", func() {

					res1, err := h2.Data.Add(dirpath1)
					So(err, ShouldBeNil)

					Convey("There should still be only 2 datafiles in the exchange folder", func() {

						exchange := filepath.Join(h2.path, "DataExchange")
						files, err := ioutil.ReadDir(exchange)
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 3) //2 files + 1 db = 3
					})

					Convey("Dropping the subdirectory should keep all files", func() {

						err := h2.Data.Drop(res2)
						So(err, ShouldBeNil)

						exchange := filepath.Join(h2.path, "DataExchange")
						files, err := ioutil.ReadDir(exchange)
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 3) //2 files + 1 db = 3

						Convey("and keep the whole directory accassible from the other node", func() {

							newpath := filepath.Join(path, "results3")
							res1path, err := h1.Data.Write(ctx, res1, newpath)
							defer os.RemoveAll(newpath)

							So(err, ShouldBeNil)
							So(compareDirectories(res1path, dirpath1), ShouldBeTrue)

							h1.Data.Drop(res1)
						})
					})

					Convey("Dropping parent dir", func() {

						err = h2.Data.Drop(res1)
						So(err, ShouldBeNil)

						Convey("There should be no datafiles in the exchange folder", func() {

							exchange := filepath.Join(h2.path, "DataExchange")
							files, err := ioutil.ReadDir(exchange)
							So(err, ShouldBeNil)
							So(len(files), ShouldEqual, 1) //0 files + 1 db = 1
						})

						Convey("and neither directory should be accessible", func() {

							So(h1.Data.Fetch(ctx, res1), ShouldNotBeNil)
							So(h1.Data.Fetch(ctx, res2), ShouldNotBeNil)

							So(h2.Data.Fetch(ctx, res1), ShouldNotBeNil)
							So(h2.Data.Fetch(ctx, res2), ShouldNotBeNil)
						})
					})
				})
			})
		})
	})
}

/*

func TestSwarmDataService(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	//generate a testfile
	block := repeatableBlock(10)
	p2pblock, _ := getP2PBlock(block)
	fileblock := p2pblock.(P2PFileBlock)
	testfilepath := filepath.Join(path, "testfile")
	ioutil.WriteFile(testfilepath, fileblock.Data, 0644)

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
			So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)

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
				So(bytes.Equal(data[:n], fileblock.Data), ShouldBeTrue)
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
}*/
