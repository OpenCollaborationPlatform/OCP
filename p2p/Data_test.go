package p2p

import (
	"bytes"
	"context"

	//	"io"
	"io/ioutil"
	//	"net/http"
	//	_ "net/http/pprof"
	"os"
	"path/filepath"
	"testing"
	"time"

	//	blockstore "github.com/ipfs/go-ipfs-blockstore"
	. "github.com/smartystreets/goconvey/convey"
)

/*
func TestBlockStore(t *testing.T) {

	//make temporary folder for the data
	path, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(path)

	go func() {
		http.ListenAndServe("localhost:6060", nil)
	}()

	count := 0
	Convey("Setting up the store.", t, func() {

		//each time we want a new store to use convey correctly
		count++
		bstore, err := NewBitswapStore(path)
		So(err, ShouldBeNil)
		defer bstore.Close()
		defer os.RemoveAll(path) //delete it after each execution to get a new empty store for multiple runs

		Convey("Adding single block data should work", func() {

			data := repeatableBlock(512)
			has, err := bstore.Has(data.Cid())
			So(err, ShouldBeNil)
			So(has, ShouldBeFalse)

			err = bstore.Put(data)
			So(err, ShouldBeNil)

			has, err = bstore.Has(data.Cid())
			So(err, ShouldBeNil)
			So(has, ShouldBeTrue)

			Convey("and creates a file for storage.", func() {

				//check if the files were added to the exchange folder
				files, err := ioutil.ReadDir(path)
				So(err, ShouldBeNil)
				So(len(files), ShouldEqual, 2) //1 file + 1 db = 2
			})

			Convey("Data should be correctly read", func() {

				stored, err := bstore.Get(data.Cid())
				So(err, ShouldBeNil)
				So(bytes.Equal(data.RawData(), stored.RawData()), ShouldBeTrue)
			})

			Convey("and correct errors shall be returned", func() {

				random := randomBlock(10)
				_, err := bstore.Get(random.Cid())
				So(err, ShouldNotBeNil)
				So(err, ShouldEqual, blockstore.ErrNotFound)
			})

			Convey("Removing the block", func() {

				err := bstore.DeleteBlock(data.Cid())
				So(err, ShouldBeNil)

				Convey("removes the file", func() {

					//check if the files were added to the exchange folder
					files, err := ioutil.ReadDir(path)
					So(err, ShouldBeNil)
					So(len(files), ShouldEqual, 1) //0 file + 1 db = 1
				})

				Convey("As well as the entry in the DB", func() {

					has, err := bstore.Has(data.Cid())
					So(err, ShouldBeNil)
					So(has, ShouldBeFalse)
				})
			})
		})
	})
}

func TestBitswap(t *testing.T) {

	//make temporary folder for the data
	tmppath, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(tmppath)

	Convey("Creating two blockstores,", t, func() {

		path := filepath.Join(tmppath, "current")
		defer os.RemoveAll(path) //make sure to get empty blockstores each run

		//build blockstore
		store1, err := NewBitswapStore(filepath.Join(path, "store1"))
		So(err, ShouldBeNil)

		store2, err := NewBitswapStore(filepath.Join(path, "store2"))
		So(err, ShouldBeNil)

		Convey("Setting up Bitswaps for two random hosts,", func() {

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
	})

}

func TestDataService(t *testing.T) {

	//make temporary folder for the data
	tmppath, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(tmppath)

	Convey("Setting up two random hosts,", t, func() {

		path := filepath.Join(tmppath, "current")
		defer os.RemoveAll(path) //ake sure empty bitswaps are created each run

		//Setup the hosts
		h1, _ := temporaryHost(path)
		defer h1.Stop()

		h2, _ := temporaryHost(path)
		defer h1.Stop()

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		Convey("Adding small data to one host should be possible", func() {

			//generate a testfile
			filedata := repeatableData(555)
			testfilepath := filepath.Join(path, "testfile")
			ioutil.WriteFile(testfilepath, filedata, 0644)

			ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
			res, err := h1.Data.Add(ctx, testfilepath)
			So(err, ShouldBeNil)

			reader, err := h1.Data.GetFile(ctx, res)
			So(err, ShouldBeNil)

			data := make([]byte, len(filedata))
			n, err := reader.Read(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 555)
			So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

			Convey("and retreiving from the other shall be possible too.", func() {

				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				reader, err := h2.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)
				io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data

				ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
				reader, err = h2.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)

				data := make([]byte, len(filedata))
				n, err := reader.Read(data)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, 555)
				So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

				Convey("Afterwards droping the file from the first host is possible", func() {

					err := h1.Data.Drop(ctx, res)
					So(err, ShouldBeNil)

					Convey("while it is still accessing from the second host", func() {
						ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
						err := h2.Data.Fetch(ctx, res)
						So(err, ShouldBeNil)
					})
				})
			})

			Convey("Droping the file from the first host is possible", func() {

				err := h1.Data.Drop(ctx, res)
				So(err, ShouldBeNil)

				Convey("it is not accessible in any host", func() {
					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					err := h1.Data.Fetch(ctx, res)
					So(err, ShouldNotBeNil)
					err = h2.Data.Fetch(ctx, res)
					So(err, ShouldNotBeNil)
				})
			})
		})

		Convey("Creating a large file (which need splitting up)", func() {

			//generate a testfile
			filesize := int(float32(blocksize) * 4.2)
			filedata := repeatableData(filesize)
			testfilepath := filepath.Join(path, "testfile2")
			ioutil.WriteFile(testfilepath, filedata, 0644)

			Convey("Adding data to one host should be possible", func() {

				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				res, err := h1.Data.Add(ctx, testfilepath)
				So(err, ShouldBeNil)

				reader, err := h1.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)

				data := make([]byte, filesize)
				n, err := reader.Read(data)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, filesize)
				So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

				Convey("and retreiving from the other shall be possible too.", func() {

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
					So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

					Convey("Afterwards droping the file from the first host is possible", func() {

						err := h1.Data.Drop(ctx, res)
						So(err, ShouldBeNil)

						Convey("while it is still accessing from the second host", func() {
							ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
							err := h2.Data.Fetch(ctx, res)
							So(err, ShouldBeNil)
						})
					})
				})

				Convey("Droping the file from the first host is possible", func() {

					err := h1.Data.Drop(ctx, res)
					So(err, ShouldBeNil)

					Convey("it is not accessible in any host", func() {
						ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
						err := h1.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
						err = h2.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
					})
				})
			})
		})

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
				res2, err := h1.Data.Add(ctx, dirpath2)
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
					So(compareDirectories(res2path, dirpath2), ShouldBeNil)
				})

				Convey("and retreiving from the other shall be possible too", func() {

					err := h2.Data.Fetch(ctx, res2)
					So(err, ShouldBeNil)

					newpath := filepath.Join(path, "results2")
					res2path, err := h2.Data.Write(ctx, res2, newpath)
					defer os.RemoveAll(newpath)

					So(err, ShouldBeNil)
					So(compareDirectories(res2path, dirpath2), ShouldBeNil)
				})

				Convey("Afterwards adding the parent directory from the other node", func() {

					res1, err := h2.Data.Add(ctx, dirpath1)
					So(err, ShouldBeNil)

					Convey("There should still be only 2 datafiles in the exchange folder", func() {

						exchange := filepath.Join(h2.path, "DataExchange")
						files, err := ioutil.ReadDir(exchange)
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 3) //2 files + 1 db = 3
					})

					Convey("Dropping the subdirectory should keep all files", func() {

						err := h1.Data.Drop(ctx, res2)
						So(err, ShouldBeNil)
						err = h2.Data.Drop(ctx, res2)
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
							So(compareDirectories(res1path, dirpath1), ShouldBeNil)

							h1.Data.Drop(ctx, res1)
						})
					})

					Convey("Dropping parent dir", func() {

						err = h1.Data.Drop(ctx, res2)
						So(err, ShouldBeNil)
						err = h2.Data.Drop(ctx, res1)
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
}*/

func TestSwarmDataService(t *testing.T) {

	//make temporary folder for the data
	tmppath, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(tmppath)

	Convey("Setting up two random hosts with a shared swarm,", t, func() {

		path := filepath.Join(tmppath, "current")
		defer os.RemoveAll(path) //ake sure empty bitswaps are created each run

		//clear all registered replicas in the overlord
		overlord.Clear()

		//Setup the hosts
		h1, _ := temporaryHost(path)
		defer h1.Stop()

		h2, _ := temporaryHost(path)
		defer h1.Stop()

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())

		//setup swarms
		sw1 := h1.CreateSwarm(SwarmID("MySwarm"))
		sw2 := h2.CreateSwarm(SwarmID("MySwarm"))

		ctx, _ := context.WithTimeout(context.Background(), 5*time.Second)
		sw1.AddPeer(ctx, h2.ID(), AUTH_READWRITE)
		sw2.AddPeer(ctx, h1.ID(), AUTH_READWRITE)

		Convey("Adding small data to one host swarm should be possible", func() {

			//generate a testfile
			filedata := repeatableData(555)
			testfilepath := filepath.Join(path, "testfile")
			ioutil.WriteFile(testfilepath, filedata, 0644)

			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			res, err := sw1.Data.Add(ctx, testfilepath)
			So(err, ShouldBeNil)
			time.Sleep(50 * time.Millisecond)

			reader, err := sw1.Data.GetFile(ctx, res)
			So(err, ShouldBeNil)

			data := make([]byte, len(filedata))
			n, err := reader.Read(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 555)
			So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

			Convey("and distribute the file automatically to the other host", func() {

				time.Sleep(100 * time.Millisecond) //time needed for fetching
				has, _ := sw2.Data.(*swarmDataService).data.store.Has(res)
				So(has, ShouldBeTrue)

				Convey("Afterwards droping the file from the second host swarm is possible", func() {

					err := sw2.Data.Drop(ctx, res)
					So(err, ShouldBeNil)
					time.Sleep(50 * time.Millisecond)

					Convey("and makes it inaccessible by any host", func() {

						err := sw1.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
						has, _ := sw1.Data.(*swarmDataService).data.store.Has(res)
						So(has, ShouldBeFalse)

						err = sw2.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
						has, _ = sw2.Data.(*swarmDataService).data.store.Has(res)
						So(has, ShouldBeFalse)
					})
				})
			})
			/*
				Convey("Droping the file from the first host is possible", func() {

					err := h1.Data.Drop(ctx, res)
					So(err, ShouldBeNil)

					Convey("it is not accessible in any host", func() {
						ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
						err := h1.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
						err = h2.Data.Fetch(ctx, res)
						So(err, ShouldNotBeNil)
					})
				})*/
		})
		/*
			Convey("Creating a large file (which need splitting up)", func() {

				//generate a testfile
				filesize := int(float32(blocksize) * 4.2)
				filedata := repeatableData(filesize)
				testfilepath := filepath.Join(path, "testfile2")
				ioutil.WriteFile(testfilepath, filedata, 0644)

				Convey("Adding data to one host should be possible", func() {

					ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
					res, err := h1.Data.Add(ctx, testfilepath)
					So(err, ShouldBeNil)

					reader, err := h1.Data.GetFile(ctx, res)
					So(err, ShouldBeNil)

					data := make([]byte, filesize)
					n, err := reader.Read(data)
					So(err, ShouldBeNil)
					So(n, ShouldEqual, filesize)
					So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

					Convey("and retreiving from the other shall be possible too.", func() {

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
						So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

						Convey("Afterwards droping the file from the first host is possible", func() {

							err := h1.Data.Drop(ctx, res)
							So(err, ShouldBeNil)

							Convey("while it is still accessing from the second host", func() {
								ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
								err := h2.Data.Fetch(ctx, res)
								So(err, ShouldBeNil)
							})
						})
					})

					Convey("Droping the file from the first host is possible", func() {

						err := h1.Data.Drop(ctx, res)
						So(err, ShouldBeNil)

						Convey("it is not accessible in any host", func() {
							ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
							err := h1.Data.Fetch(ctx, res)
							So(err, ShouldNotBeNil)
							err = h2.Data.Fetch(ctx, res)
							So(err, ShouldNotBeNil)
						})
					})
				})
			})

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
					res2, err := h1.Data.Add(ctx, dirpath2)
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
						So(compareDirectories(res2path, dirpath2), ShouldBeNil)
					})

					Convey("and retreiving from the other shall be possible too", func() {

						err := h2.Data.Fetch(ctx, res2)
						So(err, ShouldBeNil)

						newpath := filepath.Join(path, "results2")
						res2path, err := h2.Data.Write(ctx, res2, newpath)
						defer os.RemoveAll(newpath)

						So(err, ShouldBeNil)
						So(compareDirectories(res2path, dirpath2), ShouldBeNil)
					})

					Convey("Afterwards adding the parent directory from the other node", func() {

						res1, err := h2.Data.Add(ctx, dirpath1)
						So(err, ShouldBeNil)

						Convey("There should still be only 2 datafiles in the exchange folder", func() {

							exchange := filepath.Join(h2.path, "DataExchange")
							files, err := ioutil.ReadDir(exchange)
							So(err, ShouldBeNil)
							So(len(files), ShouldEqual, 3) //2 files + 1 db = 3
						})

						Convey("Dropping the subdirectory should keep all files", func() {

							err := h1.Data.Drop(ctx, res2)
							So(err, ShouldBeNil)
							err = h2.Data.Drop(ctx, res2)
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
								So(compareDirectories(res1path, dirpath1), ShouldBeNil)

								h1.Data.Drop(ctx, res1)
							})
						})

						Convey("Dropping parent dir", func() {

							err = h1.Data.Drop(ctx, res2)
							So(err, ShouldBeNil)
							err = h2.Data.Drop(ctx, res1)
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
			})*/
	})
}
