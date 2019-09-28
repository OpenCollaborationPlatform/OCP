package p2p

import (
	"bytes"
	"context"

//	"io"
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

			Convey("And owner can be set", func() {

				err := bstore.SetOwnership(data, "testowner")
				So(err, ShouldBeNil)

				owners, err := bstore.GetOwner(data)
				So(err, ShouldBeNil)
				So(len(owners), ShouldEqual, 1)
				So(owners[0], ShouldEqual, "testowner")

				Convey("Garbage Collect should not find the file", func() {
					files, err := bstore.GarbageCollect()
					So(err, ShouldBeNil)
					So(len(files), ShouldEqual, 0)
				})

				Convey("Once the owner was released", func() {
					last, err := bstore.ReleaseOwnership(data, "testowner")
					So(err, ShouldBeNil)
					So(last, ShouldBeTrue)
					owners, err := bstore.GetOwner(data)
					So(err, ShouldBeNil)
					So(len(owners), ShouldEqual, 0)

					Convey("Garbage Collect should find the file", func() {
						files, err := bstore.GarbageCollect()
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 1)
						So(files[0].Equals(data.Cid()), ShouldBeTrue)
					})
				})
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
			defer h1.Stop(context.Background())

			h2, err := randomHostWithoutDataSerivce()
			So(err, ShouldBeNil)
			bs2, err := NewBitswap(store2, h2)
			So(err, ShouldBeNil)
			defer bs2.Close()
			defer h2.Stop(context.Background())

			h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
			h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
			h1.Connect(context.Background(), h2.ID())

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
		defer h1.Stop(context.Background())

		h2, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h1.Connect(context.Background(), h2.ID())

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

			Convey("and makes it retreivable by owner", func() {
				ids, err := h1.Data.(*hostDataService).store.GetCidsForOwner("global")
				So(err, ShouldBeNil)
				So(len(ids), ShouldEqual, 1)
				So(ids[0].Equals(res), ShouldBeTrue)
			})

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

				Convey("and makes it retreivable by owner", func() {
					ids, err := h1.Data.(*hostDataService).store.GetCidsForOwner("global")
					So(err, ShouldBeNil)
					So(len(ids), ShouldEqual, 6)
				})

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

				Convey("and makes it retreivable by owner", func() {
					ids, err := h1.Data.(*hostDataService).store.GetCidsForOwner("global")
					So(err, ShouldBeNil)
					So(len(ids), ShouldEqual, 8)
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

					Convey("and makes it retreivable by owner", func() {
						ids, err := h2.Data.(*hostDataService).store.GetCidsForOwner("global")
						So(err, ShouldBeNil)
						So(len(ids), ShouldEqual, 9)
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
}

func TestDataStreaming(t *testing.T) {

	//make temporary folder for the data
	tmppath, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(tmppath)

	Convey("Setting up two random hosts,", t, func() {

		path := filepath.Join(tmppath, "current")
		defer os.RemoveAll(path) //make sure empty bitswaps are created each run

		//Setup the hosts
		h1, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h1.Connect(context.Background(), h2.ID())
		
		Convey("it is possible to stream small data to one host", func() {
			
			data := repeatableData(10)
			stream := NewStreamBlockifyer()
			
			c := stream.Start()
			c <- data
			stream.Stop()
			
			ctx,_ := context.WithTimeout(context.Background(), 5*time.Second)
			id, err := h1.Data.AddBlocks(ctx, stream)
			So(err, ShouldBeNil)
			
			Convey("and afterwards it is streamable by both hosts", func() {
			
				c, err := h1.Data.ReadChannel(ctx, id)
				So(err, ShouldBeNil)
				So(c, ShouldNotBeNil)
				res := <-c 		
				So(bytes.Equal(data, res), ShouldBeTrue)			
				_, more := <-c
				So(more, ShouldBeFalse)

				c, err = h2.Data.ReadChannel(ctx, id)
				So(err, ShouldBeNil)
				So(c, ShouldNotBeNil)
				res = <-c 				
				So(bytes.Equal(data, res), ShouldBeTrue)
				_, more = <-c
				So(more, ShouldBeFalse)
			})
		})

		Convey("also is possible to stream large data to the other host", func() {
			
			datasize := int(float32(blocksize) * 4.2)
			data := repeatableData(datasize)
			stream := NewStreamBlockifyer()
			
			c := stream.Start()
			reader := bytes.NewReader(data)
			buf := make([]byte, blocksize)
			for {
				n, err := reader.Read(buf)
				if n == 0 || err != nil {
		        		break
		  	  	}
				c <- buf[:n]
			}			
			stream.Stop()
			
			ctx,_ := context.WithTimeout(context.Background(), 5*time.Second)
			id, err := h1.Data.AddBlocks(ctx, stream)
			So(err, ShouldBeNil)
			
			Convey("and afterwards it is streamable by both hosts", func() {
			
				c, err := h1.Data.ReadChannel(ctx, id)
				So(err, ShouldBeNil)
				So(c, ShouldNotBeNil)
				
				res := make([]byte, 0)
				loop:
				for {
					select {
					case d, more := <-c:
						if !more {
							break loop
						}
						res = append(res, d...)	
					}						
				}
				So(bytes.Equal(data, res), ShouldBeTrue)			
				_, more := <-c
				So(more, ShouldBeFalse)

				c, err = h2.Data.ReadChannel(ctx, id)
				So(err, ShouldBeNil)
				So(c, ShouldNotBeNil)
				
				res = make([]byte, 0)
				loop2:
				for {
					select {
					case d, more := <-c:
						if !more {
							break loop2
						}
						res = append(res, d...)	
					}						
				}
					
				So(bytes.Equal(data, res), ShouldBeTrue)
				_, more = <-c
				So(more, ShouldBeFalse)
			})
		})
	})
}

func TestSwarmDataService(t *testing.T) {

	//make temporary folder for the data
	tmppath, _ := ioutil.TempDir("", "p2p")
	defer os.RemoveAll(tmppath)

	Convey("Setting up two random hosts with a shared swarm,", t, func() {

		path := filepath.Join(tmppath, "current")
		defer os.RemoveAll(path) //ake sure empty bitswaps are created each run

		//Setup the hosts
		h1, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2, _ := temporaryHost(path)
		defer h1.Stop(context.Background())

		h2.SetMultipleAdress(h1.ID(), h1.OwnAddresses())
		h1.SetMultipleAdress(h2.ID(), h2.OwnAddresses())
		h1.Connect(context.Background(), h2.ID())

		//setup swarms
		sw1, err := h1.CreateSwarm(context.Background(), NoStates())
		So(err, ShouldBeNil)
		sw1.AddPeer(context.Background(), h2.ID(), AUTH_READWRITE)
		sw2, err := h2.JoinSwarm(context.Background(), sw1.ID, NoStates(), SwarmPeers(h1.ID()))
		So(err, ShouldBeNil)

		Convey("Adding small data to one host swarm should be possible", func() {

			//generate a testfile
			filedata := repeatableData(555)
			testfilepath := filepath.Join(path, "testfile")
			ioutil.WriteFile(testfilepath, filedata, 0644)

			ctx, _ := context.WithTimeout(context.Background(), 10*time.Second)
			res, err := sw1.Data.Add(ctx, testfilepath)
			So(err, ShouldBeNil)
			time.Sleep(150 * time.Millisecond)

			reader, err := sw1.Data.GetFile(ctx, res)
			So(err, ShouldBeNil)

			data := make([]byte, len(filedata))
			n, err := reader.Read(data)
			So(err, ShouldBeNil)
			So(n, ShouldEqual, 555)
			So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

			Convey("and makes it retreivable by owner", func() {
				ids, err := sw1.Data.(*swarmDataService).data.store.GetCidsForOwner(string(sw1.ID))
				So(err, ShouldBeNil)
				So(len(ids), ShouldEqual, 1)
				So(ids[0].Equals(res), ShouldBeTrue)
			})

			Convey("and distribute the file automatically to the other host", func() {

				has, _ := sw2.Data.(*swarmDataService).data.store.Has(res)
				So(has, ShouldBeTrue)

				Convey("Afterwards droping the file from the second host swarm is possible", func() {

					err := sw2.Data.Drop(ctx, res)
					So(err, ShouldBeNil)
					time.Sleep(100 * time.Millisecond)

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

			Convey("Droping the file from the first host is possible", func() {

				err := sw1.Data.Drop(ctx, res)
				So(err, ShouldBeNil)
				time.Sleep(100 * time.Millisecond)

				Convey("it is not accessible in any host", func() {
					ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
					err := sw1.Data.Fetch(ctx, res)
					So(err, ShouldNotBeNil)
					err = sw2.Data.Fetch(ctx, res)
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

			Convey("Adding data to one swarm should be possible", func() {

				ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
				res, err := sw1.Data.Add(ctx, testfilepath)
				So(err, ShouldBeNil)
				time.Sleep(100 * time.Millisecond)

				reader, err := sw1.Data.GetFile(ctx, res)
				So(err, ShouldBeNil)

				data := make([]byte, filesize)
				n, err := reader.Read(data)
				So(err, ShouldBeNil)
				So(n, ShouldEqual, filesize)
				So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

				Convey("and makes it retreivable by owner", func() {
					ids, err := sw1.Data.(*swarmDataService).data.store.GetCidsForOwner(string(sw1.ID))
					So(err, ShouldBeNil)
					So(len(ids), ShouldEqual, 6) //1mb each, hence 5 data blocks for 4.2mb and one mfile block
					So(ids[0].Equals(res), ShouldBeTrue)
				})

				Convey("and retreiving from the other shall be possible too.", func() {

					ctx, _ := context.WithTimeout(context.Background(), 3*time.Second)
					reader, err := sw2.Data.GetFile(ctx, res)
					So(err, ShouldBeNil)
					io.Copy(ioutil.Discard, reader) //ensure the reader fetches all data

					ctx, _ = context.WithTimeout(context.Background(), 3*time.Second)
					reader, err = sw2.Data.GetFile(ctx, res)
					So(err, ShouldBeNil)

					data := make([]byte, filesize)
					n, err := reader.Read(data)
					So(err, ShouldBeNil)
					So(n, ShouldEqual, filesize)
					So(bytes.Equal(data[:n], filedata), ShouldBeTrue)

					Convey("Droping the file from the first host is possible", func() {

						err := sw1.Data.Drop(ctx, res)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						Convey("it is not accessible in any host", func() {
							ctx, _ := context.WithTimeout(context.Background(), 1*time.Second)
							err := sw1.Data.Fetch(ctx, res)
							So(err, ShouldNotBeNil)
							err = sw2.Data.Fetch(ctx, res)
							So(err, ShouldNotBeNil)
						})
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
				res2, err := sw1.Data.Add(ctx, dirpath2)
				So(err, ShouldBeNil)
				time.Sleep(100 * time.Millisecond)

				//check if the files were added to the exchange folder
				exchange := filepath.Join(h1.path, "DataExchange")
				files, err := ioutil.ReadDir(exchange)
				So(len(files), ShouldEqual, 3) //2 files + 1 db = 3

				Convey("as well as accessing it from ourself", func() {
					//try write it to the original path
					newpath := filepath.Join(path, "results")
					res2path, err := sw1.Data.Write(ctx, res2, newpath)
					defer os.RemoveAll(newpath)

					So(err, ShouldBeNil)
					So(compareDirectories(res2path, dirpath2), ShouldBeNil)
				})

				Convey("and makes it retreivable by owner", func() {
					ids, err := sw1.Data.(*swarmDataService).data.store.GetCidsForOwner(string(sw1.ID))
					So(err, ShouldBeNil)
					So(len(ids), ShouldEqual, 8) //6 mfile, 1 file, 1 directory
					So(ids[0].Equals(res2), ShouldBeTrue)
				})

				Convey("and retreiving from the other shall be possible too", func() {

					err := sw2.Data.Fetch(ctx, res2)
					So(err, ShouldBeNil)

					newpath := filepath.Join(path, "results2")
					res2path, err := sw2.Data.Write(ctx, res2, newpath)
					defer os.RemoveAll(newpath)

					So(err, ShouldBeNil)
					So(compareDirectories(res2path, dirpath2), ShouldBeNil)
				})

				Convey("Afterwards adding the parent directory from the other node", func() {

					res1, err := sw2.Data.Add(ctx, dirpath1)
					So(err, ShouldBeNil)
					time.Sleep(100 * time.Millisecond)

					Convey("There should still be only 2 datafiles in the exchange folder", func() {

						exchange := filepath.Join(h2.path, "DataExchange")
						files, err := ioutil.ReadDir(exchange)
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 3) //2 files + 1 db = 3
					})

					Convey("and makes it retreivable by owner", func() {
						ids, err := sw1.Data.(*swarmDataService).data.store.GetCidsForOwner(string(sw1.ID))
						So(err, ShouldBeNil)
						So(len(ids), ShouldEqual, 9) //6 mfile, 1 file, 2 directory
						So(ids[0].Equals(res2), ShouldBeTrue)
					})

					Convey("Dropping the subdirectory should keep all files", func() {

						err := sw1.Data.Drop(ctx, res2)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						exchange := filepath.Join(h2.path, "DataExchange")
						files, err := ioutil.ReadDir(exchange)
						So(err, ShouldBeNil)
						So(len(files), ShouldEqual, 3) //2 files + 1 db = 3

						Convey("and keep the whole directory accassible from the other node", func() {

							newpath := filepath.Join(path, "results3")
							res1path, err := sw1.Data.Write(ctx, res1, newpath)
							defer os.RemoveAll(newpath)

							So(err, ShouldBeNil)
							So(compareDirectories(res1path, dirpath1), ShouldBeNil)

							sw1.Data.Drop(ctx, res1)
						})
					})

					Convey("Dropping sub dir and then the parent dir", func() {

						err = sw2.Data.Drop(ctx, res2)
						So(err, ShouldBeNil)
						err = sw1.Data.Drop(ctx, res1)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						Convey("There should be no datafiles in the exchange folder", func() {

							exchange := filepath.Join(h2.path, "DataExchange")
							files, err := ioutil.ReadDir(exchange)
							So(err, ShouldBeNil)
							So(len(files), ShouldEqual, 1) //0 files + 1 db = 1
						})

						Convey("and neither directory should be accessible", func() {

							So(sw1.Data.Fetch(ctx, res1), ShouldNotBeNil)
							So(sw1.Data.Fetch(ctx, res2), ShouldNotBeNil)

							So(sw2.Data.Fetch(ctx, res1), ShouldNotBeNil)
							So(sw2.Data.Fetch(ctx, res2), ShouldNotBeNil)
						})
					})

					Convey("Dropping prent first and then subdir", func() {

						err = sw2.Data.Drop(ctx, res1)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						err = sw1.Data.Drop(ctx, res2)
						So(err, ShouldBeNil)
						time.Sleep(100 * time.Millisecond)

						Convey("should also remove all files", func() {

							exchange := filepath.Join(h2.path, "DataExchange")
							files, err := ioutil.ReadDir(exchange)
							So(err, ShouldBeNil)
							So(len(files), ShouldEqual, 1) //0 files + 1 db = 1
						})
					})
				})
			})
		})
	})
}
