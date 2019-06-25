package p2p

//special blockstore who does two extra things compared to normal blockstores:
//- it stores multiple owners per block
//- it stores metainformation in a database but the filedata in files on the harddrive

//Note: the filestore and key value store part should be abstracted into their own
//		interfaces to allow s3/mongo implementation

import (
	"CollaborationNode/utils"
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/boltdb/bolt"

	blocks "github.com/ipfs/go-block-format"
	cid "github.com/ipfs/go-cid"
	bstore "github.com/ipfs/go-ipfs-blockstore"
)

var (
	DirKey   = []byte("directories")
	MfileKey = []byte("multifiles")
	FileKey  = []byte("files")
	BlockKey = []byte("blocks")
	OwnerKey = []byte("owners")
)

func intToByte(val int64) []byte {
	buf := make([]byte, binary.MaxVarintLen64)
	binary.PutVarint(buf, val)
	return buf
}

func byteToInt(data []byte) int64 {
	buf := bytes.NewBuffer(data)
	val, _ := binary.ReadVarint(buf)
	return val
}

func copyKey(key []byte) []byte {
	buf := make([]byte, len(key))
	copy(buf, key)
	return buf
}

func NewBitswapStore(path string) (BitswapStore, error) {

	//make sure the path exist...
	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return BitswapStore{}, utils.StackError(err, "Cannot open path %s", path)
	}

	//build the blt db
	dbpath := filepath.Join(path, "bitswap.db")
	db, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return BitswapStore{}, utils.StackError(err, "Unable to open bolt db: %s", dbpath)
	}

	//make sure we have the default buckets
	err = db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucketIfNotExists(DirKey)
		tx.CreateBucketIfNotExists(MfileKey)
		tx.CreateBucketIfNotExists(FileKey)
		return nil
	})

	return BitswapStore{db, path}, err
}

/*
// Blockstore wraps a Datastore block-centered methods and provides a layer
// of abstraction which allows to add different caching strategies.
type Blockstore interface {

	// GetSize returns the CIDs mapped BlockSize
	GetSize(cid.Cid) (int, error)

}*/

//A store based on boldDB that implements the ipfs Datastore, TxnDatastore and Batching interface
type BitswapStore struct {
	db   *bolt.DB
	path string
}

/*
DB [
	directories [
		cid [
			data: bytes
			owners [
				string:string
			]
	]
	multifiles [
		cid [
			name: string
			size: int64
			owners [
				string:string
			]
			blocks [
				cid: int64
			]
		]
	]
	files [
		cid [
			name: string
			owners [
				string: string
			]
		]
	]
]
*/

/******************************************************************************
							Custom functions
******************************************************************************/
func (self BitswapStore) GetOwnership(block blocks.Block, owner string) error {

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return err
	}

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()

	var key []byte
	switch p2pblock.Type() {

	case BlockDirectory:
		key = DirKey
	case BlockFile:
		key = FileKey
	case BlockMultiFile:
		key = MfileKey
	default:
		tx.Rollback()
		return fmt.Errorf("Can only set owner for Directory, MFile and File")
	}

	//TODO: Directory ownership should mark the files of that dorectory owned too

	//we add ourself to the owner list of that block!
	bucket := tx.Bucket(key)
	bucket = bucket.Bucket(block.Cid().Bytes())
	if bucket == nil {
		tx.Rollback()
		return fmt.Errorf("Block does not exist")
	}
	bucket = bucket.Bucket(OwnerKey)
	if bucket == nil {
		tx.Rollback()
		return fmt.Errorf("Block is not setup correctly in the blockstore")
	}

	//first see if the owner already is noted
	c := bucket.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if bytes.Equal(k, []byte(owner)) {
			tx.Rollback()
			return nil
		}
	}

	//if we are here the owne was not found: add ourself!
	return bucket.Put([]byte(owner), []byte(owner))
}

//Removes the owner, returns true if it was the last owner and the block is
//ownerless
func (self BitswapStore) ReleaseOwnership(block blocks.Block, owner string) (bool, error) {

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return false, err
	}

	tx, err := self.db.Begin(true)
	if err != nil {
		return false, err
	}
	defer tx.Commit()

	var key []byte
	switch p2pblock.Type() {

	case BlockDirectory:
		key = DirKey
	case BlockFile:
		key = FileKey
	case BlockMultiFile:
		key = MfileKey
	default:
		tx.Rollback()
		return false, fmt.Errorf("Can only set owner for Directory, MFile and File")
	}

	//we add ourself to the owner list of that block!
	bucket := tx.Bucket(key)
	bucket = bucket.Bucket(block.Cid().Bytes())
	if bucket == nil {
		tx.Rollback()
		return false, fmt.Errorf("Block does not exist")
	}
	bucket = bucket.Bucket(OwnerKey)
	if bucket == nil {
		tx.Rollback()
		return false, fmt.Errorf("Block is not setup correctly in the blockstore")
	}

	//delete the owner
	bucket.Delete([]byte(owner))

	//check if there are more or if we have been the last one
	c := bucket.Cursor()
	if k, _ := c.Next(); k == nil {
		return true, nil
	}
	return false, nil
}

//Returns all blocks without a owner
func (self BitswapStore) GarbageCollect() ([]cid.Cid, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	result := make([]cid.Cid, 0)
	keys := [][]byte{DirKey, FileKey, MfileKey}

	for _, key := range keys {

		//we add ourself to the owner list of that block!
		bucket := tx.Bucket(key)

		c := bucket.Cursor()
		if k, _ := c.Next(); k == nil {

			bucket = bucket.Bucket(k)
			bucket = bucket.Bucket(OwnerKey)
			if bucket == nil {
				tx.Rollback()
				return nil, fmt.Errorf("Block is not setup correctly in the blockstore")
			}
			//check if owners are empty
			c2 := bucket.Cursor()
			if k2, _ := c2.Next(); k2 == nil {
				key, err := cid.Cast(copyKey(k))
				if err != nil {
					return nil, err
				}
				result = append(result, key)
			}
		}
	}

	//we don't need to go over raw blocks as they are deleted together with the multifile

	return result, nil
}

/******************************************************************************
							Blockstore interface
******************************************************************************/

// Put puts a given block into the store
func (self BitswapStore) Put(block blocks.Block) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return err
	}
	err = self.put(tx, block.Cid(), p2pblock)
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (self BitswapStore) PutMany(blocklist []blocks.Block) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()

	for _, block := range blocklist {

		p2pblock, err := getP2PBlock(block)
		if err != nil {
			return err
		}
		err = self.put(tx, block.Cid(), p2pblock)

		if err != nil {
			tx.Rollback()
			return err
		}
	}
	return nil
}

func (self BitswapStore) DeleteBlock(key cid.Cid) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Commit()

	//check if it is a directory
	bucket := tx.Bucket(DirKey)
	res := bucket.Bucket(key.Bytes())
	if res != nil {
		return bucket.DeleteBucket(key.Bytes())
	}

	//maybe a multifile
	bucket = tx.Bucket(MfileKey)
	res = bucket.Bucket(key.Bytes())
	if res != nil {
		bucket.DeleteBucket(key.Bytes())
		//delete the file
		path := filepath.Join(self.path, key.String())
		os.Remove(path)
	}

	//or a normal file
	bucket = tx.Bucket(FileKey)
	res = bucket.Bucket(key.Bytes())
	if res != nil {
		bucket.DeleteBucket(key.Bytes())
		//delete the file
		path := filepath.Join(self.path, key.String())
		os.Remove(path)
	}

	//seems to be a raw data block. this one we do not delete but set -1
	mfile, err := findMfileForRawBlock(tx, key)
	if err == nil {

		bucket := tx.Bucket(MfileKey)
		bucket = bucket.Bucket(mfile.Bytes())
		bucket = bucket.Bucket(BlockKey)
		bucket.Put(key.Bytes(), intToByte(-1))
	}

	return nil
}

func (self BitswapStore) Has(key cid.Cid) (bool, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()

	//check if it is a directory
	bucket := tx.Bucket(DirKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		return true, nil
	}

	//maybe a multifile
	bucket = tx.Bucket(MfileKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		return true, nil
	}

	//or a normal file
	bucket = tx.Bucket(FileKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		return true, nil
	}

	//seems to be a raw data block. check if it is -1 when available
	mfile, err := findMfileForRawBlock(tx, key)
	if err == nil {

		bucket := tx.Bucket(MfileKey)
		bucket = bucket.Bucket(mfile.Bytes())
		bucket = bucket.Bucket(BlockKey)
		val := bucket.Get(key.Bytes())
		if val == nil {
			return false, fmt.Errorf("block found, but no value set. This is not ok for raw blocks")
		}
		if byteToInt(val) != -1 {
			return true, nil
		}
	}

	return false, nil
}

func (self BitswapStore) Get(key cid.Cid) (blocks.Block, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	//check if it is a directory
	bucket := tx.Bucket(DirKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		//rebuild the block from raw data
		res := bucket.Get([]byte("data"))
		if res == nil {
			return nil, fmt.Errorf("Data not correctly setup")
		}
		return blocks.NewBlock(res), nil
	}

	//maybe a multifile
	bucket = tx.Bucket(MfileKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		name := string(bucket.Get([]byte("name")))
		size := byteToInt(bucket.Get([]byte("size")))

		//get all blocks!
		blocks := make([]cid.Cid, 0)
		blockbucket := bucket.Bucket(BlockKey)
		err := blockbucket.ForEach(func(k, v []byte) error {
			blockcid, err := cid.Cast(copyKey(k))
			if err != nil {
				return err
			}
			blocks = append(blocks, blockcid)
			return nil
		})
		if err != nil {
			return nil, err
		}

		block := P2PMultiFileBlock{size, name, blocks}
		return block.ToBlock(), nil
	}

	//or a normal file
	bucket = tx.Bucket(FileKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		name := string(bucket.Get([]byte("name")))

		//read in the data
		path := filepath.Join(self.path, key.String())
		fi, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer fi.Close()
		data := make([]byte, blocksize)
		n, err := fi.Read(data)
		if err != nil {
			return nil, err
		}
		block := P2PFileBlock{name, data[:n]}
		return block.ToBlock(), nil
	}

	//seems to be a raw data block
	mfile, err := findMfileForRawBlock(tx, key)
	if err == nil {

		bucket := tx.Bucket(MfileKey)
		bucket = bucket.Bucket(mfile.Bytes())

		bucket = bucket.Bucket(BlockKey)
		val := bucket.Get(key.Bytes())
		if val == nil {
			return nil, fmt.Errorf("block found, but no value set. This is not ok for raw blocks")
		}
		offset := byteToInt(val)
		if offset == -1 {
			return nil, bstore.ErrNotFound
		}

		//read in the data
		path := filepath.Join(self.path, mfile.String())
		fi, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		defer fi.Close()
		data := make([]byte, blocksize)
		n, err := fi.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			return nil, fmt.Errorf("Unable to read any data")
		}
		block := P2PRawBlock{offset, data[:n]}
		return block.ToBlock(), nil
	}

	return nil, bstore.ErrNotFound
}

func (self BitswapStore) HashOnRead(enabled bool) {
	//not implemented
}

func (self BitswapStore) AllKeysChan(ctx context.Context) (<-chan cid.Cid, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	res := make(chan cid.Cid, 0)

	//we need to collect all keys, as we do not know if it is ok to block the
	//store while the channel is open...
	keys := make([]cid.Cid, 0)

	//collect all directories
	bucket := tx.Bucket(DirKey)
	err = bucket.ForEach(func(k, v []byte) error {
		c, err := cid.Cast(copyKey(k))
		if err != nil {
			return err
		}
		keys = append(keys, c)
		return nil
	})
	if err != nil {
		close(res)
		return nil, err
	}

	//collect all files
	bucket = tx.Bucket(FileKey)
	err = bucket.ForEach(func(k, v []byte) error {
		c, err := cid.Cast(copyKey(k))
		if err != nil {
			return err
		}
		keys = append(keys, c)
		return nil
	})
	if err != nil {
		close(res)
		return nil, err
	}

	//collect all Mfiles and raw blocks
	bucket = tx.Bucket(FileKey)
	err = bucket.ForEach(func(k, v []byte) error {
		c, err := cid.Cast(copyKey(k))
		if err != nil {
			return err
		}
		keys = append(keys, c)

		blocks := bucket.Bucket(k).Bucket(BlockKey)

		err = blocks.ForEach(func(k2, v2 []byte) error {
			c, err := cid.Cast(copyKey(k2))
			if err != nil {
				return err
			}
			keys = append(keys, c)
			return nil
		})
		if err != nil {
			close(res)
			return err
		}
		return nil
	})
	if err != nil {
		close(res)
		return nil, err
	}

	//start a gorutine which serves the channel and respects the context
	go func() {

	loop:
		for _, key := range keys {

			select {
			case <-ctx.Done():
				break loop
			case res <- key:
				continue loop
			}
		}
		close(res)
	}()

	return res, nil
}

// GetSize returns the CIDs mapped BlockSize
func (self BitswapStore) GetSize(key cid.Cid) (int, error) {

	block, err := self.Get(key)
	if err != nil {
		return 0, nil
	}

	return len(block.RawData()), nil
}

func (self BitswapStore) Close() {
	self.db.Close()
}

func (self BitswapStore) put(tx *bolt.Tx, c cid.Cid, block P2PDataBlock) error {

	switch block.Type() {

	case BlockDirectory:
		return self.putDirectory(tx, c, block.(P2PDirectoryBlock))

	case BlockMultiFile:
		return self.putMultiFile(tx, c, block.(P2PMultiFileBlock))

	case BlockFile:
		return self.putFile(tx, c, block.(P2PFileBlock))

	case BlockRaw:
		return self.putRaw(tx, c, block.(P2PRawBlock))
	}

	return fmt.Errorf("Unknown block type")
}

//directories are simple: just store the raw data.
func (self BitswapStore) putDirectory(tx *bolt.Tx, c cid.Cid, block P2PDirectoryBlock) error {

	directories := tx.Bucket(DirKey)
	if directories == nil {
		return fmt.Errorf("Datastore is badly set up!")
	}
	directory, err := directories.CreateBucketIfNotExists(c.Bytes())
	if err != nil {
		return err
	}

	//make sure the owner bucket exists
	_, err = directory.CreateBucketIfNotExists(OwnerKey)
	if err != nil {
		return err
	}

	//write the data
	return directory.Put([]byte("data"), block.ToData())
}

//we store the raw blocks individual
func (self BitswapStore) putMultiFile(tx *bolt.Tx, c cid.Cid, block P2PMultiFileBlock) error {

	mfiles := tx.Bucket(MfileKey)
	if mfiles == nil {
		return fmt.Errorf("Datastore is badly set up!")
	}

	mfile, err := mfiles.CreateBucketIfNotExists(c.Bytes())
	if err != nil {
		return fmt.Errorf("Unable to setup file in datastore")
	}

	//put the data in
	mfile.Put([]byte("name"), []byte(block.Name))
	mfile.Put([]byte("size"), intToByte(block.Size))

	//create a empty blocks bucket
	blocks := mfile.Bucket(BlockKey)
	if blocks != nil {
		//we delete the bucket to make sure we do not have any old blocks in it
		err := mfile.DeleteBucket(BlockKey)
		if err != nil {
			return err
		}
	}
	blocks, err = mfile.CreateBucket(BlockKey)
	if err != nil {
		return err
	}

	//set all blocks to unset
	for _, blockcid := range block.Blocks {
		err := blocks.Put(blockcid.Bytes(), intToByte(-1))
		if err != nil {
			return err
		}
	}

	//make sure the owner bucket exists
	_, err = mfile.CreateBucketIfNotExists(OwnerKey)
	return err
}

func findMfileForRawBlock(tx *bolt.Tx, rawCid cid.Cid) (cid.Cid, error) {

	//we need to find the mutifile this block belongs to
	mfiles := tx.Bucket(MfileKey)
	if mfiles == nil {
		return cid.Cid{}, fmt.Errorf("Datastore is badly set up!")
	}

	found := false
	var blockfilecid cid.Cid
	c := mfiles.Cursor()
	for k, v := c.First(); k != nil; k, v = c.Next() {

		if v != nil {
			return cid.Cid{}, fmt.Errorf("mutifiles should contain only buckets, but does not")
		}

		mfile := mfiles.Bucket(k)
		blocks := mfile.Bucket(BlockKey)
		if blocks == nil {
			return cid.Cid{}, fmt.Errorf("multifiles should have a block bucket, but they do not")
		}

		c2 := blocks.Cursor()
		for k2, _ := c2.First(); k2 != nil; k2, _ = c2.Next() {

			if bytes.Equal(k2, rawCid.Bytes()) {
				//we found the block!
				found = true
				blockfilecid, _ = cid.Cast(copyKey(k))

				break
			}
		}

		if found {
			break
		}
	}

	if !found {
		return cid.Cid{}, fmt.Errorf("Unable to find mutlifile the block belongs too")
	}

	return blockfilecid, nil
}

func (self BitswapStore) putRaw(tx *bolt.Tx, c cid.Cid, block P2PRawBlock) error {

	blockfilecid, err := findMfileForRawBlock(tx, c)
	if err != nil {
		return err
	}

	//write the block data
	mfiles := tx.Bucket(MfileKey)
	mfile := mfiles.Bucket(blockfilecid.Bytes())
	blocks := mfile.Bucket(BlockKey)
	err = blocks.Put(c.Bytes(), intToByte(block.Offset))
	if err != nil {
		return err
	}

	//we found the multifile! lets put the data into the file
	path := filepath.Join(self.path, blockfilecid.String())
	fi, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	defer fi.Close()

	n, err := fi.WriteAt(block.Data, block.Offset)
	if err != nil {
		return err
	}
	if n != len(block.Data) {
		return fmt.Errorf("Could not write all data into file")
	}
	return nil
}

func (self BitswapStore) putFile(tx *bolt.Tx, c cid.Cid, block P2PFileBlock) error {

	//we need to find the mutifile this block belongs to
	files := tx.Bucket(FileKey)
	if files == nil {
		return fmt.Errorf("Datastore is badly set up!")
	}

	//store the block
	file, err := files.CreateBucketIfNotExists(c.Bytes())
	if err != nil {
		return err
	}
	file.Put([]byte("name"), []byte(block.Name))

	//make sure the owner bucket exists
	_, err = file.CreateBucketIfNotExists(OwnerKey)
	if err != nil {
		return err
	}

	//write the file
	path := filepath.Join(self.path, c.String())
	fi, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return err
	}
	defer fi.Close()

	n, err := fi.Write(block.Data)
	if err != nil {
		return err
	}
	if n != len(block.Data) {
		return fmt.Errorf("Could not write all data into file")
	}
	return nil
}
