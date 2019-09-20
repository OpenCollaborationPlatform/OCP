package p2p

//special blockstore who does two extra things compared to normal blockstores:
//- it stores multiple owners per block
//- it stores metainformation in a database but the filedata in files on the harddrive

//Note: the filestore and key value store part should be abstracted into their own
//		interfaces to allow s3/mongo implementation

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/ickby/CollaborationNode/utils"

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
daatbase layout
DB [
	directories [
		cid [
			name: string
			blocks [
				cid: int64
			]
			owners [
				string:string
			]
	]
	multifiles [
		cid [
			name: string
			size: int64
			blocks [
				cid: int64  //id : offset in file
			]
			owners [
				string:string
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
func (self BitswapStore) SetOwnership(block blocks.Block, owner string) error {

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return utils.StackError(err, "Unable to set ownership")
	}

	tx, err := self.db.Begin(true)
	if err != nil {
		return utils.StackError(err, "Unable to set ownership")
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

//Removes the owner, returns true if it was the last owner and the block is now ownerless
func (self BitswapStore) ReleaseOwnership(block blocks.Block, owner string) (bool, error) {

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return false, utils.StackError(err, "Unable to release ownership")
	}

	tx, err := self.db.Begin(true)
	if err != nil {
		return false, utils.StackError(err, "Unable to release ownership")
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

	//we remove ourself to the owner list of that block!
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
	if k, _ := c.Next(); k != nil {
		return false, nil
	}

	//we have been the last one! check if there is a higher level owner
	has, err := self.hasParentDirectoryWithOwner(block.Cid().Bytes(), tx)
	if err != nil {
		return false, utils.StackError(err, "Unable to release ownership")
	}

	return !has, nil
}

func (self BitswapStore) GetOwner(block blocks.Block) ([]string, error) {

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get owner")
	}

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get owner")
	}
	defer tx.Rollback()

	var key []byte
	switch p2pblock.Type() {

	case BlockDirectory:
		key = DirKey
	case BlockFile:
		key = FileKey
	case BlockMultiFile:
		key = MfileKey
	default:
		return nil, fmt.Errorf("Can only set owner for Directory, MFile and File")
	}

	//collect all owners!
	bucket := tx.Bucket(key)
	bucket = bucket.Bucket(block.Cid().Bytes())
	if bucket == nil {
		return nil, fmt.Errorf("Block does not exist")
	}
	bucket = bucket.Bucket(OwnerKey)
	if bucket == nil {
		return nil, fmt.Errorf("Block is not setup correctly in the blockstore")
	}

	owners := make([]string, 0)
	c := bucket.Cursor()
	if k, _ := c.Next(); k == nil {
		owners = append(owners, string(copyKey(k)))
	}
	return owners, nil
}

//Returns all blocks without a owner (multiffile subblocks are not returned)
func (self BitswapStore) GarbageCollect() ([]cid.Cid, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, utils.StackError(err, "Unable to collect garbage")
	}
	defer tx.Rollback()

	result := make([]cid.Cid, 0)
	keys := [][]byte{DirKey, FileKey, MfileKey}

	for _, key := range keys {

		bucket := tx.Bucket(key)
		c := bucket.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {

			ownerbucket := bucket.Bucket(k).Bucket(OwnerKey)
			if bucket == nil {
				tx.Rollback()
				return nil, fmt.Errorf("Block is not setup correctly in the blockstore")
			}
			//check if owners are empty
			c2 := ownerbucket.Cursor()
			if k2, _ := c2.Next(); k2 == nil {

				//no owners! The only reason not to delete it would be if it is part of a directory!
				if has, _ := self.hasParentDirectoryWithOwner(k, tx); !has {

					key, err := cid.Cast(copyKey(k))
					if err != nil {
						return nil, utils.StackError(err, "Unable to collect garbage")
					}
					result = append(result, key)
				}
			}
		}
	}

	//we don't need to go over raw blocks as they are deleted together with the multifile

	return result, nil
}

//checks if the given cid is part of a directory with a owner
func (self BitswapStore) hasParentDirectoryWithOwner(key []byte, tx *bolt.Tx) (bool, error) {

	bucket := tx.Bucket(DirKey)
	if bucket == nil {
		return false, fmt.Errorf("Store not correctly setup!")
	}

	//we go through all directories and check if the cid is part of any!
	c := bucket.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {

		dirbucket := bucket.Bucket(k)
		blocks := dirbucket.Bucket(BlockKey)
		if blocks == nil {
			return false, fmt.Errorf("Directory not set up correctly: no blocks bucket!")
		}
		result := blocks.Get(key)
		if result != nil {

			//we found a directory that owns the key. let's check if it has a owner!
			owners := dirbucket.Bucket(OwnerKey)
			if owners == nil {
				return false, fmt.Errorf("Directory not set up correctly: no owners bucket!")
			}

			c2 := owners.Cursor()
			if k2, _ := c2.First(); k2 == nil {
				//the direct parent does not have a owner! But we still need to check if the parent
				//may have a directory parent with a owner. Let's go recursive!
				return self.hasParentDirectoryWithOwner(k, tx)

			} else {
				return true, nil
			}
		}
	}

	return false, nil
}

/******************************************************************************
							Blockstore interface
******************************************************************************/

// Put puts a given block into the store
func (self BitswapStore) Put(block blocks.Block) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return utils.StackError(err, "Unable to put block")
	}
	defer tx.Commit()

	p2pblock, err := getP2PBlock(block)
	if err != nil {
		return utils.StackError(err, "Unable to put block")
	}
	err = self.put(tx, block.Cid(), p2pblock)
	if err != nil {
		tx.Rollback()
		return utils.StackError(err, "Unable to put block")
	}
	return nil
}

func (self BitswapStore) PutMany(blocklist []blocks.Block) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return utils.StackError(err, "Unable to put many blocks")
	}
	defer tx.Commit()

	for _, block := range blocklist {

		p2pblock, err := getP2PBlock(block)
		if err != nil {
			return utils.StackError(err, "Unable to put many blocks")
		}
		err = self.put(tx, block.Cid(), p2pblock)

		if err != nil {
			tx.Rollback()
			return utils.StackError(err, "Unable to put many blocks")
		}
	}
	return nil
}

func (self BitswapStore) DeleteBlock(key cid.Cid) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return utils.StackError(err, "Unable to delete block")
	}
	defer tx.Commit()

	return self.basicDelete(key, tx)
}

func (self BitswapStore) DeleteBlocks(keys []cid.Cid) error {

	tx, err := self.db.Begin(true)
	if err != nil {
		return utils.StackError(err, "Unable to delete blocks")
	}
	defer tx.Commit()

	for _, key := range keys {
		self.basicDelete(key, tx)
	}
	return nil
}

func (self BitswapStore) basicDelete(key cid.Cid, tx *bolt.Tx) error {

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
		return os.Remove(path)
	}

	//or a normal file
	bucket = tx.Bucket(FileKey)
	res = bucket.Bucket(key.Bytes())
	if res != nil {
		bucket.DeleteBucket(key.Bytes())
		//delete the file
		path := filepath.Join(self.path, key.String())
		return os.Remove(path)
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

//basic version of Has without opening a transaction
func (self BitswapStore) has(key cid.Cid, tx *bolt.Tx) (bool, error) {

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

func (self BitswapStore) Has(key cid.Cid) (bool, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return false, utils.StackError(err, "Unable to check for existance of block")
	}
	defer tx.Rollback()

	return self.has(key, tx)
}

//returnes a list of keys the store does not have
func (self BitswapStore) DoesNotHave(keys []cid.Cid) ([]cid.Cid, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, utils.StackError(err, "Unable to check for block existance")
	}
	defer tx.Rollback()

	result := make([]cid.Cid, 0, len(keys))
	for _, key := range keys {

		has, err := self.has(key, tx)
		if err != nil {
			return nil, utils.StackError(err, "Unable to check for block existance")
		}
		if !has {
			result = append(result, key)
		}
	}

	return result, nil
}

func (self BitswapStore) Get(key cid.Cid) (blocks.Block, error) {

	tx, err := self.db.Begin(false)
	if err != nil {
		return nil, utils.StackError(err, "Unable to get block")
	}
	defer tx.Rollback()

	//check if it is a directory
	bucket := tx.Bucket(DirKey)
	bucket = bucket.Bucket(key.Bytes())
	if bucket != nil {
		//rebuild the block!
		name := string(bucket.Get([]byte("name")))
		//get all blocks!
		blocks := make([]cid.Cid, 0)
		blockbucket := bucket.Bucket(BlockKey)
		err := blockbucket.ForEach(func(k, v []byte) error {
			blockcid, err := cid.Cast(copyKey(k))
			if err != nil {
				return utils.StackError(err, "Unable to get block")
			}
			blocks = append(blocks, blockcid)
			return nil
		})
		if err != nil {
			return nil, utils.StackError(err, "Unable to get block")
		}
		dirblock := P2PDirectoryBlock{name, blocks}
		return dirblock.ToBlock(), nil
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
				return utils.StackError(err, "Unable to get block")
			}
			blocks = append(blocks, blockcid)
			return nil
		})
		if err != nil {
			return nil, utils.StackError(err, "Unable to get block")
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
			return nil, utils.StackError(err, "Unable to get block")
		}
		defer fi.Close()
		data := make([]byte, blocksize)
		n, err := fi.Read(data)
		if err != nil {
			return nil, utils.StackError(err, "Unable to get block")
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
			return nil, utils.StackError(err, "Unable to get block")
		}
		defer fi.Close()
		data := make([]byte, blocksize)
		n, err := fi.ReadAt(data, offset)
		if err != nil && err != io.EOF {
			return nil, utils.StackError(err, "Unable to get block")
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
		return nil, utils.StackError(err, "Unable to collect keys for channel")
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
			return utils.StackError(err, "Unable to collect keys for channel")
		}
		keys = append(keys, c)
		return nil
	})
	if err != nil {
		close(res)
		return nil, utils.StackError(err, "Unable to collect keys for channel")
	}

	//collect all files
	bucket = tx.Bucket(FileKey)
	err = bucket.ForEach(func(k, v []byte) error {
		c, err := cid.Cast(copyKey(k))
		if err != nil {
			return utils.StackError(err, "Unable to collect keys for channel")
		}
		keys = append(keys, c)
		return nil
	})
	if err != nil {
		close(res)
		return nil, utils.StackError(err, "Unable to collect keys for channel")
	}

	//collect all Mfiles and raw blocks
	bucket = tx.Bucket(FileKey)
	err = bucket.ForEach(func(k, v []byte) error {
		c, err := cid.Cast(copyKey(k))
		if err != nil {
			return utils.StackError(err, "Unable to collect keys for channel")
		}
		keys = append(keys, c)

		blocks := bucket.Bucket(k).Bucket(BlockKey)

		err = blocks.ForEach(func(k2, v2 []byte) error {
			c, err := cid.Cast(copyKey(k2))
			if err != nil {
				return utils.StackError(err, "Unable to collect keys for channel")
			}
			keys = append(keys, c)
			return nil
		})
		if err != nil {
			close(res)
			return utils.StackError(err, "Unable to collect keys for channel")
		}
		return nil
	})
	if err != nil {
		close(res)
		return nil, utils.StackError(err, "Unable to collect keys for channel")
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
		return 0, utils.StackError(err, "Unable to cget size for key")
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
		return utils.StackError(err, "Unable to put directory")
	}

	//make sure the owner bucket exists
	_, err = directory.CreateBucketIfNotExists(OwnerKey)
	if err != nil {
		return utils.StackError(err, "Unable to put directory")
	}

	//write the data
	directory.Put([]byte("name"), []byte(block.Name))

	//create a empty blocks bucket
	blockBucket := directory.Bucket(BlockKey)
	if blockBucket != nil {
		//we delete the bucket to make sure we do not have any old blocks in it
		err := directory.DeleteBucket(BlockKey)
		if err != nil {
			return utils.StackError(err, "Unable to put directory")
		}
	}
	blockBucket, err = directory.CreateBucket(BlockKey)
	if err != nil {
		return utils.StackError(err, "Unable to put directory")
	}

	//set all sub blocks
	for _, blockcid := range block.Blocks {
		err := blockBucket.Put(blockcid.Bytes(), blockcid.Bytes())
		if err != nil {
			return utils.StackError(err, "Unable to put directory")
		}
	}

	return nil
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
			return utils.StackError(err, "Unable to put multifile")
		}
	}
	blocks, err = mfile.CreateBucket(BlockKey)
	if err != nil {
		return utils.StackError(err, "Unable to put multifile")
	}

	//set all blocks to unset
	for _, blockcid := range block.Blocks {
		err := blocks.Put(blockcid.Bytes(), intToByte(-1))
		if err != nil {
			return utils.StackError(err, "Unable to put multifile")
		}
	}

	//make sure the owner bucket exists
	_, err = mfile.CreateBucketIfNotExists(OwnerKey)
	if err != nil {
		return utils.StackError(err, "Unable to put multifile")
	}
	return nil
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
		return utils.StackError(err, "Unable to put raw block")
	}

	//write the block data
	mfiles := tx.Bucket(MfileKey)
	mfile := mfiles.Bucket(blockfilecid.Bytes())
	blocks := mfile.Bucket(BlockKey)
	err = blocks.Put(c.Bytes(), intToByte(block.Offset))
	if err != nil {
		return utils.StackError(err, "Unable to put raw block")
	}

	//we found the multifile! lets put the data into the file
	path := filepath.Join(self.path, blockfilecid.String())
	fi, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return utils.StackError(err, "Unable to put raw block")
	}
	defer fi.Close()

	n, err := fi.WriteAt(block.Data, block.Offset)
	if err != nil {
		return utils.StackError(err, "Unable to put raw block")
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
		return utils.StackError(err, "Unable to put file block")
	}
	file.Put([]byte("name"), []byte(block.Name))

	//make sure the owner bucket exists
	_, err = file.CreateBucketIfNotExists(OwnerKey)
	if err != nil {
		return utils.StackError(err, "Unable to put file block")
	}

	//write the file
	path := filepath.Join(self.path, c.String())
	fi, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return utils.StackError(err, "Unable to put file block")
	}
	defer fi.Close()

	n, err := fi.Write(block.Data)
	if err != nil {
		return utils.StackError(err, "Unable to put file block")
	}
	if n != len(block.Data) {
		return fmt.Errorf("Could not write all data into file")
	}
	return nil
}
