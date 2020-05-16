package p2p

import (
	"context"
	"errors"
	"fmt"
	"io"
	gopath "path"

	"github.com/ipfs/go-cid"
	chunker "github.com/ipfs/go-ipfs-chunker"
	"github.com/ipfs/go-ipfs-files"
	"github.com/ipfs/go-ipfs-posinfo"
	ipld "github.com/ipfs/go-ipld-format"
	dag "github.com/ipfs/go-merkledag"
	"github.com/ipfs/go-mfs"
	"github.com/ipfs/go-unixfs"
	"github.com/ipfs/go-unixfs/importer/balanced"
	ihelper "github.com/ipfs/go-unixfs/importer/helpers"
	mh "github.com/multiformats/go-multihash"
)

var liveCacheSize = uint64(256 << 10)

type syncer interface {
	Sync() error
}

// NewAdder Returns a new Adder used for a file add operation.
func NewAdder(ctx context.Context, ds ipld.DAGService) (*Adder, error) {
	
	bufferedDS := ipld.NewBufferedDAG(ctx, ds)

	return &Adder{
		ctx:        ctx,
		dagService: ds,
		bufferedDS: bufferedDS,
		CidBuilder: cid.V1Builder{Codec: cid.DagCBOR, MhType: mh.SHA2_256},
		liveNodes: 0,
	}, nil
}

// Adder builds merkle trees from files and adds the to bitswap
type Adder struct {
	ctx        context.Context
	dagService ipld.DAGService
	bufferedDS *ipld.BufferedDAG
	mroot      *mfs.Root
	tempRoot   cid.Cid
	CidBuilder cid.Builder
	liveNodes uint64
}

func (self *Adder) mfsRoot() (*mfs.Root, error) {
	if self.mroot != nil {
		return self.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	rnode.SetCidBuilder(self.CidBuilder)
	mr, err := mfs.NewRoot(self.ctx, self.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	self.mroot = mr
	return self.mroot, nil
}

// Constructs a node from reader's data, and adds it. This constructs a merkle DAG
// for the given data and returns the root node of that DAG
func (self *Adder) add(reader io.Reader) (ipld.Node, error) {
	
	chnk, err := chunker.FromString(reader, "")
	if err != nil {
		return nil, err
	}

	params := ihelper.DagBuilderParams{
		Dagserv:    self.bufferedDS,
		Maxlinks:   ihelper.DefaultLinksPerBlock,
		NoCopy:     false,
		CidBuilder: self.CidBuilder,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}
	
	node, err := balanced.Layout(db)
	if err != nil {
		return nil, err
	}

	return node, self.bufferedDS.Commit()
}


//this function takes a node (root of a merkle dag) and puts it into the mfs
//at the specified path
func (self *Adder) addNode(node ipld.Node, path string) error {
	// patch it into the root
	if path == "" {
		path = node.Cid().String()
	}

	if pi, ok := node.(*posinfo.FilestoreNode); ok {
		node = pi.Node
	}

	mr, err := self.mfsRoot()
	if err != nil {
		return err
	}
	dir := gopath.Dir(path)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: self.CidBuilder,
		}
		if err := mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	if err := mfs.PutNode(mr, path, node); err != nil {
		return err
	}

	return nil
}

// Add adds the given request's files. It creates a DAG structure which fits the
// file and returns the root node
func (self *Adder) Add(file files.Node) (ipld.Node, error) {

	//build and add the DAG tree
	if err := self.addFileNode("", file, true); err != nil {
		return nil, err
	}

	// get root
	mr, err := self.mfsRoot()
	if err != nil {
		return nil, err
	}
	var root mfs.FSNode
	rootdir := mr.GetDirectory()
	root = rootdir

	err = root.Flush()
	if err != nil {
		return nil, err
	}

	// if adding a file without directory, swap the root to it (when adding a
	// directory, mfs root is the directory)
	_, dir := file.(files.Directory)
	var name string
	if !dir {
		children, err := rootdir.ListNames(self.ctx)
		if err != nil {
			return nil, err
		}

		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child dir, got none")
		}

		// Replace root with the first child
		name = children[0]
		root, err = rootdir.Child(name)
		if err != nil {
			return nil, err
		}
	}

	err = mr.Close()
	if err != nil {
		return nil, err
	}

	nd, err := root.GetNode()
	if err != nil {
		return nil, err
	}

	if asyncDagService, ok := self.dagService.(syncer); ok {
		err = asyncDagService.Sync()
		if err != nil {
			return nil, err
		}
	}

	return nd, nil
}

func (self *Adder) addFileNode(path string, file files.Node, toplevel bool) error {

	defer file.Close()

	if self.liveNodes >= liveCacheSize {

		mr, err := self.mfsRoot()
		if err != nil {
			return err
		}
		if err := mr.FlushMemFree(self.ctx); err != nil {
			return err
		}

		self.liveNodes = 0
	}
	self.liveNodes++

	switch f := file.(type) {
	case files.Directory:
		return self.addDir(path, f, toplevel)
	case *files.Symlink:
		return self.addSymlink(path, f)
	case files.File:
		return self.addFile(path, f)
	default:
		return errors.New("unknown file type")
	}
}

func (self *Adder) addSymlink(path string, l *files.Symlink) error {
	
	sdata, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return err
	}

	dagnode := dag.NodeWithData(sdata)
	dagnode.SetCidBuilder(self.CidBuilder)
	err = self.dagService.Add(self.ctx, dagnode)
	if err != nil {
		return err
	}

	return self.addNode(dagnode, path)
}

func (self *Adder) addFile(path string, file files.File) error {

	//build node
	dagnode, err := self.add(file)
	if err != nil {
		return err
	}

	// patch it into the root
	return self.addNode(dagnode, path)
}

func (self *Adder) addDir(path string, dir files.Directory, toplevel bool) error {

	if !(toplevel && path == "") {
		mr, err := self.mfsRoot()
		if err != nil {
			return err
		}
		err = mfs.Mkdir(mr, path, mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: self.CidBuilder,
		})
		if err != nil {
			return err
		}
	}

	it := dir.Entries()
	for it.Next() {
		fpath := gopath.Join(path, it.Name())
		err := self.addFileNode(fpath, it.Node(), false)
		if err != nil {
			return err
		}
	}

	return it.Err()
}
