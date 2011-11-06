package fs

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
)

// Provide access to the raw byte storage.
type BlockStore interface {

	// Get the root hierarchical index node
	Root() FsNode

	Index() *BlockIndex

	// Given a strong checksum of a block, get the bytes for that block.
	ReadBlock(strong string) ([]byte, error)

	// Given the strong checksum of a file, start and end positions, get those bytes.
	ReadInto(strong string, from int64, length int64, writer io.Writer) (int64, error)
}

// A local file implementation of BlockStore
type LocalStore interface {
	BlockStore

	RelPath(fullpath string) (relpath string)

	Relocate(fullpath string) (relocFullpath string, err error)

	Resolve(relpath string) string

	RootPath() string

	reindex() error
}

type localBase struct {
	rootPath string
	index    *BlockIndex
	relocs   map[string]string
}

type LocalDirStore struct {
	*localBase
	dir *Dir
}

type LocalFileStore struct {
	*localBase
	file *File
}

func NewLocalStore(rootPath string) (local LocalStore, err error) {
	rootInfo, err := os.Stat(rootPath)
	if err != nil {
		return nil, err
	}

	localBase := &localBase{rootPath: rootPath}
	if rootInfo.IsDirectory() {
		local = &LocalDirStore{localBase: localBase}
	} else if rootInfo.IsRegular() {
		local = &LocalFileStore{localBase: localBase}
	}

	localBase.relocs = make(map[string]string)

	if err := local.reindex(); err != nil {
		return nil, err
	}

	return local, nil
}

func (store *LocalDirStore) reindex() (err error) {
	store.dir = IndexDir(store.RootPath(), nil)
	if store.dir == nil {
		return errors.New(fmt.Sprintf("Failed to reindex root: %s", store.RootPath()))
	}

	store.index = IndexBlocks(store.dir)
	return nil
}

func (store *LocalFileStore) reindex() (err error) {
	store.file, err = IndexFile(store.RootPath())
	if err != nil {
		return err
	}

	store.index = IndexBlocks(store.file)
	return nil
}

func (store *localBase) RelPath(fullpath string) (relpath string) {
	relpath = strings.Replace(fullpath, store.RootPath(), "", 1)
	relpath = strings.TrimLeft(relpath, "/\\")
	return relpath
}

const RELOC_PREFIX string = "_reloc"

func (store *localBase) Relocate(fullpath string) (relocFullpath string, err error) {
	relocFh, err := ioutil.TempFile(store.RootPath(), RELOC_PREFIX)
	if err != nil {
		return "", err
	}

	relocFullpath = relocFh.Name()

	err = relocFh.Close()
	if err != nil {
		return "", err
	}

	err = os.Remove(relocFh.Name())
	if err != nil {
		return "", err
	}

	err = Move(fullpath, relocFullpath)
	if err != nil {
		return "", err
	}

	relpath := store.RelPath(fullpath)
	relocRelpath := store.RelPath(relocFullpath)

	store.relocs[relpath] = relocRelpath
	return relocFullpath, nil
}

func (store *localBase) Resolve(relpath string) string {
	if relocPath, hasReloc := store.relocs[relpath]; hasReloc {
		relpath = relocPath
	}

	return filepath.Join(store.RootPath(), relpath)
}

func (store *localBase) RootPath() string { return store.rootPath }

func (store *LocalDirStore) Root() FsNode { return store.dir }

func (store *LocalFileStore) Root() FsNode { return store.file }

func (store *localBase) Index() *BlockIndex { return store.index }

func (store *localBase) ReadBlock(strong string) ([]byte, error) {
	block, has := store.index.StrongBlock(strong)
	if !has {
		return nil, errors.New(
			fmt.Sprintf("Block with strong checksum %s not found", strong))
	}

	buf := &bytes.Buffer{}
	_, err := store.ReadInto(block.Parent().Strong(), block.Offset(), int64(BLOCKSIZE), buf)
	if err == nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (store *localBase) ReadInto(strong string, from int64, length int64, writer io.Writer) (int64, error) {

	file, has := store.index.StrongFile(strong)
	if !has {
		return 0,
			errors.New(fmt.Sprintf("File with strong checksum %s not found", strong))
	}

	path := store.Resolve(RelPath(file))

	fh, err := os.Open(path)
	if fh == nil {
		return 0, err
	}

	_, err = fh.Seek(from, 0)
	if err != nil {
		return 0, err
	}

	n, err := io.CopyN(writer, fh, length)
	if err != nil {
		return n, err
	}

	return n, nil
}
