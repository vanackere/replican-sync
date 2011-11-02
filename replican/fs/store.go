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
	Root() *Dir

	Index() *BlockIndex

	// Given a strong checksum of a block, get the bytes for that block.
	ReadBlock(strong string) ([]byte, error)

	// Given the strong checksum of a file, start and end positions, get those bytes.
	ReadInto(strong string, from int64, length int64, writer io.Writer) (int64, error)
}

// A local file implementation of BlockStore
type LocalStore struct {
	rootPath string
	root     *Dir
	index    *BlockIndex
	relocs   map[string]string
}

func NewLocalStore(rootPath string) (*LocalStore, error) {
	local := &LocalStore{rootPath: rootPath}
	local.relocs = make(map[string]string)

	if err := local.reindex(); local.root == nil {
		return nil, err
	}

	return local, nil
}

func (store *LocalStore) reindex() (err error) {
	store.root, err = IndexDir(store.rootPath)
	if store.root == nil {
		return err
	}

	store.index = IndexBlocks(store.root)
	return nil
}

func (store *LocalStore) RelPath(fullpath string) (relpath string) {
	relpath = strings.Replace(fullpath, store.rootPath, "", 1)
	relpath = strings.TrimLeft(relpath, "/\\")
	return relpath
}

const RELOC_PREFIX string = "_reloc"

func (store *LocalStore) Relocate(fullpath string) (relocFullpath string, err error) {
	relocFh, err := ioutil.TempFile(store.rootPath, RELOC_PREFIX)
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

func (store *LocalStore) Resolve(relpath string) string {
	if relocPath, hasReloc := store.relocs[relpath]; hasReloc {
		relpath = relocPath
	}

	return filepath.Join(store.rootPath, relpath)
}

func (store *LocalStore) Root() *Dir { return store.root }

func (store *LocalStore) Index() *BlockIndex { return store.index }

func (store *LocalStore) ReadBlock(strong string) ([]byte, error) {
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

func (store *LocalStore) ReadInto(strong string, from int64, length int64, writer io.Writer) (int64, error) {

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
