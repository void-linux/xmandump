package xrepo

import (
	"archive/tar"
	"bytes"
	"crypto/sha1"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"

	"github.com/void-linux/xmandump/internal/nxtools/xbps"

	"github.com/klauspost/compress/zstd"
	"howett.net/plist"
)

var etagEncoding = base64.RawURLEncoding

const repoIndexFile = "index.plist"
const defaultRepository = "current"

// ErrNoIndex is returned if the repository's index property list isn't found.
var ErrNoIndex = fmt.Errorf("index not found: %s", repoIndexFile)

// FilterFunc is a function that returns true if a package matches the filter, and false otherwise.
// Filters must not modify the packages they filter.
type FilterFunc func(*Package) bool

// packageMap is a package name (minus version and revision) to *Package map.
type packageMap map[string]*Package

// RepoData describes an XBPS repository.
type RepoData struct {
	root      packageMap
	index     Packages
	nameIndex []string
	etag      string
}

// NewRepoData allocates a new, empty repodata. It must be populated using LoadRepo.
func NewRepoData() *RepoData {
	return &RepoData{
		root: packageMap{},
	}
}

// LoadRepo attempts to load repodata from the given path and assigns packages the given repo string
// as their repository (not a field formally defined by an XBPS repository). If repo is an empty
// string, it attempts to determine the repository from the path.
//
// If an error is returned, the receiver is in an undefined state.
func (rd *RepoData) LoadRepo(path, repo string) error {
	fi, err := os.Open(path)
	if err != nil {
		return err
	}
	defer fi.Close()

	return rd.ReadRepo(fi, repo)
}

// Index returns the complete set of packages held in the RepoData.
// Callers must not modify the returned slice or packages.
func (rd *RepoData) Index() Packages {
	if rd == nil {
		return nil
	}
	return rd.index
}

// NameIndex returns the name of all packages, without version and revision.
func (rd *RepoData) NameIndex() []string {
	if rd == nil {
		return nil
	}
	return rd.nameIndex
}

// ReadRepo reads a repository's repodata from the given io.Reader.
// It assigns all packages in r the given repo string. If repo is an empty string, it attempts to
func (rd *RepoData) ReadRepo(r io.Reader, repo string) error {
	gr, err := zstd.NewReader(r)
	if err != nil {
		return err
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if hdr.Name == repoIndexFile {
			return rd.ReadRepoIndex(tr, repo)
		}
	}
	return ErrNoIndex
}

func copyToMemory(r io.Reader) (*bytes.Reader, error) {
	p, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(p), nil
}

// ReadRepoIndex reads a repository's repodata index property list and adds all package data from
// that to the receiver RepoData. This is rarely called directly.
func (rd *RepoData) ReadRepoIndex(r io.Reader, repo string) error {
	var err error
	rs, ok := r.(io.ReadSeeker)
	if !ok {
		if rs, err = copyToMemory(r); err != nil {
			return err
		}
	}

	if repo == "" {
		repo = defaultRepository
	}

	pkg := packageMap{}
	err = plist.NewDecoder(rs).Decode(pkg)
	if err != nil {
		return err
	}

	// Merge indices and maps -- this gets around a flaw in howett.net/plist where decoding into
	// an existing dataset will result in an invalid use of the reflect package and panic.
	index := rd.index
	for k, p := range pkg {
		old, ok := rd.root[k]
		if p.Name == "" {
			p.Name = k
			_, p.Version, p.Revision, _ = parseVersionedName(p.PackageVersion)
		}

		p.Repository = repo

		p.ETag, err = p.computeETag()
		if err != nil {
			// This really shouldn't happen -- it would mean JSON encoding of packages
			// was broken.
			return err
		}

		rd.root[k] = p
		if ok {
			index[old.Index] = p
		} else {
			index = append(index, p)
		}
	}

	sort.Slice(index, func(i, j int) bool {
		return index[i].Name < index[j].Name
	})

	for i, p := range index {
		p.Index = i
	}

	rd.index = index

	names := rd.nameIndex[:0]
	for _, p := range rd.index {
		names = append(names, p.Name)
	}
	rd.nameIndex = names

	etag, err := rd.computeETag()
	if err != nil {
		return err
	}
	rd.etag = etag

	return nil
}

// Package returns the package, if any, identified by name.
// If no such package exists, it returns nil.
func (rd *RepoData) Package(name string) *Package {
	if rd == nil {
		return nil
	}
	return rd.root[name]
}

func (rd *RepoData) computeETag() (string, error) {
	h := sha1.New()

	index := rd.Index()
	if err := binary.Write(h, binary.LittleEndian, int64(len(index))); err != nil {
		return "", err
	}

	for _, p := range index {
		binary.Write(h, binary.LittleEndian, int64(len(p.PackageVersion)+len(p.ETag)))
		io.WriteString(h, p.PackageVersion)
		io.WriteString(h, p.ETag)
	}

	sum := h.Sum(make([]byte, 0, h.Size()))
	etag := `W/"` + etagEncoding.EncodeToString(sum) + `"`
	return etag, nil
}

// ETag returns the precomputed etag of the received.
func (rd *RepoData) ETag() string {
	return rd.etag
}

func parseVersionedName(s string) (name, version string, revision int, err error) {
	pkgver, err := xbps.ParsePkgVer(s)
	return pkgver.Name, pkgver.Version, pkgver.Revision, err
}
