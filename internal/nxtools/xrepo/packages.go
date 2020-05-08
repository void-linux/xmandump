package xrepo

import (
	"crypto/sha1"
	"encoding/json"
	"net/url"
	"time"

	"golang.org/x/tools/container/intsets"
)

// Package describes an XBPS package as stored in a repository's repodata.
type Package struct {
	PackageVersion string `plist:"pkgver" json:"-"`
	Name           string `plist:"-" json:"name,omitempty"`
	Version        string `plist:"-" json:"version,omitempty"`
	Revision       int    `plist:"-" json:"revision,omitempty"`

	Repository     string `plist:"-" json:"repository,omitempty"`
	Architecture   string `plist:"architecture" json:"architecture,omitempty"`
	BuildDate      Time   `plist:"build-date" json:"build_date,omitempty"`
	BuildOptions   string `plist:"build-options" json:"build_options,omitempty"`
	FilenameSHA256 string `plist:"filename-sha256" json:"filename_sha256,omitempty"`
	FilenameSize   int64  `plist:"filename-size" json:"filename_size,omitempty"`
	Homepage       *URL   `plist:"homepage" json:"homepage,omitempty"`
	InstalledSize  int64  `plist:"installed_size" json:"installed_size,omitempty"`
	License        string `plist:"license" json:"license,omitempty"`
	Maintainer     string `plist:"maintainer" json:"maintainer,omitempty"`
	ShortDesc      string `plist:"short_desc" json:"short_desc,omitempty"`
	Preserve       bool   `plist:"preserve" json:"preserve,omitempty"`

	SourceRevisions string `plist:"source-revisions" json:"source_revisions,omitempty"`

	RunDepends []string `plist:"run_depends" json:"run_depends,omitempty"`

	ShlibRequires []string `plist:"shlib-requires" json:"shlib_requires,omitempty"`
	ShlibProvides []string `plist:"shlib-provides" json:"shlib_provides,omitempty"`

	Conflicts []string `plist:"conflicts" json:"conflicts,omitempty"`
	Reverts   []string `plist:"reverts" json:"reverts,omitempty"`

	Replaces     []string            `plist:"replaces" json:"replaces,omitempty"`
	Alternatives map[string][]string `plist:"alternatives" json:"alternatives,omitempty"`

	ConfFiles []string `plist:"conf_files" json:"conf_files,omitempty"`

	Index int    `plist:"-" json:"-"`
	ETag  string `plist:"-" json:"-"`
}

func (p *Package) computeETag() (string, error) {
	h := sha1.New()
	if err := json.NewEncoder(h).Encode(p); err != nil {
		return "", nil
	}
	sum := h.Sum(make([]byte, 0, h.Size()))
	etag := `W/"` + etagEncoding.EncodeToString(sum) + `"`
	return etag, nil
}

// Packages holds a slice of packages.
type Packages []*Package

const (
	minSplitFilter   = 3000
	minIndexCapacity = 16
	splitSize        = 2000
)

// Filter returns a new Packages slice containing only packages that match the filter.
// The filter must not modify packages.
func (ps Packages) Filter(filter FilterFunc) Packages {
	if len(ps) <= minSplitFilter {
		return ps.singleFilter(filter)
	}

	return ps.splitFilter(filter)
}

// singleFilter walks the receiver's set of packages and generates a new set of packages matching
// filter.
func (ps Packages) singleFilter(fn FilterFunc) Packages {
	ps2 := make(Packages, 0, 16)
	for _, p := range ps {
		if fn(p) {
			ps2 = append(ps2, p)
		}
	}
	return ps2
}

// splitFilter walks the receiver's set of packages, splitting it into subsets that are filtered
// concurrently.
func (ps Packages) splitFilter(fn FilterFunc) Packages {
	var (
		want    int
		index   intsets.Sparse
		subsets = make(chan *intsets.Sparse)
	)

	for i := 0; i < len(ps); i += splitSize {
		min := i
		max := min + splitSize
		if max > len(ps) {
			max = len(ps)
		}
		want++
		set := ps[min:max]

		go func() {
			var sub intsets.Sparse
			for i, p := range set {
				if fn(p) {
					sub.Insert(min + i)
				}
			}
			subsets <- &sub
		}()
	}

	for ; want > 0; want-- {
		index.UnionWith(<-subsets)
	}

	ps2 := make(Packages, 0, index.Len())
	for i := 0; index.TakeMin(&i); {
		ps2 = append(ps2, ps[i])
	}

	return ps2
}

// URL is a repodata-marshaling-friendly url.URL.
type URL url.URL

// URL returns the receiver as a url.URL.
func (u *URL) URL() *url.URL {
	return (*url.URL)(u)
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (u *URL) UnmarshalText(p []byte) error {
	uu, err := url.Parse(string(p))
	if err != nil {
		return err
	}
	*u = URL(*uu)
	return nil
}

// MarshalText implements encoding.TextMarshaler.
func (u *URL) MarshalText() ([]byte, error) {
	if u == nil {
		return nil, nil
	}

	return []byte((*url.URL)(u).String()), nil
}

// Time is repodata-marshaling-friendly time.Time.
type Time time.Time

// Time returns the receiver as a time.Time.
func (t Time) Time() time.Time {
	return time.Time(t)
}

// MarshalText implements encoding.TextMarshaler.
func (t Time) MarshalText() ([]byte, error) {
	return []byte(t.Time().Format(time.RFC3339)), nil
}

// UnmarshalText implements encoding.TextUnmarshaler.
func (t *Time) UnmarshalText(p []byte) error {
	const layout = `2006-01-02 15:04 MST`
	tt, err := time.ParseInLocation(layout, string(p), time.UTC)
	if err != nil {
		return err
	}
	*t = Time(tt.UTC())
	return nil
}
