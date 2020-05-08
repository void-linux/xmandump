package xbps

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// PkgVer describes the name, version, and revision of a package.
type PkgVer struct {
	Name     string
	Version  string
	Revision int
}

// Errors that may be in the Err field of *PkgVerError returned by ParsePkgVer.
var (
	ErrPkgVerNoName           = errors.New("missing name")
	ErrPkgVerNoVersion        = errors.New("missing version")
	ErrPkgVerNoRevision       = errors.New("missing revision")
	ErrPkgVerBadRevision      = errors.New("revision is not a valid integer >= 1")
	ErrPkgVerMalformedVersion = errors.New("version must not contain the characters : (colon) or - (hyphen)")
)

// PkgVerError is an error returned by ParsePkgVer.
type PkgVerError struct {
	PkgVer string
	Err    error
}

func (e *PkgVerError) Error() string {
	return fmt.Sprintf("pkgver: cannot parse %q: %v", e.PkgVer, e.Err)
}

// ParsePkgVer parses a pkgver string of the form <name>-<version>_<revision>.
// The revision must be a positive, non-zero integer.
// Version is any non-empty string that may not contain hyphens (-) or colons (:).
// All errors returned by ParsePkgVer are of the type *PkgVerError.
func ParsePkgVer(s string) (pkgver PkgVer, err error) {
	// Extract and validate revision
	revSep := strings.LastIndexByte(s, '_')
	if revSep == -1 || revSep == len(s)-1 {
		return pkgver, &PkgVerError{s, ErrPkgVerNoRevision}
	}

	rev, err := strconv.Atoi(s[revSep+1:])
	if err != nil || rev <= 0 {
		return pkgver, &PkgVerError{s, ErrPkgVerBadRevision}
	}

	// Extract and validate version
	versionSep := strings.LastIndexByte(s[:revSep], '-')
	if versionSep == -1 {
		return pkgver, &PkgVerError{s, ErrPkgVerNoVersion}
	}

	ver := s[versionSep+1 : revSep]
	if len(ver) == 0 {
		return pkgver, &PkgVerError{s, ErrPkgVerNoVersion}
	}

	for _, r := range ver {
		if r == ':' || r == '-' {
			return pkgver, &PkgVerError{s, ErrPkgVerMalformedVersion}
		}
	}

	// Extract name
	name := s[:versionSep]
	if len(name) == 0 {
		return pkgver, &PkgVerError{s, ErrPkgVerNoName}
	}

	return PkgVer{name, ver, rev}, nil
}

// String returns the PkgVer as a string of the form '<name>-<version>_<revision>".
func (p PkgVer) String() string {
	return p.Name + "-" + p.Version + "_" + strconv.Itoa(p.Revision)
}
