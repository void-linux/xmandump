package main

import (
	"archive/tar"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ulikunitz/xz"
	"go.spiff.io/nxtools/xrepo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

const (
	cacheVersion = 1
)

type cacheRecords struct {
	Version int                 `json:"version"`
	Cache   map[string][]string `json:"cache-v1"`
}

func main() {
	// TODO: Make this code less disgusting.
	var (
		openLimit int64  = 64
		flagLevel        = zap.WarnLevel
		ctx              = context.Background()
		flagMode  string = "755"
		fileMode  os.FileMode
		cacheFile string
		cache     cacheRecords
	)

	maxLimit, limErr := getFileLimit()
	if limErr == nil && maxLimit < openLimit {
		openLimit = maxLimit
	}

	if stat, err := os.Stat("."); err == nil {
		flagMode = fmt.Sprintf("%03o", uint32(stat.Mode()&0x1ff))
	}

	flag.StringVar(&cacheFile, "c", "", "cache file")
	flag.StringVar(&flagMode, "m", flagMode, "directory permissions")
	flag.Var(&flagLevel, "v", "log level")
	flag.Int64Var(&openLimit, "L", openLimit, "concurrent file limit")
	flag.Parse()

	logLevel := zap.NewAtomicLevelAt(flagLevel)
	logger, err := NewLogger(logLevel)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: unable to create logger: %v\n", err)
		os.Exit(1)
	}

	zap.ReplaceGlobals(logger)
	ctx = WithLogger(ctx, logger)

	// Load cache (if any)
	if cacheFile != "" {
		p, err := ioutil.ReadFile(cacheFile)
		if err == nil {
			err = json.Unmarshal(p, &cache)
		} else if os.IsNotExist(err) {
			logger.Warn("Cache file not found", logFile(cacheFile))
			err = nil
		}
		if err != nil {
			logger.Fatal("Invalid cache file", logFile(cacheFile), zap.Error(err))
		}
	}

	switch cache.Version {
	// TODO: Add migration of other cache versions' data where relevant.
	case 0, cacheVersion: // Nothing
	}

	// Parse file mode
	parsedMode, err := strconv.ParseUint(flagMode, 8, 32)
	if err != nil {
		logger.Fatal("Invalid file mode: cannot be parsed", zap.Error(err))
	} else if parsedMode == 0 {
		logger.Fatal("Invalid file mode: may not be 0")
	}
	fileMode = os.FileMode(parsedMode)

	// Check limit
	if openLimit < 2 {
		logger.Fatal("Invalid limit -- must be >= 2", zap.Int64("limit", openLimit))
	} else if openLimit > maxLimit {
		logger.Fatal("Invalid limit -- must be <= nofiles", zap.Int64("nofiles", maxLimit), zap.Int64("limit", openLimit))
	}

	// Semaphore controls no. of open files via goroutines -- all acquisitions have a weight of
	// 2 -- one for the package, one for a new file.
	sema := semaphore.NewWeighted(openLimit)
	wg, ctx := errgroup.WithContext(ctx)

	dumper := &Dumper{
		DirMode: fileMode,
		Sema:    sema,
		Cache:   cache.Cache,
		Updates: map[string][]string{},
	}

	filerefs := map[string]struct{}{}

	for _, files := range dumper.Cache {
		for _, file := range files {
			filerefs[file] = struct{}{}
		}
	}

	for _, file := range flag.Args() {
		file := file
		wg.Go(func() error {
			return dumper.processRepoData(ctx, file)
		})
	}

	if err := wg.Wait(); err != nil {
		logger.Fatal("Fatal error processing files", zap.Error(err))
	}

	for _, files := range dumper.Updates {
		for _, file := range files {
			delete(filerefs, file)
		}
	}

	for file, _ := range filerefs {
		if filepath.IsAbs(file) || strings.Contains(filepath.ToSlash(file), "../") {
			// This is to prevent removal of paths like /usr/share/man/... in case
			// someone munges and then passes a .vmandump file in.
			logger.Debug("Skipping removal of absolute file path", logFile(file))
			continue
		}
		logger.Debug("Removing unused file", logFile(file))
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			logger.Error("Error removing old file", logFile(file), zap.Error(err))
		}
	}

	cache = cacheRecords{
		Version: cacheVersion,
		Cache:   dumper.Updates,
	}
	p, err := json.Marshal(cache)
	if err != nil {
		logger.Fatal("Error encoding cache", zap.Error(err))
	}

	if err := ioutil.WriteFile(cacheFile, p, 0600); err != nil {
		logger.Fatal("Error writing cache", logFile(cacheFile), zap.Error(err))
	}
}

const (
	manPathPrefix     = "usr/share/man/man"
	manPathTrimPrefix = "usr/share/man/"
)

// TODO: Propagate list of created files up to caller so that they can be tracked relative as
// new files.

// Dumper processes packages and dumps manpage files to the current directory in the form manN/file.
type Dumper struct {
	DirMode os.FileMode
	Sema    *semaphore.Weighted

	m       sync.Mutex
	Cache   map[string][]string
	Updates map[string][]string
}

func (d *Dumper) recordChange(pkg string, paths ...string) {
	d.m.Lock()
	defer d.m.Unlock()
	if len(paths) == 0 && len(d.Updates[pkg]) == 0 {
		// This is a convenience for stripping two bytes per empty package off the cache JSON
		d.Updates[pkg] = []string{}
	} else {
		d.Updates[pkg] = append(d.Updates[pkg], paths...)
	}
}

func (d *Dumper) processRepoData(ctx context.Context, file string) (err error) {
	rd, err := d.readRepoData(ctx, file)
	if os.IsNotExist(err) {
		return nil
	}

	wg, ctx := errgroup.WithContext(ctx)
	dir := filepath.Dir(file)
	index := rd.Index()
	for _, pkg := range index {
		pkg := pkg
		pkgfile := filepath.Join(dir, pkg.PackageVersion+"."+pkg.Architecture+".xbps")

		if err := d.Sema.Acquire(ctx, 2); err != nil {
			return err
		}

		wg.Go(func() error {
			defer d.Sema.Release(2)
			return d.processPackage(ctx, pkg, pkgfile)
		})
	}

	return wg.Wait()
}

func (d *Dumper) readRepoData(ctx context.Context, file string) (*xrepo.RepoData, error) {
	ctx = WithFields(ctx, logRepoData(file))

	timer := Elapsed("elapsed")
	Info(ctx, "Processing repodata")
	defer Info(ctx, "Finished processing repodata", timer())

	f, err := os.Open(file)
	if os.IsNotExist(err) {
		Warn(ctx, "File does not exist")
		return nil, err
	} else if err != nil {
		Error(ctx, "Cannot open file", zap.Error(err))
		return nil, err
	}
	defer logClose(ctx, f)

	rd := xrepo.NewRepoData()
	if err := rd.ReadRepo(f, ""); err != nil {
		Error(ctx, "Unable to read repodata", zap.Error(err))
		return nil, err
	}

	return rd, nil
}

// processPackage processes an XBPS package and extracts all manpages under the current directory.
func (d *Dumper) processPackage(ctx context.Context, pkg *xrepo.Package, file string) (err error) {
	ctx = WithFields(ctx, logFile(file))

	if entries, ok := d.Cache[pkg.FilenameSHA256]; ok {
		Debug(ctx, "Package already dumped")
		d.recordChange(pkg.FilenameSHA256, entries...)
		return nil
	}

	Info(ctx, "Processing file")
	timer := Elapsed("elapsed")
	defer Info(ctx, "Finished processing file", timer())

	f, err := os.Open(file)
	if os.IsNotExist(err) {
		Warn(ctx, "File does not exist")
		return nil
	} else if err != nil {
		Error(ctx, "Cannot open file", zap.Error(err))
		return err
	}
	defer logClose(ctx, f)

	dec, err := xz.NewReader(f)
	if err != nil {
		Error(ctx, "Unable to create decompressor", zap.Error(err))
		return err
	}

	tf := tar.NewReader(dec)

	for {
		hdr, err := tf.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			Error(ctx, "Error encountered reading package", zap.Error(err))
			return err
		}

		err = d.processPackageFile(ctx, pkg, hdr, tf)
		if err != nil {
			Error(ctx, "Error processing package file", logPkgFile(hdr.Name), zap.Error(err))
			return err
		}
	}

	d.recordChange(pkg.FilenameSHA256)

	return nil
}

// processPackageFile checks the tar header to see if the packaged file is a manpage and, if it is,
// extracts it. If the packaged file is a manpage symlink or link, it is ignored.
func (d *Dumper) processPackageFile(ctx context.Context, pkg *xrepo.Package, hdr *tar.Header, r io.Reader) (err error) {
	ctx = WithFields(ctx, logPkgFile(hdr.Name))

	switch hdr.Typeflag {
	case tar.TypeReg:
	case tar.TypeLink, tar.TypeSymlink:
		// TODO: Handle manpage symlinks at all?
		// return processManLink(ctx, hdr)
		return nil
	default:
		return nil
	}

	pkgfile := path.Clean(hdr.Name)
	if !strings.HasPrefix(pkgfile, manPathPrefix) {
		return nil
	}

	Debug(ctx, "Found manpage")

	relpath := strings.TrimPrefix(pkgfile, manPathTrimPrefix)
	relpath = filepath.FromSlash(relpath)
	reldir := filepath.Dir(relpath)

	ctx = WithFields(ctx, logDumpFile(relpath))

	if err = os.MkdirAll(reldir, d.DirMode); err != nil {
		Error(ctx, "Unable to create directory for manpage", zap.Error(err))
		return err
	}

	// TODO: Dump manpage to filesystem after stripping usr/share/ prefix
	f, err := os.Create(relpath)
	if err != nil {
		Error(ctx, "Unable to create dumped file")
		return err
	}
	defer logClose(ctx, f)

	if _, err := io.Copy(f, r); err != nil {
		Error(ctx, "Error copying pkgfile to dumpfile", zap.Error(err))
		return err
	}

	d.recordChange(pkg.FilenameSHA256, relpath)

	return nil
}

func logClose(ctx context.Context, c io.Closer) (err error) {
	if err = c.Close(); err != nil {
		Warn(ctx, "Encountered Close error", zap.Error(err))
	}
	return err
}
