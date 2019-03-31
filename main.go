package main

import (
	"archive/tar"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/ulikunitz/xz"
	"go.spiff.io/nxtools/xrepo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func main() {
	var (
		openLimit int64  = 10
		flagLevel        = zap.WarnLevel
		ctx              = context.Background()
		flagMode  string = "755"
		fileMode  os.FileMode
	)

	maxLimit, limErr := getFileLimit()
	if limErr == nil && maxLimit < openLimit {
		openLimit = maxLimit
	}

	if stat, err := os.Stat("."); err == nil {
		flagMode = fmt.Sprintf("%03o", uint32(stat.Mode()&0x1ff))
	}

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
	}

	for _, file := range flag.Args() {
		file := file
		wg.Go(func() error {
			_, err := dumper.processRepoData(ctx, file)
			return err
		})
	}

	if err := wg.Wait(); err != nil {
		logger.Fatal("Fatal error processing files", zap.Error(err))
	}
}

const (
	manPathPrefix     = "usr/share/man/man"
	manPathTrimPrefix = "usr/share/man/"
)

// TODO: Propagate list of created files up to caller so that they can be tracked relative as
// new files.

type Semaphore interface {
	Acquire(context.Context, int64) error
	Release(int64)
}

// Dumper processes packages and dumps manpage files to the current directory in the form manN/file.
type Dumper struct {
	DirMode os.FileMode
	Sema    Semaphore
}

func (d *Dumper) processRepoData(ctx context.Context, file string) (names []string, err error) {
	rd, err := d.readRepoData(ctx, file)
	if os.IsNotExist(err) {
		return nil, nil
	}

	results := make(chan []string, 1)
	wg, ctx := errgroup.WithContext(ctx)
	dir := filepath.Dir(file)
	index := rd.Index()
	for _, pkg := range index {
		pkg := pkg
		pkgfile := filepath.Join(dir, pkg.PackageVersion+"."+pkg.Architecture+".xbps")

		if err := d.Sema.Acquire(ctx, 2); err != nil {
			return nil, err
		}

		wg.Go(func() error {
			defer d.Sema.Release(2)

			pkgnames, err := d.processPackage(ctx, pkgfile)
			if err != nil {
				return err
			}

			select {
			case results <- pkgnames:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	for pkgnames := range results {
		names = append(names, pkgnames...)
	}

	return names, nil
}

func (d *Dumper) readRepoData(ctx context.Context, file string) (*xrepo.RepoData, error) {
	timer := Elapsed("elapsed")
	ctx = WithFields(ctx, logRepoData(file))

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
func (d *Dumper) processPackage(ctx context.Context, file string) (names []string, err error) {
	timer := Elapsed("elapsed")
	ctx = WithFields(ctx, logFile(file))

	Info(ctx, "Processing file")
	defer Info(ctx, "Finished processing file", timer())

	f, err := os.Open(file)
	if os.IsNotExist(err) {
		Warn(ctx, "File does not exist")
		return nil, nil
	} else if err != nil {
		Error(ctx, "Cannot open file", zap.Error(err))
		return nil, err
	}
	defer logClose(ctx, f)

	dec, err := xz.NewReader(f)
	if err != nil {
		Error(ctx, "Unable to create decompressor", zap.Error(err))
		return nil, err
	}

	tf := tar.NewReader(dec)

	for {
		hdr, err := tf.Next()
		if err == io.EOF {
			break
		} else if err != nil {
			Error(ctx, "Error encountered reading package", zap.Error(err))
			return names, err
		}

		dumped, err := d.processPackageFile(ctx, hdr, tf)
		if err != nil {
			Error(ctx, "Error processing package file", logPkgFile(hdr.Name), zap.Error(err))
			return names, err
		}

		if dumped {
			names = append(names, hdr.Name)
		}
	}

	return names, nil
}

// processPackageFile checks the tar header to see if the packaged file is a manpage and, if it is,
// extracts it. If the packaged file is a manpage symlink or link, it is ignored.
func (d *Dumper) processPackageFile(ctx context.Context, hdr *tar.Header, r io.Reader) (dumped bool, err error) {
	ctx = WithFields(ctx, logPkgFile(hdr.Name))

	switch hdr.Typeflag {
	case tar.TypeReg:
	case tar.TypeLink, tar.TypeSymlink:
		// TODO: Handle manpage symlinks at all?
		// return processManLink(ctx, hdr)
		return false, nil
	default:
		return false, nil
	}

	pkgfile := path.Clean(hdr.Name)
	if !strings.HasPrefix(pkgfile, manPathPrefix) {
		return false, nil
	}

	Debug(ctx, "Found manpage")

	relpath := strings.TrimPrefix(pkgfile, manPathTrimPrefix)
	relpath = filepath.FromSlash(relpath)
	reldir := filepath.Dir(relpath)

	ctx = WithFields(ctx, logDumpFile(relpath))

	if err = os.MkdirAll(reldir, d.DirMode); err != nil {
		Error(ctx, "Unable to create directory for manpage", zap.Error(err))
		return false, err
	}

	// TODO: Dump manpage to filesystem after stripping usr/share/ prefix
	f, err := os.Create(relpath)
	if err != nil {
		Error(ctx, "Unable to create dumped file")
		return false, err
	}
	defer logClose(ctx, f)

	if _, err := io.Copy(f, r); err != nil {
		Error(ctx, "Error copying pkgfile to dumpfile", zap.Error(err))
		return false, err
	}

	return true, nil
}

func logClose(ctx context.Context, c io.Closer) (err error) {
	if err = c.Close(); err != nil {
		Warn(ctx, "Encountered Close error", zap.Error(err))
	}
	return err
}
