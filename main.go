package main

import (
	"archive/tar"
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"strings"

	"github.com/ulikunitz/xz"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

func main() {
	var (
		openLimit int64 = 10
		flagLevel       = zap.WarnLevel
		ctx             = context.Background()
	)

	maxLimit, limErr := getFileLimit()
	if limErr == nil && maxLimit < openLimit {
		openLimit = maxLimit
	}

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

	// Check limit
	if openLimit <= 0 {
		logger.Fatal("Invalid limit -- must be >= 0", zap.Int64("limit", openLimit))
	} else if openLimit > maxLimit {
		logger.Fatal("Invalid limit -- must be <= nofiles", zap.Int64("nofiles", maxLimit), zap.Int64("limit", openLimit))
	}

	// Semaphore controls no. of open files via goroutines -- all acquisitions have a weight of 1
	sema := semaphore.NewWeighted(openLimit)
	wg, ctx := errgroup.WithContext(ctx)

	for _, file := range flag.Args() {
		file := file
		wg.Go(func() error {
			if err := sema.Acquire(ctx, 1); err != nil {
				return err
			}
			defer sema.Release(1)
			return processPackage(ctx, file)
		})
	}

	if err := wg.Wait(); err != nil {
		logger.Fatal("Fatal error processing files", zap.Error(err))
	}
}

const (
	manPathPrefix = "usr/share/man/man"
)

// TODO: Propagate list of created files up to caller so that they can be tracked relative as
// new files.

// processPackage processes an XBPS package and extracts all manpages under the current directory.
func processPackage(ctx context.Context, file string) error {
	timer := Elapsed("elapsed")
	ctx = WithFields(ctx, logFile(file))

	Info(ctx, "Processing file")
	defer Info(ctx, "Finished processing file", timer())

	f, err := os.Open(file)
	if err != nil {
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
			return nil
		} else if err != nil {
			Error(ctx, "Error encountered reading package", zap.Error(err))
			return err
		}

		if err := processPackageFile(ctx, hdr, tf); err != nil {
			Error(ctx, "Error processing package file", logPkgFile(hdr.Name), zap.Error(err))
			return err
		}
	}

	return nil
}

// processPackageFile checks the tar header to see if the packaged file is a manpage and, if it is,
// extracts it. If the packaged file is a manpage symlink or link, it is ignored.
func processPackageFile(ctx context.Context, hdr *tar.Header, r io.Reader) error {
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

	Debug(ctx, "Found manpage", logPkgFile(hdr.Name))

	// TODO: Dump manpage to filesystem after stripping usr/share/ prefix
	_, err := io.Copy(os.Stdout, r)

	return err
}

func logClose(ctx context.Context, c io.Closer) (err error) {
	if err = c.Close(); err != nil {
		Warn(ctx, "Encountered Close error", zap.Error(err))
	}
	return err
}
