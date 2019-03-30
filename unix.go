package main

import (
	"math"

	"golang.org/x/sys/unix"
)

func getFileLimit() (limit int64, err error) {
	var rlim unix.Rlimit
	if err = unix.Getrlimit(unix.RLIMIT_NOFILE, &rlim); err != nil {
		return 0, err
	}
	if rlim.Cur > math.MaxInt64 {
		return math.MaxInt64, nil
	}
	return int64(rlim.Cur), nil
}
