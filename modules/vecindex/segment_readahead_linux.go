//go:build linux

package vecindex

import (
	"os"

	"golang.org/x/sys/unix"
)

func segmentFileReadAhead(file *os.File, offset, length int64) {
	if file == nil || length <= 0 {
		return
	}
	_ = unix.Fadvise(int(file.Fd()), offset, length, unix.FADV_WILLNEED)
}
