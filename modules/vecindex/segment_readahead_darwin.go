//go:build darwin

package vecindex

import (
	"os"
	"unsafe"

	"golang.org/x/sys/unix"
)

func segmentFileReadAhead(file *os.File, offset, length int64) {
	if file == nil || length <= 0 {
		return
	}
	if length > 2147483647 {
		length = 2147483647
	}
	advisory := unix.Radvisory_t{
		Offset: offset,
		Count:  int32(length),
	}
	_, _ = unix.FcntlInt(file.Fd(), unix.F_RDADVISE, int(uintptr(unsafe.Pointer(&advisory))))
}
