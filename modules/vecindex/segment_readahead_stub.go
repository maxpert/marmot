//go:build !linux && !darwin

package vecindex

import "os"

func segmentFileReadAhead(file *os.File, offset, length int64) {}
