package caches

import (
	"errors"
	"io"
	"os"
)

//MemFile for in mem cached files
type MemFile struct {
	offset    int64
	dirOffset int
	Content   []byte
}

func (f *MemFile) Read(p []byte) (n int, err error) {
	if len(f.Content)-int(f.offset) >= len(p) {
		n = len(p)
	} else {
		n = len(f.Content) - int(f.offset)
		err = io.EOF
	}
	copy(p, f.Content[f.offset:f.offset+int64(n)])
	f.offset += int64(n)
	return
}

var errWhence = errors.New("Seek: invalid whence")
var errOffset = errors.New("Seek: invalid offset")

//Seek exported for ServeContent call
func (f *MemFile) Seek(offset int64, whence int) (ret int64, err error) {
	switch whence {
	default:
		return 0, errWhence
	case os.SEEK_SET:
	case os.SEEK_CUR:
		offset += f.offset
	case os.SEEK_END:
		offset += int64(len(f.Content))
	}
	if offset < 0 || int(offset) > len(f.Content) {
		return 0, errOffset
	}
	f.offset = offset
	return f.offset, nil
}
