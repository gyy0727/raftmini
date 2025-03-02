package fileutil

import (
	"os"
	"syscall"
)

// *文件数据和元数据
func Fsync(f *os.File) error {
	return f.Sync()
}

// *仅文件数据
func Fdatasync(f *os.File) error {
	return syscall.Fdatasync(int(f.Fd()))
}
