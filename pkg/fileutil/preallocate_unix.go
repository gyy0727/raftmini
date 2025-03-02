package fileutil

import (
	"os"
	"syscall"
)

//*扩展文件大小
func preallocExtend(f *os.File, sizeInBytes int64) error {
	//*扩展文件大小
	err := syscall.Fallocate(int(f.Fd()), 0, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)

		//*错误是 ENOTSUP（不支持）或 EINTR（被中断）
		if ok && (errno == syscall.ENOTSUP || errno == syscall.EINTR) {
			return preallocExtendTrunc(f, sizeInBytes)
		}
	}
	return err
}

//*预分配文件空间，但不改变文件大小
func preallocFixed(f *os.File, sizeInBytes int64) error {
	//*调用 syscall.Fallocate，使用模式 1（保持文件大小）
	err := syscall.Fallocate(int(f.Fd()), 1, 0, sizeInBytes)
	if err != nil {
		errno, ok := err.(syscall.Errno)
		if ok && errno == syscall.ENOTSUP {
			return nil
		}
	}
	return err
}
