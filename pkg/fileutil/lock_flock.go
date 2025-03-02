package fileutil

import (
	"os"
	"syscall"
)

// *非阻塞尝试锁定文件。如果文件已被锁定，立即返回错误
func flockTryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	//*打开文件
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	//*使用 syscall.Flock 尝试对文件进行独占锁定（LOCK_EX），并设置为非阻塞模式（LOCK_NB）
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		//*表示文件已被锁定
		if err == syscall.EWOULDBLOCK {
			err = ErrLocked
		}
		return nil, err
	}
	return &LockedFile{f}, nil
}

//*阻塞式锁定文件 
func flockLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{f}, err
}
