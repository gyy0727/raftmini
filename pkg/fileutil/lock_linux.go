package fileutil

import (
	"os"
	"syscall"
)

const (
	F_OFD_GETLK  = 37 //*用于获取文件锁状态的常量
	F_OFD_SETLK  = 37 //*用于设置文件锁（非阻塞）的常量
	F_OFD_SETLKW = 38 //*用于设置文件锁（阻塞）的常量
)

var (
	//*文件写锁Flock_t 结构体
	wrlck = syscall.Flock_t{
		Type:   syscall.F_WRLCK,    //*写锁
		Whence: int16(os.SEEK_SET), //*从文件开头开始锁
		Start:  0,                  //*锁的起始偏移量，0 表示从文件开头
		Len:    0,                  //*锁的长度，0 表示锁定整个文件
	}

	linuxTryLockFile = flockTryLockFile
	linuxLockFile    = flockLockFile
)

func init() {
	//*读锁 
	getlk := syscall.Flock_t{Type: syscall.F_RDLCK}
	//*测试系统是否支持OFD锁 
	if err := syscall.FcntlFlock(0, F_OFD_GETLK, &getlk); err == nil {
		linuxTryLockFile = ofdTryLockFile
		linuxLockFile = ofdLockFile
	}
}

//*调用的是常量
func TryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return linuxTryLockFile(path, flag, perm)
}


func ofdTryLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	//*打开文件 
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}
	//*写锁 
	flock := wrlck
	//*对文件进行非阻塞锁定 
	if err = syscall.FcntlFlock(f.Fd(), F_OFD_SETLK, &flock); err != nil {
		f.Close()
		if err == syscall.EWOULDBLOCK {
			err = ErrLocked
		}
		return nil, err
	}
	return &LockedFile{f}, nil
}

func LockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	return linuxLockFile(path, flag, perm)
}

//*阻塞写锁 
func ofdLockFile(path string, flag int, perm os.FileMode) (*LockedFile, error) {
	f, err := os.OpenFile(path, flag, perm)
	if err != nil {
		return nil, err
	}

	flock := wrlck
	err = syscall.FcntlFlock(f.Fd(), F_OFD_SETLKW, &flock)

	if err != nil {
		f.Close()
		return nil, err
	}
	return &LockedFile{f}, err
}
