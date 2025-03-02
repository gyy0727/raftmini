package wal

import (
	"fmt"
	"os"
	"path"

	"github.com/coreos/etcd/pkg/fileutil"
)

// *实现了一个预分配磁盘空间的文件管道，专门用于高效管理WAL（预写式日志）的文件创建
type filePipeline struct {
	//*文件路径
	dir string
	//*预分配文件大小
	size int64
	//*已经生成的文件计数器
	count int
	//*传递文件的通道
	filec chan *fileutil.LockedFile
	//*错误传递的通道
	errc chan error
	//*关闭信号的通道
	donec chan struct{}
}

// *立即启动后台任务持续预生成文件
func newFilePipeline(dir string, fileSize int64) *filePipeline {
	fp := &filePipeline{
		dir:   dir,
		size:  fileSize,
		filec: make(chan *fileutil.LockedFile),
		errc:  make(chan error, 1),
		donec: make(chan struct{}),
	}
	go fp.run()
	return fp
}

// *通过 双通道监听机制 从预分配文件管道中安全获取一个已准备好的文件
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
	select {
	case f = <-fp.filec:
	case err = <-fp.errc:
	}
	return
}

// *关闭
func (fp *filePipeline) Close() error {
	close(fp.donec)
	return <-fp.errc
}

// *负责预分配文件
func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
	//*1. 生成轮换文件名（如 /wal/0.tmp 和 /wal/1.tmp 交替）
	fpath := path.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))
	//*2. 创建文件并加锁（互斥访问保障）
	if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
		return nil, err
	}
	//* 预分配磁盘空间（关键性能优化）
	if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
		plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
		f.Close()
		return nil, err
	}
	fp.count++
	return f, nil
}

func (fp *filePipeline) run() {
	//*函数退出时关闭错误通道（关键可靠性保障）
	defer close(fp.errc)
	for {
		//*预分配 
		f, err := fp.alloc()
		if err != nil {
			fp.errc <- err
			return
		}
		select {
			//*存入通道 
		case fp.filec <- f:
		case <-fp.donec:
			os.Remove(f.Name())
			f.Close()
			return
		}
	}
}
