package fileutil

//*定期清理文件

import (
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"go.uber.org/zap"
)

func PurgeFile(dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}) <-chan error {
	return purgeFile(dirname, suffix, max, interval, stop, nil)
}

func purgeFile(dirname string, suffix string, max uint, interval time.Duration, stop <-chan struct{}, purgec chan<- string) <-chan error {
	errC := make(chan error, 1)
	//*启动单独的协程处理
	go func() {
		for {
			//*读取目录下的所有文件名
			fnames, err := ReadDir(dirname)
			if err != nil {
				errC <- err
				return
			}
			newfnames := make([]string, 0)
			//*使用 strings.HasSuffix 函数检查当前文件名 fname 是否以 suffix 结尾
			for _, fname := range fnames {
				if strings.HasSuffix(fname, suffix) {
					newfnames = append(newfnames, fname)
				}
			}
			sort.Strings(newfnames)
			fnames = newfnames
			for len(newfnames) > int(max) {
				//*拼凑出完整路径+文件名
				f := path.Join(dirname, newfnames[0])
				//*上锁 
				l, err := TryLockFile(f, os.O_WRONLY, PrivateFileMode)
				if err != nil {
					break
				}
				//*删除文件 
				if err = os.Remove(f); err != nil {
					errC <- err
					return
				}
				if err = l.Close(); err != nil {
					zap.L().Error("error unlocking file when purging file",
						zap.String("file", l.Name()),
						zap.Error(err),
					)
					errC <- err
					return
				}
				zap.L().Info("purged file successfully",
					zap.String("file", f),
				)
				//* 更新切片
				newfnames = newfnames[1:]
			}
			//*这段代码的作用是 将需要清理的文件名发送到 purgec 通道
			if purgec != nil {
				for i := 0; i < len(fnames)-len(newfnames); i++ {
					purgec <- fnames[i] 
				}
			}
			select {
			case <-time.After(interval):
				//*用于定时清理
			case <-stop:
				return
			}
		}
	}()
	return errC
}
