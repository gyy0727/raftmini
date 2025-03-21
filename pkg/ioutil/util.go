package ioutil
import (
	"io"
	"os"
	"github.com/coreos/etcd/pkg/fileutil"
)

//*写入数据的同时刷新到磁盘 
func WriteAndSyncFile(filename string, data []byte, perm os.FileMode) error {
	f, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	n, err := f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = fileutil.Fsync(f)
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}
