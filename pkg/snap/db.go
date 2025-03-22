package snap

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"

	"github.com/gyy0727/raftmini/pkg/fileutil"
	"go.uber.org/zap"
)

// *从给定的 io.Reader 中读取数据并保存为数据库快照文件
func (s *Snapshotter) SaveDBFrom(r io.Reader, id uint64) (int64, error) {
	f, err := ioutil.TempFile(s.dir, "tmp")
	if err != nil {
		return 0, err
	}
	var n int64
	n, err = io.Copy(f, r)
	if err == nil {
		err = fileutil.Fsync(f)
	}
	f.Close()
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}
	fn := path.Join(s.dir, fmt.Sprintf("%016x.snap.db", id))
	if fileutil.Exist(fn) {
		os.Remove(f.Name())
		return n, nil
	}
	err = os.Rename(f.Name(), fn)
	if err != nil {
		os.Remove(f.Name())
		return n, err
	}

	logger.Info("saved database snapshot to disk",
		zap.Int64("total_bytes", n),
	)

	return n, nil
}

//*查找数据库快照文件的路径并返回文件路径
func (s *Snapshotter) DBFilePath(id uint64) (string, error) {
	fns, err := fileutil.ReadDir(s.dir)
	if err != nil {
		return "", err
	}
	wfn := fmt.Sprintf("%016x.snap.db", id)
	for _, fn := range fns {
		if fn == wfn {
			return path.Join(s.dir, fn), nil
		}
	}
	return "", fmt.Errorf("snap: snapshot file doesn't exist")
}
