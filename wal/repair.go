// *Repair 函数的主要功能是修复损坏的 WAL 文件，具体步骤如下：
// *打开最后一个 WAL 文件。
// *逐条解码记录，验证 CRC 校验和。
// *如果遇到意外文件结束，创建备份文件并截断原文件。
// *同步文件内容到磁盘。
// *返回修复结果。
package wal

import (
	"io"
	"os"
	"path"

	"github.com/gyy0727/raftmini/pkg/fileutil"
	"github.com/gyy0727/raftmini/wal/walpb"
	"go.uber.org/zap"
)

func Repair(dirpath string) bool {
	f, err := openLast(dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	rec := &walpb.Record{}
	decoder := newDecoder(f)
	for {
		lastOffset := decoder.lastOffset()
		err := decoder.decode(rec)
		switch err {
		case nil:

			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}
				decoder.updateCRC(rec.Crc)
			}
			continue
		case io.EOF:
			return true
		case io.ErrUnexpectedEOF:
			logger.Info("repairing file", zap.String("file", f.Name()))

			bf, bferr := os.Create(f.Name() + ".broken")
			if bferr != nil {
				logger.Error("could not repair file, failed to create backup file", zap.String("file", f.Name()))

				return false
			}
			defer bf.Close()

			if _, err = f.Seek(0, os.SEEK_SET); err != nil {
				logger.Error("could not repair file, failed to read file", zap.String("file", f.Name()))

				return false
			}

			if _, err = io.Copy(bf, f); err != nil {
				logger.Error("could not repair file, failed to copy file", zap.String("file", f.Name()))

				return false
			}

			if err = f.Truncate(int64(lastOffset)); err != nil {
				logger.Error("could not repair file, failed to truncate file", zap.String("file", f.Name()))

				return false
			}
			if err = fileutil.Fsync(f.File); err != nil {
				logger.Error("could not repair file, failed to sync file", zap.String("file", f.Name()))

				return false
			}
			return true
		default:
			logger.Error("could not repair error", zap.Error(err))

			return false
		}
	}
}

// *用于打开 WAL（Write-Ahead Log）目录中的最后一个文件，并返回一个加锁的文件对象
func openLast(dirpath string) (*fileutil.LockedFile, error) {
	names, err := readWalNames(dirpath)
	if err != nil {
		return nil, err
	}
	last := path.Join(dirpath, names[len(names)-1])
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
