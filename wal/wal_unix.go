package wal

import (
	"os"

	"github.com/coreos/etcd/pkg/fileutil"
)

// *用于将临时 WAL（Write-Ahead Log）目录重命名为正式目录，并完成 WAL 的初始化
// *在 WAL 初始化过程中，为了避免部分写入或损坏的 WAL 文件影响系统
// *通常会先将 WAL 文件写入临时目录，待所有操作完成后，再将临时目录重命名为正式目录
func (w *WAL) renameWal(tmpdirpath string) (*WAL, error) {

	if err := os.RemoveAll(w.dir); err != nil {
		return nil, err
	}
	if err := os.Rename(tmpdirpath, w.dir); err != nil {
		return nil, err
	}

	w.fp = newFilePipeline(w.dir, segmentSizeBytes)
	df, err := fileutil.OpenDir(w.dir)
	w.dirFile = df
	return w, err
}
