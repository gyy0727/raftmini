package fileutil

import (
	"fmt"
	"os"
	"path"
	"sort"

	"go.uber.org/zap"
)

// *文件权限设置为 0600，表示只有文件拥有者有读写权限
const (
	//*文件权限设置为 0600，表示只有文件拥有者有读写权限
	PrivateFileMode = 0600
	//*表示只有目录拥有者有读写权限
	PrivateDirMode = 0700
)

var (
	logger, _ = zap.NewDevelopment()
)

// *检查目录 dir 是否可写
func IsDirWriteable(dir string) error {
	f := path.Join(dir, ".touch")

	if err := os.WriteFile(f, []byte(""), PrivateFileMode); err != nil {
		logger.Error("目录不可写:", zap.String("dir", dir))
		return err
	}
	//*检查完删除临时文件
	return os.Remove(f)
}

// *ReadDir 函数用于读取指定目录下的所有文件名，并将这些文件名按字母顺序排序后返回
func ReadDir(dirpath string) ([]string, error) {
	//*打开指定的目录
	dir, err := os.Open(dirpath)
	if err != nil {
		return nil, err
	}
	defer dir.Close()
	//*读取目录中的文件名
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	return names, nil
}

// *用于创建目录并确保该目录可写
func TouchDirAll(dir string) error {
	//*创建目录
	err := os.MkdirAll(dir, PrivateDirMode)
	if err != nil {
		return err
	}
	//*检查是否可写
	return IsDirWriteable(dir)
}

// *用于创建目录并确保该目录为空
func CreateDirAll(dir string) error {
	//*创建目录并保证可写
	err := TouchDirAll(dir)
	if err == nil {
		var ns []string
		//*读取目录下的所有文件名并排序返回
		ns, err = ReadDir(dir)
		if err != nil {
			return err
		}
		//*目录不为空
		if len(ns) != 0 {
			err = fmt.Errorf("expected %q to be empty, got %q", dir, ns)
			logger.Error("Directory is not empty", zap.String("dir", dir), zap.Strings("files", ns), zap.Error(err))
		}
	}
	return err
}

// *于检查指定路径的文件或目录是否存在
func Exist(name string) bool {
	_, err := os.Stat(name)
	return err == nil
}

// *用于将文件从当前偏移位置到文件末尾的内容清零，并确保文件的大小和磁盘块分配保持不变
func ZeroToEnd(f *os.File) error {
	//*获取当前偏移位置
	off, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	//*获取文件长度
	lenf, lerr := f.Seek(0, os.SEEK_END)
	if lerr != nil {
		return lerr
	}
	//*使用 f.Truncate(off) 将文件从当前偏移位置截断，相当于将文件从当前位置到末尾的内容清零
	if err = f.Truncate(off); err != nil {
		return err
	}
	//*调用 Preallocate 函数，确保文件的磁盘块分配不变
	if err = Preallocate(f, lenf, true); err != nil {
		return err
	}
	//*恢复偏移位置
	_, err = f.Seek(off, os.SEEK_SET)
	return err
}
