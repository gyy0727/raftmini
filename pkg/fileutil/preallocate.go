package fileutil

import "os"


func Preallocate(f *os.File, sizeInBytes int64, extendFile bool) error {
	if extendFile {
		return preallocExtend(f, sizeInBytes)
	}
	return preallocFixed(f, sizeInBytes)
}
//*通过截断文件的方式扩展文件大小
func preallocExtendTrunc(f *os.File, sizeInBytes int64) error {
	curOff, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	size, err := f.Seek(sizeInBytes, os.SEEK_END)
	if err != nil {
		return err
	}
	if _, err = f.Seek(curOff, os.SEEK_SET); err != nil {
		return err
	}
	if sizeInBytes > size {
		return nil
	}
	return f.Truncate(sizeInBytes)
}
