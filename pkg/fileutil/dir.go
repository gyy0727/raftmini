package fileutil

import "os"

//*打开对应的路径,返回文件指针 
func OpenDir(path string) (*os.File, error) { return os.Open(path) }