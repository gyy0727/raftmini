

package fileutil

import (
	"errors"
	"os"
)

var (
	ErrLocked = errors.New("fileutil: file already locked")
)

//*被锁定的文件 
type LockedFile struct{ *os.File }
