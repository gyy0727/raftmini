package wal

import (
	"errors"
	"fmt"
	"strings"

	"github.com/gyy0727/raftmini/pkg/fileutil"
	"go.uber.org/zap"
)

var (
	badWalName = errors.New("bad wal name") //*错误的wal文件名 
)

//*判断目录是否存在 
func Exist(dirpath string) bool {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return false
	}
	return len(names) != 0
}


func searchIndex(names []string, index uint64) (int, bool) {
	for i := len(names) - 1; i >= 0; i-- {
		name := names[i]
		_, curIndex, err := parseWalName(name)
		if err != nil {
			logger.Error("parse correct name should never fail: ",zap.Error(err) )
		}
		if index >= curIndex {
			return i, true
		}
	}
	return -1, false
}


func isValidSeq(names []string) bool {
	var lastSeq uint64
	for _, name := range names {
		curSeq, _, err := parseWalName(name)
		if err != nil {
			logger.Error("parse correct name should never fail: ", zap.Error(err))
		}
		if lastSeq != 0 && lastSeq != curSeq-1 {
			return false
		}
		lastSeq = curSeq
	}
	return true
}
func readWalNames(dirpath string) ([]string, error) {
	names, err := fileutil.ReadDir(dirpath)
	if err != nil {
		return nil, err
	}
	wnames := checkWalNames(names)
	if len(wnames) == 0 {
		return nil, ErrFileNotFound
	}
	return wnames, nil
}

func checkWalNames(names []string) []string {
	//*空字符串
	wnames := make([]string, 0)
	for _, name := range names {
		if _, _, err := parseWalName(name); err != nil {
			if !strings.HasSuffix(name, ".tmp") {
				logger.Warn("ignored file in wal ", zap.String("name",name))
			}
			continue
		}
		wnames = append(wnames, name)
	}
	return wnames
}

//*解析wal文件名获取序列号和索引 
func parseWalName(str string) (seq, index uint64, err error) {
	if !strings.HasSuffix(str, ".wal") {
		return 0, 0, badWalName
	}
	_, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)
	return seq, index, err
}

//*用过传入的序列号和索引,拼凑出一个wal文件名
func walName(seq, index uint64) string {
	return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
