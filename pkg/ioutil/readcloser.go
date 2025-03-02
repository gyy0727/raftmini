package ioutil

import (
	"fmt"
	"io"
)

type ReaderAndCloser struct {
	io.Reader //*读取器
	io.Closer //*关闭器
}

var (
	//*表示读取的字节数少于预期
	ErrShortRead = fmt.Errorf("ioutil: short read")
	//*表示读取的字节数超过了预期，应该遇到 EOF
	ErrExpectEOF = fmt.Errorf("ioutil: expect EOF")
)

// *totalBytes int64：期望读取的总字节数
func NewExactReadCloser(rc io.ReadCloser, totalBytes int64) io.ReadCloser {
	return &exactReadCloser{rc: rc, totalBytes: totalBytes}
}

type exactReadCloser struct {
	rc         io.ReadCloser
	br         int64 //*已读取的字节数（byte read）
	totalBytes int64 //*期望读取的总字节数
}

func (e *exactReadCloser) Read(p []byte) (int, error) {
	//*调用底层读取器读取数据到缓冲区 p
	n, err := e.rc.Read(p)
	//*更新已经读取的字节数 
	e.br += int64(n)
	//*超出期望读取的字节数 
	if e.br > e.totalBytes {
		return 0, ErrExpectEOF
	}
	//*不足期望读取的字节数 
	if e.br < e.totalBytes && n == 0 {
		return 0, ErrShortRead
	}
	return n, err
}

//*关闭读取并返回 错误
func (e *exactReadCloser) Close() error {
	if err := e.rc.Close(); err != nil {
		return err
	}
	if e.br < e.totalBytes {
		return ErrShortRead
	}
	return nil
}
