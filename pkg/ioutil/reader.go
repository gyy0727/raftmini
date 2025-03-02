package ioutil

import "io"

func NewLimitedBufferReader(r io.Reader, n int) io.Reader {
	return &limitedBufferReader{
		r: r,
		n: n,
	}
}

//*通过存储底层读取器和限制大小，控制每次读取的行为
type limitedBufferReader struct {
	r io.Reader
	n int
}

//*无论传入的缓冲区大小如何,只会读取n个指定大小的字节 
func (r *limitedBufferReader) Read(p []byte) (n int, err error) {
	np := p
	if len(np) > r.n {
		np = np[:r.n]
	}
	return r.r.Read(np)
}
