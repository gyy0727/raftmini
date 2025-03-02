package ioutil

import (
	"io"
)

// *默认缓冲区大小
var defaultBufferBytes = 128 * 1024

// *PageWriter 实现了 io.Writer 接口，确保写入按页面大小对齐，或通过刷新（flush）完成
type PageWriter struct {
	//*底层写入目标（如文件、网络等
	w io.Writer
	//*缓冲区起始位置相对于页面边界的偏移量
	pageOffset int
	//*每页的字节数（由用户指定）
	pageBytes int
	//*缓冲区中当前已缓存但未写入的字节数
	bufferedBytes int
	//*实际的缓冲区，存储待写入的数据
	buf []byte
	//*缓冲区的水位线（上限），超过此值需要刷新
	bufWatermarkBytes int
}

//*新建 
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

//*写入数据
func (pw *PageWriter) Write(p []byte) (n int, err error) {
	//*没有达到水位线 
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		//*将数据复制到缓冲区,从当前偏移量开始
		copy(pw.buf[pw.bufferedBytes:], p)
		//*调整缓冲区的字节数 
		pw.bufferedBytes += len(p)
		//*返回成功写入的数据大小 
		return len(p), nil
	}
	//*超出了水位线 
	//*计算距离下一个页面边界还需多少字节
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	//*如果不是恰好处在页面边界 
	if slack != pw.pageBytes {
		//*检查数据是否足够填满 slack
		partial := slack > len(p)
		if partial {
			slack = len(p)
		}
		//*将数据的前 slack 字节复制到缓冲区
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]
		if partial {
			return n, nil
		}
	}
	//*将缓冲区内容写入底层，确保当前是页面对齐的
	if err = pw.Flush(); err != nil {
		return n, err
	}
	//*如果剩余数据超过一个页面
	if len(p) > pw.pageBytes {
		//*计算完整页面数
		pages := len(p) / pw.pageBytes
		//*直接将完整页面写入底层（不经过缓冲区）
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}
		p = p[pages*pw.pageBytes:]
	}
	//*递归写入剩余尾部数据
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

func (pw *PageWriter) Flush() error {
	//*不需要刷新,没有数据要刷新 
	if pw.bufferedBytes == 0 {
		return nil
	}
	//*将缓冲区的所有数据写入 
	_, err := pw.w.Write(pw.buf[:pw.bufferedBytes])
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes
	pw.bufferedBytes = 0
	return err
}
