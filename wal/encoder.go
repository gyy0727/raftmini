package wal

import (
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"github.com/gyy0727/raftmini/pkg/crc"
	"github.com/gyy0727/raftmini/pkg/ioutil"
	"github.com/gyy0727/raftmini/wal/walpb"
)

// *确保每次写入对齐到 SSD 物理页大小，防止部分写入（torn write）
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu        sync.Mutex         //*互斥锁
	bw        *ioutil.PageWriter //*页面对齐写入器
	crc       hash.Hash32        //*crc校验码
	buf       []byte             //*缓冲区
	uint64buf []byte             //*uint64buf
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:        ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc:       crc.New(prevCrc, crcTable),
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// *创建一个新的编码器（encoder），并将其与一个文件（*os.File）关联
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, os.SEEK_CUR)
	if err != nil {
		return nil, err
	}
	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	e.mu.Lock() //*并发保护
	defer e.mu.Unlock()

	e.crc.Write(rec.Data) //*写入crc校验
	rec.Crc = e.crc.Sum32()
	var (
		data []byte //*用于存储编码后的数据
		err  error  //*用于存储错误信息
		n    int
	)
	//*如果 rec 的大小超过了 e.buf 的长度（即 1MB），说明 e.buf 无法容纳 rec 的编码结果
	//*在这种情况下，直接调用 rec.Marshal()，它会动态分配一个新的字节切片来存储编码结果
	if rec.Size() > len(e.buf) {
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	lenField, padBytes := encodeFrameSize(len(data))
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}
	_, err = e.bw.Write(data)
	return err
}

// *计算数据帧的长度字段和填充字节数，确保数据帧按 8 字节对齐
func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	lenField = uint64(dataBytes)

	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		lenField |= uint64(0x80|padBytes) << 56
	}
	return
}

//*其实就是刷新底层的io流 
func (e *encoder) flush() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.bw.Flush()
}

func writeUint64(w io.Writer, n uint64, buf []byte) error {
	binary.LittleEndian.PutUint64(buf, n)
	_, err := w.Write(buf)
	return err
}
