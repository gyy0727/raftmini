package wal

import (
	"bufio"
	"encoding/binary"
	"hash"
	"io"
	"sync"

	"github.com/gyy0727/raftmini/pkg/crc"
	"github.com/gyy0727/raftmini/pkg/pbutil"
	"github.com/gyy0727/raftmini/raftpb"
	"github.com/gyy0727/raftmini/wal/walpb"
)

// *表示磁盘的最小扇区大小（常见值如 512 字节或 4096 字节）
const minSectorSize = 512

type decoder struct {
	mu           sync.Mutex      //*互斥锁
	brs          []*bufio.Reader //* 缓冲读取器列表（多文件支持）
	lastValidOff int64           //*最后有效记录偏移
	crc          hash.Hash32     //* CRC32校验器
}

func newDecoder(r ...io.Reader) *decoder {
	readers := make([]*bufio.Reader, len(r))
	for i := range r {
		//*将ioreader包装成带缓冲区的bufreader
		readers[i] = bufio.NewReader(r[i])
	}
	return &decoder{
		brs: readers,
		crc: crc.New(0, crcTable), //*CRC32校验器初始化
	}
}

func (d *decoder) decode(rec *walpb.Record) error {
	rec.Reset() //*避免里面有旧数据
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.decodeRecord(rec)
}

func (d *decoder) decodeRecord(rec *walpb.Record) error {
	if len(d.brs) == 0 {
		return io.EOF //*要解码的数据为空
	}
	l, err := readInt64(d.brs[0]) //*确定后续数据长度
	if err == io.EOF || (err == nil && l == 0) {
		d.brs = d.brs[1:] //*处理下一个文件
		if len(d.brs) == 0 {
			return io.EOF
		}
		d.lastValidOff = 0
		return d.decodeRecord(rec) //*递归处理下一个文件
	}
	if err != nil {
		return err
	}
	recBytes, padBytes := decodeFrameSize(l)
	data := make([]byte, recBytes+padBytes)
	if _, err = io.ReadFull(d.brs[0], data); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return err
	}
	if err := rec.Unmarshal(data[:recBytes]); err != nil {
		if d.isTornEntry(data) {
			return io.ErrUnexpectedEOF
		}
		return err
	}

	if rec.Type != crcType {
		d.crc.Write(rec.Data)
		if err := rec.Validate(d.crc.Sum32()); err != nil {
			if d.isTornEntry(data) {
				return io.ErrUnexpectedEOF
			}
			return err
		}
	}

	d.lastValidOff += recBytes + padBytes + 8
	return nil
}

// *bit 63 | bit 62-59 | bit 58-56 | bit 55-0
// *1/0  |   未使用  | 填充长度   | 有效数据长度
// *(有无填充)         (0-7)       (0-2^56-1)
// *该函数用于 解析二进制帧的长度字段，分离出有效数据长度（recBytes）和填充长度（padBytes）
func decodeFrameSize(lenField int64) (recBytes int64, padBytes int64) {
	//*从64位长度字段中提取低56位作为有效数据长度
	recBytes = int64(uint64(lenField) & ^(uint64(0xff) << 56)) //

	//*若长度字段为负数（最高位为1），表示存在填充
	if lenField < 0 { //*检查最高位（符号位）是否为1
		//*从高8位中提取低3位作为填充长度
		padBytes = int64((uint64(lenField) >> 56) & 0x7) //
	}
	return
}

// *判断 WAL 文件的最后一个日志条目是否因为“撕裂写”而部分写入并损坏
// *撕裂写是指在写入磁盘时，数据只写了一部分（例如由于断电或崩溃），导致数据不完整
func (d *decoder) isTornEntry(data []byte) bool {
	//*当前不是在处理最后一个文件
	if len(d.brs) != 1 {
		return false
	}
	//*文件其实偏移量,跳过八个字节
	fileOff := d.lastValidOff + 8
	//*当前偏移量
	curOff := 0
	//*二维字节切片，用于存储按扇区边界分割后的数据块
	chunks := [][]byte{}
	//*扇区大小分割数据
	for curOff < len(data) {
		//*计算当前文件偏移相对于扇区边界的余数
		chunkLen := int(minSectorSize - (fileOff % minSectorSize))
		//*计算到下一个扇区边界的距离，作为当前块的长度
		if chunkLen > len(data)-curOff {
			chunkLen = len(data) - curOff
		}
		//*如果剩余数据不足以填满一个块（chunkLen > len(data)-curOff），则调整为剩余数据长度
		chunks = append(chunks, data[curOff:curOff+chunkLen])
		fileOff += int64(chunkLen)
		curOff += chunkLen
	}

	//*如果 data 中某个扇区大小的块全为 0，说明该部分可能未成功写入，可能是撕裂写的结果
	for _, sect := range chunks {
		isZero := true
		for _, v := range sect {
			if v != 0 {
				isZero = false
				break
			}
		}
		if isZero {
			return true
		}
	}
	return false
}

//*根据传入的crc更新本地 
func (d *decoder) updateCRC(prevCrc uint32) {
	d.crc = crc.New(prevCrc, crcTable)
} 

//*返回当前校验码
func (d *decoder) lastCRC() uint32 {
	return d.crc.Sum32()
}

//*返回解码器记录的最后一个有效偏移量
func (d *decoder) lastOffset() int64 { return d.lastValidOff }

//*将字节切片反序列化为 Raft 日志条目
func mustUnmarshalEntry(d []byte) raftpb.Entry {
	var e raftpb.Entry
	pbutil.MustUnmarshal(&e, d)
	return e
}

//*将字节切片反序列化为 Raft 硬状态
func mustUnmarshalState(d []byte) raftpb.HardState {
	var s raftpb.HardState
	pbutil.MustUnmarshal(&s, d)
	return s
}

//*从输入流中读取一个 64 位整数
func readInt64(r io.Reader) (int64, error) {
	var n int64
	err := binary.Read(r, binary.LittleEndian, &n)
	return n, err
}
