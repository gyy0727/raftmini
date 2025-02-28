package crc

import (
	"hash"
	"hash/crc32"
)

// *定义 CRC-32 校验和的字节长度（32位=4字节）
const Size = 4

// * digest 结构体实现 hash.Hash32 接口
type digest struct {
	crc uint32       //*crc校验和
	tab *crc32.Table //*CRC 多项式表（例如 IEEE 标准表）
}

// *新建一个支持增量计算的 CRC 实例
func New(prev uint32, tab *crc32.Table) hash.Hash32 {
	return &digest{prev, tab}
}

// * 实现 hash.Hash 接口的 Size 方法（返回校验和字节长度）
func (d *digest) Size() int {
	return Size
}

// *实现 hash.Hash 接口的 BlockSize 方法（返回数据块大小）
// *此处返回 1 表示按字节处理（无缓冲区块优化）
func (d *digest) BlockSize() int { return 1 }

// * Reset 方法重置计算状态到初始 CRC 值
func (d *digest) Reset() { d.crc = 0 }

// * Write 方法更新 CRC 值（实现 io.Writer 接口）
func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = crc32.Update(d.crc, d.tab, p)
	return len(p), nil
}

// *Sum32 方法返回当前 CRC 的 32 位值（hash.Hash32 接口要求）
func (d *digest) Sum32() uint32 { return d.crc }

// *Sum 方法将当前 CRC 值追加到输入字节切片（hash.Hash 接口要求）
func (d *digest) Sum(in []byte) []byte {
	s := d.Sum32()
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}
