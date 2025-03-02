package wal

import (
	"hash"
	"sync"

	"github.com/coreos/etcd/pkg/ioutil"
)

// *确保每次写入对齐到 SSD 物理页大小，防止部分写入（torn write）
const walPageBytes = 8 * minSectorSize

type encoder struct {
	mu sync.Mutex
	bw *ioutil.PageWriter
	crc       hash.Hash32
	buf       []byte
	uint64buf []byte
}
