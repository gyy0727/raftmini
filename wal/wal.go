package wal

import (
	"errors"
	"hash/crc32"
	"os"
	"sync"
	"time"

	"github.com/gyy0727/raftmini/pkg/fileutil"
	"github.com/gyy0727/raftmini/raftpb"
	"github.com/gyy0727/raftmini/wal/walpb"
	"go.uber.org/zap"
)

const (
	metadataType     int64              = iota + 1 //*元数据记录，存储集群的元信息
	entryType                                      //*日志条目记录
	stateType                                      //*状态记录
	crcType                                        //*crc校验记录
	snapshotType                                   //*快照记录
	segmentSizeBytes = 64 * 1000 * 1000            //*段大小
	//*如果 fsync 操作耗时超过 1 秒，则记录警告日志
	//*帮助监控和诊断性能问题，例如磁盘 I/O 性能下降
	warnSyncDuration = time.Second //*定义 fsync 操作的警告阈值
)

var (
	plog, _ = zap.NewDevelopment()
	//*元数据冲突，表示发现不一致的元数据
	ErrMetadataConflict = errors.New("wal: conflicting metadata found")
	//*文件未找到，表示指定的 WAL 文件不存在
	ErrFileNotFound = errors.New("wal: file not found")
	//*CRC 校验失败，表示数据完整性检查未通过
	ErrCRCMismatch = errors.New("wal: crc mismatch")
	//*快照不匹配，表示快照与预期不一致
	ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
	//*快照未找到，表示指定的快照不存在
	ErrSnapshotNotFound = errors.New("wal: snapshot not found")
	//*定义 CRC 校验表，用于计算数据的 CRC 校验值
	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

type WAL struct {
	dir       string                 //*存储 WAL 文件的目录路径
	dirFile   *os.File               //*指向 WAL 目录的文件描述符
	metadata  []byte                 //*存储 WAL 的元数据
	state     raftpb.HardState       //*存储 Raft 的硬状态（HardState）
	start     walpb.Snapshot         //*存储快照信息，表示从哪个快照开始读取日志
	decoder   *decoder               //*解码器，用于从 WAL 文件中读取并解码记录
	readClose func() error           //*关闭解码器的函数
	mu        sync.Mutex             //*互斥锁
	enti      uint64                 //*存储最后一条日志的索引
	encoder   *encoder               //*编码器
	locks     []*fileutil.LockedFile //*存储 WAL 文件的锁定文件列表
	fp        *filePipeline          //*文件管道，用于管理 WAL 文件的分配和预分配
}


func Create(dirpath string,metadata []byte)(*WAL,error){
	if Exist(dirpath){
		return nil,os.ErrExist
	}
}