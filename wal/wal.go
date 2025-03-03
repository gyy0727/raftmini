package wal

import (
	"bytes"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"sync"
	"time"

	raft "github.com/gyy0727/raftmini"
	fu "github.com/gyy0727/raftmini/pkg/fileutil"
	"github.com/gyy0727/raftmini/pkg/pbutil"
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
	dir       string           //*存储 WAL 文件的目录路径
	dirFile   *os.File         //*指向 WAL 目录的文件描述符
	metadata  []byte           //*存储 WAL 的元数据
	state     raftpb.HardState //*存储 Raft 的硬状态（HardState）
	start     walpb.Snapshot   //*存储快照信息，表示从哪个快照开始读取日志
	decoder   *decoder         //*解码器，用于从 WAL 文件中读取并解码记录
	readClose func() error     //*关闭解码器的函数
	mu        sync.Mutex       //*互斥锁
	enti      uint64           //*存储最后一条日志的索引
	encoder   *encoder         //*编码器
	locks     []*fu.LockedFile //*存储 WAL 文件的锁定文件列表
	fp        *filePipeline    //*文件管道，用于管理 WAL 文件的分配和预分配
}

func Create(dirpath string, metadata []byte) (*WAL, error) {
	if Exist(dirpath) {
		return nil, os.ErrExist
	}
	//*创建一个临时目录路径，通过在 dirpath 后添加 .tmp 后缀
	tmpdirpath := path.Clean(dirpath) + ".tmp"
	//*如果已经存在临时目录,那就删除目录
	if fu.Exist(tmpdirpath) {
		if err := os.RemoveAll(tmpdirpath); err != nil {
			return nil, err
		}
	}
	//*创建临时目录。如果创建失败，返回错误
	if err := fu.CreateDirAll(tmpdirpath); err != nil {
		return nil, err
	}
	//*构造 WAL 文件的路径，walName(0, 0) 生成一个文件名
	p := path.Join(tmpdirpath, walName(0, 0))
	//*给文件添加写锁,并打开文件
	f, err := fu.LockFile(p, os.O_WRONLY|os.O_CREATE, fu.PrivateFileMode)
	if err != nil {
		return nil, err
	}
	//*将文件指针移动到文件末尾，以便后续写入
	if _, err = f.Seek(0, os.SEEK_END); err != nil {
		return nil, err
	}
	//*预分配文件空间，以确保文件有足够的空间来存储数据
	if err = fu.Preallocate(f.File, segmentSizeBytes, true); err != nil {
		return nil, err
	}
	w := &WAL{
		dir:      dirpath,
		metadata: metadata,
	}
	w.encoder, err = newFileEncoder(f.File, 0)
	if err != nil {
		return nil, err
	}
	w.locks = append(w.locks, f)
	if err = w.saveCrc(0); err != nil {
		return nil, err
	}
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
		return nil, err
	}
	if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
		return nil, err
	}

	if w, err = w.renameWal(tmpdirpath); err != nil {
		return nil, err
	}

	pdir, perr := fu.OpenDir(path.Dir(w.dir))
	if perr != nil {
		return nil, perr
	}
	if perr = fu.Fsync(pdir); perr != nil {
		return nil, perr
	}
	if perr = pdir.Close(); err != nil {
		return nil, perr
	}

	return w, nil
}

// *用于打开一个已经存在的 WAL（Write-Ahead Log）并准备读取其中的记录
func Open(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	w, err := openAtIndex(dirpath, snap, true)
	if err != nil {
		return nil, err
	}
	if w.dirFile, err = fu.OpenDir(w.dir); err != nil {
		return nil, err
	}
	return w, nil
}

// *用于以只读模式打开 WAL（Write-Ahead Log）文件
func OpenForRead(dirpath string, snap walpb.Snapshot) (*WAL, error) {
	return openAtIndex(dirpath, snap, false)
}

// *用于根据给定的快照（snap）和模式（write）打开 WAL（Write-Ahead Log）文件
func openAtIndex(dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
	//*调用 readWalNames 函数读取 WAL 目录中的所有文件名
	names, err := readWalNames(dirpath)
	if err != nil {
		return nil, err
	}
	//*调用 searchIndex 函数查找快照 snap 对应的文件索引 nameIndex
	nameIndex, ok := searchIndex(names, snap.Index)
	if !ok || !isValidSeq(names[nameIndex:]) {
		return nil, ErrFileNotFound
	}

	//*用于存储 io.ReadCloser 接口实例（可读取和关闭的文件）
	rcs := make([]io.ReadCloser, 0)
	//*用于存储 io.Reader 接口实例（可读取的文件）
	rs := make([]io.Reader, 0)
	//*用于存储 fileutil.LockedFile 实例（锁定的文件）
	ls := make([]*fu.LockedFile, 0)
	for _, name := range names[nameIndex:] {
		p := path.Join(dirpath, name)
		if write {
			l, err := fu.TryLockFile(p, os.O_RDWR, fu.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			ls = append(ls, l)
			rcs = append(rcs, l)
		} else {
			rf, err := os.OpenFile(p, os.O_RDONLY, fu.PrivateFileMode)
			if err != nil {
				closeAll(rcs...)
				return nil, err
			}
			ls = append(ls, nil)
			rcs = append(rcs, rf)
		}
		rs = append(rs, rcs[len(rcs)-1])
	}

	closer := func() error { return closeAll(rcs...) }

	w := &WAL{
		dir:       dirpath,
		start:     snap,
		decoder:   newDecoder(rs...),
		readClose: closer,
		locks:     ls,
	}

	if write {

		w.readClose = nil
		if _, _, err := parseWalName(path.Base(w.tail().Name())); err != nil {
			closer()
			return nil, err
		}
		w.fp = newFilePipeline(w.dir, segmentSizeBytes)
	}

	return w, nil
}

// *用于从 WAL（Write-Ahead Log）中读取所有记录
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := &walpb.Record{}
	decoder := w.decoder

	var match bool
	for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
		switch rec.Type {
		case entryType:
			e := mustUnmarshalEntry(rec.Data)
			if e.Index > w.start.Index {
				ents = append(ents[:e.Index-w.start.Index-1], e)
			}
			w.enti = e.Index
		case stateType:
			state = mustUnmarshalState(rec.Data)
		case metadataType:
			if metadata != nil && !bytes.Equal(metadata, rec.Data) {
				state.Reset()
				return nil, state, nil, ErrMetadataConflict
			}
			metadata = rec.Data
		case crcType:
			crc := decoder.crc.Sum32()
			if crc != 0 && rec.Validate(crc) != nil {
				state.Reset()
				return nil, state, nil, ErrCRCMismatch
			}
			decoder.updateCRC(rec.Crc)
		case snapshotType:
			var snap walpb.Snapshot
			pbutil.MustUnmarshal(&snap, rec.Data)
			if snap.Index == w.start.Index {
				if snap.Term != w.start.Term {
					state.Reset()
					return nil, state, nil, ErrSnapshotMismatch
				}
				match = true
			}
		default:
			state.Reset()
			return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
		}
	}
	//*检查当前wal文件是否为空
	switch w.tail() {
	case nil:

		if err != io.EOF && err != io.ErrUnexpectedEOF {
			state.Reset()
			return nil, state, nil, err
		}
	default:

		if err != io.EOF {
			state.Reset()
			return nil, state, nil, err
		}
		//*将文件指针移动到解码器的最后一个偏移量位置，以便继续读取
		if _, err = w.tail().Seek(w.decoder.lastOffset(), os.SEEK_SET); err != nil {
			return nil, state, nil, err
		}
		if err = fu.ZeroToEnd(w.tail().File); err != nil {
			return nil, state, nil, err
		}
	}

	err = nil
	if !match {
		err = ErrSnapshotNotFound
	}
	if w.readClose != nil {
		w.readClose()
		w.readClose = nil
	}
	w.start = walpb.Snapshot{}

	w.metadata = metadata

	if w.tail() != nil {

		w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
		if err != nil {
			return
		}
	}
	w.decoder = nil

	return metadata, state, ents, err
}

// *用于在 WAL（Write-Ahead Log）文件达到一定大小或需要分段时，创建一个新的段文件，并确保数据的一致性和完整性
func (w *WAL) cut() error {
	off, serr := w.tail().Seek(0, os.SEEK_CUR)
	if serr != nil {
		return serr
	}
	if err := w.tail().Truncate(off); err != nil {
		return err
	}
	if err := w.sync(); err != nil {
		return err
	}

	fpath := path.Join(w.dir, walName(w.seq()+1, w.enti+1))

	newTail, err := w.fp.Open()
	if err != nil {
		return err
	}

	w.locks = append(w.locks, newTail)
	prevCrc := w.encoder.crc.Sum32()
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}
	if err = w.saveCrc(prevCrc); err != nil {
		return err
	}
	if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
		return err
	}
	if err = w.saveState(&w.state); err != nil {
		return err
	}
	if err = w.sync(); err != nil {
		return err
	}

	off, err = w.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}

	if err = os.Rename(newTail.Name(), fpath); err != nil {
		return err
	}
	if err = fu.Fsync(w.dirFile); err != nil {
		return err
	}

	newTail.Close()

	if newTail, err = fu.LockFile(fpath, os.O_WRONLY, fu.PrivateFileMode); err != nil {
		return err
	}
	if _, err = newTail.Seek(off, os.SEEK_SET); err != nil {
		return err
	}

	w.locks[len(w.locks)-1] = newTail

	prevCrc = w.encoder.crc.Sum32()
	//*在 ReadAll 方法中，WAL 文件可能被读取到末尾，或者读取过程中发生了某些操作（如快照匹配、错误处理等）
	//*这些操作可能会影响编码器的状态，因此需要重置编码器以确保后续写入操作的正确性
	w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
	if err != nil {
		return err
	}
	logger.Info("segmented wal file is created", zap.String("fpath", fpath))
	return nil
}

func (w *WAL) sync() error {
	if w.encoder != nil {
		if err := w.encoder.flush(); err != nil {
			return err
		}
	}
	start := time.Now()
	err := fu.Fdatasync(w.tail().File)
	//*计算同步操作的耗时
	duration := time.Since(start)
	if duration > warnSyncDuration {
		logger.Warn("sync duration of %v, expected less than %v", zap.Duration("duration", duration), zap.Duration("warnSyncDuration", warnSyncDuration))

	}
	//*将同步耗时（以秒为单位）记录到指标系统（如 Prometheus）中，用于监控和分析
	syncDurations.Observe(duration.Seconds())

	return err
}

// *用于释放 WAL 文件中指定索引之前的锁
func (w *WAL) ReleaseLockTo(index uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	var smaller int
	found := false

	for i, l := range w.locks {
		_, lockIndex, err := parseWalName(path.Base(l.Name()))
		if err != nil {
			return err
		}
		if lockIndex >= index {
			smaller = i - 1
			found = true
			break
		}
	}

	if !found && len(w.locks) != 0 {
		smaller = len(w.locks) - 1
	}

	if smaller <= 0 {
		return nil
	}

	for i := 0; i < smaller; i++ {
		if w.locks[i] == nil {
			continue
		}
		w.locks[i].Close()
	}
	w.locks = w.locks[smaller:]

	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.fp != nil {
		w.fp.Close()
		w.fp = nil
	}

	if w.tail() != nil {
		if err := w.sync(); err != nil {
			return err
		}
	}
	for _, l := range w.locks {
		if l == nil {
			continue
		}
		if err := l.Close(); err != nil {
			logger.Error("failed to unlock during closing wal",
				zap.Error(err),
			)

		}
	}
	return w.dirFile.Close()
}

// *保存单条日志
func (w *WAL) saveEntry(e *raftpb.Entry) error {

	b := pbutil.MustMarshal(e)
	rec := &walpb.Record{Type: entryType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}
	w.enti = e.Index
	return nil
}

// *保存单个状态
func (w *WAL) saveState(s *raftpb.HardState) error {
	if raft.IsEmptyHardState(*s) {
		return nil
	}
	w.state = *s
	b := pbutil.MustMarshal(s)
	rec := &walpb.Record{Type: stateType, Data: b}
	return w.encoder.encode(rec)
}

// *批量保存日志和状态
func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if raft.IsEmptyHardState(st) && len(ents) == 0 {
		return nil
	}

	mustSync := mustSync(st, w.state, len(ents))

	for i := range ents {
		if err := w.saveEntry(&ents[i]); err != nil {
			return err
		}
	}
	if err := w.saveState(&st); err != nil {
		return err
	}

	curOff, err := w.tail().Seek(0, os.SEEK_CUR)
	if err != nil {
		return err
	}
	if curOff < segmentSizeBytes {
		if mustSync {
			return w.sync()
		}
		return nil
	}

	return w.cut()
}

// *用于将 Raft 快照（Snapshot）写入 WAL 文件
func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	b := pbutil.MustMarshal(&e)
	rec := &walpb.Record{Type: snapshotType, Data: b}
	if err := w.encoder.encode(rec); err != nil {
		return err
	}

	if w.enti < e.Index {
		w.enti = e.Index
	}
	return w.sync()
}

func (w *WAL) saveCrc(prevCrc uint32) error {
	return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

// *当前文件是否为空
func (w *WAL) tail() *fu.LockedFile {
	if len(w.locks) > 0 {
		return w.locks[len(w.locks)-1]
	}
	return nil
}

// *获取 WAL（Write-Ahead Log）文件尾部段（tail segment）的序列号
func (w *WAL) seq() uint64 {
	t := w.tail()
	if t == nil {
		return 0
	}
	seq, _, err := parseWalName(path.Base(t.Name()))
	if err != nil {
		logger.Fatal("bad wal name",
			zap.String("name", t.Name()),
			zap.Error(err),
		)

	}
	return seq
}

// *是否需要将数据同步到磁盘
func mustSync(st, prevst raftpb.HardState, entsnum int) bool {
	return entsnum != 0 || st.Vote != prevst.Vote || st.Term != prevst.Term
}

// *关闭多个 io.ReadCloser 对象
func closeAll(rcs ...io.ReadCloser) error {
	for _, f := range rcs {
		if err := f.Close(); err != nil {
			return err
		}
	}
	return nil
}
