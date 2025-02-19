package raftmini

import (
	"errors"
	"sync"

	pb "github.com/gyy0727/raftmini/raftpb"
)

// *某个索引已经被快照保存时返回该错误
var ErrCompacted = errors.New("requested index is unavailable due to compaction")

// *当调用Storage.CreateSnapshot函数时传入的索引比当前的快照索引更小时返回
var ErrSnapOutOfDate = errors.New("requested index is older than the existing snapshot")

// *请求的log不可用时返回
var ErrUnavailable = errors.New("requested entry at index is unavailable")

// *快照暂时不可用时返回
var ErrSnapshotTemporarilyUnavailable = errors.New("snapshot is temporarily unavailable")

type Storage interface {
	//*返回保存的初始状态
	InitialState() (pb.HardState, pb.ConfState, error)

	//*返回索引范围在[lo,hi)之内并且不大于maxSize的entries数组
	Entries(lo, hi, maxSize uint64) ([]pb.Entry, error)

	//*传入一个索引值，返回这个索引值对应的任期号，如果不存在则error不为空，其中：
	//*ErrCompacted：表示传入的索引数据已经找不到，说明已经被压缩成快照数据了。
	//*ErrUnavailable：表示传入的索引值大于当前的最大索引
	Term(i uint64) (uint64, error)

	//*获得最后一条数据的索引值
	LastIndex() (uint64, error)

	//*返回第一条数据的索引值
	FirstIndex() (uint64, error)

	//*返回最新的快照数据
	Snapshot() (pb.Snapshot, error)
}

// *使用在内存中的数组来实现Storage接口的结构体
type MemoryStorage struct {
	//*互斥锁
	sync.Mutex
	//*硬状态,包括当前任期号，已经提交的索引号，已经投票的节点ID
	hardState pb.HardState
	//*快照数据
	snapshot pb.Snapshot
	//*日志数据
	ents []pb.Entry
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		//* 数组的第一条数据是一条dummy数据，为什么？？？,占位
		ents: make([]pb.Entry, 1),
	}
}

//*实现的storage接口
func (ms *MemoryStorage) InitialState() (pb.HardState, pb.ConfState, error) {
	return ms.hardState, ms.snapshot.Metadata.ConfState, nil
}

//*设置硬状态
func (ms *MemoryStorage) SetHardState(st pb.HardState) error {
	ms.Lock()
	defer ms.Unlock()
	ms.hardState = st
	return nil
}


func (ms *MemoryStorage)lastIndex()uint64{
	return ms.ents[0].Index+uint64(len(ms.ents))-1
}

func (ms* MemoryStorage)LastIndex()(uint64,error){
	ms.Lock()
	defer ms.Unlock()
	return ms.lastIndex(),nil
}


// Entries implements the Storage interface.
func (ms *MemoryStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	if lo <= offset {
		return nil, ErrCompacted
	}
	if hi > ms.lastIndex()+1 {
		raftLogger.Panicf("entries' hi(%d) is out of bound lastindex(%d)", hi, ms.lastIndex())
	}
	// only contains dummy entries.
	if len(ms.ents) == 1 {
		return nil, ErrUnavailable
	}

	ents := ms.ents[lo-offset : hi-offset]
	return limitSize(ents, maxSize), nil
}

// Term implements the Storage interface.
// 传入一个索引，返回它的term
func (ms *MemoryStorage) Term(i uint64) (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 如果比当前数据最小的索引还小，说明已经被compact过了
	if i < offset {
		return 0, ErrCompacted
	}
	// 如果超过当前数组大小，返回不可用
	if int(i-offset) >= len(ms.ents) {
		return 0, ErrUnavailable
	}
	return ms.ents[i-offset].Term, nil
}



// FirstIndex implements the Storage interface.
func (ms *MemoryStorage) FirstIndex() (uint64, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.firstIndex(), nil
}

func (ms *MemoryStorage) firstIndex() uint64 {
	return ms.ents[0].Index + 1
}

// Snapshot implements the Storage interface.
func (ms *MemoryStorage) Snapshot() (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	return ms.snapshot, nil
}

// ApplySnapshot overwrites the contents of this Storage object with
// those of the given snapshot.
// 使用快照数据进行数据还原
func (ms *MemoryStorage) ApplySnapshot(snap pb.Snapshot) error {
	ms.Lock()
	defer ms.Unlock()

	//handle check for old snapshot being applied
	msIndex := ms.snapshot.Metadata.Index
	snapIndex := snap.Metadata.Index
	if msIndex >= snapIndex {
		// 索引过期
		return ErrSnapOutOfDate
	}

	ms.snapshot = snap
	// 这里也插入了一条空数据
	ms.ents = []pb.Entry{{Term: snap.Metadata.Term, Index: snap.Metadata.Index}}
	return nil
}

// CreateSnapshot makes a snapshot which can be retrieved with Snapshot() and
// can be used to reconstruct the state at that point.
// If any configuration changes have been made since the last compaction,
// the result of the last ApplyConfChange must be passed in.
// 根据传入的数据创建快照并且返回
func (ms *MemoryStorage) CreateSnapshot(i uint64, cs *pb.ConfState, data []byte) (pb.Snapshot, error) {
	ms.Lock()
	defer ms.Unlock()
	// 检查传入的索引如果比已经有的快照索引更小，说明已经过时了
	if i <= ms.snapshot.Metadata.Index {
		return pb.Snapshot{}, ErrSnapOutOfDate
	}

	offset := ms.ents[0].Index
	// 传入的索引不能超过最大索引
	if i > ms.lastIndex() {
		raftLogger.Panicf("snapshot %d is out of bound lastindex(%d)", i, ms.lastIndex())
	}

	// 更新快照中的数据
	ms.snapshot.Metadata.Index = i
	ms.snapshot.Metadata.Term = ms.ents[i-offset].Term
	if cs != nil {
		ms.snapshot.Metadata.ConfState = *cs
	}
	ms.snapshot.Data = data
	return ms.snapshot, nil
}

// Compact discards all log entries prior to compactIndex.
// It is the application's responsibility to not attempt to compact an index
// greater than raftLog.applied.
// 数据压缩，将compactIndex之前的数据丢弃掉
func (ms *MemoryStorage) Compact(compactIndex uint64) error {
	ms.Lock()
	defer ms.Unlock()
	offset := ms.ents[0].Index
	// 小于当前索引，说明已经被压缩过了
	if compactIndex <= offset {
		return ErrCompacted
	}
	// 大于当前最大索引，panic
	if compactIndex > ms.lastIndex() {
		raftLogger.Panicf("compact %d is out of bound lastindex(%d)", compactIndex, ms.lastIndex())
	}

	i := compactIndex - offset
	ents := make([]pb.Entry, 1, 1+uint64(len(ms.ents))-i)
	// 这里也是先写一个空数据
	ents[0].Index = ms.ents[i].Index
	ents[0].Term = ms.ents[i].Term
	// 然后再append进来
	ents = append(ents, ms.ents[i+1:]...)
	ms.ents = ents
	return nil
}

// Append the new entries to storage.
// TODO (xiangli): ensure the entries are continuous and
// entries[0].Index > ms.entries[0].Index
// 添加数据
func (ms *MemoryStorage) Append(entries []pb.Entry) error {
	if len(entries) == 0 {
		return nil
	}

	ms.Lock()
	defer ms.Unlock()

	// 得到当前第一条数据的索引
	first := ms.firstIndex()
	// 得到传入的最后一条数据的索引
	last := entries[0].Index + uint64(len(entries)) - 1

	// shortcut if there is no new entry.
	// 检查合法性
	if last < first {
		return nil
	}

	// truncate compacted entries
	// 如果当前已经包含传入数据中的一部分，那么已经有的那部分数据可以不用重复添加进来
	if first > entries[0].Index {
		entries = entries[first-entries[0].Index:]
	}

	// 计算传入数据到当前已经保留数据的偏移量
	offset := entries[0].Index - ms.ents[0].Index
	switch {
	case uint64(len(ms.ents)) > offset:
		// 如果当前数据量大于偏移量，说明offset之前的数据从原有数据中取得，之后的数据从传入的数据中取得
		ms.ents = append([]pb.Entry{}, ms.ents[:offset]...)
		ms.ents = append(ms.ents, entries...)
	case uint64(len(ms.ents)) == offset:
		// offset刚好等于当前数据量，说明传入的数据刚好紧挨着当前的数据，所以直接添加进来就好了
		ms.ents = append(ms.ents, entries...)
	default:
		raftLogger.Panicf("missing log entry [last: %d, append at: %d]",
			ms.lastIndex(), entries[0].Index)
	}
	return nil
}























