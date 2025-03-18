package raftmini

import (
	"fmt"
	"log"

	pb "github.com/gyy0727/raftmini/raftpb"
)

type raftLog struct {
	storage   Storage  //*用于保存自从最后一次snapshot之后的所有数据
	unstable  unstable //*用于保存没有持久化的数据,最终会保存到storage中
	committed uint64   //*已经提交的数据的最大索引
	applied   uint64   //*已经应用的数据的最大索引
	logger    Logger   //*日志输出器
}

// *根据传入的持久化日志建立一个新的raftlog
func newLog(storage Storage, logger Logger) *raftLog {
	if storage == nil {
		log.Panic("storage must not be nil")
	}
	log := &raftLog{
		storage: storage,
		logger:  logger,
	}
	firstIndex, err := storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	lastIndex, err := storage.LastIndex()
	if err != nil {
		panic(err)
	}
	//*offset从持久化之后的最后一个index的下一个开始
	log.unstable.offset = lastIndex + 1
	log.unstable.logger = logger
	//*committed和applied从持久化的第一个index的前一个开始
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	return log
}

// *字符串输出raftlog
func (l *raftLog) String() string {
	return fmt.Sprintf("committed=%d, applied=%d, unstable.offset=%d, len(unstable.Entries)=%d", l.committed, l.applied, l.unstable.offset, len(l.unstable.entries))
}

// *尝试添加一组日志，如果不能添加则返回(0,false)，否则返回(新的日志的索引,true)
// *index：从哪里开始的日志条目
// *logTerm：这一组日志对应的term
// *committed：leader上的committed索引
// *ents：需要提交的一组日志，因此这组数据的最大索引为index+len(ents)
func (l *raftLog) maybeAppend(index, logTerm, committed uint64, ents ...pb.Entry) (lastnewi uint64, ok bool) {
	if l.matchTerm(index, logTerm) {
		//*首先需要保证传入的index和logTerm能匹配的上才能走入这里，否则直接返回false
		//*首先得到传入数据的最后一条索引
		lastnewi = index + uint64(len(ents))
		//*查找传入的数据从哪里开始找不到对应的Term了
		ci := l.findConflict(ents)
		switch {
		case ci == 0:
			//*没有冲突，忽略
		case ci <= l.committed:
			//*找到的数据索引小于committed，说明传入的数据是错误的
			l.logger.Panicf("entry %d conflict with committed entry [committed(%d)]", ci, l.committed)
		default:
			//*ci > 0的情况下来到这里
			offset := index + 1
			//*从查找到的数据索引开始，将这之后的数据放入到unstable存储中
			l.append(ents[ci-offset:]...)
		}
		//*选择committed和lastnewi中的最小者进行commit
		l.commitTo(min(committed, lastnewi))
		return lastnewi, true
	}
	return 0, false
}

// *添加数据到未持久化的unstable数据中，返回最后一条日志的索引
func (l *raftLog) append(ents ...pb.Entry) uint64 {
	//*没有数据，直接返回最后一条日志索引
	if len(ents) == 0 {
		return l.lastIndex()
	}
	//*如果索引小于committed，则说明该数据是非法的
	if after := ents[0].Index - 1; after < l.committed {
		l.logger.Panicf("after(%d) is out of range [committed(%d)]", after, l.committed)
	}
	//*放入unstable存储中
	l.unstable.truncateAndAppend(ents)
	return l.lastIndex()
}

// *返回第一个在entry数组中，index中的term与当前存储的数据不同的索引
// *如果没有冲突数据，而且当前存在的日志条目包含所有传入的日志条目，返回0；
// *如果没有冲突数据，而且传入的日志条目有新的数据，则返回新日志条目的第一条索引
// *一个日志条目在其索引值对应的term与当前相同索引的term不相同时认为是有冲突的数据。
func (l *raftLog) findConflict(ents []pb.Entry) uint64 {
	//*遍历传入的ents数组
	for _, ne := range ents {
		//*找到第一个任期号不匹配的，即当前在raftLog存储的该索引数据的任期号，不是ent数据的任期号
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				//*如果不匹配任期号的索引数据，小于当前最后一条日志索引，就打印错误日志
				l.logger.Infof("found conflict at index %d [existing term: %d, conflicting term: %d]",
					ne.Index, l.zeroTermOnErrCompacted(l.term(ne.Index)), ne.Term)
			}
			//*返回第一条任期号与索引号不匹配的数据索引
			return ne.Index
		}
	}
	return 0
}

// *返回unstable存储的数据
func (l *raftLog) unstableEntries() []pb.Entry {
	if len(l.unstable.entries) == 0 {
		return nil
	}
	return l.unstable.entries
}

// *返回commit但是还没有apply的所有数据
func (l *raftLog) nextEnts() (ents []pb.Entry) {
	//*首先得到applied和firstindex的最大值
	off := max(l.applied+1, l.firstIndex())
	if l.committed+1 > off { //*如果commit索引比前面得到的值还大，说明还有没有commit了但是还没apply的数据，将这些数据返回
		ents, err := l.slice(off, l.committed+1, noLimit)
		if err != nil {
			l.logger.Panicf("unexpected error when getting unapplied entries (%v)", err)
		}
		return ents
	}
	return nil
}

// *hasNextEnts returns if there is any available entries for execution. This
// *is a fast check without heavy raftLog.slice() in raftLog.nextEnts().
// *这个函数的功能跟前面nextEnts类似，只不过这个函数做判断而不返回实际数据
func (l *raftLog) hasNextEnts() bool {
	off := max(l.applied+1, l.firstIndex())
	return l.committed+1 > off
}

// *返回快照数据
func (l *raftLog) snapshot() (pb.Snapshot, error) {
	if l.unstable.snapshot != nil {
		//*如果没有保存的数据有快照，就返回
		return *l.unstable.snapshot, nil
	}
	//*否则返回持久化存储的快照数据
	return l.storage.Snapshot()
}

func (l *raftLog) firstIndex() uint64 {
	//*首先尝试在未持久化数据中看有没有快照数据
	//*快照数据一定是已经提交到状态机的
	if i, ok := l.unstable.maybeFirstIndex(); ok {
		return i
	}
	//*否则才返回持久化数据的firsttIndex
	index, err := l.storage.FirstIndex()
	if err != nil {
		panic(err)
	}
	return index
}

func (l *raftLog) lastIndex() uint64 {
	//*如果有未持久化的日志，返回未持久化日志的最后索引
	if i, ok := l.unstable.maybeLastIndex(); ok {
		return i
	}
	//*否则返回持久化日志的最后索引
	i, err := l.storage.LastIndex()
	if err != nil {
		panic(err)
	}
	return i
}

// *将raftlog的commit索引，修改为tocommit
func (l *raftLog) commitTo(tocommit uint64) {
	//*never decrease commit
	//*首先需要判断，commit索引绝不能变小
	if l.committed < tocommit {
		if l.lastIndex() < tocommit {
			//*传入的值如果比lastIndex大则是非法的
			l.logger.Panicf("tocommit(%d) is out of range [lastIndex(%d)]. Was the raft log corrupted, truncated, or lost?", tocommit, l.lastIndex())
		}
		l.committed = tocommit
		l.logger.Infof("commit to %d", tocommit)
	}
}

// *修改applied索引
func (l *raftLog) appliedTo(i uint64) {
	if i == 0 {
		return
	}
	//*判断合法性
	//*新的applied ID既不能比committed大，也不能比当前的applied索引小
	if l.committed < i || i < l.applied {
		l.logger.Panicf("applied(%d) is out of range [prevApplied(%d), committed(%d)]", i, l.applied, l.committed)
	}
	l.applied = i
}

// *传入数据索引，该索引表示在这个索引之前的数据应用层都进行了持久化，修改unstable的数据
func (l *raftLog) stableTo(i, t uint64) { l.unstable.stableTo(i, t) }

// *传入数据索引，该索引表示在这个索引之前的数据应用层都进行了持久化，修改unstable的快照数据
func (l *raftLog) stableSnapTo(i uint64) { l.unstable.stableSnapTo(i) }

// *返回最后一个索引的term
func (l *raftLog) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panicf("unexpected error when getting the last term (%v)", err)
	}
	return t
}

// *返回index为i的term
func (l *raftLog) term(i uint64) (uint64, error) {
	//*the valid term range is [index of dummy entry, last index]
	dummyIndex := l.firstIndex() - 1
	//*先判断范围是否正确
	if i < dummyIndex || i > l.lastIndex() {

		return 0, nil
	}

	//*尝试从unstable中查询term
	if t, ok := l.unstable.maybeTerm(i); ok {
		return t, nil
	}

	//*尝试从storage中查询term
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	//*只有这两种错可以接受
	if err == ErrCompacted || err == ErrUnavailable {
		return 0, err
	}
	panic(err)
}

// *获取从i开始的entries返回，大小不超过maxsize
func (l *raftLog) entries(i, maxsize uint64) ([]pb.Entry, error) {
	if i > l.lastIndex() {
		return nil, nil
	}
	return l.slice(i, l.lastIndex()+1, maxsize)
}

// *allEntries returns all entries in the log.
func (l *raftLog) allEntries() []pb.Entry {
	ents, err := l.entries(l.firstIndex(), noLimit)
	if err == nil {
		return ents
	}
	if err == ErrCompacted { //*try again if there was a racing compaction
		return l.allEntries()
	}

	panic(err)
}

// **判断是否比当前节点的日志更新：1）term是否更大 2）term相同的情况下，索引是否更大
func (l *raftLog) isUpToDate(lasti, term uint64) bool {
	return term > l.lastTerm() || (term == l.lastTerm() && lasti >= l.lastIndex())
}

// **判断索引i的term是否和term一致
func (l *raftLog) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *raftLog) maybeCommit(maxIndex, term uint64) bool {
	//*只有在传入的index大于当前commit索引，以及maxIndex对应的term与传入的term匹配时，才使用这些数据进行commit
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

// *使用快照数据进行恢复
func (l *raftLog) restore(s pb.Snapshot) {
	l.logger.Infof("log [%s] starts to restore snapshot [index: %d, term: %d]", l, s.Metadata.Index, s.Metadata.Term)
	l.committed = s.Metadata.Index
	l.unstable.restore(s)
}

// *slice returns a slice of log entries from lo through hi-1, inclusive.
// *返回[lo,hi-1]之间的数据，这些数据的大小总和不超过maxSize
func (l *raftLog) slice(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo < l.unstable.offset {
		//*lo 小于unstable的offset，说明前半部分在持久化的storage中

		//*注意传入storage.Entries的hi参数取hi和unstable offset的较小值
		storedEnts, err := l.storage.Entries(lo, min(hi, l.unstable.offset), maxSize)
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			l.logger.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.unstable.offset))
		} else if err != nil {
			panic(err)
		}

		//*check if ents has reached the size limitation
		if uint64(len(storedEnts)) < min(hi, l.unstable.offset)-lo {
			return storedEnts, nil
		}

		ents = storedEnts
	}

	if hi > l.unstable.offset {
		//*hi大于unstable offset，说明后半部分在unstable中取得
		unstable := l.unstable.slice(max(lo, l.unstable.offset), hi)
		if len(ents) > 0 {
			ents = append([]pb.Entry{}, ents...)
			ents = append(ents, unstable...)
		} else {
			ents = unstable
		}
	}
	return limitSize(ents, maxSize), nil
}

// *判断传入的lo，hi是否超过范围了
// *l.firstIndex <= lo <= hi <= l.firstIndex + len(l.entries)
func (l *raftLog) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panicf("invalid slice %d > %d", lo, hi)
	}
	fi := l.firstIndex()
	if lo < fi {
		return ErrCompacted
	}

	length := l.lastIndex() + 1 - fi
	if lo < fi || hi > fi+length {
		l.logger.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, fi, l.lastIndex())
	}
	return nil
}

// *如果传入的err是nil，则返回t；如果是ErrCompacted则返回0，其他情况都panic
func (l *raftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	l.logger.Panicf("unexpected error (%v)", err)
	return 0
}
