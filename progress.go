package raftmini

import "fmt"

type ProgressStateType uint64

const (
	//*探测状态用于检查该节点的网络状态,只有在应答leader的app信息之后，才会从probe切换到replicate状态
	//*在probe状态下，leader一次只能向这个节点发送一条app信息，在应答这条app信息之前，不能向这个节点继续发送其他app信息。
	//*在网络不可用，或者拒绝了某条app信息之后，都会切换到这个状态
	//*这个状态也是节点的初始状态
	ProgressStateProbe     ProgressStateType = iota //*探测状态
	ProgressStateReplicate                          //*复制状态
	ProgressStateSnapshot                           //*快照状态
)

// **字符串表示
var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}

// **返回对应的字符串表示
func (st ProgressStateType) String() string { return prstmap[uint64(st)] }

// **用于在leader中保存每个follower的状态信息，leader将根据这些信息决定发送给节点的日志
type Progress struct {
	//*Next保存的是下一次leader发送append消息时传送过来的日志索引
	//*当选举出新的leader时，首先初始化Next为该leader最后一条日志+1
	//*如果向该节点append日志失败，则递减Next回退日志，一直回退到索引匹配为止
	//*Match保存在该节点上保存的日志的最大索引，初始化为0
	//*正常情况下，Next = Match + 1
	//*以下情况下不是上面这种情况：
	//*1. 切换到Probe状态时，如果上一个状态是Snapshot状态，即正在接收快照，那么Next = max(pr.Match+1, pendingSnapshot+1)
	//*2. 当该follower不在Replicate状态时，说明不是正常的接收副本状态。
	//*   此时当leader与follower同步leader上的日志时，可能出现覆盖的情况，即此时follower上面假设Match为3，但是索引为3的数据会被
	//*   leader覆盖，此时Next指针可能会一直回溯到与leader上日志匹配的位置，再开始正常同步日志，此时也会出现Next != Match + 1的情况出现
	//! Match follower 已经确认复制的最高日志索引
	//! Next leader 将要发送给 follower 的下一个日志索引
	Match, Next uint64
	//*ProgressStateProbe：在每次heartbeat消息间隔期最多发一条同步日志消息给该节点
	//*ProgressStateReplicate：正常的接受副本数据状态。当处于该状态时，leader在发送副本消息之后，
	//*就修改该节点的next索引为发送消息的最大索引+1
	//*ProgressStateSnapshot：接收快照状态
	State ProgressStateType
	//*用于保存该节点的最后一次活动时间
	//*在状态切换到Probe状态以后，该follower就标记为Paused，此时将暂停同步日志到该节点
	Paused bool
	//*如果向该节点发送快照消息，PendingSnapshot用于保存快照消息的索引
	//*当PendingSnapshot不为0时，该节点也被标记为暂停状态。
	//*raft只有在这个正在进行中的快照同步失败以后，才会重传快照消息
	PendingSnapshot uint64
	//*RecentActive 如果进度最近是活跃的则为真。接收到来自相应 follower 的任何消息
	//*都表明该进度是活跃的。
	//*RecentActive 可以在选举超时后被重置为 false。
	RecentActive bool
	//*用于实现滑动窗口，用来做流量控制
	ins *inflights
}

// *重置状态
func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

// *修改为probe状态
func (pr *Progress) becomeProbe() {

	if pr.State == ProgressStateSnapshot {
		//*如果当前状态是接受快照状态，那么可以知道该节点已经成功接受处理了该快照，此时修改next索引需要根据max和快照索引来判断
		pendingSnapshot := pr.PendingSnapshot
		pr.resetState(ProgressStateProbe)
		//*取两者的最大值+1
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

// *转换成复制状态
func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

// *转换成快照复制状态
func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// *收到appresp的成功应答之后，leader更新节点的索引数据
// *如果传入的n小于等于当前的match索引，则索引就不会更新，返回false；否则更新索引返回true
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool
	if pr.Match < n {
		pr.Match = n
		updated = true
		pr.resume()
	}
	if pr.Next < n+1 {
		pr.Next = n + 1
	}
	return updated
}

// *更新Next索引到n+1
func (pr *Progress) optimisticUpdate(n uint64) { pr.Next = n + 1 }

// *maybeDecrTo函数在传入的索引不在范围内的情况下返回false
// *否则将把该节点的index减少到min(rejected,last)然后返回true
// *rejected是被拒绝的append消息的索引，last是拒绝该消息的节点的最后一条日志索引
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {
		//*如果当前在接收副本状态
		if rejected <= pr.Match {
			//*如果 rejected 小于等于 Match，说明 follower 已经确认复制了这些日志，拒绝的消息是过时的，不需要处理
			//*此时不需要回退索引，返回false
			return false
		}
		//*否则直接修改next到match+1
		pr.Next = pr.Match + 1
		return true
	}

	//*以下都不是接收副本状态的情况
	//*为什么这里不是对比Match？因为Next涉及到下一次给该Follower发送什么日志，
	//*所以这里对比和下面修改的是Next索引
	//*Match只表示该节点上存放的最大日志索引，而当leader发生变化时，可能会覆盖一些日志
	if pr.Next-1 != rejected {
		//*这种情况说明返回的情况已经过期，不需要回退索引，返回false
		return false
	}

	//*到了这里就回退Next为两者的较小值
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}
	pr.resume()
	return true
}

// *将节点状态设置为pause
func (pr *Progress) pause() { pr.Paused = true }

// * resume将节点状态设置为非pause
func (pr *Progress) resume() { pr.Paused = false }

// *一个节点当前处于暂停状态原因有几个：
// *1)拒绝了最近的append消息(ProgressStateProbe状态)
// *2)接受snapshot状态中(ProgressStateSnapshot状态)
// *3)已经达到了需要限流的程度(ProgressStateReplicate状态且滑动窗口已满)
// *ispause在以下情况中返回true：
// *1）等待接收快照
// *2) inflight数组已满，需要进行限流
// *3) 进入ProgressStateProbe状态，这种状态说明最近拒绝了msgapps消息
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
		return pr.Paused
	case ProgressStateReplicate:
		//*如果在replicate状态，pause与否取决于infilght数组是否满了
		return pr.ins.full()
	case ProgressStateSnapshot:
		//*处理快照时一定是paused的
		return true
	default:
		panic("unexpected state")
	}
}

// *标记快照发送失败，重置 PendingSnapshot
func (pr *Progress) snapshotFailure() { pr.PendingSnapshot = 0 }

// *可以中断快照的情况：当前为接收快照，同时match已经大于等于快照索引
// *因为match已经大于快照索引了，所以这部分快照数据可以不接收了，也就是可以被中断的快照操作
// *因为在节点落后leader数据很多的情况下，可能leader会多次通过snapshot同步数据给节点，
// *而当 pr.Match >= pr.PendingSnapshot的时候，说明通过快照来同步数据的流程完成了，这时可以进入正常的接收同步数据状态了。
func (pr *Progress) needSnapshotAbort() bool {
	return pr.State == ProgressStateSnapshot && pr.Match >= pr.PendingSnapshot
}

// *字符串输出当前的进度信息
func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}

// **该数据结构用于在leader中保存每个follower的状态信息，leader将根据这些信息决定发送给节点的日志
// *用于流量窗口的控制
type inflights struct {

	//*数组中的起始索引
	start int
	//*数组中的数据量
	count int

	//*数组大小
	size int

	//*缓冲区
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// **向inflights中添加inflight
func (in *inflights) add(inflight uint64) {
	if in.full() {
		panic("cannot add into a full inflights")
	}
	//*计算inflights中的索引
	next := in.start + in.count
	size := in.size
	//*环形数组
	if next >= size {
		//*索引如果超过size了，要回绕回来
		next -= size
	}
	//*buffer不够就要增加
	if next >= len(in.buffer) {
		in.growBuf()
	}
	in.buffer[next] = inflight
	in.count++
}

// **扩容inflights的buffer,基于当前两倍扩容
func (in *inflights) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
		newSize = 1
	} else if newSize > in.size {
		newSize = in.size
	}
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// ** 释放 in 中小于等于 to 的 inflights
func (in *inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		//*窗口为空，或者传入的参数小于窗口第一个元素的值
		return
	}
	i, idx := 0, in.start
	for i = 0; i < in.count; i++ {
		//*寻找比to大的最小的值，找到了就退出循环
		if to < in.buffer[idx] {
			break
		}
		//*处理需要回绕的情况
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}
	//*数量减少那些小于to的数据
	in.count -= i
	//*开始索引从第一个大于to的元素开始
	in.start = idx
	if in.count == 0 {
		//*如果经过这次缩减，count为0，那么start从0开始
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() { in.freeTo(in.buffer[in.start]) }

// **返回inflights是否已满
func (in *inflights) full() bool {
	return in.count == in.size
}

// **重置inflights
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
