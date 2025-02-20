package raftmini

import (
	"errors"

	pb "github.com/gyy0727/raftmini/raftpb"
	"golang.org/x/net/context"
)

// **快照状态
type SnapshotStatus int

const (
	//*快照传输成功
	SnapshotFinish SnapshotStatus = 1
	//*快照传输失败
	SnapshotFailure SnapshotStatus = 2
)

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: stopped")
)

// **软状态是异变的，包括：当前集群leader、当前节点状态
// **这部分数据不需要存储到持久化中
type SoftState struct {
	Lead      uint64 //*需要保持原子性访问
	RaftState StateType
}

// *比较两个SoftState是否相等
func (a SoftState) Equal(b SoftState) bool {
	return a.Lead == b.Lead && a.RaftState == b.RaftState
}

// *Ready结构体用于保存已经处于ready状态的日志和消息，这些都是准备保存到持久化存储中、提交或者发送给其他节点的
// *Ready结构体的所有数据都是只读状态
type Ready struct {
	//*软状态是异变的，包括：当前集群leader、当前节点状态
	*SoftState

	//*硬状态需要被保存，包括：节点当前Term、Vote、Commit
	//*如果当前这部分没有更新，则等于空状态
	pb.HardState

	//*保存ready状态的readindex数据信息
	ReadStates []ReadState

	//*需要在消息发送之前被写入到持久化存储中的entries数据数组
	Entries []pb.Entry

	//*需要写入到持久化存储中的快照数据
	Snapshot pb.Snapshot

	//*需要输入到状态机中的数据数组，这些数据之前已经被保存到持久化存储中了
	CommittedEntries []pb.Entry

	//*在entries被写入持久化存储中以后，需要发送出去的数据
	Messages []pb.Message
}

// *比较硬状态是否相等
func isHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

// *检查是不是空的硬状态
func IsEmptyHardState(st pb.HardState) bool {
	return isHardStateEqual(st, emptyState)
}

// *检查快照是否为空
func IsEmptySnap(sp pb.Snapshot) bool {
	return sp.Metadata.Index == 0
}

// *检查Ready是否包含更新
func (rd Ready) containsUpdates() bool {
	return rd.SoftState != nil || !IsEmptyHardState(rd.HardState) ||
		!IsEmptySnap(rd.Snapshot) || len(rd.Entries) > 0 ||
		len(rd.CommittedEntries) > 0 || len(rd.Messages) > 0 || len(rd.ReadStates) != 0
}

// *node表示raft中的一个节点
type Node struct {
	//*应用层每次tick时需要调用该函数，将会由这里驱动raft的一些操作比如选举等。
	//*至于tick的单位是多少由应用层自己决定，只要保证是恒定时间都会来调用一次就好了
	Tick func()

	//*调用该函数将驱动节点进入候选人状态，进而将竞争leader
	Campaign func(ctx context.Context) error

	//*提议写入数据到日志中，可能会返回错误
	Propose func(ctx context.Context, data []byte) error

	//*提交配置变更
	ProposeConfChange func(ctx context.Context, cc pb.ConfChange) error

	//*将消息msg灌入状态机
	Step func(ctx context.Context, msg pb.Message) error

	//*这里是核心函数，将返回Ready的channel，应用层需要关注这个channel，当发生变更时将其中的数据进行操作
	Ready func() <-chan Ready

	//*Advance函数是当使用者已经将上一次Ready数据处理之后，调用该函数告诉raft库可以进行下一步的操作
	Advance func()

	//*提交集群配置更改
	ApplyConfChange func(cc pb.ConfChange) *pb.ConfState

	//*leader迁移
	TransferLeadership func(ctx context.Context, lead, transferee uint64)

	//*一致性读相关
	ReadIndex func(ctx context.Context, rctx []byte) error

	//*获取当前节点的状态
	Status func() Status

	//*报告节点状态
	ReportUnreachable func(id uint64)

	//*报告快照传输状态
	ReportSnapshot func(id uint64, status SnapshotStatus)

	//*关闭节点
	Stop func()
}

// *远程节点
type Peer struct {
	ID      uint64 //*节点ID
	Context []byte //*上下文
}


func StartNode(c *Config, peers []Peer) Node{

	
	return Node{}
}