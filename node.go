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
	emptyState = pb.HardState{}              //*空的硬状态
	ErrStopped = errors.New("raft: stopped") //*停止节点
)

// **软状态是异变的，包括：当前集群leader、当前节点状态
// **这部分数据不需要存储到持久化中
type SoftState struct {
	Lead      uint64    //*需要保持原子性访问
	RaftState StateType //*当前节点状态
}

// *比较两个SoftState是否相等
func (a SoftState) equal(b *SoftState) bool {
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
type Node interface {
	//*应用层每次tick时需要调用该函数，将会由这里驱动raft的一些操作比如选举等。
	//*至于tick的单位是多少由应用层自己决定，只要保证是恒定时间都会来调用一次就好了
	Tick()

	//*调用该函数将驱动节点进入候选人状态，进而将竞争leader
	Campaign(ctx context.Context) error

	//*提议写入数据到日志中，可能会返回错误
	Propose(ctx context.Context, data []byte) error

	//*提交配置变更
	ProposeConfChange(ctx context.Context, cc pb.ConfChange) error

	//*将消息msg灌入状态机
	Step(ctx context.Context, msg pb.Message) error

	//*这里是核心函数，将返回Ready的channel，应用层需要关注这个channel，当发生变更时将其中的数据进行操作
	Ready() <-chan Ready

	//*Advance函数是当使用者已经将上一次Ready数据处理之后，调用该函数告诉raft库可以进行下一步的操作
	Advance()

	//*提交集群配置更改
	ApplyConfChange(cc pb.ConfChange) *pb.ConfState

	//*leader迁移
	TransferLeadership(ctx context.Context, lead, transferee uint64)

	//*一致性读相关
	ReadIndex(ctx context.Context, rctx []byte) error

	//*获取当前节点的状态
	Status() Status

	//*报告节点状态
	ReportUnreachable(id uint64)

	//*报告快照传输状态
	ReportSnapshot(id uint64, status SnapshotStatus)

	//*关闭节点
	Stop()
}

// *远程节点
type Peer struct {
	ID      uint64 //*节点ID
	Context []byte //*上下文
}

func StartNode(c *Config, peers []Peer) Node {
	r := newRaft(c)
	//*初次启动以term为1来启动
	r.becomeFollower(1, None)
	for _, peer := range peers {
		cc := pb.ConfChange{Type: pb.ConfChangeAddNode, NodeID: peer.ID, Context: peer.Context}
		d, err := cc.Marshal()
		if err != nil {
			panic("unexpected marshal error")
		}
		e := pb.Entry{Type: pb.EntryConfChange, Term: 1, Index: r.raftLog.lastIndex() + 1, Data: d}
		r.raftLog.append(e)
	}
	r.raftLog.committed = r.raftLog.lastIndex()
	for _, peer := range peers {
		r.addNode(peer.ID)
	}
	n := newNode()
	n.logger = c.Logger
	go n.run(r)
	return &n
}

// *新建并启动一个Node
func RestartNode(c *Config) Node {
	r := newRaft(c)

	n := newNode()
	n.logger = c.Logger
	//*单独的协程里面启动
	go n.run(r)

	return &n
}

type node struct {
	//*提交本地请求数据用的channel
	propc chan pb.Message
	//*接收外部请求数据用的channel
	recvc chan pb.Message
	//*接收配置更新的channel
	confc chan pb.ConfChange
	//*接收最新配置状态的channel
	confstatec chan pb.ConfState
	//*这是用于通知应用层 Raft 状态机已准备好数据的通道
	readyc chan Ready
	//*这是一个信号通道，用于通知节点可以进行下一步操作
	advancec chan struct{}
	//*这是一个定时信号通道，与 Tick 方法相关
	tickc chan struct{}
	//*这是一个完成信号通道，可能用于标记节点运行的结束
	done chan struct{}
	//*这是一个停止信号通道，用于主动关闭节点
	stop chan struct{}
	//*这是一个嵌套通道，用于获取节点状态
	status chan chan Status

	logger Logger
}

func newNode() node {
	return node{
		propc:      make(chan pb.Message),
		recvc:      make(chan pb.Message),
		confc:      make(chan pb.ConfChange),
		confstatec: make(chan pb.ConfState),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		tickc:      make(chan struct{}, 128),
		done:       make(chan struct{}),
		stop:       make(chan struct{}),
		status:     make(chan chan Status),
	}
}

func (n *node) Stop() {
	select {
	//*尝试往Stop通道发送
	case n.stop <- struct{}{}:
		//*检查n.done关闭与否
	case <-n.done:
		return
	}
	<-n.done
}

func (n *node) run(r *raft) {
	//*提交本地请求数据用的通道
	var propc chan pb.Message
	//*这是用于通知应用层 Raft 状态机已准备好数据的通道
	var readyc chan Ready
	//*这是一个信号通道，用于通知节点可以进行下一步操作
	var advancec chan struct{}
	//*表示上一次 Ready 中未提交日志条目的最后索引,表示上一次 Ready 中未提交日志条目的最后任期
	var prevLastUnstablei, prevLastUnstablet uint64
	//*表示是否存在未提交的日志条目
	var havePrevLastUnstablei bool
	//*表示上一次 Ready 中快照的索引
	var prevSnapi uint64
	//*表示当前准备发送给应用层的 Ready 数据
	var rd Ready
	//*当前节点的领导者
	lead := None
	//*当前节点的软状态
	prevSoftSt := r.softState()
	//*当前节点的硬状态
	prevHardSt := emptyState

	for {
		if advancec != nil {
			//*advance channel不为空，说明还在等应用调用Advance接口通知已经处理完毕了本次的ready数据
			readyc = nil
		} else {
			//*prevSoftSt: 上一次的软状态（Leader 和节点状态）
			//*prevHardSt: 上一次的硬状态（Term、Vote、Commit）
			rd = newReady(r, prevSoftSt, prevHardSt)
			if rd.containsUpdates() {
				readyc = n.readyc
			} else {
				readyc = nil
			}
		}
		//*检查leader是否发生变化
		if lead != r.lead {
			//*如果当前有leader
			if r.hasLeader() {
				//*如果以前没有leader
				if lead == None {
					//*领导者被选举出来
					r.logger.Infof("raft.node: %x elected leader %x at term %d", r.id, r.lead, r.Term)
				} else {
					//*leader发生了改变
					r.logger.Infof("raft.node: %x changed leader from %x to %x at term %d", r.id, lead, r.lead, r.Term)
				}
				//*有leader，那么可以进行数据提交，prop channel不为空
				propc = n.propc
			} else {
				//*否则，prop channel为空
				r.logger.Infof("raft.node: %x lost leader %x at term %d", r.id, lead, r.Term)
				propc = nil
			}
			lead = r.lead
		}
		select {
		//TODO 建议可能需要缓冲配置提议（参考 Raft 论文），暗示当前实现有改进空间。
		case m := <-propc:
			//*处理本地应用提交的日志条目
			m.From = r.id
			r.Step(m)
		case m := <-n.recvc:
			//*其他节点发过来的消息
			if _, ok := r.prs[m.From]; ok || !IsResponseMsg(m.Type) {
				//*需要确保节点在集群中或者不是应答类消息的情况下才进行处理
				r.Step(m)
			}
		case cc := <-n.confc:
			//*配置变更的消息
			if cc.NodeID == None {
				//*NodeId为空的情况，只需要直接返回当前的nodes就好
				r.resetPendingConf()
				select {
				//*往配置变更的通道存入
				case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:
				case <-n.done:
				}
				break
			}
			switch cc.Type {
			case pb.ConfChangeAddNode:
				r.addNode(cc.NodeID)
			case pb.ConfChangeRemoveNode:
				if cc.NodeID == r.id {
					propc = nil
				}
				r.removeNode(cc.NodeID)
			case pb.ConfChangeUpdateNode:
				r.resetPendingConf()
			default:
				panic("unexpected conf type")
			}
			select {
			case n.confstatec <- pb.ConfState{Nodes: r.nodes()}:
			case <-n.done:
			}
		case <-n.tickc:
			r.tick()
		case readyc <- rd:
			//*通过channel写入ready数据
			//*以下先把ready的值保存下来，等待下一次循环使用，或者当advance调用完毕之后用于修改raftLog的
			if rd.SoftState != nil {
				prevSoftSt = rd.SoftState
			}
			if len(rd.Entries) > 0 {
				//*保存上一次还未持久化的entries的index、term
				prevLastUnstablei = rd.Entries[len(rd.Entries)-1].Index
				prevLastUnstablet = rd.Entries[len(rd.Entries)-1].Term
				//*标识是否还有没有持久化的日志
				havePrevLastUnstablei = true
			}
			if !IsEmptyHardState(rd.HardState) {
				prevHardSt = rd.HardState
			}
			if !IsEmptySnap(rd.Snapshot) {
				prevSnapi = rd.Snapshot.Metadata.Index
			}
			r.msgs = nil
			r.readStates = nil
			//*修改advance channel不为空，等待接收advance消息
			//*NOTE 建立与n.advancec的绑定，等待持久化完成信号
			advancec = n.advancec
		case <-advancec:
			//*收到advance channel的消息
			if prevHardSt.Commit != 0 {
				//*将committed的消息applied
				r.raftLog.appliedTo(prevHardSt.Commit)
			}
			if havePrevLastUnstablei {
				//*将还没有持久化的数据进行持久化
				r.raftLog.stableTo(prevLastUnstablei, prevLastUnstablet)
				havePrevLastUnstablei = false
			}
			r.raftLog.stableSnapTo(prevSnapi)
			advancec = nil
		case c := <-n.status:
			c <- getStatus(r)
		case <-n.stop:
			close(n.done)
			return
		}
	}
}

func (n *node) Tick() {
	select {
	//*向tick channel写入空数据，唤醒之
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.logger.Warningf("A tick missed to fire. Node blocks too long!")
	}
}

func (n *node) Campaign(ctx context.Context) error { return n.step(ctx, pb.Message{Type: pb.MsgHup}) }

// *将客户端数据打包成Raft提案，提交给当前Leader节点进行一致性复制
func (n *node) Propose(ctx context.Context, data []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Data: data}}})
}

// *提交集群配置变更请求（节点增删）
func (n *node) Step(ctx context.Context, m pb.Message) error {
	//*过滤网络消息，只处理来自其他节点的Raft协议消息（AppendEntries/Vote等），丢弃节点自身生成的本地消息（心跳/选举）
	if IsLocalMsg(m.Type) {
		return nil
	}
	return n.step(ctx, m)

}

func (n *node) ProposeConfChange(ctx context.Context, cc pb.ConfChange) error {
	data, err := cc.Marshal()
	if err != nil {
		return err
	}
	return n.Step(ctx, pb.Message{Type: pb.MsgProp, Entries: []pb.Entry{{Type: pb.EntryConfChange, Data: data}}})
}

// *将Raft消息路由到不同处理通道：提案消息（MsgProp）进入propc通道由主状态机处理
// *其他消息进入recvc通道由常规流程处理，实现消息分类和优先级调度
func (n *node) step(ctx context.Context, m pb.Message) error {
	ch := n.recvc
	if m.Type == pb.MsgProp {
		ch = n.propc
	}

	select {
	case ch <- m:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
}

func (n *node) Ready() <-chan Ready { return n.readyc }

func (n *node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

// *应用配置更新，返回当前配置状态
func (n *node) ApplyConfChange(cc pb.ConfChange) *pb.ConfState {
	var cs pb.ConfState
	select {
	//*向配置更新channel写入要更新的配置
	case n.confc <- cc:
	case <-n.done:
	}
	select {
	//*等待最新的配置状态返回
	case cs = <-n.confstatec:
	case <-n.done:
	}
	return &cs
}

func (n *node) Status() Status {
	c := make(chan Status)
	select {
	case n.status <- c:
		return <-c
	case <-n.done:
		return Status{}
	}
}

// *将某个节点不可达的消息传入通道
func (n *node) ReportUnreachable(id uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgUnreachable, From: id}:
	case <-n.done:
	}
}

// *向Raft状态机报告快照传输结果
func (n *node) ReportSnapshot(id uint64, status SnapshotStatus) {
	rej := status == SnapshotFailure

	select {
	case n.recvc <- pb.Message{Type: pb.MsgSnapStatus, From: id, Reject: rej}:
	case <-n.done:
	}
}

// *触发领导权转移流程
func (n *node) TransferLeadership(ctx context.Context, lead, transferee uint64) {
	select {
	case n.recvc <- pb.Message{Type: pb.MsgTransferLeader, From: transferee, To: lead}:
	case <-n.done:
	case <-ctx.Done():
	}
}

// *触发线性一致读流程：Leader记录当前commit index，向多数节点发起心跳确认领导权有效后
// *返回该索引值给客户端，确保后续读操作不会返回过期数据
func (n *node) ReadIndex(ctx context.Context, rctx []byte) error {
	return n.step(ctx, pb.Message{Type: pb.MsgReadIndex, Entries: []pb.Entry{{Data: rctx}}})
}

func newReady(r *raft, prevSoftSt *SoftState, prevHardSt pb.HardState) Ready {
	rd := Ready{
		//*entries保存的是没有持久化的数据数组
		Entries: r.raftLog.unstableEntries(),
		//*保存committed但是还没有applied的数据数组
		CommittedEntries: r.raftLog.nextEnts(),
		//*保存待发送的消息
		Messages: r.msgs,
	}
	if softSt := r.softState(); !softSt.equal(prevSoftSt) {
		rd.SoftState = softSt
	}
	if hardSt := r.hardState(); !isHardStateEqual(hardSt, prevHardSt) {
		rd.HardState = hardSt
	}
	if r.raftLog.unstable.snapshot != nil {
		//*如果未持久化的快照数据存在，也需要返回去
		rd.Snapshot = *r.raftLog.unstable.snapshot
	}
	if len(r.readStates) != 0 {
		rd.ReadStates = r.readStates
	}
	return rd
}
