package raftmini

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	pb "github.com/gyy0727/raftmini/raftpb"
)

// *表示竞选的类型,在 Raft 协议中，竞选是指节点尝试成为领导者的过程
// *使用 string 类型的原因是字符串比较和填充 Raft 条目更简单
type CampaignType string

type stepFunc func(r *raft, m pb.Message)

// *当前节点的状态
type StateType uint64

// *节点状态的字符串表示
var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
	"StatePreCandidate",
}

// *返回对应的字符串表示
func (st StateType) String() string {
	return stmap[uint64(st)]
}

const (
	StateFollower     StateType = iota //*跟随者
	StateCandidate                     //*候选人
	StateLeader                        //*领导者
	StatePreCandidate                  //*预候选人
	numStates                          //*状态的数量
)

// *None 是一个占位nodeID，没有领导者的时候使用
const None uint64 = 0
const noLimit = math.MaxUint64

// *表示只读请求的状态
type ReadOnlyOption int

// * ReadOnlySafe 表示只有当大多数节点都提交了一个日志条目时，才能提交该日志条目
// * ReadOnlyLeaseBased 表示只有领导者的租约还有效时，才能提交日志条目
const (
	ReadOnlySafe ReadOnlyOption = iota
	ReadOnlyLeaseBased
)

// *表示选举的类型
const (
	//*CampaignPreElection 表示预选举
	campaignPreElection CampaignType = "CampaignPreElection"
	//*CampaignElection 表示选举
	campaignElection CampaignType = "CampaignElection"
	//*CampaignTransfer 表示领导节点发生转移的选举
	campaignTransfer CampaignType = "CampaignTransfer"
)

// *封装 rand.Rand 并提供同步机制
type lockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand //*随机数生成器
}

// *返回一个取值范围在[0,n)的伪随机int值，如果n<=0会panic
func (r *lockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

// *初始化了一个全局随机数生成器
var globalRand = &lockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

// *config结构体包含了启动一个raft节点所需要的所有配置信息
type Config struct {
	//*本地节点的标识
	ID uint64

	//*集群中所有节点的标识
	peers []uint64

	//*选举超时tick
	ElectionTick int

	//*心跳超时
	HeartbeatTick int

	//*Raft 生成的日志条目和状态将存储在 Storage 中。
	//*Raft 在需要时从 Storage 中读取持久化的条目和状态。
	//*Raft 在重新启动时从 Storage 中读取先前的状态和配置
	Storage Storage

	//*Applied 是最后应用的索引。仅在重新启动 Raft 时设置。
	//*Raft 不会返回小于或等于 Applied 的条目给应用程序。
	//*如果在重新启动时未设置 Applied，Raft 可能会返回先前应用的条目。这是一个非常依赖于应用程序的配置
	Applied uint64

	//*限制每个追加消息的最大大小
	MaxSizePerMsg uint64

	//*MaxInflightMsgs 限制在乐观复制阶段的最大飞行追加消息数。
	//*应用程序传输层通常有自己的 TCP/UDP 发送缓冲区。设置 MaxInflightMsgs 以避免溢出该发送缓冲区。
	MaxInflightMsgs int

	//*标记leader是否需要检查集群中超过半数节点的活跃性，如果在选举超时内没有满足该条件，leader切换到follower状态
	CheckQuorum bool

	//*PreVote 启用 Raft 论文第 9.6 节中描述的预投票算法。
	//*这可以防止一个被分区隔离的节点重新加入集群时的干扰。
	PreVote bool

	//*ReadOnlyOption 表示只读请求的状态
	ReadOnlyOption ReadOnlyOption

	Logger Logger
}

// *检查配置是否有效
func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	if c.MaxInflightMsgs <= 0 {
		return errors.New("max inflight messages must be greater than 0")
	}

	if c.Logger == nil {
		c.Logger = raftLogger
	}

	return nil
}

// *raft结构体包含了raft节点的所有状态信息
type raft struct {
	//*节点的标识
	id uint64
	//*当前任期
	Term uint64
	//*当前任期投票的节点
	Vote uint64
	//*读状态
	readStates []ReadState
	//*日志
	raftLog *raftLog
	//*最大飞行消息数
	maxInflight int
	//*最大消息大小
	maxMsgSize uint64
	//* 是一个映射，键是节点 ID，值是指向 Progress 结构体的指针，用于跟踪每个节点的进度。
	prs map[uint64]*Progress
	//*当前节点的状态
	state StateType

	//*该map存放哪些节点投票给了本节点
	votes map[uint64]bool
	//*存储待发送的消息
	msgs []pb.Message

	//*领导者的id
	lead uint64

	//*leader转让的目标节点id
	leadTransferee uint64
	//*标识当前是否有未应用的配置数据
	pendingConf bool
	//*是一个指向 readOnly 结构体的指针，用于处理只读请求
	readOnly *readOnly
	//*是自上次选举超时以来的 tick 数，当节点是领导者或候选人时使用。
	//*当节点是跟随者时，表示自上次选举超时或收到当前领导者的有效消息以来的 tick 数。
	electionElapsed int
	//*是自上次心跳超时以来的 tick 数，仅领导者维护。
	heartbeatElapsed int
	//*指示是否检查法定人数的活动性
	checkQuorum bool
	//*指示是否启用预投票算法
	preVote bool
	//*心跳超时时间
	heartbeatTimeout int
	//*选举超时时间
	electionTimeout int
	//*是一个随机数，范围在 [electionTimeout, 2 * electionTimeout - 1] 之间。当 Raft 状态变为跟随者或候选人时重置
	randomizedElectionTimeout int

	//*tick函数，在到期的时候调用，不同的角色该函数不同
	tick func()
	//*是一个 stepFunc 类型的函数，用于处理不同状态下的消息
	step stepFunc

	logger Logger
}

// *创建一个新的raft节点
func newRaft(c *Config) *raft {
	//*检查传入的配置是否合法
	if err := c.validate(); err != nil {
		panic(err.Error())
	}
	raftLog := newLog(c.Storage, c.Logger)
	hs, cs, err := c.Storage.InitialState()
	if err != nil {
		//*检查初始化是否出问题了
		panic(err.Error())
	}
	//*传入的配置的集群节点信息
	peers := c.peers

	if len(cs.Nodes) > 0 {
		//*如果传入的配置中有节点信息
		if len(peers) > 0 {
			//*如果本地的配置中有节点信息，但是又传入了peers，那么就会出问题
			panic("cannot specify both newRaft(peers) and ConfState.Nodes")
		}
		peers = cs.Nodes
	}
	r := &raft{
		id:               c.ID,
		lead:             None,
		raftLog:          raftLog,
		maxMsgSize:       c.MaxSizePerMsg,
		maxInflight:      c.MaxInflightMsgs,
		prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
		logger:           c.Logger,
		checkQuorum:      c.CheckQuorum,
		preVote:          c.PreVote,
		readOnly:         newReadOnly(c.ReadOnlyOption), //*实现线性一致读的接口
	}

	for _, p := range peers {
		r.prs[p] = &Progress{Next: 1, ins: newInflights(r.maxInflight)}
	}

	//*如果不是第一次启动而是从之前的数据进行恢复
	if !isHardStateEqual(hs, emptyState) {
		r.loadState(hs)
	}
	if c.Applied > 0 {
		raftlog.appliedTo(c.Applied)
	}
	//*启动都是follower状态
	r.becomeFollower(r.Term, None)

	var nodesStrs []string
	for _, n := range r.nodes() {
		nodesStrs = append(nodesStrs, fmt.Sprintf("%x", n))
	}

	r.logger.Infof("newRaft %x [peers: [%s], term: %d, commit: %d, applied: %d, lastindex: %d, lastterm: %d]",
		r.id, strings.Join(nodesStrs, ","), r.Term, r.raftLog.committed, r.raftLog.applied, r.raftLog.lastIndex(), r.raftLog.lastTerm())
	return r

}

// *检查当前节点是否有领导者
func (r *raft) hasLeader() bool { return r.lead != None }

// *返回一个当前节点的软状态
func (r *raft) softState() *SoftState { return &SoftState{Lead: r.lead, RaftState: r.state} }

// *返回当前节点的硬状态
func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.Term,
		Vote:   r.Vote,
		Commit: r.raftLog.committed,
	}
}

// *超过半数的节点的数量
func (r *raft) quorum() int { return len(r.prs)/2 + 1 }

// *返回排序之后的节点ID数组
func (r *raft) nodes() []uint64 {
	nodes := make([]uint64, 0, len(r.prs))
	for id := range r.prs {
		nodes = append(nodes, id)
	}
	//*注意这里进行了排序
	sort.Sort(uint64Slice(nodes))
	return nodes
}

// *消息的发送
func (r *raft) send(m pb.Message) {
	m.From = r.id
	//* NOTE 理解发送的细节
	if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
		if m.Term == 0 {
			panic(fmt.Sprintf("term should be set when sending %s", m.Type))
		}
	} else {
		if m.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", m.Type, m.Term))
		}
		//* prop消息和readindex消息不需要带上term参数
		if m.Type != pb.MsgProp && m.Type != pb.MsgReadIndex {
			m.Term = r.Term
		}

	}
	r.msgs = append(r.msgs, m)
}

// *向to节点发送append消息
func (r *raft) sendAppend(to uint64) {
	//*获取to节点的进度progress
	pr := r.prs[to]
	if pr.IsPaused() {
		//*节点暂停了
		r.logger.Infof("node %d paused", to)
		return
	}

	//*要发送的消息
	m := pb.Message{}
	m.To = to
	//*从该节点的Next的上一条数据获取term,就是当前节点的最后一条日志
	term, errt := r.raftLog.term(pr.Next - 1)
	//*获取该节点的Next之后的日志条目,总数不超过maxMsgSize
	ents, erre := r.raftLog.entries(pr.Next, r.maxMsgSize)

	//*如果获取term或entries失败,证明数据已经写入到了快照
	if errt != nil || erre != nil {
		if !pr.RecentActive {
			//*如果该节点当前不可用
			r.logger.Debugf("ignore sending snapshot to %x since it is not recently active", to)
			return
		}
		//*尝试发送快照
		m.Type = pb.MsgSnap
		snapshot, err := r.raftLog.snapshot()
		if err != nil {
			if err == ErrSnapshotTemporarilyUnavailable {
				r.logger.Debugf("%x failed to send snapshot to %x because snapshot is temporarily unavailable", r.id, to)
				return
			}
			panic(err) //*快照不可用
		}
		//*不能发送空快照
		if IsEmptySnap(snapshot) {
			panic("need non-empty snapshot")
		}
		m.Snapshot = snapshot
		sindex, sterm := snapshot.Metadata.Index, snapshot.Metadata.Term
		r.logger.Debugf("%x [firstindex: %d, commit: %d] sent snapshot[index: %d, term: %d] to %x [%s]",
			r.id, r.raftLog.firstIndex(), r.raftLog.committed, sindex, sterm, to, pr)
		//*将该节点的状态进度更新为接收快照的状态
		pr.becomeSnapshot(sindex)
		r.logger.Debugf("%x paused sending replication messages to %x [%s]", r.id, to, pr)
	} else {
		//*正常情况下发送append消息
		m.Type = pb.MsgApp
		m.Index = pr.Next - 1
		m.LogTerm = term
		m.Entries = ents
		//*append消息需要告知当前leader的commit索引
		m.Commit = r.raftLog.committed
		if n := len(m.Entries); n != 0 { //*如果发送过去的entries不为空
			switch pr.State {
			case ProgressStateReplicate:
				//*如果该节点在接受副本的状态
				//*得到待发送数据的最后一条索引
				last := m.Entries[n-1].Index
				//*直接使用该索引更新Next索引
				pr.optimisticUpdate(last)
				pr.ins.add(last)
			case ProgressStateProbe:
				//*在probe状态时，每次只能发送一条app消息
				pr.pause()
			default:
				r.logger.Panicf("%x is sending append in unhandled state %s", r.id, pr.State)
			}
		}
	}
	r.send(m)
}

// *发送心跳
func (r *raft) sendHeartbeat(to uint64, ctx []byte) {
	//*比较leader节点已经提交的索引和follower节点已经提交的索引，取最小值
	commit := min(r.prs[to].Match, r.raftLog.committed)
	m := pb.Message{
		To:      to,
		Type:    pb.MsgHeartbeat,
		Commit:  commit,
		Context: ctx,
	}
	r.send(m)
}

// *向所有节点广播append消息
func (r *raft) bcastAppend() {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendAppend(id)
	}
}

// *广播心跳
func (r *raft) bcastHeartbeat() {
	lastCtx := r.readOnly.lastPendingRequestCtx()
	if len(lastCtx) == 0 {
		r.bcastHeartbeatWithCtx(nil)
	} else {
		r.bcastHeartbeatWithCtx([]byte(lastCtx))
	}
}

// *携带上下文广播心跳
func (r *raft) bcastHeartbeatWithCtx(ctx []byte) {
	for id := range r.prs {
		if id == r.id {
			continue
		}
		r.sendHeartbeat(id, ctx)
	}
}

// *提交当前日志
func (r *raft) maybeCommit() bool {

	mis := make(uint64Slice, 0, len(r.prs))
	//*将当前所有节点的进度添加到mis
	for id := range r.prs {
		mis = append(mis, r.prs[id].Match)
	}
	//*逆序排列
	sort.Sort(sort.Reverse(mis))
	//*排列之后拿到中位数的Match，因为如果这个位置的Match对应的Term也等于当前的Term
	//*说明有过半的节点至少comit了mci这个索引的数据，这样leader就可以以这个索引进行commit了
	mci := mis[r.quorum()-1]
	//*raft日志尝试commit
	return r.raftLog.maybeCommit(mci, r.Term)
}

// *重置raft的状态
// *reset(term uint64) 函数会在以下情况下被调用：
// *节点检测到更高的任期（收到更高任期的消息）。
// *领导者退回到跟随者角色。
// *节点启动新一轮选举（转换为候选者）。
// *节点初始化或从故障恢复。
// *NOTE每个节点都要有 Progress 吗？
// * 是的，在代码实现上，每个节点（包括 Follower）在 r.prs 中都有一个 Progress 条目。这是设计选择的结果，而非 Raft 算法的硬性要求。
// * Follower 节点需要 Progress 吗？
// * 不需要。从 Raft 算法的逻辑上看，Follower 不使用 Progress，它只在 Leader 角色下有意义。但在实现中，Follower 仍然持有 Progress，是为了在可能的角色切换（成为 Leader）时无缝过渡。
// * 为什么这样设计？
// * 这种设计是为了简化代码逻辑、保持状态一致性，并为动态角色切换做准备。尽管对 Follower 来说是“多余”的，但它在整体实现中提供了便利性和健壮性。
func (r *raft) reset(term uint64) {
	if r.Term != term {
		//*如果是新的任期，那么保存任期号，同时将投票节点置空
		r.Term = term
		r.Vote = None
	}
	r.lead = None
	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	//*重置选举超时
	r.resetRandomizedElectionTimeout()

	r.abortLeaderTransfer()
	r.votes = make(map[uint64]bool)
	for id := range r.prs {
		r.prs[id] = &Progress{Next: r.raftLog.lastIndex() + 1, ins: newInflights(r.maxInflight)}
		if id == r.id {
			r.prs[id].Match = r.raftLog.lastIndex()
		}
	}
	r.pendingConf = false
	r.readOnly = newReadOnly(r.readOnly.option)

}

// *批量append一堆entries
func (r *raft) appendEntry(es ...pb.Entry) {
	//*当前节点的最后一条日志的索引
	li := r.raftLog.lastIndex()
	for i := range es {
		es[i].Term = r.Term
		es[i].Index = li + uint64(i) + 1
	}
	r.raftLog.append(es...)
	//*更新当前节点的进度
	r.prs[r.id].maybeUpdate(r.raftLog.lastIndex())
	//*将新的日志条目发送给其他节点
	r.maybeCommit()
}

// *follower以及candidate的tick(心跳)函数，在r.electionTimeout(选举超时)之后被调用
func (r *raft) tickElection() {
	r.electionElapsed++
	if r.promotable() && r.pastElectionTimeout() {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.id, Type: pb.MsgHup})
	}
}

// *leader的tick(心跳)函数，在r.heartbeatTimeout(心跳超时)之后被调用
func (r *raft) tickHeartbeat() {
	// NOTE理解electionElapsed和heartbeatElapsed
	r.heartbeatElapsed++
	r.electionElapsed++

	if r.electionElapsed >= r.electionTimeout {
		r.electionElapsed = 0
		if r.checkQuorum {
			r.step(pb.Message{From: r.id, Type: pb.MsgCheckQuorum})
		}
		if r.state == StateLeader && r.leadTransferee != None {
			//*当前在迁移leader的流程，但是过了选举超时新的leader还没有产生，那么旧的leader重新成为leader
			r.abortLeaderTransfer()
		}
	}

	if r.state != StateLeader {
		//*不是leader
		return
	}

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		//*向集群中其他节点发送广播消息
		r.heartbeatElapsed = 0
		//*尝试发送MsgBeat消息
		r.Step(pb.Message{From: r.id, Type: pb.MsgBeat})
	}

}

// *将节点转换成follower状态
func (r *raft) becomeFollower(term uint64, lead uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.lead = lead
	r.state = StateFollower
	r.logger.Infof("%x became follower at term %d", r.id, r.Term)
}

// *将节点转换成候选人状态
func (r *raft) becomeCandidate() {
	//*NOTE leader应该转换成follower再转换成candidate,这才是raft的正常逻辑
	if r.state == StateLeader {
		panic("invalid transition [leader -> pre-candidate]")
	}
	r.step = stepCandidate
	//*因为进入candidate状态，意味着需要重新进行选举了，所以reset的时候传入的是Term+1
	r.reset(r.Term + 1)
	r.tick = r.tickElection
	//*给自己投票
	r.Vote = r.id
	r.state = StateCandidate
	r.logger.Infof("%x became candidate at term %d", r.id, r.Term)
}

// *将节点转换成预候选人状态
func (r *raft) becomePreCandidate() {
	//*NOTE leader应该转换成follower再转换成candidate,这才是raft的正常逻辑
	if r.state == StatePreCandidate {
		panic("invalid transition [pre-candidate -> pre-candidate]")
	}
	//*prevote不会递增term，也不会先进行投票，而是等prevote结果出来再进行决定
	r.step = stepCandidate
	r.tick = r.tickElection
	r.state = StatePreCandidate
	r.logger.Infof("%x became pre-candidate at term %d", r.id, r.Term)
}

// *转换节点状态为 leader
func (r *raft) becomeLeader() {
	//*NOTE 应该由candidate转换成leader的逻辑
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.Term)
	r.tick = r.tickHeartbeat
	r.lead = r.id
	r.state = StateLeader
	//*获取未提交的日志条目
	ents, err := r.raftLog.entries(r.raftLog.committed+1, noLimit)
	if err != nil {
		r.logger.Panicf("unexpected error getting uncommitted entries (%v)", err)
	}

	//* 变成leader之前，这里还有没commit的配置变化消息
	//*统计 ents 中未提交的配置变更条目数量（EntryConfChange 类型）
	nconf := numOfPendingConf(ents)
	//*NOTE Raft 要求一次只处理一个配置变更（参考论文 Section 6），避免复杂的状态冲突
	if nconf > 1 {
		panic("unexpected multiple uncommitted config entry")
	}
	if nconf == 1 {
		r.pendingConf = true
	}

	//*为什么成为leader之后需要传入一个空数据？
	r.appendEntry(pb.Entry{Data: nil})
	r.logger.Infof("%x became leader at term %d", r.id, r.Term)
}

// *发起选举
func (r *raft) campaign(t CampaignType) {
	//*任期号
	var term uint64
	//*投票消息
	var voteMsg pb.MessageType
	if t == campaignPreElection {
		//*预选举
		r.becomePreCandidate()
		voteMsg = pb.MsgPreVote //*预投票
		//*NOET Pre-Vote 是为了在正式选举前探测集群是否支持该节点成为 Leader，避免直接递增任期带来的干扰
		term = r.Term + 1
	} else {
		//*选举
		r.becomeCandidate()
		term = r.Term
		voteMsg = pb.MsgVote //*正式投票
	}
	//*调用poll函数给自己投票，同时返回当前投票给本节点的节点数量
	//*NOTE 假设有五个节点,为什么此处不会是 poll = 4？
	//*campaign 函数的逻辑是：
	//*	给自己投票（r.poll(...)）。
	//*	检查是否立即获胜（单节点情况
	//*	如果不满足，继续向其他节点发送投票请求。
	//* TAG 仅仅对于单节点的集群
	if r.quorum() == r.poll(r.id, voteRespMsgType(voteMsg), true) {
		//*有半数投票，说明通过，切换到下一个状态
		if t == campaignPreElection {
			r.campaign(campaignElection)
		} else {
			//*如果给自己投票之后，刚好超过半数的通过，那么就成为新的leader
			r.becomeLeader()
		}
		return

	}
	//*向集群的其他节点发送投票消息
	for id := range r.prs {
		if id == r.id {
			//*过滤掉自己
			continue
		}
		r.logger.Infof("%x [logterm: %d, index: %d] sent %s request to %x at term %d",
			r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), voteMsg, id, r.Term)

		var ctx []byte
		if t == campaignTransfer {
			ctx = []byte(t)
		}
		r.send(pb.Message{Term: term, To: id, Type: voteMsg, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm(), Context: ctx})
	}

}

// *轮询集群中所有节点，返回一共有多少节点已经进行了投票
func (r *raft) poll(id uint64, t pb.MessageType, v bool) (granted int) {
	//*v 为 true 时，表示当前节点（r.id）收到了来自其他节点（id）的投票支持，记录日志
	if v {
		r.logger.Infof("%x received %s from %x at term %d", r.id, t, id, r.Term)
	} else {
		//*v 为 false 时，表示当前节点（r.id）收到了来自节点（id）的投票拒绝，记录日志。
		r.logger.Infof("%x received %s rejection from %x at term %d", r.id, t, id, r.Term)
	}
	//*如果id没有投票过，那么更新id的投票情况
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}
	//*计算下都有多少节点已经投票给自己了
	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}

// *raft 状态机的核心函数，处理消息
func (r *raft) Step(m pb.Message) error {
	r.logger.Infof("from:%d, to:%d, type:%s, term:%d, state:%v", m.From, m.To, m.Type, r.Term, r.state)

	switch {
	case m.Term == 0:
		//*来自本地的消息
	case m.Term > r.Term:
		//*来自更高任期的消息
		//*是预投票或者投票的消息
		lead := m.From
		if m.Type == pb.MsgVote || m.Type == pb.MsgPreVote {
			//*检查是否是强制选举:强制集群中的某个节点（或所有节点）参与一次新的选举，即使当前 Leader 仍然有效且集群状态正常
			//*一般是领导权转移
			force := bytes.Equal(m.Context, []byte(campaignTransfer))
			//*是否在租约期以内
			inLease := r.checkQuorum && r.lead != None && r.electionElapsed < r.electionTimeout
			if !force && inLease {
				//*如果非强制，而且又在租约期以内，就不做任何处理
				//*非强制又在租约期内可以忽略选举消息，见论文的4.2.3，这是为了阻止已经离开集群的节点再次发起投票请求
				//*已经离开集群的非法节点可能以为自己是集群的一部分发起选举
				r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] ignored %s from %x [logterm: %d, index: %d] at term %d: lease is not expired (remaining ticks: %d)",
					r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term, r.electionTimeout-r.electionElapsed)
				return nil
			}

			//*否则将lead置为空
			//*能走到这里证明现在是正常的选举流程,将lead置为空是表明当前节点不认可from节点的领导权
			lead = None
		}
		switch {
		//*预投票
		case m.Type == pb.MsgPreVote:
			//*在应答一个prevote消息时不对任期term做修改
		//*对自己发出的预投票的消息的响应且没有拒绝
		case m.Type == pb.MsgPreVoteResp && !m.Reject:

		default:
			r.logger.Infof("%x [term: %d] received a %s message with higher term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
			//*如果是投票消息，那么将自己的状态转换为follower
			r.becomeFollower(m.Term, lead)
		}

	case m.Term < r.Term:
		//*消息的Term小于节点自身的Term，同时消息类型是心跳消息或者是append消息
		if r.checkQuorum && (m.Type == pb.MsgHeartbeat || m.Type == pb.MsgApp) {
			//*收到了一个节点发送过来的更小的term消息。这种情况可能是因为消息的网络延时导致，但是也可能因为该节点由于网络分区导致了它递增了term到一个新的任期。
			//*，这种情况下该节点不能赢得一次选举，也不能使用旧的任期号重新再加入集群中。如果checkQurom为false，这种情况可以使用递增任期号应答来处理。
			//*但是如果checkQurom为True，
			//*此时收到了一个更小的term的节点发出的HB或者APP消息，于是应答一个appresp消息，试图纠正它的状态
			r.send(pb.Message{To: m.From, Type: pb.MsgAppResp})
		} else {

			//*除了上面的情况以外，忽略任何term小于当前节点所在任期号的消息
			r.logger.Infof("%x [term: %d] ignored a %s message with lower term from %x [term: %d]",
				r.id, r.Term, m.Type, m.From, m.Term)
		}
		//*在消息的term小于当前节点的term时，不往下处理直接返回了
		return nil
	}

	switch m.Type {
	case pb.MsgHup:
		//*收到HUP消息，说明准备进行选举
		if r.state != StateLeader {
			//*当前不是leader

			//*取出[applied+1,committed+1]之间的消息，即得到还未进行applied的日志列表
			ents, err := r.raftLog.slice(r.raftLog.applied+1, r.raftLog.committed+1, noLimit)
			if err != nil {
				r.logger.Panicf("unexpected error getting unapplied entries (%v)", err)
			}
			//*如果其中有config消息，并且commited > applied，说明当前还有没有apply的config消息，这种情况下不能开始投票
			if n := numOfPendingConf(ents); n != 0 && r.raftLog.committed > r.raftLog.applied {
				r.logger.Warningf("%x cannot campaign at term %d since there are still %d pending configuration changes to apply", r.id, r.Term, n)
				return nil
			}

			r.logger.Infof("%x is starting a new election at term %d", r.id, r.Term)
			//*进行选举
			if r.preVote {
				r.campaign(campaignPreElection)
			} else {
				r.campaign(campaignElection)
			}
		} else {
			r.logger.Debugf("%x ignoring MsgHup because already leader", r.id)
		}

	case pb.MsgVote, pb.MsgPreVote:
		//*收到投票类的消息

		if (r.Vote == None || m.Term > r.Term || r.Vote == m.From) && r.raftLog.isUpToDate(m.Index, m.LogTerm) {
			//*如果当前没有给任何节点投票（r.Vote == None）或者投票的节点term大于本节点的（m.Term > r.Term）
			//*或者是之前已经投票的节点（r.Vote == m.From）
			//*同时还满足该节点的消息是最新的（r.raftLog.isUpToDate(m.Index, m.LogTerm)），那么就接收这个节点的投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] cast %s for %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type)})
			if m.Type == pb.MsgVote {
				//*保存下来给哪个节点投票了
				r.electionElapsed = 0
				r.Vote = m.From
			}
		} else {
			//*否则拒绝投票
			r.logger.Infof("%x [logterm: %d, index: %d, vote: %x] rejected %s from %x [logterm: %d, index: %d] at term %d",
				r.id, r.raftLog.lastTerm(), r.raftLog.lastIndex(), r.Vote, m.Type, m.From, m.LogTerm, m.Index, r.Term)
			r.send(pb.Message{To: m.From, Type: voteRespMsgType(m.Type), Reject: true})
		}

	default:
		//*其他情况下进入各种状态下自己定制的状态机函数
		r.step(r, m)
	}
	return nil
}

func stepLeader(r *raft, m pb.Message) {
	//*根据消息的类型进行处理
	switch m.Type {
	case pb.MsgBeat:
		r.bcastHeartbeat()
		return
	case pb.MsgCheckQuorum:
		if !r.checkQuorumActive() {
			//*检查集群的可用性,是否还支持节点成为领导
			r.logger.Warningf("%x stepped down to follower since quorum is not active", r.id)
			r.becomeFollower(r.Term, None)
		}
		return
	case pb.MsgProp:
		//*提交日志
		if len(m.Entries) == 0 {
			//*当前没有可以提交的日志
		}
		//*检查自己是否还在集群中
		if _, ok = r.prs[r.id]; !ok {
			//*不在集群中,不在集群那就不能做出干扰集群的行为
			return
		}
		if r.leadTransferee != None {
			r.logger.Infof("%x [term %d] transfer leadership to %x is in progress; dropping proposal", r.id, r.Term, r.leadTransferee)
			return
		}
		//*开始提交日志
		for i, e := range m.Entries {
			//*检查是否是配置变更
			if e.Type == pb.EntryConfChange {
				if r.pendingConf {
					//*如果当前存在还没有处理的,则其他的配置先忽略掉
					r.logger.Infof("propose conf %s ignored since pending unapplied configuration", e)
					m.Entries[i] = pb.Entry{Type: pb.EntryNormal}
				} else {
					//*标明当前有未处理的配置变化
					r.pendingConf = true
				}
			}
		}
		//*将消息添加到raftLog中
		r.appendEntry(m.Entries...)
		r.bcastAppend()
		return
	case pb.MsgReadIndex:
		//*处理只读请求
		//*检查集群是多节点还是单节点
		if r.quorum() > 1 {
			//*检查当前节点是否有提交过日志
			//* NOTE 日志压缩错误
			if r.raftLog.zeroTermOnErrCompacted(r.raftLog.term(r.raftLog.committed)) != r.Term {
				//*Raft 要求 Leader 提交当前任期的日志以证明其领导权（论文 Section 8）。
				//*新 Leader 未提交日志，可能未被多数节点认可，读可能不安全。
				return
			}
			switch r.readOnly.option {
			case ReadOnlySafe:
				//*NOTE 详细了解
				//*把读请求到来时的committed索引保存下来
				r.readOnly.addRequest(r.raftLog.committed, m)
				//*广播消息出去，其中消息的CTX是该读请求的唯一标识
				//*在应答是Context要原样返回，将使用这个ctx操作readOnly相关数据
				r.bcastHeartbeatWithCtx(m.Entries[0].Data)
			case ReadOnlyLeaseBased:
				//* NOTE 详细了解租约模式
				var ri uint64
				if r.checkQuorum {
					ri = r.raftLog.committed
				}
				if m.From == None || m.From == r.id {
					r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
				} else {
					r.send(pb.Message{To: m.From, Type: pb.MsgReadIndexResp, Index: ri, Entries: m.Entries})
				}

			}
		} else {
			r.readStates = append(r.readStates, ReadState{Index: r.raftLog.committed, RequestCtx: m.Entries[0].Data})
		}

		return

	}

	//*All other message types require a progress for m.From (pr).
	//*检查消息发送者当前是否在集群中,避免受到其他废弃节点的干扰
	pr, prOk := r.prs[m.From]
	if !prOk {
		r.logger.Debugf("%x no progress available for %x", r.id, m.From)
		return
	}

	switch m.Type {
	case pb.MsgAppResp:
		//*处理append消息的应答
		pr.RecentActive = true
		//*如果是拒绝的消息
		if m.Reject {
			//* 如果拒绝了append消息，说明term、index不匹配
			r.logger.Debugf("%x received msgApp rejection(lastindex: %d) from %x for index %d",
				r.id, m.RejectHint, m.From, m.Index)
			//*rejecthint带来的是拒绝该app请求的节点，其最大日志的索引
			if pr.maybeDecrTo(m.Index, m.RejectHint) {
				//*如果拒绝了append消息，那么就减小Next
				r.logger.Debugf("%x decreased progress of %x to [%s]", r.id, m.From, pr)
				if pr.State == ProgressStateReplicate {
					//*如果是在复制状态，那么就将状态切换为探测状态
					pr.becomeProbe()
				}
				//*重新发送append消息
				r.sendAppend(m.From)
			}
		} else {
			//*通过该append请求
			oldPaused := pr.IsPaused()
			if pr.maybeUpdate(m.Index) {
				switch {
				case pr.State == ProgressStateProbe:
					//*如果当前该节点在探测状态，切换到可以接收副本状态
					pr.becomeReplicate()
				case pr.State == ProgressStateSnapshot && pr.needSnapshotAbort():
					//*如果当前该接在在接受快照状态，而且已经快照数据同步完成了
					r.logger.Debugf("%x snapshot aborted, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
					//*切换到探测状态
					pr.becomeProbe()
				case pr.State == ProgressStateReplicate:
					//*如果当前该节点在接收副本状态，因为通过了该请求，所以滑动窗口可以释放在这之前的索引了
					pr.ins.freeTo(m.Index)
				}
				if r.maybeCommit() {
					//*如果可以commit日志，那么广播append消息
					r.bcastAppend()
				} else if oldPaused {
					//*如果该节点之前状态是暂停，继续发送append消息给它
					r.sendAppend(m.From)
				}

				if m.From == r.leadTransferee && pr.Match == r.raftLog.lastIndex() {
					//*迁移过去的新leader，其日志已经追上了旧的leader
					r.logger.Infof("%x sent MsgTimeoutNow to %x after received MsgAppResp", r.id, m.From)
					r.sendTimeoutNow(m.From)
				}
			}

		}
	case pb.MsgHeartbeatResp:
		//*该节点当前处于活跃状态
		pr.RecentActive = true
		//*这里调用resume是因为当前可能处于probe状态，而这个状态在两个heartbeat消息的间隔期只能收一条同步日志消息，因此在收到HB消息时就停止pause标记
		pr.resume()

		if pr.State == ProgressStateReplicate && pr.ins.full() {
			pr.ins.freeFirstOne()
		}
		//*该节点的match节点小于当前最大日志索引，可能已经过期了，尝试添加日志
		if pr.Match < r.raftLog.lastIndex() {
			r.sendAppend(m.From)
		}

		//*只有readonly safe方案，才会继续往下走
		if r.readOnly.option != ReadOnlySafe || len(m.Context) == 0 {
			return
		}

		//*收到应答调用recvAck函数返回当前针对该消息已经应答的节点数量
		ackCount := r.readOnly.recvAck(m)
		if ackCount < r.quorum() {
			//*小于集群半数以上就返回不往下走了
			return
		}

		//*调用advance函数尝试丢弃已经被确认的read index状态
		rss := r.readOnly.advance(m)
		for _, rs := range rss { //*遍历准备被丢弃的readindex状态
			req := rs.req
			if req.From == None || req.From == r.id { // from local member
				//*如果来自本地
				r.readStates = append(r.readStates, ReadState{Index: rs.index, RequestCtx: req.Entries[0].Data})
			} else {
				//*否则就是来自外部，需要应答
				r.send(pb.Message{To: req.From, Type: pb.MsgReadIndexResp, Index: rs.index, Entries: req.Entries})
			}
		}
	case pb.MsgSnapStatus:
		//*当前该节点状态已经不是在接受快照的状态了，直接返回
		if pr.State != ProgressStateSnapshot {
			return
		}
		if !m.Reject {
			//*接收快照成功
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot succeeded, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		} else {
			//*接收快照失败
			pr.snapshotFailure()
			pr.becomeProbe()
			r.logger.Debugf("%x snapshot failed, resumed sending replication messages to %x [%s]", r.id, m.From, pr)
		}
		//*先暂停等待下一次被唤醒
		pr.pause()
	case pb.MsgUnreachable:
		//*检测到节点不可达
		if pr.State == ProgressStateReplicate {
			pr.becomeProbe()
		}
		r.logger.Debugf("%x failed to send message to %x because it is unreachable [%s]", r.id, m.From, pr)
		//*领导权转移
	case pb.MsgTransferLeader:
		leadTransferee := m.From
		lastLeadTransferee := r.leadTransferee
		if lastLeadTransferee != None {
			//*判断是否已经有相同节点的leader转让流程在进行中
			if lastLeadTransferee == leadTransferee {
				r.logger.Infof("%x [term %d] transfer leadership to %x is in progress, ignores request to same node %x",
					r.id, r.Term, leadTransferee, leadTransferee)
				//*如果是，直接返回
				return
			}
			//*否则中断之前的转让流程
			r.abortLeaderTransfer()
			r.logger.Infof("%x [term %d] abort previous transferring leadership to %x", r.id, r.Term, lastLeadTransferee)
		}
		//*判断是否转让过来的leader是否本节点，如果是也直接返回，因为本节点已经是leader了
		if leadTransferee == r.id {
			r.logger.Debugf("%x is already leader. Ignored transferring leadership to self", r.id)
			return
		}
		r.logger.Infof("%x [term %d] starts to transfer leadership to %x", r.id, r.Term, leadTransferee)
		r.electionElapsed = 0
		r.leadTransferee = leadTransferee
		if pr.Match == r.raftLog.lastIndex() {
			//*如果日志已经匹配了，那么就发送timeoutnow协议过去
			r.sendTimeoutNow(leadTransferee)
			r.logger.Infof("%x sends MsgTimeoutNow to %x immediately as %x already has up-to-date log", r.id, leadTransferee, leadTransferee)
		} else {
			//*否则继续追加日志
			r.sendAppend(leadTransferee)
		}
	}
}

// *处理候选人的状态机
func stepCandidate(r *raft, m pb.Message) {
	var myVoteRespType pb.MessageType
	//*如果是预选举
	if r.state == StatePreCandidate {
		myVoteRespType = pb.MsgPreVoteResp
	} else {
		myVoteRespType = pb.MsgVoteResp
	}

	//*以下转换成follower状态时，为什么不判断消息的term是否至少大于当前节点的term？？？
	switch m.Type {
	//*探测状态
	case pb.MsgProp:
		r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
		return
		//*添加日志的请求
	case pb.MsgApp:
		//*说明当前集群已经有领导者
		//*转换成follower状态
		r.becomeFollower(m.Term, m.From)
		//*开始添加日志
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		//*说明当前集群已经有领导者
		//*转换成follower状态
		r.becomeFollower(m.Term, m.From)
		//*开始处理心跳消息
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		//*说明当前集群已经有领导者
		//*转换成follower状态
		r.becomeFollower(m.Term, m.From)
		//*开始处理快照消息
		r.handleSnapshot(m)
	case myVoteRespType:
		//*计算当前集群中有多少节点给自己投了票
		gr := r.poll(m.From, m.Type, !m.Reject)
		r.logger.Infof("%x [quorum:%d] has received %d %s votes and %d vote rejections", r.id, r.quorum(), gr, m.Type, len(r.votes)-gr)
		switch r.quorum() {
		case gr: //*如果进行投票的节点数量正好是半数以上节点数量
			if r.state == StatePreCandidate {
				//*开始竞选
				r.campaign(campaignElection)
			} else {
				//*变成leader,因为当前分支当前节点的状态就是候选人,而且已经投票完成
				r.becomeLeader()
				r.bcastAppend()
				//*广播添加日志的请求
			}
		case len(r.votes) - gr: //*如果是半数以上节点拒绝了投票
			//*变成follower
			r.becomeFollower(r.Term, None)
		}
	case pb.MsgTimeoutNow:
		//*忽略不处理当前指令
		r.logger.Infof("%x [term %d state %v] ignored MsgTimeoutNow from %x [term %d]", r.id, r.Term, r.state, m.From, m.Term)

	}
}

// *处理follower状态机
func stepFollower(r *raft, m pb.Message) {
	switch m.Type {
	case pb.MsgProp:
		//*本节点提交的值,探测leader
		if r.lead == None {
			//*没有leader则提交失败，忽略,因为追随者只会探测leader
			r.logger.Infof("%x no leader at term %d; dropping proposal", r.id, r.Term)
			return
		}
		//*向leader进行redirect
		m.To = r.lead
		r.send(m)
	case pb.MsgApp:
		//*处理append消息
		//*收到leader的app消息，重置选举tick计时器，因为这样证明leader还存活
		r.electionElapsed = 0
		r.lead = m.From
		r.handleAppendEntries(m)
	case pb.MsgHeartbeat:
		//*处理心跳消息
		r.electionElapsed = 0
		r.lead = m.From
		r.handleHeartbeat(m)
	case pb.MsgSnap:
		//*处理快照消息
		r.electionElapsed = 0
		r.lead = m.From
		r.handleSnapshot(m)
	case pb.MsgTransferLeader:
		//*处理leader转让消息
		//*NOTE Raft 论文规定领导权转移必须由当前 Leader 发起（见论文 6.4 节）
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping leader transfer msg", r.id, r.Term)
			return
		}
		m.To = r.lead
		r.send(m)
	case pb.MsgTimeoutNow:
		//*处理超时消息
		//*由 Leader 主动发送给目标节点
		//*强制目标节点立即发起选举（跳过选举超时等待）
		//*用于加速领导权转移流程
		if r.promotable() {
			//*如果本节点可以提升为leader，那么就发起新一轮的竞选
			r.logger.Infof("%x [term %d] received MsgTimeoutNow from %x and starts an election to get leadership.", r.id, r.Term, m.From)
			//*timeout消息用在leader转让中，所以不需要prevote即使开了这个选项
			r.campaign(campaignTransfer)
		} else {
			r.logger.Infof("%x received MsgTimeoutNow from %x but is not promotable", r.id, m.From)
		}
	case pb.MsgReadIndex:
		if r.lead == None {
			r.logger.Infof("%x no leader at term %d; dropping index reading msg", r.id, r.Term)
			return
		}
		//*只读请求
		//*向leader转发此类型消息
		m.To = r.lead
		r.send(m)
	case pb.MsgReadIndexResp:
		//*来自于leader的响应
		if len(m.Entries) != 1 {
			r.logger.Errorf("%x invalid format of MsgReadIndexResp from %x, entries count: %d", r.id, m.From, len(m.Entries))
			return
		}
		//*更新readstates数组
		r.readStates = append(r.readStates, ReadState{Index: m.Index, RequestCtx: m.Entries[0].Data})

	}
}
