package raftmini

import (
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


//*重置raft的状态
//*reset(term uint64) 函数会在以下情况下被调用：
//*节点检测到更高的任期（收到更高任期的消息）。
//*领导者退回到跟随者角色。
//*节点启动新一轮选举（转换为候选者）。
//*节点初始化或从故障恢复。
//*NOTE每个节点都要有 Progress 吗？
//* 是的，在代码实现上，每个节点（包括 Follower）在 r.prs 中都有一个 Progress 条目。这是设计选择的结果，而非 Raft 算法的硬性要求。
//* Follower 节点需要 Progress 吗？
//* 不需要。从 Raft 算法的逻辑上看，Follower 不使用 Progress，它只在 Leader 角色下有意义。但在实现中，Follower 仍然持有 Progress，是为了在可能的角色切换（成为 Leader）时无缝过渡。
//* 为什么这样设计？
//* 这种设计是为了简化代码逻辑、保持状态一致性，并为动态角色切换做准备。尽管对 Follower 来说是“多余”的，但它在整体实现中提供了便利性和健壮性。
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


//*批量append一堆entries
