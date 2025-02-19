package raftmini

import (
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"
)

// *表示竞选的类型,在 Raft 协议中，竞选是指节点尝试成为领导者的过程
// *使用 string 类型的原因是字符串比较和填充 Raft 条目更简单
type CampaignType string

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

// *C++ using ReadOnlyOption = int;
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


