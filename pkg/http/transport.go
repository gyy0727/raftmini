package http

import (
	"context"
	"net/http"
	"time"

	raft "github.com/gyy0727/raftmini"
	"github.com/gyy0727/raftmini/pkg/snap"
	"github.com/gyy0727/raftmini/raftpb"
	"golang.org/x/time/rate"
)

type Raft interface {
	Process(ctx context.Context, m raftpb.Message) error  //*处理消息
	IsIDRemoved(id uint64) bool                           //*检查id是否还存在于集群中
	ReportUnreachable(id uint64)                          //*报告节点不可达
	ReportSnapshot(id uint64, status raft.SnapshotStatus) //*报告快照状态
}

type Transporter interface {
	//*开启http传输器
	Start() error

	Handler() http.Handler

	Send(m []raftpb.Message)

	AddPeer(id int, urls []string)

	RemovePeer(id int)

	Stop()
}

type Transport struct {
	DialTimeout        time.Duration     //*连接超时
	DialRetryFrequency rate.Limit        //*限流器
	ID                 int               //*当前节点id
	URLs               string            //*当前节点的网址
	ClusterID          int               //*集群id
	Raft               Raft              //*raft实例
	Snapshotter        *snap.Snapshotter //*快照管理器
	ErrorC             chan error
}
