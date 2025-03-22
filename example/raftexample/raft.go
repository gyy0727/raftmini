package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"net/http"
	"net/url"

	raft "github.com/gyy0727/raftmini"
	rafthttp "github.com/gyy0727/raftmini/pkg/http"
	"github.com/gyy0727/raftmini/pkg/stats"
	"github.com/gyy0727/raftmini/raftpb"
	"github.com/gyy0727/raftmini/wal"
	"github.com/gyy0727/raftmini/wal/walpb"
	"golang.org/x/net/context"
)

// *基于Raft共识算法实现的键值数据流
type raftNode struct {
	proposeC    <-chan string            //*接收客户端提出的新日志条目（通常包含键值对操作），用于写入 Raft 日志
	confChangeC <-chan raftpb.ConfChange //*接收集群配置变更请求（如新增/移除节点），用于动态调整 Raft 集群成员
	commitC     chan<- *string           //*将已提交的日志条目传递给上层应用执行（如状态机应用操作）
	errorC      chan<- error             //*报告 Raft 内部发生的错误
	id          int                      //*节点id
	peers       []string                 //*集群中所有节点的网络地址列表（例如 ["http://node1:2379", "http://node2:2379"]）
	join        bool                     //*标识当前节点是否以「加入者」身份启动（连接到已有集群，而非初始化新集群）
	waldir      string                   //*Write-Ahead Log (WAL) 的存储目录，用于持久化未提交的日志条目
	lastIndex   uint64                   //*index of log at start
	node        raft.Node                //*Raft 协议的核心实现，管理领导选举、日志复制、状态机提交等核心逻辑
	raftStorage *raft.MemoryStorage      //*内存中的 Raft 日志存储，缓存未持久化的日志条目和元数据
	wal         *wal.WAL                 //*持久化存储的 Write-Ahead Log，确保日志在节点崩溃后能恢复。
	transport   *rafthttp.Transport      //*HTTP 网络传输层，用于节点间心跳、日志复制等通信
	stopc       chan struct{}            //*关闭信号通道，用于优雅停止提案处理
	httpstopc   chan struct{}            //*关闭信号通道，用于停止 HTTP 服务
	httpdonec   chan struct{}            //*通知通道，确认 HTTP 服务已完全关闭
}

// *默认一万条日志才触发日志压缩
var defaultSnapCount uint64 = 10000

func newRaftNode(id int, peers []string, join bool, proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error) {

	commitC := make(chan *string)
	errorC := make(chan error)
	//遍历peers
	for i := range peers {
		fmt.Println(peers[i])
		time.Sleep(1*time.Second)
	}
	rc := &raftNode{
		proposeC:    proposeC,
		confChangeC: confChangeC,
		commitC:     commitC,
		errorC:      errorC,
		id:          id,
		peers:       peers,
		join:        join,
		waldir:      fmt.Sprintf("raftexample-%d", id),
		raftStorage: raft.NewMemoryStorage(),
		stopc:       make(chan struct{}),
		httpstopc:   make(chan struct{}),
		httpdonec:   make(chan struct{}),
	}
	go rc.startRaft()
	return commitC, errorC
}

func (rc *raftNode) publishEntries(ents []raftpb.Entry) bool {
	for i := range ents {
		switch ents[i].Type {
		case raftpb.EntryNormal:
			//*普通日志条目
			if len(ents[i].Data) == 0 {
				break

			}
			s := string(ents[i].Data)
			select {
			case rc.commitC <- &s:
			case <-rc.stopc:
				return false
			}
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			cc.Unmarshal(ents[i].Data)
			rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					rc.transport.AddPeer(cc.NodeID, []string{string(cc.Context)})
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == uint64(rc.id) {
					log.Println("I've been removed from the cluster! Shutting down.")
					return false
				}
				rc.transport.RemovePeer(cc.NodeID)
			}
		}
		if ents[i].Index == rc.lastIndex {
			select {
			//*标识所有日志回放已经完成
			case rc.commitC <- nil:
			case <-rc.stopc:
				return false
			}
		}
	}
	return true
}

// *返回对应目录下的wal实例
func (rc *raftNode) openWAL() *wal.WAL {
	if !wal.Exist(rc.waldir) {
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}

		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	w, err := wal.Open(rc.waldir, walpb.Snapshot{})
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}

	return w
}

// *Raft节点在启动或恢复时用于重放预写日志（WAL）以重建状态的关键步骤
func (rc *raftNode) replayWAL() *wal.WAL {
	w := rc.openWAL()
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	// append to storage so raft starts at the right place in log
	rc.raftStorage.Append(ents)
	// send nil once lastIndex is published so client knows commit channel is current
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
	rc.raftStorage.SetHardState(st)
	return w
}

// *该函数是Raft节点的“紧急停止按钮”，确保在错误发生时：
// *停止接收外部请求
// *清理内部通信渠道
// *上报错误信息
// *安全释放所有资源
func (rc *raftNode) writeError(err error) {
	rc.stopHTTP()
	close(rc.commitC)
	rc.errorC <- err
	close(rc.errorC)
	rc.node.Stop()
}

// *启动raft
func (rc *raftNode) startRaft() {
	oldwal := wal.Exist(rc.waldir)
	rc.wal = rc.replayWAL()

	rpeers := make([]raft.Peer, len(rc.peers))

	for i := range rpeers {
		rpeers[i] = raft.Peer{ID: uint64(i + 1)}
		//*TODO
		fmt.Println("添加peers***********", rpeers[i])
		time.Sleep(time.Second)
	}
	c := &raft.Config{
		ID:              uint64(rc.id),
		ElectionTick:    10,
		HeartbeatTick:   1,
		Storage:         rc.raftStorage,
		MaxSizePerMsg:   1024 * 1024,
		MaxInflightMsgs: 256,
	}

	if oldwal {
		fmt.Println("+++++ old val +++++")
		time.Sleep(2 * time.Second)
		rc.node = raft.RestartNode(c)
	} else {
		fmt.Println("+++++ new val +++++")
		time.Sleep(2 * time.Second)
		startPeers := rpeers
		if rc.join {
			startPeers = nil
		}
		rc.node = raft.StartNode(c, startPeers)
	}

	ss := &stats.ServerStats{}
	ss.Initialize()

	rc.transport = &rafthttp.Transport{
		ID:        rc.id,
		ClusterID: 0x1000,
		Raft:      rc,
		ErrorC:    make(chan error),
	}

	rc.transport.Start()
	for i := range rc.peers {
		if i+1 != rc.id {
			rc.transport.AddPeer(uint64(i+1), []string{rc.peers[i]})
		}
	}

	go rc.serveRaft()
	go rc.serveChannels()
}

// * stop closes http, closes all channels, and stops raft
func (rc *raftNode) stop() {
	rc.stopHTTP()
	close(rc.commitC)
	close(rc.errorC)
	rc.node.Stop()
}

func (rc *raftNode) stopHTTP() {
	rc.transport.Stop()
	close(rc.httpstopc)
	<-rc.httpdonec
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *raftNode) serveChannels() {
	defer rc.wal.Close()
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	go func() {
		var confChangeCount uint64 = 0
		for rc.proposeC != nil && rc.confChangeC != nil {
			select {
			case prop, ok := <-rc.proposeC:
				if !ok {
					rc.proposeC = nil
				} else {
					rc.node.Propose(context.TODO(), []byte(prop))
				}
			case cc, ok := <-rc.confChangeC:
				if !ok {
					rc.confChangeC = nil
				} else {
					confChangeCount += 1
					cc.ID = confChangeCount
					rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		close(rc.stopc)
	}()
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		case rd := <-rc.node.Ready():
			//*将HardState，entries写入持久化存储中
			rc.wal.Save(rd.HardState, rd.Entries)
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rd.CommittedEntries); !ok {
				rc.stop()
				return
			}
			rc.node.Advance()
		case err := <-rc.transport.ErrorC:
			rc.writeError(err)
			return

		case <-rc.stopc:
			rc.stop()
			return
		}
	}
}

func (rc *raftNode) serveRaft() {
	url, err := url.Parse(rc.peers[rc.id-1])
	if err != nil {
		log.Fatalf("raftexample: Failed parsing URL (%v)", err)
	}

	ln, err := newStoppableListener(url.Host, rc.httpstopc)
	if err != nil {
		log.Fatalf("raftexample: Failed to listen rafthttp (%v)", err)
	}

	err = (&http.Server{Handler: rc.transport.Handler()}).Serve(ln)
	select {
	case <-rc.httpstopc:
	default:
		log.Fatalf("raftexample: Failed to serve rafthttp (%v)", err)
	}
	close(rc.httpdonec)
}

func (rc *raftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *raftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (rc *raftNode) ReportUnreachable(id uint64) {

}
func (rc *raftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {

}
