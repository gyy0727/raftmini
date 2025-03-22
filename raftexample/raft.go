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
	"github.com/gyy0727/raftmini/pkg/snap"
	"github.com/gyy0727/raftmini/pkg/stats"
	"github.com/gyy0727/raftmini/raftpb"
	"github.com/gyy0727/raftmini/wal"
	"github.com/gyy0727/raftmini/wal/walpb"
	"golang.org/x/net/context"
)

// *基于Raft共识算法实现的键值数据流
type raftNode struct {
	proposeC         <-chan string            //*接收客户端提出的新日志条目（通常包含键值对操作），用于写入 Raft 日志
	confChangeC      <-chan raftpb.ConfChange //*接收集群配置变更请求（如新增/移除节点），用于动态调整 Raft 集群成员
	commitC          chan<- *string           //*将已提交的日志条目传递给上层应用执行（如状态机应用操作）
	errorC           chan<- error             //*报告 Raft 内部发生的错误
	id               int                      //*节点id
	peers            []string                 //*集群中所有节点的网络地址列表（例如 ["http://node1:2379", "http://node2:2379"]）
	join             bool                     //*标识当前节点是否以「加入者」身份启动（连接到已有集群，而非初始化新集群）
	waldir           string                   //*Write-Ahead Log (WAL) 的存储目录，用于持久化未提交的日志条目
	snapdir          string                   //*快照（Snapshot）存储目录，用于定期压缩已提交的日志，提升恢复效率
	getSnapshot      func() ([]byte, error)   //*回调函数，用于生成当前状态机的快照数据
	lastIndex        uint64                   //*日志的最后一个索引号
	confState        raftpb.ConfState         //*当前节点配置状态
	snapshotIndex    uint64                   //*最后一次快照对应的日志索引
	appliedIndex     uint64                   //*已应用到状态机的最高日志索引
	node             raft.Node                //*Raft 协议的核心实现，管理领导选举、日志复制、状态机提交等核心逻辑
	raftStorage      *raft.MemoryStorage      //*内存中的 Raft 日志存储，缓存未持久化的日志条目和元数据
	wal              *wal.WAL                 //*持久化存储的 Write-Ahead Log，确保日志在节点崩溃后能恢复。
	snapshotter      *snap.Snapshotter        //*快照管理器，负责生成/加载快照以压缩日志历史
	snapshotterReady chan *snap.Snapshotter   //*异步通知通道，表示快照管理器已初始化完成
	snapCount        uint64                   //*触发快照的日志条目数量阈值（例如每 1000 条日志生成一次快照）
	transport        *rafthttp.Transport      //*HTTP 网络传输层，用于节点间心跳、日志复制等通信
	stopc            chan struct{}            //*关闭信号通道，用于优雅停止提案处理
	httpstopc        chan struct{}            //*关闭信号通道，用于停止 HTTP 服务
	httpdonec        chan struct{}            //*通知通道，确认 HTTP 服务已完全关闭
}

// *默认一万条日志才触发日志压缩
var defaultSnapCount uint64 = 10000

func newRaftNode(id int, peers []string, join bool, getSnapshot func() ([]byte, error), proposeC <-chan string,
	confChangeC <-chan raftpb.ConfChange) (<-chan *string, <-chan error, <-chan *snap.Snapshotter) {

	commitC := make(chan *string)
	errorC := make(chan error)

	rc := &raftNode{
		proposeC:         proposeC,
		confChangeC:      confChangeC,
		commitC:          commitC,
		errorC:           errorC,
		id:               id,
		peers:            peers,
		join:             join,
		waldir:           fmt.Sprintf("raftexample-%d", id),
		snapdir:          fmt.Sprintf("raftexample-%d-snap", id),
		getSnapshot:      getSnapshot,
		snapCount:        defaultSnapCount,
		stopc:            make(chan struct{}),
		httpstopc:        make(chan struct{}),
		httpdonec:        make(chan struct{}),
		snapshotterReady: make(chan *snap.Snapshotter, 1),
	}
	rc.snapshotter = snap.New(rc.snapdir)
	go rc.startRaft()
	return commitC, errorC, rc.snapshotterReady
}

// *保存快照
func (rc *raftNode) saveSnap(snap raftpb.Snapshot) error {
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	//*将快照元数据写入 WAL 文件
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	//*释放 WAL 中早于该快照索引的日志条目占用的资源（如文件句柄）
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

// *传入一个commited entries数组，返回其中需要进行apply操作的数据数组
func (rc *raftNode) entriesToApply(ents []raftpb.Entry) (nents []raftpb.Entry) {
	//*空数组检查
	if len(ents) == 0 {
		return
	}
	//*拿到传入的第一个数据的索引
	firstIdx := ents[0].Index
	//*检查合法性，committed数组的第一个索引不能大于applied+1
	if firstIdx > rc.appliedIndex+1 {
		log.Fatalf("first index of committed entry[%d] should <= progress.appliedIndex[%d] 1", firstIdx, rc.appliedIndex)
	}
	//*committed数组中有没有applied的数组，就进行apply
	if rc.appliedIndex-firstIdx+1 < uint64(len(ents)) {
		nents = ents[rc.appliedIndex-firstIdx+1:]
	}
	return
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
			rc.confState = *rc.node.ApplyConfChange(cc)
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
		rc.appliedIndex = ents[i].Index
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

// *加载快照
func (rc *raftNode) loadSnapshot() *raftpb.Snapshot {
	snapshot, err := rc.snapshotter.Load()
	if err != nil && err != snap.ErrNoSnapshot {
		log.Fatalf("raftexample: error loading snapshot (%v)", err)
	}
	return snapshot
}

// *返回对应目录下的wal实例
func (rc *raftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	//*判断目录是否存在
	if !wal.Exist(rc.waldir) {
		//*不存在就创建目录
		if err := os.Mkdir(rc.waldir, 0750); err != nil {
			log.Fatalf("raftexample: cannot create dir for wal (%v)", err)
		}
		//*根据wal目录创建wal结构体
		w, err := wal.Create(rc.waldir, nil)
		if err != nil {
			log.Fatalf("raftexample: create wal error (%v)", err)
		}
		w.Close()
	}

	walsnap := walpb.Snapshot{}
	if snapshot != nil {
		walsnap.Index, walsnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	log.Printf("loading WAL at term %d and index %d", walsnap.Term, walsnap.Index)
	w, err := wal.Open(rc.waldir, walsnap)
	if err != nil {
		log.Fatalf("raftexample: error loading wal (%v)", err)
	}
	//*返回wal实例
	return w
}

// *Raft节点在启动或恢复时用于重放预写日志（WAL）以重建状态的关键步骤
func (rc *raftNode) replayWAL() *wal.WAL {
	log.Printf("replaying WAL of member %d", rc.id)
	//*先获得快照
	snapshot := rc.loadSnapshot()
	if snapshot == nil {
		return nil
	}
	//*打开WAL并读取数据
	w := rc.openWAL(snapshot)
	_, st, ents, err := w.ReadAll()
	if err != nil {
		log.Fatalf("raftexample: failed to read WAL (%v)", err)
	}
	//*创建新的内存存储（MemoryStorage），替换旧存储，确保从干净状态开始
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		rc.raftStorage.ApplySnapshot(*snapshot)
	}
	rc.raftStorage.SetHardState(st)
	rc.raftStorage.Append(ents)
	if len(ents) > 0 {
		rc.lastIndex = ents[len(ents)-1].Index
	} else {
		rc.commitC <- nil
	}
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
		rc.node = raft.RestartNode(c)
	} else {
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

// *用于将快照应用到状态机并更新节点状态
func (rc *raftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	//*检查快照有效性
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}

	log.Printf("publishing snapshot at index %d", rc.snapshotIndex)
	defer log.Printf("finished publishing snapshot at index %d", rc.snapshotIndex)
	//*快照索引必须严格大于当前已应用索引
	//*快照代表最新状态，应用旧快照会导致状态回滚，破坏一致性。
	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		log.Fatalf("snapshot index [%d] should > progress.appliedIndex [%d] + 1", snapshotToSave.Metadata.Index, rc.appliedIndex)
	}
	//*发送空数据通知kvstore加载快照数据
	//*向提交通道发送 nil 作为信号，通知状态机（如 kvstore）加载快照
	rc.commitC <- nil //*trigger kvstore to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

// *用于在 Raft 节点中触发快照创建和日志压缩
func (rc *raftNode) maybeTriggerSnapshot() {
	//*当已应用的日志条目数（appliedIndex - snapshotIndex）超过阈值 snapCount 时触发快照
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	log.Printf("start snapshot [applied index: %d | last snapshot index: %d]", rc.appliedIndex, rc.snapshotIndex)
	data, err := rc.getSnapshot()
	if err != nil {
		log.Panic(err)
	}
	//*生成快照对象
	snap, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		panic(err)
	}
	//*保存快照
	if err := rc.saveSnap(snap); err != nil {
		panic(err)
	}
	//*计算压缩索引,确定日志压缩的截止位置
	//*NOTE表示至少保留索引 1 的日志（防止空日志）
	compactIndex := uint64(1)
	//*appliedIndex当前已应用索引 snapshotCatchUpEntriesN预设的保留条目数
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	//*删除compactIndex之前的日志
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		panic(err)
	}

	log.Printf("compacted log at index %d", compactIndex)
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *raftNode) serveChannels() {
	snap, err := rc.raftStorage.Snapshot()
	if err != nil {
		panic((err))
	}
	rc.confState = snap.Metadata.ConfState
	rc.snapshotIndex = snap.Metadata.Index
	rc.appliedIndex = snap.Metadata.Index

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
			if !raft.IsEmptySnap(rd.Snapshot) {
				//*如果快照数据不为空，也需要保存快照数据到持久化存储中
				rc.saveSnap(rd.Snapshot)
				rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			rc.raftStorage.Append(rd.Entries)
			rc.transport.Send(rd.Messages)
			if ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries)); !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot()
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
