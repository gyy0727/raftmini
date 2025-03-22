package http

import (
	"bytes"
	"context"
	"fmt"
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

func (t *Transport) Start() error {
	t.DialTimeout = 10 * time.Second
	return nil
}

func (t *Transport) Handler() http.Handler {
	// 创建 RaftHandler 实例
	raftHandler := RaftHandler{
		Raft_: t.Raft,
	}
	mux := http.NewServeMux()
	mux.Handle(RaftPrefix, raftHandler)
	return mux
}

func (t *Transport) Send(messages []raftpb.Message) {
	for _, msg := range messages {
		to := msg.To
		if t.Raft.IsIDRemoved(to) {
			continue
		}

		url, ok := peers.Urls[to]
		if !ok {
			t.ErrorC <- fmt.Errorf("peer %d not found", to)
			continue
		}

		// 序列化消息
		body, err := (&msg).Marshal()
		if err != nil {
			t.ErrorC <- fmt.Errorf("failed to marshal message: %v", err)
			continue
		}

		// 创建 HTTP 请求
		newURL := url.JoinPath("/raft") // 将路径附加到 URL
		req, err := http.NewRequest(http.MethodPost, newURL.String(), bytes.NewBuffer(body))
		if err != nil {
			t.ErrorC <- fmt.Errorf("failed to create request: %v", err)
			continue
		}
		req.Header.Set("Content-Type", "application/json")

		// 发送请求
		client := &http.Client{Timeout: t.DialTimeout}
		resp, err := client.Do(req)
		if err != nil {
			t.ErrorC <- fmt.Errorf("failed to send message to %d: %v", to, err)
			t.Raft.ReportUnreachable(to)
			continue
		}
		defer resp.Body.Close()

		// 检查响应状态码
		if resp.StatusCode != http.StatusOK {
			t.ErrorC <- fmt.Errorf("received non-OK response from %d: %s", to, resp.Status)
		}
	}
}

func (t *Transport) Stop() {
	// 关闭错误通道，表示传输器已停止
	if t.ErrorC != nil {
		close(t.ErrorC)
	}
}
