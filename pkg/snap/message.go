package snap

import (
	"io"

	"github.com/gyy0727/raftmini/pkg/ioutil"
	"github.com/gyy0727/raftmini/raftpb"
)

// *用于处理 Raft 协议中的快照消息（MsgSnap）
type Message struct {
	raftpb.Message               //*Raft 协议的消息，类型必须是 MsgSnap
	ReadCloser     io.ReadCloser //*用于读取快照数据的 io.ReadCloser 接口
	TotalSize      int64         //*消息的总大小，包括 Raft 消息和快照数据
	closeC         chan bool     //*一个通道，用于通知消息发送是否成功
}

func NewMessage(rs raftpb.Message, rc io.ReadCloser, rcSize int64) *Message {
	return &Message{
		Message:    rs,
		ReadCloser: ioutil.NewExactReadCloser(rc, rcSize),
		TotalSize:  int64(rs.Size()) + rcSize,
		closeC:     make(chan bool, 1),
	}
}

//*用于通知消息发送是否成功
func (m Message) CloseNotify() <-chan bool {
	return m.closeC
}

//*用于关闭 ReadCloser 并通知消息发送结果
func (m Message) CloseWithError(err error) {
	if cerr := m.ReadCloser.Close(); cerr != nil {
		err = cerr
	}
	if err == nil {
		m.closeC <- true
	} else {
		m.closeC <- false
	}
}
