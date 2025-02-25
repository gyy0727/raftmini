package raftmini

import (
	"errors"

	pb "github.com/gyy0727/raftmini/raftpb"
)

//*当尝试通过Step()方法处理MsgHup/MsgBeat等本地消息时
var ErrStepLocalMsg = errors.New("raft: cannot step raft local message")

//*处理消息时发现来源节点不在当前集群配置中
var ErrStepPeerNotFound = errors.New("raft: cannot step as peer not found")


type RawNode struct {
	raft       *raft
	prevSoftSt *SoftState
	prevHardSt pb.HardState
}

func (rn *RawNode) newReady() Ready {
	return newReady(rn.raft, rn.prevSoftSt, rn.prevHardSt)
}

