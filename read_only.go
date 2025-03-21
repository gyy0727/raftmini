package raftmini


import pb "github.com/gyy0727/raftmini/raftpb"

//* TAG  实现线性一致性


//*用于保存读请求到来时的节点状态
type ReadState struct {
	//*保存接收该读请求时的committed index
	Index      uint64
	//*保存读请求ID，全局唯一的定义一次读请求
	RequestCtx []byte
}


//*用于追踪leader向follower发送的心跳信息
type readIndexStatus struct {
	//* 保存原始的readIndex请求消息
	req   pb.Message
	//* 保存收到该readIndex请求时的leader commit索引
	index uint64
	//* 保存有什么节点进行了应答，从这里可以计算出来是否有超过半数应答了
	acks  map[uint64]struct{}
}


//*readOnly用于管理全局的readIndx数据
type readOnly struct {
	//*是否开启线性一致读
	option           ReadOnlyOption
	//* 使用entry的数据为key，保存当前pending的readIndex状态
	pendingReadIndex map[string]*readIndexStatus
	//* 保存entry数据的队列，pending的readindex状态在这个队列中进行排队
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}


//*方法将一个只读请求添加到 readOnly 结构体中
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	ctx := string(m.Entries[0].Data)
	//* 判断是否重复添加
	if _, ok := ro.pendingReadIndex[ctx]; ok {
		return
	}
	ro.pendingReadIndex[ctx] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)
}


//*收到某个节点对一个HB消息的应答，这个函数中尝试查找该消息是否在readonly数据中
func (ro *readOnly) recvAck(m pb.Message) int {
	//*根据context内容到map中进行查找
	rs, ok := ro.pendingReadIndex[string(m.Context)]
	if !ok {
		//*找不到就返回，说明这个HB消息没有带上readindex相关的数据
		return 0
	}

	//*将该消息的ack map加上该应答节点
	rs.acks[m.From] = struct{}{}
	//*add one to include an ack from local node
	//*返回当前ack的节点数量，+1是因为包含了leader
	return len(rs.acks) + 1
}





//*advance 方法用于推进 readOnly 结构体中维护的只读请求队列。
//*它会出队请求，直到找到与给定消息 m 具有相同上下文的只读请求。
//*在确保某个心跳消息被集群中半数以上节点应答后，尝试在 readIndexQueue 中查找可以丢弃的只读请求数据。
//*最后返回被丢弃的数据队列
//*在确保某HB消息被集群中半数以上节点应答了，此时尝试在readindex队列中查找，看一下队列中的readindex数据有哪些可以丢弃了（也就是已经被应答过了）
//*最后返回被丢弃的数据队列
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
			//* 不可能出现在map中找不到该数据的情况
			panic("cannot find corresponding read state from pending map")
		}
		//* 都加入应答队列中，后面用于根据这个队列的数据来删除pendingReadIndex中对应的数据
		rss = append(rss, rs)
		if okctx == ctx {
			//* 找到了就终止循环
			found = true
			break
		}
	}

	if found {
		//* 找到了，就丢弃在这之前的队列readonly数据了
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			//* 遍历队列从map中删除
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

//*从readonly队列中返回最后一个数据
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}
	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
