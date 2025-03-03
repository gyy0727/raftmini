// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package stats

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	raft "github.com/gyy0727/raftmini"
)

// ServerStats encapsulates various statistics about an EtcdServer and its
// communication with other members of the cluster
type ServerStats struct {
	Name       string         `json:"name"`      //*节点名称
	ID         string         `json:"id"`        //*节点raftid
	State      raft.StateType `json:"state"`     //*节点状态
	StartTime  time.Time      `json:"startTime"` //*节点启动时间
	LeaderInfo struct {
		Name      string    `json:"leader"`    //*leader名称
		Uptime    string    `json:"uptime"`    //*leader运行时间
		StartTime time.Time `json:"startTime"` //*leader启动时间
	} `json:"leaderInfo"`

	RecvAppendRequestCnt uint64      `json:"recvAppendRequestCnt,"`       //*接受到的append请求的计数
	RecvingPkgRate       float64     `json:"recvPkgRate,omitempty"`       //*接收速率
	RecvingBandwidthRate float64     `json:"recvBandwidthRate,omitempty"` //*接收带宽速率
	SendAppendRequestCnt uint64      `json:"sendAppendRequestCnt"`        //*发送的 Append 请求计数
	SendingPkgRate       float64     `json:"sendPkgRate,omitempty"`       //*发送速率
	SendingBandwidthRate float64     `json:"sendBandwidthRate,omitempty"` //*发送宽带速率速率
	sendRateQueue        *statsQueue //*用于计算发送的队列
	recvRateQueue        *statsQueue //*用与计算接收的队列

	sync.Mutex
}

// *将 ServerStats 结构体序列化为 JSON 格式
func (ss *ServerStats) JSON() []byte {
	ss.Lock()
	stats := *ss
	ss.Unlock()
	stats.LeaderInfo.Uptime = time.Since(stats.LeaderInfo.StartTime).String()
	stats.SendingPkgRate, stats.SendingBandwidthRate = stats.SendRates()
	stats.RecvingPkgRate, stats.RecvingBandwidthRate = stats.RecvRates()
	b, err := json.Marshal(stats)
	if err != nil {
		log.Printf("stats: error marshalling server stats: %v", err)
	}
	return b
}

// *初始化 ServerStats，重置启动时间和 Leader 信息，并初始化发送和接收速率队列
func (ss *ServerStats) Initialize() {
	if ss == nil {
		return
	}
	now := time.Now()
	ss.StartTime = now
	ss.LeaderInfo.StartTime = now
	ss.sendRateQueue = &statsQueue{
		back: -1,
	}
	ss.recvRateQueue = &statsQueue{
		back: -1,
	}
}

//*计算并返回接收速率
func (ss *ServerStats) RecvRates() (float64, float64) {
	return ss.recvRateQueue.Rate()
}

//*计算并返回发送速率
func (ss *ServerStats) SendRates() (float64, float64) {
	return ss.sendRateQueue.Rate()
}


func (ss *ServerStats) RecvAppendReq(leader string, reqSize int) {
	ss.Lock()
	defer ss.Unlock()

	now := time.Now()

	ss.State = raft.StateFollower
	if leader != ss.LeaderInfo.Name {
		ss.LeaderInfo.Name = leader
		ss.LeaderInfo.StartTime = now
	}

	ss.recvRateQueue.Insert(
		&RequestStats{
			SendingTime: now,
			Size:        reqSize,
		},
	)
	ss.RecvAppendRequestCnt++
}


func (ss *ServerStats) SendAppendReq(reqSize int) {
	ss.Lock()
	defer ss.Unlock()

	now := time.Now()

	if ss.State != raft.StateLeader {
		ss.State = raft.StateLeader
		ss.LeaderInfo.Name = ss.ID
		ss.LeaderInfo.StartTime = now
	}

	ss.sendRateQueue.Insert(
		&RequestStats{
			SendingTime: now,
			Size:        reqSize,
		},
	)

	ss.SendAppendRequestCnt++
}

func (ss *ServerStats) BecomeLeader() {
	ss.Lock()
	defer ss.Unlock()

	if ss.State != raft.StateLeader {
		ss.State = raft.StateLeader
		ss.LeaderInfo.Name = ss.ID
		ss.LeaderInfo.StartTime = time.Now()
	}
}
