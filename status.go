package raftmini

import (
	"fmt"

	pb "github.com/gyy0727/raftmini/raftpb"
)

type Status struct {
	ID           uint64              //*节点ID
	pb.HardState                     //*节点硬状态
	SoftState         //*节点软状态
	Applied      uint64              //*已经应用到状态机的日志索引
	Progress     map[uint64]Progress //*节点的进度信息
}

// *获得当前节点的状态的拷贝
func getStatus(r *raft) Status {
	s := Status{ID: r.id}
	s.HardState = r.hardState()
	s.SoftState = *r.softState()

	s.Applied = r.raftLog.applied

	if s.RaftState == StateLeader {
		s.Progress = make(map[uint64]Progress)
		for id, p := range r.prs {
			s.Progress[id] = *p
		}
	}

	return s
}


// *将状态信息转换为json格式
func (s Status) MarshalJSON() ([]byte, error) {
	j := fmt.Sprintf(`{"id":"%x","term":%d,"vote":"%x","commit":%d,"lead":"%x","raftState":%q,"progress":{`,
		s.ID, s.Term, s.Vote, s.Commit, s.Lead, s.RaftState)

	if len(s.Progress) == 0 {
		j += "}}"
	} else {
		for k, v := range s.Progress {
			subj := fmt.Sprintf(`"%x":{"match":%d,"next":%d,"state":%q},`, k, v.Match, v.Next, v.State)
			j += subj
		}
		// remove the trailing ","
		j = j[:len(j)-1] + "}}"
	}
	return []byte(j), nil
}

// *将状态信息转换为字符串
func (s Status) String() string {
	b, err := s.MarshalJSON()
	if err != nil {
		raftLogger.Panicf("unexpected error: %v", err)
	}
	return string(b)
}
