package raftmini

import (
	"testing"

	pb "github.com/gyy0727/raftmini/raftpb"
)

func TestStepLeader(t *testing.T) {
	r := &raft{
		id:       1,
		Term:     1,
		state:    StateLeader,
		readOnly: newReadOnly(ReadOnlySafe),
		raftLog:  newLog(newMemoryStorage(), nil),
		prs:      make(map[uint64]*Progress),
	}
	r.becomeLeader()
	r.prs[1] = &Progress{}

	tests := []struct {
		name     string
		message  pb.Message
		wantType pb.MessageType
	}{}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
		})
	}
}
