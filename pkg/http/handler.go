package http

import (
	"context"
	"io"
	"net/http"
	"time"

	proto "github.com/golang/protobuf/proto"
	"github.com/gyy0727/raftmini/raftpb"
)

type RaftHandler struct {
	body  raftpb.Message
	Raft_ Raft
}

func (h RaftHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodPut {
		w.WriteHeader(http.StatusMethodNotAllowed)
		w.Write([]byte("Only POST or PUT requests are allowed"))
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to read request body"))
		return
	}
	defer r.Body.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	message := &raftpb.Message{}
	if err := proto.Unmarshal(body, message); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Failed to unmarshal request body"))
		return
	}
	if err := h.Raft_.Process(ctx, *message); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to process message"))
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Message received and processed successfully"))
}

