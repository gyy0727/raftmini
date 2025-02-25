package raftexample
import (
	"io/ioutil"
	"log"
	"net/http"
	"strconv"

	"github.com/coreos/etcd/raft/raftpb"
)


type httpKVAPI struct {
	store       *kvstore
	confChangeC chan<- raftpb.ConfChange
}
