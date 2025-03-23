package main

import (
	"fmt"
	"strings"

	"github.com/gyy0727/raftmini/raftpb"
)

var (
	default_cluster = "http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379"
	default_id      = 4
	default_join    = false
	default_port    = 42380
)

func main() {
	// cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	// id := flag.Int("id", 1, "node ID")
	// kvport := flag.Int("port", 9121, "key-value server port")
	// join := flag.Bool("join", false, "join an existing cluster")
	// flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	var kvs *kvstore

	fmt.Println("start raft node")
	commitC, errorC := newRaftNode(default_id, strings.Split(*&default_cluster, ","), *&default_join, proposeC, confChangeC)
	fmt.Println("start raft node")
	kvs = newKVStore(proposeC, commitC, errorC)
	fmt.Println("start kv server")
	serveHttpKVAPI(kvs, default_port, confChangeC, errorC)
}
