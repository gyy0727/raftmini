

package main

import (
	"flag"
	"strings"

	"github.com/gyy0727/raftmini/raftpb"
)

func main() {
	cluster := flag.String("cluster", "http://127.0.0.1:9021", "comma separated cluster peers")
	id := flag.Int("id", 1, "node ID")
	kvport := flag.Int("port", 9121, "key-value server port")
	join := flag.Bool("join", false, "join an existing cluster")
	flag.Parse()

	proposeC := make(chan string)
	defer close(proposeC)
	confChangeC := make(chan raftpb.ConfChange)
	defer close(confChangeC)

	// raft provides a commit stream for the proposals from the http api
	commitC, errorC := newRaftNode(*id, strings.Split(*cluster, ","), *join, proposeC, confChangeC)

	// the key-value http handler will propose updates to raft
	serveHttpKVAPI(*kvport, proposeC, confChangeC, commitC, errorC)
}
