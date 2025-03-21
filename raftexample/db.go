package main
import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"github.com/gyy0727/raftmini/pkg/snap"
)



type kvstore struct {
	proposeC    chan<- string //* 提交更新
	mu          sync.RWMutex
	kvStore     map[string]string //*存储的键值对
	snapshotter *snap.Snapshotter
}


type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	s := &kvstore{proposeC: proposeC, kvStore: make(map[string]string), snapshotter: snapshotter}
	s.readCommits(commitC, errorC)
	go s.readCommits(commitC, errorC)
	return s
}

//*查找键对应的值
func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	v, ok := s.kvStore[key]
	s.mu.RUnlock()
	return v, ok
}

func (s *kvstore) Propose(k string, v string) {
	//*缓冲区
	var buf bytes.Buffer
	//*编码kvStore结构体并存入缓冲区
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	//*存入通道
	s.proposeC <- string(buf.Bytes())
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil { 
			//*如果是空数据就加载快照数据
			snapshot, err := s.snapshotter.Load()
			if err == snap.ErrNoSnapshot {
				return
			}
			if err != nil && err != snap.ErrNoSnapshot {
				log.Panic(err)
			}
			log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
				log.Panic(err)
			}
			continue
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.kvStore[dataKv.Key] = dataKv.Val
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
	}
}




//*序列化kvStore
func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return json.Marshal(s.kvStore)
}

//*反序列化kvstore
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.mu.Lock()
	s.kvStore = store
	s.mu.Unlock()
	return nil
}
