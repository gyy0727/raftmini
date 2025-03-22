package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"
	"sync"

	"github.com/gyy0727/raftmini/pkg/snap"
	"github.com/tidwall/buntdb"
)

type kvstore struct {
	proposeC chan<- string //* 提交更新
	mu       sync.RWMutex
	// kvStore     map[string]string
	kvStore      *buntdb.DB //*存储的键值对
	commitIndex  uint64     //* 最后提交的索引
	appliedIndex uint64     //* 最后应用的索引
	snapshotter  *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore( proposeC chan<- string, commitC <-chan *string, errorC <-chan error) *kvstore {
	fmt.Println("kvstore init")
	conn, err := buntdb.Open(":memory:")
	if err != nil {
		log.Fatal(err)
	}
	s := &kvstore{proposeC: proposeC, kvStore: conn}
	s.readCommits(commitC, errorC)
	go s.readCommits(commitC, errorC)
	fmt.Println("kvstore init")
	return s
}

// *查找键对应的值
func (s *kvstore) Lookup(key string) (string, bool) {
	s.mu.RLock()
	var value string
	err := s.kvStore.View(func(tx *buntdb.Tx) error {
		v, err := tx.Get(key)
		if err != nil {
			return err
		}
		value = v // 将查询到的值赋值给外部变量
		return nil
	})
	var ok bool
	ok = true
	if err != nil {
		ok = false
	}
	s.mu.RUnlock()
	return value, ok
}

func (s *kvstore) Update(k string, v string) {
	s.kvStore.Update(func(tx *buntdb.Tx) error {
		_, _, err := tx.Set(k, v, nil)
		if err != nil {
			fmt.Printf("Failed to insert key-value pair: key=%s, value=%s, error=%v\n", k, v, err)
			return err
		}
		fmt.Printf("Successfully inserted key-value pair: key=%s, value=%s\n", k, v)
		return nil
	})
}


func (s *kvstore) Propose(k string, v string) {
	fmt.Println("kvstore propose")
	//*缓冲区
	var buf bytes.Buffer
	//*编码kvStore结构体并存入缓冲区
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	//*存入通道
	s.proposeC <- string(buf.Bytes())
	fmt.Println("kvstore propose 已经写入 proposeC")
}

func (s *kvstore) readCommits(commitC <-chan *string, errorC <-chan error) {
	for data := range commitC {
		if data == nil {
			//*如果是空数据就加载快照数据
			// snapshot, err := s.snapshotter.Load()
			// if err == snap.ErrNoSnapshot {
			// 	return
			// }
			// if err != nil && err != snap.ErrNoSnapshot {
			// 	log.Panic(err)
			// }
			// log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
			// if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			// 	log.Panic(err)
			// }
			// continue
			return
		}

		var dataKv kv
		dec := gob.NewDecoder(bytes.NewBufferString(*data))
		if err := dec.Decode(&dataKv); err != nil {
			log.Fatalf("raftexample: could not decode message (%v)", err)
		}
		s.mu.Lock()
		s.Update(dataKv.Key, dataKv.Val)
		s.mu.Unlock()
	}
	if err, ok := <-errorC; ok {
		log.Fatal(err)
		s.kvStore.Close()
	}
}

// *序列化kvStore
func (s *kvstore) getSnapshot() ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// 创建一个字节缓冲区
	var buf bytes.Buffer

	// 将数据库内容保存到缓冲区
	err := s.kvStore.Save(&buf)
	if err != nil {
		return nil, fmt.Errorf("failed to save database: %w", err)
	}

	// 返回缓冲区中的字节
	return buf.Bytes(), nil
}

// *反序列化kvstore
func (s *kvstore) recoverFromSnapshot(snapshot []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 创建一个字节缓冲区
	buf := bytes.NewBuffer(snapshot)

	// 从字节流中加载数据到数据库
	err := s.kvStore.Load(buf)
	if err != nil {
		return fmt.Errorf("failed to load snapshot: %w", err)
	}

	return nil
}
