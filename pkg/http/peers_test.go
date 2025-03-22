package http

import (
	"fmt"
	"testing"
)

func TestPeers(t *testing.T) {
	transport := &Transport{}

	// 测试 AddPeer
	fmt.Println("=== Testing AddPeer ===")
	testAddPeer(t, transport, 1, []string{"http://example.com"}, false) // 期望成功
	testAddPeer(t, transport, 2, []string{"http://example.org"}, false) // 期望成功
	testAddPeer(t, transport, 3, []string{"invalid-url"}, true)         // 期望失败
	testAddPeer(t, transport, 4, []string{"http://example.com", "http://example.org"}, false) // 期望成功

	// 测试 RemovePeer
	fmt.Println("\n=== Testing RemovePeer ===")
	testRemovePeer(t, transport, 1, false) // 期望成功
	testRemovePeer(t, transport, 99, true) // 期望失败
}

// testAddPeer 测试 AddPeer 方法
func testAddPeer(t *testing.T, transport *Transport, id uint64, urls []string, expectError bool) {
	fmt.Printf("Before AddPeer (id: %d, urls: %v): %+v\n", id, urls, peers.Urls)
	err := transport.AddPeer(id, urls)
	if expectError {
		if err == nil {
			t.Errorf("Expected error but got none for AddPeer (id: %d, urls: %v)\n", id, urls)
		} else {
			fmt.Printf("Expected error occurred: %v\n", err)
		}
	} else {
		if err != nil {
			t.Errorf("Unexpected error for AddPeer (id: %d, urls: %v): %v\n", id, urls, err)
		} else {
			fmt.Printf("After AddPeer (id: %d, urls: %v): %+v\n", id, urls, peers.Urls)
		}
	}
}

// testRemovePeer 测试 RemovePeer 方法
func testRemovePeer(t *testing.T, transport *Transport, id uint64, expectError bool) {
	fmt.Printf("Before RemovePeer (id: %d): %+v\n", id, peers.Urls)
	err := transport.RemovePeer(id)
	if expectError {
		if err == nil {
			t.Errorf("Expected error but got none for RemovePeer (id: %d)\n", id)
		} else {
			fmt.Printf("Expected error occurred: %v\n", err)
		}
	} else {
		if err != nil {
			t.Errorf("Unexpected error for RemovePeer (id: %d): %v\n", id, err)
		} else {
			fmt.Printf("After RemovePeer (id: %d): %+v\n", id, peers.Urls)
		}
	}
}
