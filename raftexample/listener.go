package main

import (
	"errors"
	"net"
	"time"
)

type stoppableListener struct {
	*net.TCPListener
	stopc <-chan struct{}
}

// *新建一个监听器,监听指定地址
func newStoppableListener(addr string, stopc <-chan struct{}) (*stoppableListener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	return &stoppableListener{ln.(*net.TCPListener), stopc}, nil
}

// *accept函数
func (ln stoppableListener) Accept() (c net.Conn, err error) {
	//*存储连接的通道
	connc := make(chan *net.TCPConn, 1)
	//*存储错误的通道
	errc := make(chan error, 1)
	go func() {
		tc, err := ln.AcceptTCP()
		if err != nil {
			errc <- err
			return
		}
		connc <- tc
	}()
	select {
	case <-ln.stopc:
		return nil, errors.New("server stopped")
	case err := <-errc:
		return nil, err
	case tc := <-connc:
		tc.SetKeepAlive(true)
		//*默认3分钟无活动后发送探测包
		tc.SetKeepAlivePeriod(3 * time.Minute)
		return tc, nil
	}
}
