package tcp

import (
	"fmt"
	_interface "go-redis/interface"
	"go-redis/utils/logger"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type Server struct {
	address  string
	handler  _interface.Handler
	closeCh  chan struct{}
	signalCh chan os.Signal
}

func NewServer(address string, handler _interface.Handler) *Server {
	return &Server{
		address:  address,
		handler:  handler,
		closeCh:  make(chan struct{}),
		signalCh: make(chan os.Signal),
	}
}

// Start 开启服务
func (server *Server) Start() error {
	signal.Notify(server.signalCh, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT)
	// 开启goroutine，用于监听并处理signal
	go func() {
		sig := <-server.signalCh
		switch sig {
		case syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGINT:
			server.closeCh <- struct{}{} // 接收到signal后，写入closeChan
		}
	}()

	// 创建listener
	tcpAddr, err := net.ResolveTCPAddr("tcp", server.address)
	if err != nil {
		return err
	}
	tcpListener, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		return err
	}
	logger.Info(fmt.Sprintf("bind %s successful, start listening...", server.address))
	server.ListenAndServe(tcpListener)
	return nil
}

// ListenAndServe 监听并服务
func (server *Server) ListenAndServe(tcpListener *net.TCPListener) {
	errorCh := make(chan error) // 用于监听error
	defer close(errorCh)

	// 开启goroutine，用于处理signal和error
	go func() {
		select {
		case <-server.closeCh:
			logger.Info("get exit signal, shutting down...\n")
		case er := <-errorCh:
			logger.Info(fmt.Sprintf("accept error: %s, shutting down...\n", er.Error()))
		}
		// close
		_ = tcpListener.Close()
		_ = server.handler.Close()
	}()

	// 监听并服务
	var wait sync.WaitGroup
	for {
		tcpConn, err := tcpListener.AcceptTCP()
		if err != nil {
			errorCh <- err // 出现error，写入errorCh
			break
		}
		logger.Info(fmt.Sprintf("accept new connection from %s", tcpConn.RemoteAddr().String()))
		// 开启goroutine，用于handle该连接
		wait.Add(1)
		go func() {
			defer wait.Done()
			server.handler.Handle(tcpConn) // handle
		}()
	}
	wait.Wait() // 等待所有连接都handle完毕
}
